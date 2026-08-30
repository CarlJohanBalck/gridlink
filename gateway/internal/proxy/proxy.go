// Package proxy is the OpenAI-compatible HTTP server.
//
// It authenticates callers, resolves a model name to a replica, and forwards
// the request to that node's engine, streaming SSE straight through. Token
// usage is captured on the way past and reported asynchronously.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/gateway/internal/dialer"
	"gridlink/gateway/internal/router"
	"gridlink/gateway/internal/usage"
)

// dialTimeout bounds connecting to a replica and getting response headers.
// There is deliberately NO overall response deadline: a long generation is not
// a stuck request, and a write timeout would truncate legitimate streams.
const dialTimeout = 30 * time.Second

// usageWait bounds how long the response goroutine waits for usage parsing to
// finish after the body is fully forwarded. Parsing is already done in the
// normal case; this only covers a truncated or half-closed stream.
const usageWait = 5 * time.Second

// maxRequestBody caps what the gateway buffers per request. Bodies are held in
// memory so a request can be retried against another replica.
const maxRequestBody = 8 << 20 // 8 MiB

type Config struct {
	Addr    string
	Router  *router.Router
	Dialer  dialer.Dialer
	Usage   *usage.Reporter
	APIKeys map[string]string // key -> key ID
	Logger  *slog.Logger
}

// KeysFromEnv parses "k1,k2" into {k1:"key-0", k2:"key-1"}. Phase 2 only;
// real key management arrives with the ledger in Phase 3.
func KeysFromEnv(envVar string) map[string]string {
	out := map[string]string{}
	n := 0
	for _, k := range strings.Split(os.Getenv(envVar), ",") {
		if k = strings.TrimSpace(k); k != "" {
			// Numbered by position among non-empty keys, so blank entries do
			// not leave gaps in the IDs that appear in billing records.
			out[k] = fmt.Sprintf("key-%d", n)
			n++
		}
	}
	return out
}

type server struct {
	cfg Config
	log *slog.Logger
}

func Serve(ctx context.Context, cfg Config) error {
	s := &server{cfg: cfg, log: cfg.Logger}

	httpSrv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Bind before announcing. Logging "listening" from ListenAndServe's caller
	// claims success even when the bind fails, which reads as a working
	// gateway that 404s everything.
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	cfg.Logger.Info("gateway listening", "addr", ln.Addr().String(), "api_keys", len(cfg.APIKeys))
	if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/models", s.withAuth(s.handleModels))
	mux.HandleFunc("POST /v1/chat/completions", s.withAuth(s.handleInference))
	mux.HandleFunc("POST /v1/completions", s.withAuth(s.handleInference))
	return mux
}

// ---- auth ----

type ctxKey string

const keyIDCtxKey ctxKey = "api_key_id"

func (s *server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// An empty key set means auth is disabled (dev). Say so loudly rather
		// than silently accepting anonymous traffic in production.
		if len(s.cfg.APIKeys) == 0 {
			next(w, r.WithContext(context.WithValue(r.Context(), keyIDCtxKey, "anonymous")))
			return
		}
		const prefix = "Bearer "
		auth := r.Header.Get("Authorization")
		if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
			writeError(w, http.StatusUnauthorized, "invalid_request_error", "missing bearer token")
			return
		}
		keyID, ok := s.cfg.APIKeys[auth[len(prefix):]]
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid_request_error", "invalid API key")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), keyIDCtxKey, keyID)))
	}
}

func keyID(ctx context.Context) string {
	if v, ok := ctx.Value(keyIDCtxKey).(string); ok {
		return v
	}
	return ""
}

// ---- endpoints ----

func (s *server) handleModels(w http.ResponseWriter, r *http.Request) {
	names, err := s.cfg.Router.Models(r.Context())
	if err != nil {
		s.log.Error("listing models", "err", err)
		writeError(w, http.StatusServiceUnavailable, "server_error", "coordinator unavailable")
		return
	}
	data := make([]map[string]any, 0, len(names))
	for _, n := range names {
		data = append(data, map[string]any{
			"id": n, "object": "model", "created": time.Now().Unix(), "owned_by": "gridlink",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (s *server) handleInference(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "could not read body")
		return
	}

	// Peek at model/stream without disturbing the body, which is forwarded
	// byte-for-byte so unknown OpenAI fields survive the hop.
	var peek struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &peek); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "malformed JSON body")
		return
	}
	if peek.Model == "" {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}

	replica, err := s.cfg.Router.Pick(r.Context(), peek.Model)
	if err != nil {
		s.routingError(w, r.Context(), peek.Model, err)
		return
	}

	if s.forward(w, r, replica, body, peek.Model, peek.Stream) {
		return
	}

	// The first replica could not be reached. Its cache entry is stale, so
	// re-resolve and try a different node once before giving up.
	s.cfg.Router.Invalidate(peek.Model)
	alt, err := s.cfg.Router.PickExcluding(r.Context(), peek.Model, replica.GetDeploymentId())
	if err != nil {
		writeError(w, http.StatusBadGateway, "server_error", "no reachable replica for "+peek.Model)
		return
	}
	s.log.Warn("retrying on another replica",
		"model", peek.Model, "failed_node", replica.GetNodeId(), "retry_node", alt.GetNodeId())
	if !s.forward(w, r, alt, body, peek.Model, peek.Stream) {
		writeError(w, http.StatusBadGateway, "server_error", "no reachable replica for "+peek.Model)
	}
}

// routingError distinguishes "no such model" from "model exists but nothing is
// ready", because the fixes are completely different: deploy it, versus wait.
func (s *server) routingError(w http.ResponseWriter, ctx context.Context, model string, err error) {
	if errors.Is(err, router.ErrNoReplicas) {
		// Ask for deployed-but-unready models too: during a re-placement the
		// model is momentarily unroutable, and answering 404 would tell the
		// client to stop retrying something that is about to come back.
		if names, lerr := s.cfg.Router.DeployedModels(ctx); lerr == nil {
			for _, n := range names {
				if n == model {
					writeError(w, http.StatusServiceUnavailable, "server_error",
						"no ready replica for "+model)
					return
				}
			}
		}
		writeError(w, http.StatusNotFound, "model_not_found", "no model named "+model)
		return
	}
	s.log.Error("routing failed", "model", model, "err", err)
	writeError(w, http.StatusServiceUnavailable, "server_error", "could not route request")
}

// forward proxies to one replica. It reports false when the replica could not
// be reached AND nothing was written yet, so the caller may retry elsewhere.
func (s *server) forward(w http.ResponseWriter, r *http.Request, replica *computev1.Replica,
	body []byte, model string, stream bool) bool {

	target, err := url.Parse("http://" + replica.GetAddr())
	if err != nil {
		s.log.Error("bad replica address", "addr", replica.GetAddr(), "err", err)
		return false
	}

	cap := newCapture()
	failed := false
	wrote := false

	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
			pr.Out.Body = io.NopCloser(bytes.NewReader(body))
			pr.Out.ContentLength = int64(len(body))
			pr.Out.Header.Set("Content-Type", "application/json")
		},
		Transport: &http.Transport{
			// The Dialer is the transport seam: Tailscale now, reverse tunnel
			// later, with nothing here needing to know which.
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
				defer cancel()
				return s.cfg.Dialer.DialReplica(dialCtx, addr)
			},
			ResponseHeaderTimeout: dialTimeout,
		},
		// -1 flushes every write immediately, which is what makes SSE tokens
		// arrive as generated instead of in one buffered lump at the end.
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			wrote = true
			resp.Body = cap.wrap(resp.Body, stream)
			return nil
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) {
			// Only a pre-response failure is retryable; once bytes are on the
			// wire the client has a partial answer and a retry would corrupt it.
			if !wrote {
				failed = true
				return
			}
			s.log.Error("proxy error mid-response", "node_id", replica.GetNodeId(), "err", err)
		},
	}

	rp.ServeHTTP(w, r)

	if failed {
		return false
	}

	// The parser runs alongside the response copy; wait for it before
	// reporting, or a fast response outruns its own metering.
	cap.wait(usageWait)

	if u, ok := cap.usage(); ok {
		s.cfg.Usage.Report(usage.Record{
			Model:            model,
			NodeID:           replica.GetNodeId(),
			DeploymentID:     replica.GetDeploymentId(),
			APIKeyID:         keyID(r.Context()),
			PromptTokens:     u.PromptTokens,
			CompletionTokens: u.CompletionTokens,
			TimestampUnixMs:  time.Now().UnixMilli(),
		})
	} else {
		// Not fatal, but it means an unbilled request; worth noticing.
		s.log.Warn("no usage captured", "model", model, "node_id", replica.GetNodeId())
	}
	return true
}

// ---- usage capture ----

type tokenUsage struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
}

// capture extracts the usage block from a response as it streams past, without
// buffering the whole body: a streamed answer must reach the client token by
// token, so usage is scraped in passing rather than by reading the response.
//
// Parsing runs on its own goroutine, so callers MUST wait() before reading
// usage(); checking immediately after the proxy returns races the parser and
// silently loses the record.
type capture struct {
	mu   sync.Mutex
	got  bool
	u    tokenUsage
	done chan struct{}
}

func newCapture() *capture { return &capture{done: make(chan struct{})} }

func (c *capture) set(u tokenUsage) {
	c.mu.Lock()
	c.u, c.got = u, true
	c.mu.Unlock()
}

func (c *capture) usage() (tokenUsage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.u, c.got
}

// wait blocks until parsing finishes, or the timeout elapses. Bounded so a
// half-closed connection cannot pin the request goroutine.
func (c *capture) wait(timeout time.Duration) {
	if c.done == nil {
		return
	}
	select {
	case <-c.done:
	case <-time.After(timeout):
	}
}

func (c *capture) wrap(body io.ReadCloser, stream bool) io.ReadCloser {
	pr, pw := io.Pipe()
	tee := io.TeeReader(body, pw)

	go func() {
		defer close(c.done)
		if stream {
			c.scanSSE(pr)
		} else {
			c.scanJSON(pr)
		}
		// Drain so the TeeReader never blocks on a full pipe if parsing
		// finished early.
		_, _ = io.Copy(io.Discard, pr)
	}()

	return &teeCloser{Reader: tee, closer: body, pw: pw}
}

type teeCloser struct {
	io.Reader
	closer io.ReadCloser
	pw     *io.PipeWriter
}

func (t *teeCloser) Close() error {
	t.pw.Close()
	return t.closer.Close()
}

// scanJSON reads a non-streaming body and pulls out .usage.
func (c *capture) scanJSON(r io.Reader) {
	var resp struct {
		Usage *tokenUsage `json:"usage"`
	}
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return
	}
	if resp.Usage != nil {
		c.set(*resp.Usage)
	}
}

// scanSSE watches the event stream for the chunk carrying usage. The engine
// always emits one before [DONE].
func (c *capture) scanSSE(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			return
		}
		var chunk struct {
			Usage *tokenUsage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue
		}
		if chunk.Usage != nil {
			c.set(*chunk.Usage)
		}
	}
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]any{
		"error": map[string]string{"type": kind, "message": msg},
	})
}
