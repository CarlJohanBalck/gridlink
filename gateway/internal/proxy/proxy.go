// Package proxy is the OpenAI-compatible HTTP server.
//
// Implementation plan (Phase 2):
//   Endpoints:
//     GET  /v1/models          -> list served_model_names from router
//     POST /v1/chat/completions, POST /v1/completions
//       1. Auth: `Authorization: Bearer <key>` against Config.APIKeys ->
//          401 otherwise. Key ID accompanies usage reports.
//       2. Read body ONLY far enough to extract "model" and "stream" fields
//          (decode into a small struct; keep raw body for forwarding).
//       3. router.Pick(model) -> replica; dialer.DialReplica(replica.Addr).
//       4. Forward with httputil.ReverseProxy using a custom Transport whose
//          DialContext is the Dialer (SSE: FlushInterval = -1 so tokens
//          stream through unbuffered).
//       5. Usage capture WITHOUT breaking streaming:
//            - non-stream: TeeReader the response, parse `usage` JSON field
//            - stream: vLLM supports stream_options.include_usage; inject
//              {"stream_options":{"include_usage":true}} into the forwarded
//              body and parse the final SSE data chunk for `usage`
//          -> usage.Reporter.Report(...) async, never blocks the response.
//       6. On dial/proxy error: single retry against a different replica,
//          then 502.
//   Timeouts: no overall deadline on streaming responses; 30s dial+TTFB.
package proxy

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"gridlink/gateway/internal/dialer"
	"gridlink/gateway/internal/router"
	"gridlink/gateway/internal/usage"
)

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
	for i, k := range strings.Split(os.Getenv(envVar), ",") {
		if k = strings.TrimSpace(k); k != "" {
			out[k] = "key-" + string(rune('0'+i))
		}
	}
	return out
}

func Serve(ctx context.Context, cfg Config) error {
	// TODO(claude): implement per the package plan above.
	panic("not implemented")
}
