package proxy

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestKeysFromEnv(t *testing.T) {
	t.Setenv("TEST_KEYS", "alpha, beta ,,gamma")
	got := KeysFromEnv("TEST_KEYS")
	want := map[string]string{"alpha": "key-0", "beta": "key-1", "gamma": "key-2"}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q = %q, want %q", k, got[k], v)
		}
	}

	t.Setenv("TEST_EMPTY", "")
	if n := len(KeysFromEnv("TEST_EMPTY")); n != 0 {
		t.Errorf("empty env produced %d keys", n)
	}
}

// More than 10 keys must not collide, which the original rune-arithmetic
// formatting would have done ('0'+10 == ':').
func TestKeysFromEnvManyKeys(t *testing.T) {
	var parts []string
	for i := 0; i < 12; i++ {
		parts = append(parts, "k"+string(rune('a'+i)))
	}
	t.Setenv("TEST_MANY", strings.Join(parts, ","))
	got := KeysFromEnv("TEST_MANY")
	seen := map[string]string{}
	for key, id := range got {
		if prev, dup := seen[id]; dup {
			t.Errorf("keys %q and %q share key ID %q", key, prev, id)
		}
		seen[id] = key
	}
	if len(got) != 12 {
		t.Errorf("got %d keys, want 12", len(got))
	}
}

func TestAuth(t *testing.T) {
	s := &server{
		cfg: Config{APIKeys: map[string]string{"secret": "key-0"}, Logger: testLogger()},
		log: testLogger(),
	}
	var gotKeyID string
	h := s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		gotKeyID = keyID(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name, header string
		want         int
		wantKeyID    string
	}{
		{"valid key", "Bearer secret", http.StatusOK, "key-0"},
		{"case-insensitive scheme", "bearer secret", http.StatusOK, "key-0"},
		{"wrong key", "Bearer nope", http.StatusUnauthorized, ""},
		{"no header", "", http.StatusUnauthorized, ""},
		{"malformed", "secret", http.StatusUnauthorized, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKeyID = ""
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			h(rec, req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
			if gotKeyID != tt.wantKeyID {
				t.Errorf("key ID = %q, want %q", gotKeyID, tt.wantKeyID)
			}
		})
	}
}

// With no configured keys the gateway is open (dev mode) and the usage record
// is attributed to "anonymous" rather than an empty string.
func TestAuthDisabledWhenNoKeys(t *testing.T) {
	s := &server{cfg: Config{Logger: testLogger()}, log: testLogger()}
	var gotKeyID string
	h := s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		gotKeyID = keyID(r.Context())
	})
	h(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if gotKeyID != "anonymous" {
		t.Errorf("key ID = %q, want anonymous", gotKeyID)
	}
}

func TestCaptureJSONUsage(t *testing.T) {
	body := `{"id":"x","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":9,"total_tokens":20}}`
	c := newCapture()
	rc := c.wrap(io.NopCloser(strings.NewReader(body)), false)

	// The caller must still see the complete body, byte for byte.
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rc.Close()
	if string(got) != body {
		t.Errorf("body was altered:\n got %s\nwant %s", got, body)
	}

	u, ok := waitForUsage(c)
	if !ok {
		t.Fatal("no usage captured")
	}
	if u.PromptTokens != 11 || u.CompletionTokens != 9 {
		t.Errorf("usage = %+v, want 11/9", u)
	}
}

func TestCaptureSSEUsage(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"choices":[{"delta":{"role":"assistant"}}]}`,
		"",
		`data: {"choices":[{"delta":{"content":"hi"}}]}`,
		"",
		`data: {"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")

	c := newCapture()
	rc := c.wrap(io.NopCloser(strings.NewReader(stream)), true)
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rc.Close()
	if string(got) != stream {
		t.Error("SSE body was altered in transit")
	}

	u, ok := waitForUsage(c)
	if !ok {
		t.Fatal("no usage captured from stream")
	}
	if u.PromptTokens != 7 || u.CompletionTokens != 2 {
		t.Errorf("usage = %+v, want 7/2", u)
	}
}

// A response with no usage block must not report bogus zeros as if they were
// real counts.
func TestCaptureNoUsage(t *testing.T) {
	c := newCapture()
	rc := c.wrap(io.NopCloser(strings.NewReader(`{"id":"x","choices":[]}`)), false)
	_, _ = io.ReadAll(rc)
	rc.Close()
	c.wait(time.Second)
	if _, ok := c.usage(); ok {
		t.Error("reported usage for a response that carried none")
	}
}

// waitForUsage blocks on the parser finishing. Polling here would hide the
// very race that made usage vanish in production.
func waitForUsage(c *capture) (tokenUsage, bool) {
	c.wait(2 * time.Second)
	return c.usage()
}

func TestHealthAndBadRequests(t *testing.T) {
	s := &server{cfg: Config{Logger: testLogger()}, log: testLogger()}
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health = %d, want 200", resp.StatusCode)
	}

	tests := []struct {
		name, body string
		want       int
	}{
		{"malformed json", `{nope`, http.StatusBadRequest},
		{"missing model", `{"messages":[]}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				b, _ := io.ReadAll(resp.Body)
				t.Errorf("status = %d, want %d (%s)", resp.StatusCode, tt.want, b)
			}
			var e map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
				t.Errorf("error body is not JSON: %v", err)
			} else if _, ok := e["error"]; !ok {
				t.Errorf("error body missing 'error' key: %v", e)
			}
		})
	}
}
