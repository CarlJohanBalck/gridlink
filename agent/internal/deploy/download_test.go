package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// serveContent stands in for Hugging Face.
func serveContent(t *testing.T, body []byte) (*httptest.Server, *int) {
	t.Helper()
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Length", string(rune(0))) // overwritten by Write
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestModelURL(t *testing.T) {
	tests := []struct {
		name      string
		ref, file string
		revision  string
		want      string
		wantErr   bool
	}{
		{
			name: "defaults to main",
			ref:  "bartowski/Model-GGUF", file: "m-Q4_K_M.gguf",
			want: "https://hf.test/bartowski/Model-GGUF/resolve/main/m-Q4_K_M.gguf",
		},
		{
			name: "explicit revision",
			ref:  "org/repo", file: "a.gguf", revision: "abc123",
			want: "https://hf.test/org/repo/resolve/abc123/a.gguf",
		},
		{name: "missing file", ref: "org/repo", wantErr: true},
		{name: "missing ref", file: "a.gguf", wantErr: true},
		// A model_file containing a path separator would escape the cache dir
		// once joined; reject rather than sanitise.
		{name: "traversal in file", ref: "org/repo", file: "../../etc/passwd", wantErr: true},
		{name: "slash in file", ref: "org/repo", file: "sub/a.gguf", wantErr: true},
		{name: "traversal in ref", ref: "org/../..", file: "a.gguf", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := modelURL("https://hf.test", tt.ref, tt.file, tt.revision)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("modelURL() = %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("modelURL() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("modelURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDownloadVerifiesHash(t *testing.T) {
	body := []byte("pretend this is a gguf")
	srv, _ := serveContent(t, body)
	old := hfBaseURL
	hfBaseURL = srv.URL
	t.Cleanup(func() { hfBaseURL = old })

	dir := t.TempDir()
	spec := modelSpec{ModelRef: "org/repo", ModelFile: "m.gguf", SHA256: sha256Hex(body)}

	path, err := downloadModel(context.Background(), dir, spec, nil)
	if err != nil {
		t.Fatalf("downloadModel: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("content = %q, want %q", got, body)
	}
}

// The security-critical case: a file whose digest does not match must never
// land in the cache, because these weights run on a provider's machine.
func TestDownloadRejectsHashMismatch(t *testing.T) {
	srv, _ := serveContent(t, []byte("tampered payload"))
	old := hfBaseURL
	hfBaseURL = srv.URL
	t.Cleanup(func() { hfBaseURL = old })

	dir := t.TempDir()
	spec := modelSpec{
		ModelRef:  "org/repo",
		ModelFile: "m.gguf",
		SHA256:    sha256Hex([]byte("what we actually asked for")),
	}

	_, err := downloadModel(context.Background(), dir, spec, nil)
	if err == nil {
		t.Fatal("downloadModel succeeded despite a hash mismatch")
	}
	if !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Errorf("error = %v, want a sha256 mismatch", err)
	}

	// Nothing may remain behind — not the final file, not a temp file that a
	// later run could mistake for a good download.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("leftover file after failed download: %s", e.Name())
	}
}

func TestDownloadReusesCache(t *testing.T) {
	body := []byte("cached gguf")
	srv, hits := serveContent(t, body)
	old := hfBaseURL
	hfBaseURL = srv.URL
	t.Cleanup(func() { hfBaseURL = old })

	dir := t.TempDir()
	spec := modelSpec{ModelRef: "org/repo", ModelFile: "m.gguf", SHA256: sha256Hex(body)}

	if _, err := downloadModel(context.Background(), dir, spec, nil); err != nil {
		t.Fatalf("first download: %v", err)
	}
	if _, err := downloadModel(context.Background(), dir, spec, nil); err != nil {
		t.Fatalf("second download: %v", err)
	}
	if *hits != 1 {
		t.Errorf("server hit %d times, want 1 (second call should use cache)", *hits)
	}
}

// A cache entry that no longer matches its digest must be re-fetched, not
// served: otherwise one truncated download poisons the node permanently.
func TestDownloadReplacesCorruptCache(t *testing.T) {
	body := []byte("good gguf content")
	srv, hits := serveContent(t, body)
	old := hfBaseURL
	hfBaseURL = srv.URL
	t.Cleanup(func() { hfBaseURL = old })

	dir := t.TempDir()
	spec := modelSpec{ModelRef: "org/repo", ModelFile: "m.gguf", SHA256: sha256Hex(body)}

	// Plant a corrupt file under the exact cache name.
	if err := os.WriteFile(filepath.Join(dir, cacheName(spec)), []byte("truncated"), 0o600); err != nil {
		t.Fatalf("plant: %v", err)
	}

	path, err := downloadModel(context.Background(), dir, spec, nil)
	if err != nil {
		t.Fatalf("downloadModel: %v", err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(body) {
		t.Errorf("content = %q, want the re-downloaded %q", got, body)
	}
	if *hits != 1 {
		t.Errorf("server hit %d times, want 1 re-download", *hits)
	}
}

func TestCacheNameDisambiguates(t *testing.T) {
	base := modelSpec{ModelRef: "org/repo", ModelFile: "m-Q4.gguf", Revision: "main"}
	otherQuant := base
	otherQuant.ModelFile = "m-Q8.gguf"
	otherRev := base
	otherRev.Revision = "abc123"
	otherRepo := base
	otherRepo.ModelRef = "other/repo"

	seen := map[string]string{}
	for name, s := range map[string]modelSpec{
		"base": base, "otherQuant": otherQuant, "otherRev": otherRev, "otherRepo": otherRepo,
	} {
		key := cacheName(s)
		if prev, dup := seen[key]; dup {
			t.Errorf("%s and %s collide on cache name %q", name, prev, key)
		}
		seen[key] = name
		if strings.Contains(key, "/") {
			t.Errorf("cache name %q contains a path separator", key)
		}
	}
}

func TestProgressReportsCompletion(t *testing.T) {
	body := []byte(strings.Repeat("x", 4096))
	srv, _ := serveContent(t, body)
	old := hfBaseURL
	hfBaseURL = srv.URL
	t.Cleanup(func() { hfBaseURL = old })

	var last uint32
	var calls int
	_, err := downloadModel(context.Background(), t.TempDir(), modelSpec{
		ModelRef: "org/repo", ModelFile: "m.gguf", SHA256: sha256Hex(body),
	}, func(pct uint32) {
		last = pct
		calls++
	})
	if err != nil {
		t.Fatalf("downloadModel: %v", err)
	}
	// Intermediate ticks are throttled, but completion must always be reported
	// or a deployment would appear stuck at its last percentage.
	if last != 100 {
		t.Errorf("final progress = %d, want 100", last)
	}
	if calls == 0 {
		t.Error("progress never reported")
	}
}

func TestPortOf(t *testing.T) {
	tests := []struct {
		addr string
		want uint32
	}{
		{"127.0.0.1:38111", 38111},
		{"[::1]:8080", 8080},
		{"no-port", 0},
		{"127.0.0.1:abc", 0},
	}
	for _, tt := range tests {
		if got := portOf(tt.addr); got != tt.want {
			t.Errorf("portOf(%q) = %d, want %d", tt.addr, got, tt.want)
		}
	}
}
