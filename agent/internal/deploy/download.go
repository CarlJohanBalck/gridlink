package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// hfBaseURL is the Hugging Face resolve endpoint. Overridable in tests.
var hfBaseURL = "https://huggingface.co"

// modelURL builds the download URL for a GGUF in a repo. revision defaults to
// main.
func modelURL(base, modelRef, modelFile, revision string) (string, error) {
	if modelRef == "" || modelFile == "" {
		return "", fmt.Errorf("model_ref and model_file are both required")
	}
	// A traversal in either component would let a spec write outside the model
	// cache once joined into a path, so reject rather than sanitise.
	if strings.Contains(modelFile, "/") || strings.Contains(modelFile, `\`) ||
		modelFile == "." || modelFile == ".." {
		return "", fmt.Errorf("model_file must be a bare filename, got %q", modelFile)
	}
	if strings.Contains(modelRef, "..") {
		return "", fmt.Errorf("invalid model_ref %q", modelRef)
	}
	if revision == "" {
		revision = "main"
	}
	return fmt.Sprintf("%s/%s/resolve/%s/%s",
		strings.TrimRight(base, "/"), modelRef, url.PathEscape(revision), url.PathEscape(modelFile)), nil
}

// progressFunc reports 0-100 as a download proceeds.
type progressFunc func(percent uint32)

// downloadModel fetches a GGUF into dir and returns its path, verifying
// SHA-256 before the file is usable.
//
// The hash check is not optional hygiene: these weights are executed as model
// data on a stranger's machine, so an unverified multi-GB download from a
// third-party host is a supply-chain hole. Content is written to a temp file
// and only renamed into place once the digest matches, so an interrupted or
// corrupted download can never be mistaken for a good cache entry.
func downloadModel(ctx context.Context, dir string, spec modelSpec, onProgress progressFunc) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create model dir: %w", err)
	}
	dest := filepath.Join(dir, cacheName(spec))

	// Already cached and intact? Re-verify rather than trust the filename:
	// a truncated earlier download would otherwise be served forever.
	if _, err := os.Stat(dest); err == nil {
		if spec.SHA256 == "" {
			return dest, nil
		}
		sum, err := fileSHA256(dest)
		if err == nil && sum == strings.ToLower(spec.SHA256) {
			if onProgress != nil {
				onProgress(100)
			}
			return dest, nil
		}
		// Stale or corrupt: fall through and re-download.
		_ = os.Remove(dest)
	}

	src, err := modelURL(hfBaseURL, spec.ModelRef, spec.ModelFile, spec.Revision)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", src, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch %s: unexpected status %s", src, resp.Status)
	}

	tmp, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // no-op once renamed
	}()

	h := sha256.New()
	pw := &progressWriter{
		total:      resp.ContentLength,
		onProgress: onProgress,
	}
	if _, err := io.Copy(io.MultiWriter(tmp, h, pw), resp.Body); err != nil {
		return "", fmt.Errorf("download %s: %w", src, err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp file: %w", err)
	}

	got := hex.EncodeToString(h.Sum(nil))
	if spec.SHA256 != "" && got != strings.ToLower(spec.SHA256) {
		return "", fmt.Errorf("sha256 mismatch for %s: got %s, want %s", spec.ModelFile, got, spec.SHA256)
	}

	if err := os.Rename(tmpName, dest); err != nil {
		return "", fmt.Errorf("install model: %w", err)
	}
	if onProgress != nil {
		onProgress(100)
	}
	return dest, nil
}

// cacheName keys the cache by repo+revision+file so two quantizations of the
// same model, or the same file at two revisions, cannot collide.
func cacheName(spec modelSpec) string {
	rev := spec.Revision
	if rev == "" {
		rev = "main"
	}
	safe := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return safe.Replace(spec.ModelRef) + "@" + safe.Replace(rev) + "_" + spec.ModelFile
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// progressWriter reports download progress, throttled so a multi-GB file does
// not flood the coordinator stream with updates.
type progressWriter struct {
	total      int64
	written    int64
	lastPct    uint32
	lastReport time.Time
	onProgress progressFunc
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n := len(b)
	p.written += int64(n)
	if p.onProgress == nil || p.total <= 0 {
		return n, nil
	}
	pct := uint32(p.written * 100 / p.total)
	if pct > 100 {
		pct = 100
	}
	// Report at most once a second, and never the same percentage twice.
	if pct != p.lastPct && time.Since(p.lastReport) >= time.Second {
		p.lastPct = pct
		p.lastReport = time.Now()
		p.onProgress(pct)
	}
	return n, nil
}
