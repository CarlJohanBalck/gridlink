//go:build darwin && arm64 && cgo

package engine

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestManualGenerate exercises the real GPU. Skipped unless GRIDLINK_TEST_MODEL
// points at a GGUF, so `make test` stays GPU-free per CLAUDE.md.
func TestManualGenerate(t *testing.T) {
	path := os.Getenv("GRIDLINK_TEST_MODEL")
	if path == "" {
		t.Skip("set GRIDLINK_TEST_MODEL to a .gguf to run")
	}
	st, err := GPUStats()
	if err != nil {
		t.Fatalf("GPUStats: %v", err)
	}
	t.Logf("gpu=%q usable_vram_mb=%d", st.GPUName, st.UsableVRAMMb)

	m, err := Load(Params{ModelPath: path, ContextLength: 2048})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer m.Close()

	ch, err := m.Generate(context.Background(), GenerateRequest{
		Prompt:    "<|im_start|>user\nName three colours.<|im_end|>\n<|im_start|>assistant\n",
		MaxTokens: 40,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	var sb strings.Builder
	var reason string
	for tok := range ch {
		if tok.Err != nil {
			t.Fatalf("token err: %v", tok.Err)
		}
		sb.WriteString(tok.Text)
		if tok.Done {
			reason = tok.Reason
		}
	}
	if sb.Len() == 0 {
		t.Fatal("generated nothing")
	}
	t.Logf("reason=%q output=%q", reason, sb.String())

	// A second generation must not be poisoned by the first one's KV cache.
	ch2, err := m.Generate(context.Background(), GenerateRequest{
		Prompt:    "<|im_start|>user\nSay OK.<|im_end|>\n<|im_start|>assistant\n",
		MaxTokens: 10,
	})
	if err != nil {
		t.Fatalf("second Generate: %v", err)
	}
	var sb2 strings.Builder
	for tok := range ch2 {
		if tok.Err != nil {
			t.Fatalf("second token err: %v", tok.Err)
		}
		sb2.WriteString(tok.Text)
	}
	if sb2.Len() == 0 {
		t.Fatal("second generation produced nothing")
	}
	t.Logf("second=%q", sb2.String())
}
