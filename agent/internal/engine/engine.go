// Package engine runs GGUF models on the local GPU. On Apple Silicon this is
// llama.cpp linked via cgo with embedded Metal shaders — the only cgo in the
// tree. Everywhere else the build gets a stub that reports the platform is
// unsupported, so the agent still compiles (and CGO_ENABLED=0 cross-builds for
// the Pi keep working).
package engine

import (
	"context"
	"errors"
	"strings"
)

// ErrUnsupported means this build has no GPU engine: not macOS/arm64, or built
// without cgo. Callers should decline deployments rather than fail obscurely.
var ErrUnsupported = errors.New("no gpu engine in this build")

// Params configures one model load.
type Params struct {
	ModelPath     string
	ContextLength uint32 // 0 = engine default
}

// GenerateRequest is one completion. Deliberately minimal: the OpenAI surface
// lives in the HTTP layer, not here.
type GenerateRequest struct {
	Prompt      string
	MaxTokens   int
	Temperature float32
	// Stop ends generation when any of these appear. Checked on decoded text,
	// so a sequence split across tokens is still caught.
	Stop []string
}

// Token is one decoded step. Streaming and non-streaming callers both consume
// these; the HTTP layer decides whether to buffer them.
type Token struct {
	Text string
	// Done is set on the final Token. Text may still be non-empty.
	Done bool
	// Reason is set when Done: "stop" (EOG or stop sequence) or "length".
	Reason string
	Err    error
	// PromptTokens and CompletionTokens are set only on the final Token.
	// These are what Phase 3 bills on, so they come from the tokenizer rather
	// than from counting words in the output.
	PromptTokens     int
	CompletionTokens int
}

// ChatMessage is one turn in a chat request.
type ChatMessage struct {
	Role    string // "system" | "user" | "assistant"
	Content string
}

// Model is a loaded model ready to generate. Not safe for concurrent
// Generate calls: llama.cpp contexts are single-threaded, and one node serves
// one model, so the HTTP layer serializes requests instead.
type Model interface {
	// Generate streams tokens until completion, ctx cancellation, or error,
	// then closes the channel.
	Generate(ctx context.Context, req GenerateRequest) (<-chan Token, error)
	// ApplyChatTemplate renders messages into a prompt using the template
	// baked into the GGUF. Using the model's own template matters: a Llama 3
	// prompt formatted as ChatML produces confidently wrong output rather than
	// an error.
	ApplyChatTemplate(msgs []ChatMessage) (string, error)
	// Close releases the model and its GPU memory.
	Close() error
}

// indexAnyStop returns the index of the earliest stop sequence in s, or -1.
// Pure Go and separate from the cgo decode loop so it can be tested on any
// machine, GPU or not.
func indexAnyStop(s string, stops []string) int {
	best := -1
	for _, stop := range stops {
		if stop == "" {
			continue
		}
		if i := strings.Index(s, stop); i >= 0 && (best < 0 || i < best) {
			best = i
		}
	}
	return best
}

// Stats describes the GPU as the engine sees it. UsableVRAMMb is the number
// placement must use: on Apple Silicon it is Metal's
// recommendedMaxWorkingSetSize (~78% of RAM), not total unified memory.
type Stats struct {
	UsableVRAMMb uint64
	GPUName      string
}
