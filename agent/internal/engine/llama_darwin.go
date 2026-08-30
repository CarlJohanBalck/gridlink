//go:build darwin && arm64 && cgo

package engine

/*
#cgo CFLAGS: -I${SRCDIR}/third_party/llama.cpp/include -I${SRCDIR}/third_party/llama.cpp/ggml/include
#cgo LDFLAGS: -L${SRCDIR}/third_party/llama.cpp/build/src
#cgo LDFLAGS: -L${SRCDIR}/third_party/llama.cpp/build/ggml/src
#cgo LDFLAGS: -L${SRCDIR}/third_party/llama.cpp/build/ggml/src/ggml-metal
#cgo LDFLAGS: -L${SRCDIR}/third_party/llama.cpp/build/ggml/src/ggml-blas
#cgo LDFLAGS: -lllama -lggml -lggml-base -lggml-cpu -lggml-metal -lggml-blas
#cgo LDFLAGS: -lc++ -framework Metal -framework MetalKit -framework Foundation -framework Accelerate -framework CoreFoundation
#include <stdlib.h>
#include "llama.h"
#include "ggml-backend.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

// Supported reports whether this build can run models. Always true here.
func Supported() bool { return true }

var initOnce sync.Once

func backendInit() {
	initOnce.Do(func() { C.llama_backend_init() })
}

type llamaModel struct {
	model *C.struct_llama_model
	ctx   *C.struct_llama_context
	vocab *C.struct_llama_vocab

	// mu serializes Generate: one llama_context cannot decode concurrently.
	mu     sync.Mutex
	closed bool
}

// Load reads a GGUF off disk and offloads it to the GPU.
func Load(p Params) (Model, error) {
	backendInit()

	mparams := C.llama_model_default_params()
	// Offload everything. A silent CPU fallback would look like a merely slow
	// node rather than a broken one, which is far harder to diagnose later.
	mparams.n_gpu_layers = 999

	cPath := C.CString(p.ModelPath)
	defer C.free(unsafe.Pointer(cPath))

	model := C.llama_model_load_from_file(cPath, mparams)
	if model == nil {
		return nil, fmt.Errorf("load model %s: llama.cpp refused the file", p.ModelPath)
	}

	cparams := C.llama_context_default_params()
	if p.ContextLength > 0 {
		cparams.n_ctx = C.uint32_t(p.ContextLength)
	}

	ctx := C.llama_init_from_model(model, cparams)
	if ctx == nil {
		C.llama_model_free(model)
		return nil, fmt.Errorf("create context for %s", p.ModelPath)
	}

	return &llamaModel{
		model: model,
		ctx:   ctx,
		vocab: C.llama_model_get_vocab(model),
	}, nil
}

func (m *llamaModel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	C.llama_free(m.ctx)
	C.llama_model_free(m.model)
	return nil
}

func (m *llamaModel) Generate(ctx context.Context, req GenerateRequest) (<-chan Token, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("model is closed")
	}

	tokens, err := m.tokenize(req.Prompt)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	out := make(chan Token, 32)
	go func() {
		defer m.mu.Unlock()
		defer close(out)
		m.generate(ctx, req, tokens, out)
	}()
	return out, nil
}

func (m *llamaModel) tokenize(prompt string) ([]C.llama_token, error) {
	cPrompt := C.CString(prompt)
	defer C.free(unsafe.Pointer(cPrompt))

	// Ask for the count first: llama_tokenize returns -n when the buffer is
	// too small, so a fixed guess would silently truncate long prompts.
	need := C.llama_tokenize(m.vocab, cPrompt, C.int32_t(len(prompt)), nil, 0, true, true)
	if need > 0 {
		return nil, fmt.Errorf("tokenize: unexpected positive count %d for empty buffer", need)
	}
	n := -need
	if n == 0 {
		return nil, fmt.Errorf("tokenize: prompt produced no tokens")
	}
	buf := make([]C.llama_token, n)
	got := C.llama_tokenize(m.vocab, cPrompt, C.int32_t(len(prompt)),
		(*C.llama_token)(unsafe.Pointer(&buf[0])), n, true, true)
	if got < 0 {
		return nil, fmt.Errorf("tokenize failed: %d", got)
	}
	return buf[:got], nil
}

// generate runs the decode loop. Caller holds m.mu and owns closing `out`.
func (m *llamaModel) generate(ctx context.Context, req GenerateRequest, tokens []C.llama_token, out chan<- Token) {
	// Each request starts from a clean slate: this engine serves one request
	// at a time, so leftover KV cache from the previous prompt would corrupt
	// the next one.
	mem := C.llama_get_memory(m.ctx)
	C.llama_memory_clear(mem, true)

	sparams := C.llama_sampler_chain_default_params()
	smpl := C.llama_sampler_chain_init(sparams)
	defer C.llama_sampler_free(smpl)
	if req.Temperature <= 0 {
		C.llama_sampler_chain_add(smpl, C.llama_sampler_init_greedy())
	} else {
		C.llama_sampler_chain_add(smpl, C.llama_sampler_init_temp(C.float(req.Temperature)))
		C.llama_sampler_chain_add(smpl, C.llama_sampler_init_dist(C.LLAMA_DEFAULT_SEED))
	}

	batch := C.llama_batch_get_one((*C.llama_token)(unsafe.Pointer(&tokens[0])), C.int32_t(len(tokens)))
	if rc := C.llama_decode(m.ctx, batch); rc != 0 {
		out <- Token{Done: true, PromptTokens: len(tokens),
			Err: fmt.Errorf("prefill decode failed: %d", rc)}
		return
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 512
	}

	// finish stamps token counts onto the terminal Token so every exit path
	// reports usage; a path that forgets would silently bill nothing.
	generated := 0
	finish := func(t Token) Token {
		t.Done = true
		t.PromptTokens = len(tokens)
		t.CompletionTokens = generated
		return t
	}

	var sb strings.Builder
	buf := make([]C.char, 256)
	for i := 0; i < maxTokens; i++ {
		select {
		case <-ctx.Done():
			out <- finish(Token{Reason: "stop", Err: ctx.Err()})
			return
		default:
		}

		id := C.llama_sampler_sample(smpl, m.ctx, -1)
		if C.llama_vocab_is_eog(m.vocab, id) {
			out <- finish(Token{Reason: "stop"})
			return
		}

		n := C.llama_token_to_piece(m.vocab, id, &buf[0], C.int32_t(len(buf)), 0, true)
		if n < 0 {
			out <- finish(Token{Err: fmt.Errorf("token_to_piece failed: %d", n)})
			return
		}
		piece := C.GoStringN(&buf[0], n)

		// Stop sequences are matched against the accumulated text, so one
		// straddling a token boundary is still caught. Only the text before
		// the stop is emitted, and only the part not already sent.
		emitted := sb.Len()
		sb.WriteString(piece)
		if idx := indexAnyStop(sb.String(), req.Stop); idx >= 0 {
			// The stop sequence itself was generated, so it counts.
			generated++
			if idx > emitted {
				out <- Token{Text: sb.String()[emitted:idx]}
			}
			out <- finish(Token{Reason: "stop"})
			return
		}

		generated++
		out <- Token{Text: piece}

		one := []C.llama_token{id}
		b := C.llama_batch_get_one((*C.llama_token)(unsafe.Pointer(&one[0])), 1)
		if rc := C.llama_decode(m.ctx, b); rc != 0 {
			out <- finish(Token{Err: fmt.Errorf("decode failed: %d", rc)})
			return
		}
	}
	out <- finish(Token{Reason: "length"})
}

// ApplyChatTemplate renders messages with the GGUF's own chat template.
func (m *llamaModel) ApplyChatTemplate(msgs []ChatMessage) (string, error) {
	if len(msgs) == 0 {
		return "", fmt.Errorf("no messages")
	}

	tmpl := C.llama_model_chat_template(m.model, nil)
	if tmpl == nil {
		return "", fmt.Errorf("model has no chat template")
	}

	// Build the C array, keeping every CString alive until the call returns.
	cMsgs := make([]C.llama_chat_message, len(msgs))
	var total int
	for i, msg := range msgs {
		role := C.CString(msg.Role)
		content := C.CString(msg.Content)
		defer C.free(unsafe.Pointer(role))
		defer C.free(unsafe.Pointer(content))
		cMsgs[i].role = role
		cMsgs[i].content = content
		total += len(msg.Role) + len(msg.Content)
	}

	// The header recommends 2x the total message bytes; grow once if the
	// template turns out to be more verbose than that.
	size := 2*total + 512
	for attempt := 0; attempt < 2; attempt++ {
		buf := make([]C.char, size)
		n := C.llama_chat_apply_template(tmpl, &cMsgs[0], C.size_t(len(cMsgs)),
			C.bool(true), &buf[0], C.int32_t(size))
		if n < 0 {
			return "", fmt.Errorf("apply chat template failed: %d", n)
		}
		if int(n) <= size {
			return C.GoStringN(&buf[0], n), nil
		}
		size = int(n) + 1
	}
	return "", fmt.Errorf("chat template did not fit after retry")
}

// GPUStats reports what placement needs. Loading a model is not required.
func GPUStats() (Stats, error) {
	backendInit()
	dev := C.ggml_backend_dev_by_type(C.GGML_BACKEND_DEVICE_TYPE_GPU)
	if dev == nil {
		return Stats{}, ErrUnsupported
	}
	var free, total C.size_t
	C.ggml_backend_dev_memory(dev, &free, &total)
	name := C.GoString(C.ggml_backend_dev_description(dev))
	return Stats{
		UsableVRAMMb: uint64(free) / (1024 * 1024),
		GPUName:      name,
	}, nil
}
