package engine

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server exposes one loaded model over an OpenAI-compatible HTTP API. It binds
// to localhost inside the engine subprocess; the gateway reaches it through the
// agent's data-plane address, never directly.
//
// Requests are serialized: a llama.cpp context cannot decode concurrently, and
// one node serves one model (see the horizontal scale-out decision in
// CLAUDE.md), so queueing is correct rather than a limitation.
type Server struct {
	model     Model
	modelName string
	log       *slog.Logger

	mu sync.Mutex // serializes generation
}

func NewServer(model Model, modelName string, log *slog.Logger) *Server {
	return &Server{model: model, modelName: modelName, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleModels is also the readiness probe the agent polls: it only answers
// once the model is loaded, so a 200 here means the node can serve traffic.
func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, modelList{
		Object: "list",
		Data: []modelInfo{{
			ID:      s.modelName,
			Object:  "model",
			Created: time.Now().Unix(),
			OwnedBy: "gridlink",
		}},
	})
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "malformed JSON body")
		return
	}
	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "messages must not be empty")
		return
	}
	// The model name is advisory here: the coordinator routed this request to
	// this node precisely because it serves this model, so a mismatch means a
	// routing bug worth surfacing rather than silently answering.
	if req.Model != "" && req.Model != s.modelName {
		writeError(w, http.StatusNotFound, "model_not_found",
			fmt.Sprintf("this node serves %q, not %q", s.modelName, req.Model))
		return
	}

	msgs := make([]ChatMessage, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = ChatMessage{Role: m.Role, Content: m.Content}
	}
	prompt, err := s.model.ApplyChatTemplate(msgs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", err.Error())
		return
	}

	genReq := GenerateRequest{
		Prompt:      prompt,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		Stop:        req.stopSequences(),
	}

	if req.Stream {
		s.streamChat(w, r, genReq)
		return
	}
	s.blockingChat(w, r, genReq)
}

func (s *Server) blockingChat(w http.ResponseWriter, r *http.Request, req GenerateRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, err := s.model.Generate(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", err.Error())
		return
	}

	var sb strings.Builder
	finish := "stop"
	for tok := range ch {
		if tok.Err != nil {
			// Headers are unwritten until now, so a mid-generation failure can
			// still be reported as a proper error rather than a truncated 200.
			writeError(w, http.StatusInternalServerError, "server_error", tok.Err.Error())
			return
		}
		sb.WriteString(tok.Text)
		if tok.Done && tok.Reason != "" {
			finish = tok.Reason
		}
	}

	writeJSON(w, http.StatusOK, chatResponse{
		ID:      "chatcmpl-" + randomID(),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   s.modelName,
		Choices: []chatChoice{{
			Index:        0,
			Message:      &chatMessage{Role: "assistant", Content: sb.String()},
			FinishReason: finish,
		}},
	})
}

func (s *Server) streamChat(w http.ResponseWriter, r *http.Request, req GenerateRequest) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "server_error", "streaming unsupported")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ch, err := s.model.Generate(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	id := "chatcmpl-" + randomID()
	created := time.Now().Unix()

	// First chunk carries the role, as OpenAI clients expect.
	s.sendChunk(w, flusher, streamChunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: s.modelName,
		Choices: []streamChoice{{Index: 0, Delta: &chatMessage{Role: "assistant"}}},
	})

	finish := "stop"
	for tok := range ch {
		if tok.Err != nil {
			// The 200 is already sent, so the only honest signal left is to
			// end the stream; the client sees a truncated response.
			s.log.Error("generation failed mid-stream", "err", tok.Err)
			break
		}
		if tok.Text != "" {
			s.sendChunk(w, flusher, streamChunk{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: s.modelName,
				Choices: []streamChoice{{Index: 0, Delta: &chatMessage{Content: tok.Text}}},
			})
		}
		if tok.Done && tok.Reason != "" {
			finish = tok.Reason
		}
	}

	s.sendChunk(w, flusher, streamChunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: s.modelName,
		Choices: []streamChoice{{Index: 0, Delta: &chatMessage{}, FinishReason: &finish}},
	})
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func (s *Server) sendChunk(w http.ResponseWriter, f http.Flusher, c streamChunk) {
	b, err := json.Marshal(c)
	if err != nil {
		s.log.Error("marshalling stream chunk", "err", err)
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", b)
	f.Flush()
}

// ---- wire types (OpenAI-compatible subset) ----

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float32       `json:"temperature"`
	Stream      bool          `json:"stream"`
	// Stop is a string or []string in the OpenAI API, so it stays raw until
	// stopSequences normalises it.
	Stop json.RawMessage `json:"stop"`
}

// stopSequences accepts both shapes the OpenAI API allows.
func (r chatRequest) stopSequences() []string {
	if len(r.Stop) == 0 {
		return nil
	}
	var one string
	if err := json.Unmarshal(r.Stop, &one); err == nil {
		if one == "" {
			return nil
		}
		return []string{one}
	}
	var many []string
	if err := json.Unmarshal(r.Stop, &many); err == nil {
		return many
	}
	return nil
}

type chatMessage struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type chatChoice struct {
	Index        int          `json:"index"`
	Message      *chatMessage `json:"message,omitempty"`
	FinishReason string       `json:"finish_reason"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
}

type streamChoice struct {
	Index        int          `json:"index"`
	Delta        *chatMessage `json:"delta"`
	FinishReason *string      `json:"finish_reason"`
}

type streamChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []streamChoice `json:"choices"`
}

type modelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type modelList struct {
	Object string      `json:"object"`
	Data   []modelInfo `json:"data"`
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

// randomID is a short opaque completion ID. Not security-sensitive: clients
// only echo it back for correlation.
func randomID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
