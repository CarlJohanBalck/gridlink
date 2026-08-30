package engine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeModel emits a scripted token sequence. No GPU, no cgo — the HTTP layer
// is pure Go precisely so it can be tested anywhere.
type fakeModel struct {
	tokens     []Token
	genErr     error
	templateFn func([]ChatMessage) (string, error)

	gotReq GenerateRequest
}

func (f *fakeModel) Generate(ctx context.Context, req GenerateRequest) (<-chan Token, error) {
	f.gotReq = req
	if f.genErr != nil {
		return nil, f.genErr
	}
	ch := make(chan Token, len(f.tokens)+1)
	go func() {
		defer close(ch)
		for _, t := range f.tokens {
			select {
			case ch <- t:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (f *fakeModel) ApplyChatTemplate(msgs []ChatMessage) (string, error) {
	if f.templateFn != nil {
		return f.templateFn(msgs)
	}
	var sb strings.Builder
	for _, m := range msgs {
		sb.WriteString(m.Role + ": " + m.Content + "\n")
	}
	return sb.String(), nil
}

func (f *fakeModel) Close() error { return nil }

func newTestServer(t *testing.T, m Model) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewServer(m, "test-model", testLogger()).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestModelsEndpoint(t *testing.T) {
	srv := newTestServer(t, &fakeModel{})
	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got modelList
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Data) != 1 || got.Data[0].ID != "test-model" {
		t.Errorf("models = %+v, want one entry id=test-model", got.Data)
	}
}

func TestChatCompletionNonStreaming(t *testing.T) {
	m := &fakeModel{tokens: []Token{
		{Text: "Hello"},
		{Text: " world"},
		{Done: true, Reason: "stop", PromptTokens: 11, CompletionTokens: 2},
	}}
	srv := newTestServer(t, m)

	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, b)
	}
	var got chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Choices) != 1 {
		t.Fatalf("choices = %d, want 1", len(got.Choices))
	}
	if c := got.Choices[0].Message.Content; c != "Hello world" {
		t.Errorf("content = %q, want %q", c, "Hello world")
	}
	if got.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want stop", got.Choices[0].FinishReason)
	}
	if got.Object != "chat.completion" {
		t.Errorf("object = %q, want chat.completion", got.Object)
	}
	if m.gotReq.MaxTokens != 16 {
		t.Errorf("max_tokens not forwarded: %d", m.gotReq.MaxTokens)
	}
	// Usage is billing data in Phase 3, so it must be present and consistent.
	if got.Usage == nil {
		t.Fatal("response carried no usage block")
	}
	if got.Usage.PromptTokens != 11 || got.Usage.CompletionTokens != 2 {
		t.Errorf("usage = %+v, want prompt=11 completion=2", got.Usage)
	}
	if got.Usage.TotalTokens != 13 {
		t.Errorf("total_tokens = %d, want 13", got.Usage.TotalTokens)
	}
}

func TestChatCompletionStreaming(t *testing.T) {
	m := &fakeModel{tokens: []Token{
		{Text: "one"},
		{Text: "two"},
		{Done: true, Reason: "length", PromptTokens: 7, CompletionTokens: 2},
	}}
	srv := newTestServer(t, m)

	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}],"stream":true}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q, want text/event-stream", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	out := string(raw)

	if !strings.HasSuffix(strings.TrimSpace(out), "data: [DONE]") {
		t.Errorf("stream did not end with [DONE]:\n%s", out)
	}
	// Content must arrive as deltas, not one blob.
	var content strings.Builder
	var sawRole bool
	var finish string
	var streamUsage *usage
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimPrefix(line, "data: ")
		if line == "" || line == "[DONE]" {
			continue
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			t.Fatalf("bad chunk %q: %v", line, err)
		}
		if chunk.Object != "chat.completion.chunk" {
			t.Errorf("object = %q, want chat.completion.chunk", chunk.Object)
		}
		if chunk.Usage != nil {
			streamUsage = chunk.Usage
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		d := chunk.Choices[0].Delta
		if d != nil {
			if d.Role == "assistant" {
				sawRole = true
			}
			content.WriteString(d.Content)
		}
		if r := chunk.Choices[0].FinishReason; r != nil {
			finish = *r
		}
	}
	if !sawRole {
		t.Error("no chunk carried the assistant role")
	}
	if content.String() != "onetwo" {
		t.Errorf("streamed content = %q, want %q", content.String(), "onetwo")
	}
	if finish != "length" {
		t.Errorf("finish_reason = %q, want length", finish)
	}
	// Streaming must meter too, or every streamed request bills nothing.
	if streamUsage == nil {
		t.Fatal("stream carried no usage chunk")
	}
	if streamUsage.PromptTokens != 7 || streamUsage.CompletionTokens != 2 {
		t.Errorf("stream usage = %+v, want prompt=7 completion=2", streamUsage)
	}
}

func TestChatRejectsWrongModel(t *testing.T) {
	srv := newTestServer(t, &fakeModel{})
	body := `{"model":"some-other-model","messages":[{"role":"user","content":"hi"}]}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	// A mismatch means the coordinator routed wrongly; answering anyway would
	// hide the bug and bill the caller for the wrong model.
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestChatBadRequests(t *testing.T) {
	srv := newTestServer(t, &fakeModel{})
	tests := []struct {
		name string
		body string
		want int
	}{
		{"malformed json", `{not json`, http.StatusBadRequest},
		{"no messages", `{"model":"test-model","messages":[]}`, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

// A generation error before any bytes are written must surface as a real HTTP
// error, not a 200 with empty content.
func TestChatGenerateErrorIsAnHTTPError(t *testing.T) {
	m := &fakeModel{tokens: []Token{{Done: true, Err: context.DeadlineExceeded}}}
	srv := newTestServer(t, m)
	body := `{"model":"test-model","messages":[{"role":"user","content":"hi"}]}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

func TestStopSequencesAcceptBothShapes(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"string form", `{"messages":[{"role":"user","content":"x"}],"stop":"END"}`, []string{"END"}},
		{"array form", `{"messages":[{"role":"user","content":"x"}],"stop":["A","B"]}`, []string{"A", "B"}},
		{"absent", `{"messages":[{"role":"user","content":"x"}]}`, nil},
		{"empty string", `{"messages":[{"role":"user","content":"x"}],"stop":""}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &fakeModel{tokens: []Token{{Done: true, Reason: "stop"}}}
			srv := newTestServer(t, m)
			resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			resp.Body.Close()
			got := m.gotReq.Stop
			if len(got) != len(tt.want) {
				t.Fatalf("stop = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("stop[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestIndexAnyStop(t *testing.T) {
	tests := []struct {
		name  string
		s     string
		stops []string
		want  int
	}{
		{"no stops", "hello world", nil, -1},
		{"not present", "hello", []string{"END"}, -1},
		{"single match", "abcENDdef", []string{"END"}, 3},
		{"earliest of several wins", "abXcdY", []string{"Y", "X"}, 2},
		{"empty stop ignored", "abc", []string{""}, -1},
		{"match at start", "ENDabc", []string{"END"}, 0},
		// The whole point: a sequence split across two tokens still matches,
		// because matching happens on accumulated text.
		{"spans token boundary", "foo<|im_" + "end|>", []string{"<|im_end|>"}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := indexAnyStop(tt.s, tt.stops); got != tt.want {
				t.Errorf("indexAnyStop(%q, %v) = %d, want %d", tt.s, tt.stops, got, tt.want)
			}
		})
	}
}
