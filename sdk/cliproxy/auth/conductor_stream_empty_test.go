package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// openaiStreamEmptyTestExecutor streams emptyChunks from the first auth
// ExecuteStream picks and contentChunks from every later auth, mirroring an
// upstream that answers with a well-formed but empty completion on one
// credential and a real one on the next.
type openaiStreamEmptyTestExecutor struct {
	emptyChunks   [][]byte
	contentChunks [][]byte

	mu           sync.Mutex
	firstStream  string
	streamCalls  map[string]int
	streamedAuth []string
}

func (*openaiStreamEmptyTestExecutor) Identifier() string { return "stream-empty-provider" }

func (*openaiStreamEmptyTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("non-stream path not in this slice")
}

func (*openaiStreamEmptyTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (e *openaiStreamEmptyTestExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	if e.streamCalls == nil {
		e.streamCalls = map[string]int{}
	}
	e.streamCalls[auth.ID]++
	e.streamedAuth = append(e.streamedAuth, auth.ID)
	if e.firstStream == "" {
		e.firstStream = auth.ID
	}
	payloads := e.contentChunks
	if len(payloads) == 0 {
		payloads = [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"real\"},\"finish_reason\":\"stop\"}]}\n\n"),
			[]byte("data: [DONE]\n\n"),
		}
	}
	if e.firstStream == auth.ID {
		payloads = e.emptyChunks
	}
	e.mu.Unlock()

	ch := make(chan cliproxyexecutor.StreamChunk, len(payloads))
	for _, p := range payloads {
		ch <- cliproxyexecutor.StreamChunk{Payload: p}
	}
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}

func (*openaiStreamEmptyTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*openaiStreamEmptyTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *openaiStreamEmptyTestExecutor) first() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.firstStream
}

func newOpenAIStreamEmptyTestManager(t *testing.T, executor *openaiStreamEmptyTestExecutor) (*Manager, []string, string, *resultCaptureHook) {
	t.Helper()
	model := "empty-stream-model-" + uuid.NewString()
	capture := &resultCaptureHook{}
	manager := NewManager(nil, nil, capture)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(executor)

	var ids []string
	for i := 0; i < 2; i++ {
		auth := &Auth{
			ID:       "empty-stream-auth-" + uuid.NewString(),
			Provider: "stream-empty-provider",
			Status:   StatusActive,
			Metadata: map[string]any{"request_retry": float64(0)},
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
		ids = append(ids, auth.ID)
	}
	return manager, ids, model, capture
}

// collectStream drains a StreamResult and returns the concatenated payloads and
// the first error chunk, so a test can tell "client saw content" from "client
// saw a failure".
func collectStream(t *testing.T, res *cliproxyexecutor.StreamResult) (string, error) {
	t.Helper()
	if res == nil || res.Chunks == nil {
		t.Fatal("ExecuteStream returned no chunks")
	}
	var sb strings.Builder
	var firstErr error
	for chunk := range res.Chunks {
		if chunk.Err != nil && firstErr == nil {
			firstErr = chunk.Err
		}
		sb.Write(chunk.Payload)
	}
	return sb.String(), firstErr
}

func assertStreamEmptyRotates(t *testing.T, ids []string, emptyFirst, got string, capture *resultCaptureHook) {
	t.Helper()
	if emptyFirst == "" {
		t.Fatal("executor never streamed from any auth")
	}
	if !strings.Contains(got, "real") {
		t.Fatalf("stream payload = %q, want the completion from the non-empty auth", got)
	}
	other := ids[0]
	if emptyFirst == ids[0] {
		other = ids[1]
	}
	var emptyRecorded, emptySucceeded, otherSucceeded bool
	for _, r := range capture.Results() {
		if r.AuthID == emptyFirst && !r.Success {
			emptyRecorded = true
		}
		if r.AuthID == emptyFirst && r.Success {
			emptySucceeded = true
		}
		if r.AuthID == other && r.Success {
			otherSucceeded = true
		}
	}
	if emptySucceeded {
		t.Fatalf("empty-stream auth %q was recorded as success; results=%v", emptyFirst, capture.Results())
	}
	if !emptyRecorded {
		t.Fatalf("empty-stream auth %q was not recorded as a failure; results=%v", emptyFirst, capture.Results())
	}
	if !otherSucceeded {
		t.Fatalf("content auth %q was not recorded as success; results=%v", other, capture.Results())
	}
}

func TestExecuteStreamDoneOnlyRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{[]byte("data: [DONE]\n\n")},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotates(t, ids, executor.first(), got, capture)
}

func TestExecuteStreamStopWithZeroUsageRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n"),
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}]}\n\n"),
			[]byte("data: {\"choices\":[],\"usage\":{\"completion_tokens\":0}}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotates(t, ids, executor.first(), got, capture)
}

func TestExecuteStreamSkeletonToolCallRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\"}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"completion_tokens\":0}}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotates(t, ids, executor.first(), got, capture)
}

func TestExecuteStreamNonEmptyNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello from first\"}}]}\n\n"),
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want no failover", streamErr)
	}
	if !strings.Contains(got, "hello from first") {
		t.Fatalf("stream payload = %q, want the first auth's content (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("first auth should succeed without rotation, results=%v", results)
	}
}

func TestExecuteStreamMeaningfulToolCallNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"1\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"completion_tokens\":0}}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want no failover", streamErr)
	}
	if !strings.Contains(got, "lookup") {
		t.Fatalf("stream payload = %q, want the first auth's tool call (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("named tool call must not rotate, results=%v", results)
	}
}

// TestExecuteStreamMultiChoiceWaitsForRemainingChoices is the false-death
// control for the terminal-empty verdict: with n=2 the first choice may finish
// empty while the second still carries the answer. A provider that attaches
// usage to every frame would otherwise make the stop frame look terminal, and
// a live credential would be rotated away mid-answer.
func TestExecuteStreamMultiChoiceWaitsForRemainingChoices(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":0}}\n\n"),
			[]byte("data: {\"choices\":[{\"index\":1,\"delta\":{\"content\":\"second choice answer\"},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens\":7}}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model, Payload: []byte(`{"n":2}`)}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want no failover", streamErr)
	}
	if !strings.Contains(got, "second choice answer") {
		t.Fatalf("stream payload = %q, want the second choice from the first auth (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("multi-choice stream must not rotate, results=%v", results)
	}
}
