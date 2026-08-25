package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// openaiEmptyCountTestExecutor returns an empty count payload from the first
// auth ExecuteCount picks and a live count from every later auth.
type openaiEmptyCountTestExecutor struct {
	emptyPayload   []byte
	contentPayload []byte
	firstCount     string
	countCalls     map[string]int
}

func (*openaiEmptyCountTestExecutor) Identifier() string { return "claude" }

func (*openaiEmptyCountTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("execute not in this test")
}

func (e *openaiEmptyCountTestExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.countCalls == nil {
		e.countCalls = map[string]int{}
	}
	e.countCalls[auth.ID]++
	if e.firstCount == "" {
		e.firstCount = auth.ID
	}
	if e.firstCount == auth.ID {
		empty := e.emptyPayload
		if len(empty) == 0 {
			empty = []byte(`{"choices":[{"message":{"content":""},"finish_reason":"stop"}],"usage":{"completion_tokens":0}}`)
		}
		return cliproxyexecutor.Response{Payload: empty}, nil
	}
	content := e.contentPayload
	if len(content) == 0 {
		content = []byte(`{"input_tokens":12}`)
	}
	return cliproxyexecutor.Response{Payload: content}, nil
}

func (*openaiEmptyCountTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("stream not in this test")
}

func (*openaiEmptyCountTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*openaiEmptyCountTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func newOpenAIEmptyCountTestManager(t *testing.T, executor *openaiEmptyCountTestExecutor) (*Manager, []string, string, *resultCaptureHook) {
	t.Helper()
	model := "empty-count-model-" + uuid.NewString()
	capture := &resultCaptureHook{}
	manager := NewManager(nil, nil, capture)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(executor)

	var ids []string
	for i := 0; i < 2; i++ {
		auth := &Auth{
			ID:         "empty-count-auth-" + uuid.NewString(),
			Provider:   "claude",
			Attributes: map[string]string{"auth_kind": "oauth"},
			Metadata: map[string]any{
				"access_token":  "access-token",
				"refresh_token": "refresh-token",
				"request_retry": float64(0),
			},
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

func TestExecuteCountEmptyRotatesAuth(t *testing.T) {
	executor := &openaiEmptyCountTestExecutor{}
	manager, ids, model, capture := newOpenAIEmptyCountTestManager(t, executor)

	resp, err := manager.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteCount() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstCount, string(resp.Payload), "input_tokens", capture)
	var emptyCountRecorded bool
	for _, r := range capture.Results() {
		if r.AuthID == executor.firstCount && r.Error != nil && r.Error.Code == "empty_count" {
			emptyCountRecorded = true
		}
	}
	if !emptyCountRecorded {
		t.Fatalf("empty count auth %q was not recorded as empty_count; results=%v", executor.firstCount, capture.Results())
	}
}

func TestExecuteCountLiveNotRotated(t *testing.T) {
	executor := &openaiEmptyCountTestExecutor{
		emptyPayload: []byte(`{"input_tokens":7}`),
	}
	manager, _, model, capture := newOpenAIEmptyCountTestManager(t, executor)

	resp, err := manager.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteCount() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "input_tokens") {
		t.Fatalf("payload = %q, want first-auth live count (no rotation)", resp.Payload)
	}
	if executor.firstCount == "" {
		t.Fatal("executor never counted any auth")
	}
	if len(executor.countCalls) != 1 {
		t.Fatalf("countCalls = %v, want exactly one auth (live count must not rotate)", executor.countCalls)
	}
	for _, r := range capture.Results() {
		if !r.Success {
			t.Fatalf("live count recorded as failure: %+v", r)
		}
	}
}
