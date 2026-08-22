package auth

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// inBandErrorStreamExecutor sends a normal message-data chunk followed by a
// raw JSON in-band error. The test verifies that the parsed error is logged
// with its message redacted so no upstream credential or raw error text leaks.
type inBandErrorStreamExecutor struct {
	calls atomic.Int32
}

func (e *inBandErrorStreamExecutor) Identifier() string { return "gemini" }

func (e *inBandErrorStreamExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *inBandErrorStreamExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *inBandErrorStreamExecutor) Refresh(_ context.Context, a *Auth) (*Auth, error) { return a, nil }

func (e *inBandErrorStreamExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *inBandErrorStreamExecutor) ExecuteStream(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls.Add(1)
	if opts.OnStreamConnected != nil {
		opts.OnStreamConnected()
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("{\"error\":{\"message\":\"leaked api key sk-12345\",\"code\":500}}\n")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func TestStreamInBandErrorRedactedInLogs(t *testing.T) {
	previousLevel := log.GetLevel()
	log.SetLevel(log.WarnLevel)
	hook := logtest.NewLocal(log.StandardLogger())
	t.Cleanup(func() {
		hook.Reset()
		log.SetLevel(previousLevel)
	})

	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-redact", Provider: "gemini", Status: StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "gemini", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	manager.RefreshSchedulerEntry(auth.ID)
	exec := &inBandErrorStreamExecutor{}
	manager.RegisterExecutor(exec)

	result, err := manager.ExecuteStream(context.Background(), []string{"gemini"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if result == nil || result.Chunks == nil {
		t.Fatal("no stream result chunks")
	}

	for range result.Chunks {
	}

	for _, entry := range hook.AllEntries() {
		if entry.Level != log.WarnLevel {
			continue
		}
		msg := entry.Message
		if strings.Contains(msg, "leaked api key sk-12345") {
			t.Fatalf("log message contains raw in-band error text: %q", msg)
		}
		if strings.Contains(msg, "[in-band stream error redacted]") {
			return
		}
	}
	t.Fatal("did not find redacted in-band stream error in logs")
}
