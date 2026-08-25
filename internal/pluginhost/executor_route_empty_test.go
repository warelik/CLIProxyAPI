package pluginhost

import (
	"bytes"
	"context"
	"errors"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func pluginHostReturning(payload []byte) *Host {
	return newRouteModelHostWithRecords(capabilityRecord{
		id: "executor",
		plugin: pluginapi.Plugin{Capabilities: pluginapi.Capabilities{
			Executor: &fakeExecutor{
				identifier: "plugin-provider",
				execute: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
					return pluginapi.ExecutorResponse{Payload: append([]byte(nil), payload...)}, nil
				},
			},
			ExecutorInputFormats:  []string{"openai"},
			ExecutorOutputFormats: []string{"openai"},
		}},
	})
}

func executePlugin(t *testing.T, payload []byte) (coreexecutor.Response, error) {
	t.Helper()
	return pluginHostReturning(payload).ExecutePluginExecutor(
		context.Background(),
		"executor",
		coreexecutor.Request{Model: "client-model", Payload: []byte(`{"model":"client-model"}`)},
		coreexecutor.Options{},
	)
}

func assertPluginEmptyCompletion(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("ExecutePluginExecutor() = nil, want retriable empty_completion")
	}
	var authErr *coreauth.Error
	if !errors.As(err, &authErr) || !authErr.Retryable || authErr.Code != "empty_completion" {
		t.Fatalf("error = %v, want retriable empty_completion", err)
	}
}

func TestHostExecutePluginExecutorClaudeEmptyCompletion(t *testing.T) {
	_, err := executePlugin(t, []byte(`{"type":"message","role":"assistant","content":[],"stop_reason":"end_turn"}`))
	assertPluginEmptyCompletion(t, err)
}

func TestHostExecutePluginExecutorGeminiEmptyCompletion(t *testing.T) {
	_, err := executePlugin(t, []byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`))
	assertPluginEmptyCompletion(t, err)
}

func TestHostExecutePluginExecutorInteractionsEmptyCompletion(t *testing.T) {
	_, err := executePlugin(t, []byte(`{"id":"interaction_1","object":"interaction","status":"completed","steps":[],"usage":{"output_tokens":0,"total_output_tokens":0}}`))
	assertPluginEmptyCompletion(t, err)
}

func TestHostExecutePluginExecutorInteractionsSignatureOnly(t *testing.T) {
	_, err := executePlugin(t, []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","thought_signature":"sig-only"}],"usage":{"output_tokens":0}}`))
	assertPluginEmptyCompletion(t, err)
}

func TestHostExecutePluginExecutorResponsesCompletedNeverEmpty(t *testing.T) {
	payload := []byte(`{"object":"response","id":"r","status":"completed","output":[],"usage":{"output_tokens":0}}`)
	resp, err := executePlugin(t, payload)
	if err != nil {
		t.Fatalf("ExecutePluginExecutor() error = %v, want success (Responses neverEmpty)", err)
	}
	if !bytes.Equal(resp.Payload, payload) {
		t.Fatalf("payload = %q, want byte-for-byte %q", resp.Payload, payload)
	}
}

func TestHostExecutePluginExecutorNonEmptyPassThrough(t *testing.T) {
	payload := []byte(`{"choices":[{"message":{"content":"hello from plugin"},"finish_reason":"stop"}]}`)
	resp, err := executePlugin(t, payload)
	if err != nil {
		t.Fatalf("ExecutePluginExecutor() error = %v, want success", err)
	}
	if !bytes.Equal(resp.Payload, payload) {
		t.Fatalf("payload = %q, want byte-for-byte %q", resp.Payload, payload)
	}
}

func TestHostExecutePluginExecutorUnrecognizedPassThrough(t *testing.T) {
	payload := []byte(`{"vendor":"opaque","blob":"not-a-known-completion"}`)
	resp, err := executePlugin(t, payload)
	if err != nil {
		t.Fatalf("ExecutePluginExecutor() error = %v, want pass-through of unrecognized format", err)
	}
	if !bytes.Equal(resp.Payload, payload) {
		t.Fatalf("payload = %q, want byte-for-byte %q", resp.Payload, payload)
	}
}

func TestHostExecutePluginExecutorStreamResponsesNeverEmpty(t *testing.T) {
	streamChunks := make(chan pluginapi.ExecutorStreamChunk, 2)
	host := newRouteModelHostWithRecords(capabilityRecord{
		id: "executor",
		plugin: pluginapi.Plugin{Capabilities: pluginapi.Capabilities{
			Executor: &fakeExecutor{
				identifier: "plugin-provider",
				executeStream: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
					return pluginapi.ExecutorStreamResponse{Chunks: streamChunks}, nil
				},
			},
			ExecutorInputFormats:  []string{"openai"},
			ExecutorOutputFormats: []string{"openai"},
		}},
	})

	streamResult, errStream := host.ExecutePluginExecutorStream(context.Background(), "executor", coreexecutor.Request{Model: "client-model"}, coreexecutor.Options{})
	if errStream != nil {
		t.Fatalf("ExecutePluginExecutorStream() unexpected error = %v", errStream)
	}

	completed := []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")
	streamChunks <- pluginapi.ExecutorStreamChunk{Payload: completed}
	close(streamChunks)

	var aggregated []byte
	for chunk := range streamResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v, want Responses neverEmpty pass-through", chunk.Err)
		}
		aggregated = append(aggregated, chunk.Payload...)
	}
	if !bytes.Equal(aggregated, completed) {
		t.Fatalf("stream payload = %q, want byte-for-byte %q", aggregated, completed)
	}
}

func TestHostExecutePluginExecutorStreamUnrecognizedPassThrough(t *testing.T) {
	streamChunks := make(chan pluginapi.ExecutorStreamChunk, 2)
	host := newRouteModelHostWithRecords(capabilityRecord{
		id: "executor",
		plugin: pluginapi.Plugin{Capabilities: pluginapi.Capabilities{
			Executor: &fakeExecutor{
				identifier: "plugin-provider",
				executeStream: func(ctx context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
					return pluginapi.ExecutorStreamResponse{Chunks: streamChunks}, nil
				},
			},
			ExecutorInputFormats:  []string{"openai"},
			ExecutorOutputFormats: []string{"openai"},
		}},
	})

	streamResult, errStream := host.ExecutePluginExecutorStream(context.Background(), "executor", coreexecutor.Request{Model: "client-model"}, coreexecutor.Options{})
	if errStream != nil {
		t.Fatalf("ExecutePluginExecutorStream() unexpected error = %v", errStream)
	}

	first := []byte("opaque: first\n")
	second := []byte("opaque: second\n")
	streamChunks <- pluginapi.ExecutorStreamChunk{Payload: first}
	streamChunks <- pluginapi.ExecutorStreamChunk{Payload: second}
	close(streamChunks)

	var aggregated []byte
	for chunk := range streamResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v, want unrecognized pass-through", chunk.Err)
		}
		aggregated = append(aggregated, chunk.Payload...)
	}
	want := append(append([]byte(nil), first...), second...)
	if !bytes.Equal(aggregated, want) {
		t.Fatalf("stream payload = %q, want byte-for-byte %q", aggregated, want)
	}
}
