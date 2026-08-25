package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestIsEmptyCompletionPayloadClaude(t *testing.T) {
	cases := []struct {
		name     string
		payload  []byte
		expected bool
	}{
		{name: "non-stream content empty array", payload: []byte(`{"type":"message","content":[],"stop_reason":"end_turn","usage":{"output_tokens":0}}`), expected: true},
		{name: "non-stream empty message without usage", payload: []byte(`{"type":"message","id":"msg_1","role":"assistant","content":[],"stop_reason":"end_turn"}`), expected: true},
		{name: "sse message_stop only", payload: []byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"), expected: true},
		{name: "sse empty stream start delta stop", payload: []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"stop_reason\":null,\"usage\":{\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"), expected: true},
		{name: "skeleton tool_use no id name", payload: []byte(`{"type":"message","role":"assistant","content":[{"type":"tool_use","input":null}],"stop_reason":"tool_use"}`), expected: true},
		{name: "signature_delta only", payload: []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"content\":[],\"usage\":{\"output_tokens\":0}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig_only\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"), expected: true},
		{name: "text content", payload: []byte(`{"type":"message","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn"}`), expected: false},
		{name: "named tool_use", payload: []byte(`{"type":"message","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}],"stop_reason":"tool_use"}`), expected: false},
		{name: "max_tokens empty content", payload: []byte(`{"type":"message","content":[],"stop_reason":"max_tokens"}`), expected: false},
		{name: "refusal empty content", payload: []byte(`{"type":"message","content":[],"stop_reason":"refusal"}`), expected: false},
		{name: "thinking with text", payload: []byte(`{"type":"message","content":[{"type":"thinking","thinking":"let me think"}],"stop_reason":"end_turn"}`), expected: false},
		{name: "sse max_tokens", payload: []byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":0}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"), expected: false},
		{name: "gemini still unrecognized", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`), expected: false},
		{name: "responses completed still unrecognized", payload: []byte(`{"type":"response.completed","response":{"id":"r","status":"completed","output":[],"usage":{"output_tokens":0}}}`), expected: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isEmptyCompletionPayload(tc.payload)
			if got != tc.expected {
				t.Fatalf("isEmptyCompletionPayload(%q) = %v, want %v", tc.payload, got, tc.expected)
			}
		})
	}
}

func TestClaudeFormatRecognizedAndSiblingsUntouched(t *testing.T) {
	var claude emptyCompletionAccum
	if !claude.evalJSON([]byte(`{"type":"message","content":[{"type":"text","text":"hello"}]}`)) || !claude.recognized {
		t.Fatal("Claude message was not recognized")
	}

	var gemini emptyCompletionAccum
	if gemini.evalJSON([]byte(`{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]}`)) || gemini.recognized {
		t.Fatal("Gemini must stay unrecognized in this slice")
	}

	var responses emptyCompletionAccum
	if responses.evalJSON([]byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)) || responses.recognized {
		t.Fatal("Responses must stay unrecognized in this slice")
	}
}

func TestExecuteClaudeEmptyContentRotatesAuth(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"type":"message","content":[],"stop_reason":"end_turn","usage":{"output_tokens":0}}`),
	}
	manager, ids, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstExecute, string(resp.Payload), "real", capture)
}

func TestExecuteClaudeNonEmptyNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"type":"message","content":[{"type":"text","text":"hello from first"}],"stop_reason":"end_turn"}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "hello from first") {
		t.Fatalf("payload = %q, want first-auth content (no rotation)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("first auth should succeed without rotation, results=%v", results)
	}
}

func TestExecuteClaudeNamedToolUseNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"type":"message","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"q":"1"}}],"stop_reason":"tool_use"}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "lookup") {
		t.Fatalf("payload = %q, want tool_use from first auth", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("named tool_use must not rotate, results=%v", results)
	}
}

func TestExecuteClaudeMaxTokensNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"type":"message","content":[],"stop_reason":"max_tokens"}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "max_tokens") {
		t.Fatalf("payload = %q, want first-auth max_tokens body (no rotation)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("max_tokens must not rotate, results=%v", results)
	}
}

func TestExecuteStreamClaudeMessageStopRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"stop_reason\":null,\"usage\":{\"output_tokens\":0}}}\n\n"),
			[]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\n"),
			[]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
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

func TestExecuteStreamClaudeDataOnlyMessageStopRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"type\":\"message_stop\"}\n\n"),
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

func TestExecuteStreamClaudeNonEmptyNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello from first\"}}\n\n"),
			[]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
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

func TestExecuteStreamClaudeMaxTokensNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":0}}\n\n"),
			[]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
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
	if !strings.Contains(got, "max_tokens") {
		t.Fatalf("stream payload = %q, want first-auth max_tokens (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("max_tokens stream must not rotate, results=%v", results)
	}
}

func TestReadStreamBootstrapClaudeDataOnlyMessageStop(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"message_stop\"}\n\n")}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	buffered, terminalEmpty, err := readStreamBootstrap(ctx, ch)
	if err != nil {
		t.Fatalf("readStreamBootstrap error = %v, want nil", err)
	}
	if !terminalEmpty {
		t.Fatal("readStreamBootstrap terminalEmpty = false, want true")
	}
	if len(buffered) != 1 {
		t.Fatalf("len(buffered) = %d, want 1", len(buffered))
	}
}

func TestReadStreamBootstrapClaudeStopReasonEndTurnWithoutMessageStop(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 3)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"stop_reason\":null,\"usage\":{\"output_tokens\":0}}}\n\n")}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"\"}}\n\n")}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\n")}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	buffered, terminalEmpty, err := readStreamBootstrap(ctx, ch)
	if err != nil {
		t.Fatalf("readStreamBootstrap error = %v, want nil", err)
	}
	if !terminalEmpty {
		t.Fatal("readStreamBootstrap terminalEmpty = false, want true")
	}
	if len(buffered) != 3 {
		t.Fatalf("len(buffered) = %d, want 3", len(buffered))
	}
}

func TestReadStreamBootstrapWithholdsSplitClaudeEmptyCompletion(t *testing.T) {
	fragments := [][]byte{
		[]byte("event: message_start\n"),
		[]byte("data: {\"type\":\"message_start\",\"message\":{\"type\":\"message\",\"content\":[],\"stop_reason\":null,\"usage\":{\"output_tokens\":0}}}\n\n"),
		[]byte("event: message_delta\n"),
		[]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":0}}\n\n"),
		[]byte("event: message_stop\n"),
		[]byte("data: {\"type\":\"message_stop\"}\n\n"),
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, len(fragments))
	for _, fragment := range fragments {
		chunks <- cliproxyexecutor.StreamChunk{Payload: fragment}
	}
	close(chunks)

	buffered, closed, err := readStreamBootstrap(context.Background(), chunks)
	if err != nil {
		t.Fatalf("readStreamBootstrap() error = %v", err)
	}
	if !closed {
		t.Fatal("readStreamBootstrap() forwarded empty Claude stream")
	}
	if !isEmptyCompletion(buffered) {
		t.Fatal("split Claude stream was not classified as empty at close")
	}
}
