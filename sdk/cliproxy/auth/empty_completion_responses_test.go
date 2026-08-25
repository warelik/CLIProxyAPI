package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestIsEmptyCompletionPayloadResponses(t *testing.T) {
	cases := []struct {
		name     string
		payload  []byte
		expected bool
	}{
		{name: "non-stream completed empty output never empty", payload: []byte(`{"object":"response","id":"r","status":"completed","output":[],"usage":{"output_tokens":0}}`), expected: false},
		{name: "sse completed empty output never empty", payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[],\"usage\":{\"output_tokens\":0}}}\n\ndata: [DONE]\n\n"), expected: false},
		{name: "output_text", payload: []byte(`{"object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`), expected: false},
		{name: "named function_call", payload: []byte(`{"object":"response","status":"completed","output":[{"type":"function_call","name":"get_weather","arguments":"{}","call_id":"call_1"}]}`), expected: false},
		{name: "positive output_tokens", payload: []byte(`{"object":"response","status":"completed","output":[],"usage":{"output_tokens":5}}`), expected: false},
		{name: "reasoning encrypted_content", payload: []byte(`{"object":"response","status":"completed","output":[{"type":"reasoning","encrypted_content":"sig_123","summary":[]}]}`), expected: false},
		{name: "incomplete not credential empty", payload: []byte(`{"object":"response","status":"incomplete","output":[],"usage":{"output_tokens":0}}`), expected: false},
		{name: "event ping without JSON body is not a Responses frame", payload: []byte("event: ping\n\n"), expected: true},
		{name: "id-only without JSON body is not a Responses frame", payload: []byte("id: evt_1\n\n"), expected: true},
		{name: "comment-only without JSON body is not a Responses frame", payload: []byte(": keep-alive\n\n"), expected: true},
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

func TestIsEmptyCompletionPayloadInteractions(t *testing.T) {
	cases := []struct {
		name     string
		payload  []byte
		expected bool
	}{
		{name: "non-stream empty steps", payload: []byte(`{"id":"interaction_1","object":"interaction","status":"completed","steps":[],"usage":{"output_tokens":0,"total_output_tokens":0}}`), expected: true},
		{name: "sse completed empty steps", payload: []byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_1\",\"status\":\"completed\",\"steps\":[],\"usage\":{\"output_tokens\":0}}}\n\n"), expected: true},
		{name: "text content", payload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","content":[{"type":"text","text":"hello"}]}]}`), expected: false},
		{name: "named function_call", payload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"function_call","name":"get_weather","arguments":{"location":"Tokyo"}}]}`), expected: false},
		{name: "positive tokens empty steps", payload: []byte(`{"object":"interaction","status":"completed","steps":[],"usage":{"output_tokens":3,"total_output_tokens":3}}`), expected: false},
		{name: "signature only thought_signature", payload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","thought_signature":"sig-only"}],"usage":{"output_tokens":0}}`), expected: true},
		{name: "signature only encrypted_content", payload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","encrypted_content":"enc-blob"}],"usage":{"output_tokens":0}}`), expected: true},
		{name: "media data", payload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","content":[{"type":"image","data":"iVBORw0KGgo="}]}]}`), expected: false},
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

func TestResponsesAndInteractionsFormatRecognized(t *testing.T) {
	var responses emptyCompletionAccum
	if !responses.evalJSON([]byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)) || !responses.recognized {
		t.Fatal("Responses completed was not recognized")
	}
	if responses.empty() {
		t.Fatal("empty Responses completed was judged empty; neverEmpty contract forbids rotation")
	}

	var live emptyCompletionAccum
	if !live.evalJSON([]byte(`{"object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`)) || !live.recognized {
		t.Fatal("Responses output_text was not recognized")
	}
	if live.empty() {
		t.Fatal("Responses output_text was judged empty")
	}

	var interactions emptyCompletionAccum
	if !interactions.evalJSON([]byte(`{"object":"interaction","status":"completed","steps":[]}`)) || !interactions.recognized {
		t.Fatal("Interactions completed was not recognized")
	}
	if !interactions.empty() {
		t.Fatal("empty Interactions completed was not judged empty")
	}

	var ping emptyCompletionAccum
	ping.evalSSE([]byte("event: ping\n\n"))
	if ping.recognized {
		t.Fatal("bare event: ping must not be recognized as a completion format")
	}
}

func TestExecuteResponsesEmptyCompletedNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"response","id":"r","status":"completed","output":[],"usage":{"output_tokens":0}}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), `"output":[]`) {
		t.Fatalf("payload = %q, want the first auth's empty completed Responses body (neverEmpty)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("empty Responses completed must not rotate, results=%v", results)
	}
}

func TestExecuteResponsesNonEmptyNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello from first"}]}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "hello from first") {
		t.Fatalf("payload = %q, want first-auth output_text (no rotation)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("live Responses text must not rotate, results=%v", results)
	}
}

func TestExecuteResponsesNamedFunctionCallNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"response","status":"completed","output":[{"type":"function_call","name":"get_weather","arguments":"{}","call_id":"call_1"}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "get_weather") {
		t.Fatalf("payload = %q, want named function_call from first auth", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("named Responses function_call must not rotate, results=%v", results)
	}
}

func TestExecuteResponsesEncryptedContentNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"response","status":"completed","output":[{"type":"reasoning","encrypted_content":"sig_123","summary":[]}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "sig_123") {
		t.Fatalf("payload = %q, want encrypted_content from first auth", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("Responses encrypted_content must not rotate, results=%v", results)
	}
}

func TestExecuteInteractionsEmptyCompletionRotatesAuth(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload:   []byte(`{"id":"interaction_1","object":"interaction","status":"completed","steps":[],"usage":{"output_tokens":0,"total_output_tokens":0}}`),
		contentPayload: []byte(`{"id":"interaction_2","object":"interaction","status":"completed","steps":[{"type":"model_output","content":[{"type":"text","text":"real"}]}],"usage":{"output_tokens":5}}`),
	}
	manager, ids, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstExecute, string(resp.Payload), "real", capture)
}

func TestExecuteInteractionsNonEmptyNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","content":[{"type":"text","text":"hello from first"}]}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "hello from first") {
		t.Fatalf("payload = %q, want first-auth Interactions text (no rotation)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("live Interactions text must not rotate, results=%v", results)
	}
}

func TestExecuteInteractionsNamedFunctionCallNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"function_call","name":"get_weather","arguments":{"location":"Tokyo"}}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "get_weather") {
		t.Fatalf("payload = %q, want named function_call from first auth", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("named Interactions function_call must not rotate, results=%v", results)
	}
}

func TestExecuteInteractionsSignatureOnlyRotatesAuth(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload:   []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","thought_signature":"sig-only"}],"usage":{"output_tokens":0}}`),
		contentPayload: []byte(`{"object":"interaction","status":"completed","steps":[{"type":"model_output","content":[{"type":"text","text":"real"}]}]}`),
	}
	manager, ids, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstExecute, string(resp.Payload), "real", capture)
}

func TestExecuteStreamResponsesCompletedNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[],\"usage\":{\"output_tokens\":0}}}\n\n"),
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
		t.Fatalf("stream error = %v, want the first auth's neverEmpty Responses body", streamErr)
	}
	if !strings.Contains(got, "response.completed") {
		t.Fatalf("stream payload = %q, want first-auth response.completed (neverEmpty)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("empty Responses stream must not rotate, results=%v", results)
	}
}

func TestExecuteStreamResponsesNonEmptyNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello from first\"}\n\n"),
			[]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello from first\"}]}],\"usage\":{\"output_tokens\":5}}}\n\n"),
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
		t.Fatalf("stream payload = %q, want first-auth output_text (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("live Responses stream must not rotate, results=%v", results)
	}
}

func TestExecuteStreamInteractionsCompletedRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: interaction.created\ndata: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"int_1\",\"object\":\"interaction\",\"status\":\"in_progress\"}}\n\n"),
			[]byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_1\",\"status\":\"completed\",\"steps\":[],\"usage\":{\"output_tokens\":0}}}\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("event: step.delta\ndata: {\"event_type\":\"step.delta\",\"delta\":{\"type\":\"text\",\"text\":\"real\"}}\n\n"),
			[]byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_2\",\"status\":\"completed\",\"usage\":{\"output_tokens\":5}}}\n\n"),
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

func TestExecuteStreamInteractionsNonEmptyNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: step.delta\ndata: {\"event_type\":\"step.delta\",\"delta\":{\"type\":\"text\",\"text\":\"hello from first\"}}\n\n"),
			[]byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_1\",\"status\":\"completed\",\"usage\":{\"output_tokens\":5}}}\n\n"),
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
		t.Fatalf("stream payload = %q, want first-auth Interactions text (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("live Interactions stream must not rotate, results=%v", results)
	}
}

func TestReadStreamBootstrapInteractionsCompleted(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"int_1\",\"status\":\"completed\",\"steps\":[],\"usage\":{\"output_tokens\":0}}}\n\n")}

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

func TestReadStreamBootstrapResponsesNeverEmpty(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 2)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\",\"output\":[]}}\n\n")}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: [DONE]\n\n")}
	close(ch)

	buffered, terminalEmpty, err := readStreamBootstrap(context.Background(), ch)
	if err != nil {
		t.Fatalf("readStreamBootstrap error = %v, want nil", err)
	}
	if terminalEmpty {
		t.Fatal("readStreamBootstrap terminalEmpty = true for Responses completed, want false (neverEmpty)")
	}
	if len(buffered) == 0 {
		t.Fatal("readStreamBootstrap dropped Responses completed instead of forwarding")
	}
}

func TestReadStreamBootstrapMetadataOnlyPingIsNotTerminal(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("event: ping\n\n")}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, terminalEmpty, err := readStreamBootstrap(ctx, ch)
	if err == nil {
		t.Fatal("readStreamBootstrap returned before close on bare event: ping, want wait")
	}
	if terminalEmpty {
		t.Fatal("bare event: ping must not be a terminal empty completion")
	}
	if ctx.Err() == nil {
		t.Fatalf("readStreamBootstrap error = %v, want context deadline while waiting after metadata-only ping", err)
	}
}

func TestResponsesReasoningEncryptedContentIsMeaningful(t *testing.T) {
	encrypted := []byte(`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"sig_123","summary":[]}}`)
	var withEnc emptyCompletionAccum
	if !withEnc.evalJSON(encrypted) || !withEnc.hasContent {
		t.Fatal("Responses reasoning encrypted_content must count as content")
	}

	emptyReasoning := []byte(`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"","summary":[]}}`)
	var withoutEnc emptyCompletionAccum
	if !withoutEnc.evalJSON(emptyReasoning) {
		t.Fatal("empty Responses reasoning item must still be recognized")
	}
	if withoutEnc.hasContent {
		t.Fatal("empty Responses reasoning summary must not count as content")
	}
}
