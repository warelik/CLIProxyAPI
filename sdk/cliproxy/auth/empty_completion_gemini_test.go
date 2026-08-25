package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestIsEmptyCompletionPayloadGemini(t *testing.T) {
	cases := []struct {
		name     string
		payload  []byte
		expected bool
	}{
		{name: "non-stream empty candidates", payload: []byte(`{"candidates":[],"usageMetadata":{"candidatesTokenCount":0}}`), expected: true},
		{name: "non-stream empty parts STOP", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`), expected: true},
		{name: "nested response empty candidates", payload: []byte(`{"response":{"candidates":[],"usageMetadata":{"candidatesTokenCount":0}}}`), expected: true},
		{name: "sse STOP only", payload: []byte("data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n"), expected: true},
		{name: "thoughtSignature only omitted tokens", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"sig_gemini_thought_123"}]},"finishReason":"STOP"}]}`), expected: true},
		{name: "thought_signature only omitted tokens", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"thought_signature":"sig_gemini_thought_123"}]},"finishReason":"STOP"}]}`), expected: true},
		{name: "empty thoughtSignature zero tokens", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"","thoughtSignature":""}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":0}}`), expected: true},
		{name: "null functionCall", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":null}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`), expected: true},
		{name: "empty-name empty-args functionCall", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"","args":{}}}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`), expected: true},
		{name: "text content", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello"}]},"finishReason":"STOP"}]}`), expected: false},
		{name: "named functionCall", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"search","args":{}}}]},"finishReason":"STOP"}]}`), expected: false},
		{name: "functionCall args without name", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"","args":{"query":"hello"}}}]},"finishReason":"STOP"}]}`), expected: false},
		{name: "thoughtSignature with text", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello","thoughtSignature":"sig"}]},"finishReason":"STOP"}]}`), expected: false},
		{name: "thoughtSignature with positive tokens", payload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"","thoughtSignature":"sig"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":1}}`), expected: false},
		{name: "SAFETY empty parts", payload: []byte(`{"candidates":[{"content":{"parts":[]},"finishReason":"SAFETY"}]}`), expected: false},
		{name: "MAX_TOKENS empty parts", payload: []byte(`{"candidates":[{"content":{"parts":[]},"finishReason":"MAX_TOKENS"}]}`), expected: false},
		{name: "responses completed never empty by contract", payload: []byte(`{"type":"response.completed","response":{"id":"r","status":"completed","output":[],"usage":{"output_tokens":0}}}`), expected: false},
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

func TestGeminiFormatRecognizedAndSiblingsUntouched(t *testing.T) {
	var gemini emptyCompletionAccum
	if !gemini.evalJSON([]byte(`{"candidates":[{"content":{"parts":[{"text":"hello"}]},"finishReason":"STOP"}]}`)) || !gemini.recognized {
		t.Fatal("Gemini candidates were not recognized")
	}

	var claude emptyCompletionAccum
	if !claude.evalJSON([]byte(`{"type":"message","content":[{"type":"text","text":"hello"}]}`)) || !claude.recognized {
		t.Fatal("Claude message must stay recognized")
	}

	var responses emptyCompletionAccum
	if !responses.evalJSON([]byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)) || !responses.recognized {
		t.Fatal("Responses completed must be recognized")
	}
}

func TestExtractExpectedChoicesGemini(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    int
	}{
		{name: "nil payload", payload: "", want: 1},
		{name: "openai n=2", payload: `{"n":2}`, want: 2},
		{name: "generationConfig.candidateCount=2", payload: `{"generationConfig":{"candidateCount":2}}`, want: 2},
		{name: "generationConfig.candidate_count=3", payload: `{"generationConfig":{"candidate_count":3}}`, want: 3},
		{name: "generation_config.candidateCount=4", payload: `{"generation_config":{"candidateCount":4}}`, want: 4},
		{name: "nested request.generationConfig.candidateCount=2", payload: `{"request":{"generationConfig":{"candidateCount":2}}}`, want: 2},
		{name: "top-level candidateCount=3", payload: `{"candidateCount":3}`, want: 3},
		{name: "both n=2 and candidateCount=3", payload: `{"n":2,"generationConfig":{"candidateCount":3}}`, want: 3},
		{name: "invalid candidateCount=0", payload: `{"generationConfig":{"candidateCount":0}}`, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractExpectedChoices([]byte(tc.payload))
			if got != tc.want {
				t.Fatalf("extractExpectedChoices(%q) = %d, want %d", tc.payload, got, tc.want)
			}
		})
	}
}

func TestExecuteGeminiEmptyCandidatesRotatesAuth(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":0}}`),
	}
	manager, ids, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstExecute, string(resp.Payload), "real", capture)
}

func TestExecuteGeminiThoughtSignatureOnlyRotatesAuth(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"thoughtSignature":"sig_only"}]},"finishReason":"STOP"}]}`),
	}
	manager, ids, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertOpenAIEmptyRotates(t, ids, executor.firstExecute, string(resp.Payload), "real", capture)
}

func TestExecuteGeminiNonEmptyNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello from first"}]},"finishReason":"STOP"}]}`),
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

func TestExecuteGeminiNamedFunctionCallNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"search","args":{"q":"1"}}}]},"finishReason":"STOP"}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "search") {
		t.Fatalf("payload = %q, want functionCall from first auth", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("named functionCall must not rotate, results=%v", results)
	}
}

func TestExecuteGeminiSignatureWithTextNotRotated(t *testing.T) {
	executor := &openaiEmptyTestExecutor{
		emptyPayload: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hello from first","thoughtSignature":"sig"}]},"finishReason":"STOP"}]}`),
	}
	manager, _, model, capture := newOpenAIEmptyTestManager(t, executor)

	resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(string(resp.Payload), "hello from first") {
		t.Fatalf("payload = %q, want first-auth content with signature (no rotation)", resp.Payload)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.firstExecute {
		t.Fatalf("signature with text must not rotate, results=%v", results)
	}
}

func TestExecuteStreamGeminiStopRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"candidatesTokenCount\":0}}\n\n"),
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

func TestExecuteStreamGeminiThoughtSignatureOnlyRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"thoughtSignature\":\"sig_only\"}]},\"finishReason\":\"STOP\"}]}\n\n"),
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

func TestExecuteStreamGeminiNonEmptyNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello from first\"}]}}]}\n\n"),
			[]byte("data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n"),
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

func TestExecuteStreamGeminiSignatureWithTextNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello from first\",\"thoughtSignature\":\"sig\"}]},\"finishReason\":\"STOP\"}]}\n\n"),
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
		t.Fatalf("stream payload = %q, want first-auth content with signature (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("signature with text stream must not rotate, results=%v", results)
	}
}

func TestExecuteStreamGeminiMultiCandidateWaitsForRemaining(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"index\":0,\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}\n\n"),
			[]byte("data: {\"candidates\":[{\"index\":1,\"content\":{\"parts\":[{\"text\":\"hello from candidate 1\"}]},\"finishReason\":\"STOP\"}]}\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	res, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"generationConfig":{"candidateCount":2}}`),
	}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, res)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want no failover", streamErr)
	}
	if !strings.Contains(got, "hello from candidate 1") {
		t.Fatalf("stream payload = %q, want the second candidate from the first auth (no rotation)", got)
	}
	results := capture.Results()
	if len(results) != 1 || !results[0].Success || results[0].AuthID != executor.first() {
		t.Fatalf("multi-candidate stream must not rotate, results=%v", results)
	}
}

func TestReadStreamBootstrapGeminiStop(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n")}

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

func TestReadStreamBootstrapGeminiCandidateCountWaits(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 2)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"candidates\":[{\"index\":0,\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}\n\n")}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := readStreamBootstrap(ctx, ch, []byte(`{"generationConfig":{"candidateCount":2}}`))
	if err == nil {
		t.Fatal("readStreamBootstrap returned before remaining candidate, want wait")
	}
	if ctx.Err() == nil {
		t.Fatalf("readStreamBootstrap error = %v, want context deadline while waiting for candidate 1", err)
	}
}

func TestReadStreamBootstrapWithholdsSplitGeminiEmptyCompletion(t *testing.T) {
	fragments := [][]byte{
		[]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},"),
		[]byte("\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"candidatesTokenCount\":0}}\n\n"),
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
		t.Fatal("readStreamBootstrap() forwarded empty Gemini stream")
	}
	if !isEmptyCompletion(buffered) {
		t.Fatal("split Gemini stream was not classified as empty at close")
	}
}
