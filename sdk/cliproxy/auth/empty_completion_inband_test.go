package auth

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestStreamBootstrapStateEvaluatesNewlineTerminatedJSONFrameImmediately(t *testing.T) {
	var state streamBootstrapState
	state.observe([]byte("{\"error\":{\"message\":\"quota exceeded\",\"code\":429}}\n"))
	streamErr := state.streamError()
	if streamErr == nil {
		t.Fatal("bootstrap did not surface a provider error carried by a newline-terminated raw JSON frame")
	}
	if !strings.Contains(streamErr.Error(), "quota exceeded") {
		t.Fatalf("bootstrap stream error = %q, want it to carry the provider message", streamErr.Error())
	}
}

func TestStreamPayloadErrorDetectorEvaluatesNewlineTerminatedJSONFrameImmediately(t *testing.T) {
	var d streamPayloadErrorDetector
	streamErr := d.Observe([]byte("{\"error\":{\"message\":\"quota exceeded\",\"code\":429}}\n"))
	if streamErr == nil {
		t.Fatal("payload detector did not surface a provider error carried by a newline-terminated raw JSON frame")
	}
	if !strings.Contains(streamErr.Message, "quota exceeded") {
		t.Fatalf("payload detector stream error message = %q, want it to carry the provider message", streamErr.Message)
	}
}

func TestStreamBootstrapStateBuffersPrettyPrintedJSONFrame(t *testing.T) {
	var state streamBootstrapState
	frame := "{\n  \"error\": {\n    \"message\": \"quota exceeded\",\n    \"code\": 429\n  }\n}\n"
	if state.observe([]byte(frame)) {
		t.Fatal("bootstrap forwarded a pretty-printed raw JSON frame instead of buffering it to completion")
	}
	state.finish()
	streamErr := state.streamError()
	if streamErr == nil {
		t.Fatal("bootstrap did not surface a provider error carried by a pretty-printed raw JSON frame")
	}
	if !strings.Contains(streamErr.Error(), "quota exceeded") {
		t.Fatalf("bootstrap stream error = %q, want it to carry the provider message", streamErr.Error())
	}
}

func TestStreamBootstrapStateBuffersPrettyPrintedJSONFrameWithBlankLine(t *testing.T) {
	var state streamBootstrapState
	frame := "{\n\n  \"error\": {\n\n    \"message\": \"quota exceeded\",\n    \"code\": 429\n  }\n}\n"
	if state.observe([]byte(frame)) {
		t.Fatal("bootstrap forwarded a pretty-printed raw JSON frame with a blank line instead of buffering it to completion")
	}
	state.finish()
	streamErr := state.streamError()
	if streamErr == nil {
		t.Fatal("bootstrap did not surface a provider error carried by a pretty-printed raw JSON frame with a blank line")
	}
	if !strings.Contains(streamErr.Error(), "quota exceeded") {
		t.Fatalf("bootstrap stream error = %q, want it to carry the provider message", streamErr.Error())
	}
}

func TestStreamPayloadErrorDetectorBuffersPrettyPrintedJSONFrame(t *testing.T) {
	var d streamPayloadErrorDetector
	frame := "{\n  \"error\": {\n    \"message\": \"quota exceeded\",\n    \"code\": 429\n  }\n}\n"
	d.Observe([]byte(frame))
	streamErr := d.Finish()
	if streamErr == nil {
		t.Fatal("payload detector did not surface a provider error carried by a pretty-printed raw JSON frame")
	}
	if !strings.Contains(streamErr.Message, "quota exceeded") {
		t.Fatalf("payload detector stream error message = %q, want it to carry the provider message", streamErr.Message)
	}
}

func TestStreamPayloadErrorDetectorBuffersPrettyPrintedJSONFrameWithBlankLine(t *testing.T) {
	var d streamPayloadErrorDetector
	frame := "{\n\n  \"error\": {\n\n    \"message\": \"quota exceeded\",\n    \"code\": 429\n  }\n}\n"
	if streamErr := d.Observe([]byte(frame)); streamErr != nil && !strings.Contains(streamErr.Message, "quota exceeded") {
		t.Fatalf("payload detector Observe error = %q", streamErr.Message)
	}
	streamErr := d.Finish()
	if streamErr == nil {
		t.Fatal("payload detector did not surface a provider error carried by a pretty-printed raw JSON frame with a blank line")
	}
	if !strings.Contains(streamErr.Message, "quota exceeded") {
		t.Fatalf("payload detector stream error message = %q, want it to carry the provider message", streamErr.Message)
	}
}

func TestStreamPayloadErrorDetectorAssemblesSplitJSONErrorFrame(t *testing.T) {
	var d streamPayloadErrorDetector
	if err := d.Observe([]byte("data: {\"error\":{\"message\":\"over")); err != nil {
		t.Fatalf("Observe(prefix) = %v, want nil while the JSON frame is incomplete", err)
	}
	got := d.Observe([]byte("loaded\",\"type\":\"overloaded_error\"}}\n\n"))
	if got == nil {
		t.Fatal("payload detector did not assemble a JSON error split across chunk boundaries")
	}
	if !strings.Contains(got.Message, "overloaded") {
		t.Fatalf("assembled error message = %q, want overloaded", got.Message)
	}
}

func TestParseStreamErrorGRPCStatusCodeFallback(t *testing.T) {
	payload := []byte(`{"error":{"code":8,"status":"RESOURCE_EXHAUSTED"}}`)
	err := evalProviderError(payload, "")
	if err == nil {
		t.Fatal("evalProviderError() returned nil, want error")
	}
	if err.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("err.HTTPStatus = %d, want %d (RESOURCE_EXHAUSTED fallback)", err.HTTPStatus, http.StatusTooManyRequests)
	}
}

func TestEvalProviderErrorInteractionsNestedFailure(t *testing.T) {
	cases := []struct {
		name          string
		payload       string
		wantErr       bool
		wantStatus    int
		wantRetryable bool
	}{
		{
			name:          "nested rate limit error",
			payload:       `{"event_type":"interaction.failed","interaction":{"id":"int_1","status":"failed","error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED"}}}`,
			wantErr:       true,
			wantStatus:    429,
			wantRetryable: true,
		},
		{
			name:          "nested invalid request stays non retryable",
			payload:       `{"event_type":"interaction.failed","interaction":{"id":"int_1","status":"failed","error":{"code":400,"message":"invalid request","type":"invalid_request_error"}}}`,
			wantErr:       true,
			wantStatus:    400,
			wantRetryable: false,
		},
		{
			name:          "failed event without nested detail",
			payload:       `{"event_type":"interaction.failed","interaction":{"id":"int_1","status":"failed"}}`,
			wantErr:       true,
			wantStatus:    502,
			wantRetryable: true,
		},
		{
			name:          "failed status without failed event type",
			payload:       `{"event_type":"interaction.status_update","interaction":{"id":"int_1","status":"failed"}}`,
			wantErr:       true,
			wantStatus:    502,
			wantRetryable: true,
		},
		{
			name:    "completed interaction is not an error",
			payload: `{"event_type":"interaction.completed","interaction":{"id":"int_1","status":"completed","steps":[]}}`,
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evalProviderError([]byte(tc.payload), "")
			if !tc.wantErr {
				if got != nil {
					t.Fatalf("evalProviderError() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("evalProviderError() = nil, want a classified provider error so the request can fail over")
			}
			if got.HTTPStatus != tc.wantStatus {
				t.Fatalf("HTTPStatus = %d, want %d (err=%+v)", got.HTTPStatus, tc.wantStatus, got)
			}
			if got.Retryable != tc.wantRetryable {
				t.Fatalf("Retryable = %v, want %v (err=%+v)", got.Retryable, tc.wantRetryable, got)
			}
		})
	}
}

func TestReadStreamBootstrapInteractionsNestedFailureSurfacesProviderError(t *testing.T) {
	createdChunk := []byte("event: interaction.created\ndata: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"int_1\",\"object\":\"interaction\",\"status\":\"in_progress\"}}\n\n")
	failedChunk := []byte("event: interaction.failed\ndata: {\"event_type\":\"interaction.failed\",\"interaction\":{\"id\":\"int_1\",\"status\":\"failed\",\"error\":{\"code\":429,\"message\":\"Resource has been exhausted\",\"status\":\"RESOURCE_EXHAUSTED\"}}}\n\n")

	ch := make(chan cliproxyexecutor.StreamChunk, 2)
	ch <- cliproxyexecutor.StreamChunk{Payload: createdChunk}
	ch <- cliproxyexecutor.StreamChunk{Payload: failedChunk}
	close(ch)

	// Bound is on the test helper, not a live stream a caller reads (AGENTS.md:58).
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, _, err := readStreamBootstrap(ctx, ch)
	if err == nil {
		t.Fatal("readStreamBootstrap error = nil on nested interaction failure, want a provider error so the auth is rotated")
	}
	provErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("readStreamBootstrap error type = %T, want *Error", err)
	}
	if provErr.HTTPStatus != 429 {
		t.Fatalf("HTTPStatus = %d, want 429 (err=%+v)", provErr.HTTPStatus, provErr)
	}
	if !provErr.Retryable {
		t.Fatalf("Retryable = false, want true (err=%+v)", provErr)
	}
}

func TestExecuteStreamInStreamGemini429ErrorRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"error\":{\"code\":429,\"message\":\"Resource exhausted\",\"status\":\"RESOURCE_EXHAUSTED\"}}\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"gemini response\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"candidatesTokenCount\":5}}\n\n"),
		},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, stream)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotatesTo(t, ids, executor.first(), got, "gemini response", capture)
}

func TestExecuteStreamInStreamClaudeOverloadedErrorRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"claude response\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
		},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, stream)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotatesTo(t, ids, executor.first(), got, "claude response", capture)
}

func TestExecuteStreamInStream400InvalidRequestNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"error\":{\"code\":400,\"message\":\"Invalid request prompt\",\"type\":\"invalid_request_error\"}}\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"should not reach\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
		},
	}
	manager, ids, model, _ := newOpenAIStreamEmptyTestManager(t, executor)

	_, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err == nil {
		t.Fatal("ExecuteStream() want error for 400 invalid request, got nil")
	}

	other := ids[0]
	if executor.first() == ids[0] {
		other = ids[1]
	}
	if executor.streamCallCount(other) > 0 {
		t.Fatalf("second auth %q was called (%d times), want 0 calls (400 must not rotate)", other, executor.streamCallCount(other))
	}
}

func TestExecuteStreamInStreamUnknownJSONForwardedNotRotated(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"custom_future_protocol_field\":\"forward_me\"}\n\n"),
			[]byte("data: [DONE]\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"should not reach\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
		},
	}
	manager, ids, model, _ := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, stream)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want unknown JSON forwarded", streamErr)
	}
	if !strings.Contains(got, "custom_future_protocol_field") {
		t.Fatalf("payload = %q, want unknown JSON forwarded directly", got)
	}
	other := ids[0]
	if executor.first() == ids[0] {
		other = ids[1]
	}
	if executor.streamCallCount(other) > 0 {
		t.Fatalf("second auth %q was called (%d times), want 0 calls for unknown valid JSON", other, executor.streamCallCount(other))
	}
}

func TestExecuteStreamInStreamResponseFailedNestedErrorRotatesAuth(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_123\",\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit reached\"}}}\n\n"),
		},
		contentChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"rotated response\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"),
		},
	}
	manager, ids, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	got, streamErr := collectStream(t, stream)
	if streamErr != nil {
		t.Fatalf("stream error = %v, want the rotated completion", streamErr)
	}
	assertStreamEmptyRotatesTo(t, ids, executor.first(), got, "rotated response", capture)
}

func TestExecuteStreamMidStreamInStreamErrorMarksAuthFailed(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"valid prefix content\"}}]}\n\n"),
			[]byte("data: {\"error\":{\"code\":429,\"message\":\"Resource exhausted mid-stream\",\"status\":\"RESOURCE_EXHAUSTED\"}}\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() unexpected error = %v", err)
	}
	got, _ := collectStream(t, stream)
	if !strings.Contains(got, "valid prefix content") {
		t.Fatalf("stream payload missing prefix content, got: %q", got)
	}
	if !strings.Contains(got, "Resource exhausted mid-stream") && !strings.Contains(got, "REDACTED") {
		t.Fatalf("stream payload missing mid-stream error, got: %q", got)
	}

	results := capture.Results()
	if len(results) == 0 {
		t.Fatal("expected at least 1 execution result recorded, got 0")
	}
	for _, res := range results {
		if res.Success {
			t.Fatalf("recorded execution result with Success = true for mid-stream error: %+v", res)
		}
	}
}

func TestStreamSplitSSEEventAndDataAcrossChunksDetectsError(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"valid prefix content\"}}]}\n\n"),
			[]byte("event: error\n\n"),
			[]byte("data: {\"message\":\"overloaded\"}\n\n"),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() unexpected error = %v", err)
	}
	for range stream.Chunks {
	}

	results := capture.Results()
	if len(results) == 0 {
		t.Fatal("expected execution results, got 0")
	}
	for _, res := range results {
		if res.Success {
			t.Fatalf("recorded execution result with Success = true for split SSE error: %+v", res)
		}
	}
}

func TestStreamRawJSONWithoutNewlinesThenErrorDetectsFailure(t *testing.T) {
	executor := &openaiStreamEmptyTestExecutor{
		emptyChunks: [][]byte{
			[]byte(`{"choices":[{"delta":{"content":"valid prefix content"}}]}`),
			[]byte(`{"error":{"code":"rate_limit_exceeded","message":"Rate limit reached"}}`),
		},
	}
	manager, _, model, capture := newOpenAIStreamEmptyTestManager(t, executor)

	stream, err := manager.ExecuteStream(context.Background(), []string{"stream-empty-provider"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() unexpected error = %v", err)
	}
	for range stream.Chunks {
	}

	results := capture.Results()
	if len(results) == 0 {
		t.Fatal("expected execution results, got 0")
	}
	for _, res := range results {
		if res.Success {
			t.Fatalf("recorded execution result with Success = true for raw JSON error: %+v", res)
		}
	}

	auth, ok := manager.GetByID(executor.first())
	if !ok || auth == nil {
		t.Fatalf("auth %q not found", executor.first())
	}
	if !auth.Unavailable && auth.NextRetryAfter.IsZero() && !auth.Quota.Exceeded {
		t.Fatalf("auth %q was not marked unavailable or in cooldown after raw JSON error", executor.first())
	}
}

func assertStreamEmptyRotatesTo(t *testing.T, ids []string, emptyFirst, gotPayload, wantSubstr string, capture *resultCaptureHook) {
	t.Helper()
	if emptyFirst == "" {
		t.Fatal("executor never streamed from any auth")
	}
	if !strings.Contains(gotPayload, wantSubstr) {
		t.Fatalf("payload = %q, want %q from the non-empty auth", gotPayload, wantSubstr)
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
		t.Fatalf("error-stream auth %q was recorded as success; results=%v", emptyFirst, capture.Results())
	}
	if !emptyRecorded {
		t.Fatalf("error-stream auth %q was not recorded as a failure; results=%v", emptyFirst, capture.Results())
	}
	if !otherSucceeded {
		t.Fatalf("content auth %q was not recorded as success; results=%v", other, capture.Results())
	}
}

func (e *openaiStreamEmptyTestExecutor) streamCallCount(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.streamCalls[id]
}
