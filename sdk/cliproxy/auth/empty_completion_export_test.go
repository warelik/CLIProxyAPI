package auth

import "testing"

func TestExportedEmptyCompletionWrappers(t *testing.T) {
	empty := []byte(`{"choices":[{"message":{"content":""},"finish_reason":"stop"}],"usage":{"completion_tokens":0}}`)
	live := []byte(`{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}]}`)
	if got, want := IsEmptyCompletionPayload(empty), isEmptyCompletionPayload(empty); got != want || !got {
		t.Fatalf("IsEmptyCompletionPayload(empty) = %v, internal = %v, want true", got, want)
	}
	if got, want := IsEmptyCompletionPayload(live), isEmptyCompletionPayload(live); got != want || got {
		t.Fatalf("IsEmptyCompletionPayload(live) = %v, internal = %v, want false", got, want)
	}

	if EmptyCompletionError() != errEmptyCompletion {
		t.Fatal("EmptyCompletionError() is not errEmptyCompletion")
	}

	payload := []byte(`{"n":2,"generationConfig":{"candidateCount":3}}`)
	if got, want := ExtractExpectedChoices(payload), extractExpectedChoices(payload); got != want || got != 3 {
		t.Fatalf("ExtractExpectedChoices() = %d, internal = %d, want 3", got, want)
	}

	var detector StreamBootstrapDetector
	detector.SetRequestPayload(payload)
	if detector.state.acc.expectedChoices != 3 {
		t.Fatalf("SetRequestPayload expectedChoices = %d, want 3", detector.state.acc.expectedChoices)
	}
}

func TestStreamBootstrapDetectorNilReceiver(t *testing.T) {
	var detector *StreamBootstrapDetector
	if !detector.Observe([]byte("x")) {
		t.Fatal("nil Observe() = false, want true (forward conservatively)")
	}
	if detector.HasMeaningfulOutput() {
		t.Fatal("nil HasMeaningfulOutput() = true, want false")
	}
	if detector.Finish() {
		t.Fatal("nil Finish() = true, want false")
	}
	if detector.IsTerminalEmpty() {
		t.Fatal("nil IsTerminalEmpty() = true, want false")
	}
	if detector.StreamError() != nil {
		t.Fatal("nil StreamError() != nil, want nil")
	}
	detector.SetExpectedChoices(2)
	detector.SetRequestPayload([]byte(`{"n":4}`))
}

func TestStreamPayloadErrorDetectorNilReceiver(t *testing.T) {
	var detector *StreamPayloadErrorDetector
	if detector.Observe([]byte(`{"error":{"message":"x"}}`)) != nil {
		t.Fatal("nil Observe() returned error")
	}
	if detector.Finish() != nil {
		t.Fatal("nil Finish() returned error")
	}
}
