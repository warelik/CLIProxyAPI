package auth

import (
	"context"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// TestReadStreamBootstrapWaitsForUsageAfterStop covers the false-death side of
// the stream verdict: an OpenAI stream may emit finish_reason=stop before the
// final usage frame, so the prefix must not be judged empty until usage
// arrives. Judging at the stop frame would rotate a credential that is about to
// report a non-zero completion.
func TestReadStreamBootstrapWaitsForUsageAfterStop(t *testing.T) {
	ch := make(chan cliproxyexecutor.StreamChunk, 2)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[],\"usage\":{\"completion_tokens\":0}}\n\n")}
	close(ch)

	buffered, closed, err := readStreamBootstrap(context.Background(), ch)
	if err != nil {
		t.Fatalf("readStreamBootstrap() error = %v, want nil", err)
	}
	if !closed {
		t.Fatal("closed = false, want true after terminal usage with zero tokens")
	}
	if len(buffered) != 2 {
		t.Fatalf("len(buffered) = %d, want 2 (stop + usage)", len(buffered))
	}

	ch2 := make(chan cliproxyexecutor.StreamChunk, 2)
	ch2 <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")}
	ch2 <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[],\"usage\":{\"completion_tokens\":3}}\n\n")}
	close(ch2)

	buffered2, closed2, err2 := readStreamBootstrap(context.Background(), ch2)
	if err2 != nil {
		t.Fatalf("readStreamBootstrap() error = %v, want nil", err2)
	}
	if closed2 {
		t.Fatal("closed = true, want false after meaningful usage with positive tokens")
	}
	if len(buffered2) != 2 {
		t.Fatalf("len(buffered2) = %d, want 2 (stop + usage)", len(buffered2))
	}
}
