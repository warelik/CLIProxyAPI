package pluginhost

import (
	"context"
	"errors"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestWrapStreamEmptyCompletion_NormalEmptyStream(t *testing.T) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")}
	close(chunks)

	res := wrapStreamEmptyCompletion(context.Background(), &coreexecutor.StreamResult{Chunks: chunks})
	var received []coreexecutor.StreamChunk
	for c := range res.Chunks {
		received = append(received, c)
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 chunk (EmptyCompletionError), got %d", len(received))
	}
	if !errors.Is(received[0].Err, coreauth.EmptyCompletionError()) {
		t.Errorf("expected EmptyCompletionError on close, got %v", received[0].Err)
	}
}

func TestWrapStreamEmptyCompletion_ErrChunkSkipsEmptyEval(t *testing.T) {
	expectedErr := errors.New("upstream connection reset")
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"choices\":[{\"delta\":{},\"finish_reason\":null}]}\n\n")}
	chunks <- coreexecutor.StreamChunk{Err: expectedErr}
	close(chunks)

	res := wrapStreamEmptyCompletion(context.Background(), &coreexecutor.StreamResult{Chunks: chunks})
	var received []coreexecutor.StreamChunk
	for c := range res.Chunks {
		received = append(received, c)
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 chunk (original Err), got %d", len(received))
	}
	if received[0].Err != expectedErr {
		t.Errorf("expected original error %v, got %v", expectedErr, received[0].Err)
	}
}
