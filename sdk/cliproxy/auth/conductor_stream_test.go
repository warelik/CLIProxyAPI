package auth

import (
	"context"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestDiscardStreamChunksExitsOnContextCancel(t *testing.T) {
	src := make(chan cliproxyexecutor.StreamChunk)
	ctx, cancel := context.WithCancel(context.Background())
	done := discardStreamChunks(ctx, src)

	select {
	case <-done:
		t.Fatal("drain finished too early")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("discardStreamChunks goroutine did not exit on context cancellation")
	}
}

func TestDiscardStreamChunksExitsOnOpenUnclosedChannel(t *testing.T) {
	src := make(chan cliproxyexecutor.StreamChunk)
	done := drainStreamChunks(context.Background(), src, 100*time.Millisecond)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("discardStreamChunks goroutine did not exit on timeout for open unclosed channel")
	}
}
