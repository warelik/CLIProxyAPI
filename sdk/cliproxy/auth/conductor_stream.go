package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// streamDrainTimeout bounds how long an abandoned upstream source is drained
// while it produces nothing. A provider that neither sends nor closes would
// otherwise pin the drain goroutine and its connection for as long as the
// request lives, and one request may abandon several sources while failing over
// across models and credentials.
//
// This is not a timeout on any stream a caller reads: AGENTS.md:58 forbids
// those after the upstream connection is established, and by the time this
// budget starts the conductor has already decided that not one byte of this
// source will reach the client. It bounds cleanup, not delivery.
const streamDrainTimeout = 5 * time.Second

// discardStreamChunks drains an abandoned upstream source so its producer is
// unblocked and its connection released, and returns a channel closed when the
// drain is over. A bare `for range ch` is not enough: the conductor abandons a
// source it is still holding open (terminal empty, model failover), and an
// upstream that never closes the channel would leak the goroutine forever.
func discardStreamChunks(ctx context.Context, ch <-chan cliproxyexecutor.StreamChunk) <-chan struct{} {
	return drainStreamChunks(ctx, ch, streamDrainTimeout)
}

// drainStreamChunks is discardStreamChunks with an explicit idle budget. The
// budget is a parameter rather than a mutable package variable so a caller that
// needs a shorter one cannot race the drain goroutines already reading it.
func drainStreamChunks(ctx context.Context, ch <-chan cliproxyexecutor.StreamChunk, idleTimeout time.Duration) <-chan struct{} {
	done := make(chan struct{})
	if ch == nil {
		close(done)
		return done
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		defer close(done)
		timer := time.NewTimer(idleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(idleTimeout)
			}
		}
	}()
	return done
}

type streamBootstrapError struct {
	cause   error
	headers http.Header
}

func cloneHTTPHeader(headers http.Header) http.Header {
	if headers == nil {
		return nil
	}
	return headers.Clone()
}

func newStreamBootstrapError(err error, headers http.Header) error {
	if err == nil {
		return nil
	}
	return &streamBootstrapError{
		cause:   err,
		headers: cloneHTTPHeader(headers),
	}
}

func (e *streamBootstrapError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *streamBootstrapError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *streamBootstrapError) Headers() http.Header {
	if e == nil {
		return nil
	}
	return cloneHTTPHeader(e.headers)
}

func streamErrorResult(headers http.Header, err error) *cliproxyexecutor.StreamResult {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Err: sanitizeErrorTextFields(err)}
	close(ch)
	return &cliproxyexecutor.StreamResult{
		Headers: cloneHTTPHeader(headers),
		Chunks:  ch,
	}
}

func validateStreamResult(result *cliproxyexecutor.StreamResult, err error) (*cliproxyexecutor.StreamResult, error) {
	if err != nil {
		return result, err
	}
	if result == nil || result.Chunks == nil {
		return result, &Error{Code: "empty_stream", Message: "upstream stream has no source", Retryable: true}
	}
	return result, nil
}

func readStreamBootstrap(ctx context.Context, ch <-chan cliproxyexecutor.StreamChunk, requestPayloads ...[]byte) ([]cliproxyexecutor.StreamChunk, bool, error) {
	if ch == nil {
		return nil, true, nil
	}
	buffered := make([]cliproxyexecutor.StreamChunk, 0, 1)
	var bootstrap streamBootstrapState
	for _, p := range requestPayloads {
		if n := extractExpectedChoices(p); n > 1 {
			bootstrap.setExpectedChoices(n)
			break
		}
	}
	for {
		var (
			chunk cliproxyexecutor.StreamChunk
			ok    bool
		)
		if ctx != nil {
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case chunk, ok = <-ch:
			}
		} else {
			chunk, ok = <-ch
		}
		if !ok {
			// A final frame without its blank-line delimiter is only parsed by
			// finish(): without it a provider error carried by that last frame stays
			// pending and the stream looks like a clean close.
			bootstrap.finish()
			if err := bootstrap.streamError(); err != nil && !bootstrap.hasMeaningfulOutput() {
				return nil, false, err
			}
			return buffered, true, nil
		}
		if chunk.Err != nil {
			// The prefix already carries bytes the caller would lose, so this
			// is a mid-stream failure and belongs to the forwarded stream.
			// Reporting it as a bootstrap error would fail over a request that
			// has already begun answering.
			if bootstrap.hasMeaningfulOutput() {
				buffered = append(buffered, chunk)
				return buffered, false, nil
			}
			return nil, false, chunk.Err
		}
		buffered = append(buffered, chunk)
		// A non-empty frame is not the same as output: an SSE preamble, a
		// zero-choice keep-alive, or a bare [DONE] all carry bytes and nothing
		// else. Forward only once the prefix actually holds output, otherwise
		// an empty completion is committed as a success and never fails over.
		if bootstrap.observe(chunk.Payload) {
			return buffered, false, nil
		}
		if err := bootstrap.streamError(); err != nil {
			if bootstrap.hasMeaningfulOutput() {
				return buffered, false, nil
			}
			return nil, false, err
		}
		// The terminal marker already arrived with nothing in it. Report the
		// prefix as closed so the caller judges it now: an upstream that stalls
		// after its own terminal frame would otherwise hold the request open.
		if bootstrap.isTerminalEmpty() {
			return buffered, true, nil
		}
	}
}

func (m *Manager) wrapStreamResult(ctx context.Context, auth *Auth, provider, resultModel string, headers http.Header, buffered []cliproxyexecutor.StreamChunk, remaining <-chan cliproxyexecutor.StreamChunk, aliasResult OAuthModelAliasResult, ephemeralResult bool, opts cliproxyexecutor.Options) *cliproxyexecutor.StreamResult {
	out := make(chan cliproxyexecutor.StreamChunk)
	streamStart := time.Now()
	go func() {
		defer close(out)
		var failed bool
		var errorDetector streamPayloadErrorDetector
		forward := true
		var rewriter *StreamRewriter
		if aliasResult.ForceMapping && strings.TrimSpace(aliasResult.OriginalAlias) != "" {
			rewriter = NewStreamRewriter(StreamRewriteOptions{RewriteModel: aliasResult.OriginalAlias})
		}
		emit := func(chunk cliproxyexecutor.StreamChunk) bool {
			if chunk.Err != nil && !failed {
				failed = true
				chunk.Err = sanitizeErrorTextFields(chunk.Err)
				entry := logEntryWithRequestID(ctx)
				warnLogUpstreamFailure(ctx, entry, provider, resultModel, auth, time.Since(streamStart), chunk.Err)
				rerr := resultErrorFromError(chunk.Err)
				action, okAction := matchRequestScopedErrorAction(auth, chunk.Err, m.runtimeConfigSnapshot())
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: opts}
				applyRequestScopedActionToResult(action, okAction, &result)
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			}
			if len(chunk.Payload) > 0 {
				if !failed {
					if streamErr := errorDetector.Observe(chunk.Payload); streamErr != nil {
						failed = true
						streamErr = sanitizeErrorTextFields(streamErr).(*Error)
						entry := logEntryWithRequestID(ctx)
						warnLogUpstreamFailure(ctx, entry, provider, resultModel, auth, time.Since(streamStart), streamErr)
						rerr := resultErrorFromError(streamErr)
						action, okAction := matchRequestScopedErrorAction(auth, streamErr, m.runtimeConfigSnapshot())
						result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: opts}
						applyRequestScopedActionToResult(action, okAction, &result)
						m.recordExecutionResult(ctx, result, auth, ephemeralResult)
					}
				}
			}
			if !forward {
				return false
			}
			if chunk.Err != nil {
				if failed {
					chunk.Payload = redactStreamPayload(chunk.Payload)
				}
				if ctx == nil {
					out <- chunk
					return true
				}
				select {
				case <-ctx.Done():
					forward = false
					return false
				case out <- chunk:
					return true
				}
			}
			if len(chunk.Payload) == 0 {
				return true
			}
			payload := rewriteForceMappedStreamChunk(rewriter, chunk.Payload)
			if len(payload) == 0 {
				return true
			}
			if failed {
				payload = redactStreamPayload(payload)
			}
			chunk.Payload = payload
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case <-ctx.Done():
				forward = false
				return false
			case out <- chunk:
				return true
			}
		}
		for _, chunk := range buffered {
			if ok := emit(chunk); !ok {
				discardStreamChunks(ctx, remaining)
				return
			}
		}
		for chunk := range remaining {
			if ok := emit(chunk); !ok {
				discardStreamChunks(ctx, remaining)
				return
			}
		}
		if tail := finishForceMappedStreamChunks(rewriter); len(tail) > 0 {
			tailChunk := cliproxyexecutor.StreamChunk{Payload: tail}
			if !emit(tailChunk) {
				return
			}
		}
		if !failed {
			if streamErr := errorDetector.Finish(); streamErr != nil {
				failed = true
				streamErr = sanitizeErrorTextFields(streamErr).(*Error)
				entry := logEntryWithRequestID(ctx)
				warnLogUpstreamFailure(ctx, entry, provider, resultModel, auth, time.Since(streamStart), streamErr)
				rerr := resultErrorFromError(streamErr)
				action, okAction := matchRequestScopedErrorAction(auth, streamErr, m.runtimeConfigSnapshot())
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: opts}
				applyRequestScopedActionToResult(action, okAction, &result)
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			}
		}
		if !failed && (ephemeralResult || claudeOAuthRequestCancellation(ctx, auth, nil) == nil) {
			m.recordExecutionResult(ctx, Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: true, Options: opts}, auth, ephemeralResult)
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
}

func (m *Manager) replaceHomeExecutionLifecycleAuth(lifecycle cliproxyexecutor.ExecutionLifecycle, auth *Auth) {
	selection, ok := lifecycle.(*HomeDispatchSelection)
	if !ok || selection == nil {
		return
	}
	m.replaceHomeSelectionAuth(selection, auth)
}

func (m *Manager) executeStreamWithModelPool(ctx context.Context, executor ProviderExecutor, auth *Auth, provider string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, routeModel, executionModel string, execModels []string, pooled bool, aliasResult OAuthModelAliasResult, routing *apiKeyModelRoutingSnapshot, allowRetry bool, ephemeralResult bool, unauthorizedRefreshTried map[string]struct{}) (*cliproxyexecutor.StreamResult, error) {
	if executor == nil {
		return nil, &Error{Code: "executor_not_found", Message: "executor not registered"}
	}
	ctx = contextWithRequestedModelAlias(ctx, opts, routeModel)
	var lastErr error
	didRefreshOnUnauthorized := false
	if auth != nil && unauthorizedRefreshTried != nil {
		_, didRefreshOnUnauthorized = unauthorizedRefreshTried[auth.ID]
	}
	for idx, execModel := range execModels {
		ctx = newUpstreamAttemptContext(ctx)
		resultModel := m.stateModelForExecution(auth, routeModel, execModel, pooled)
		execReq := req
		execReq.Model = execModel
		if executionModel != "" {
			execReq.Model = executionModel
		}
		execOpts := opts
		var errIntercept error
		execReq, execOpts, errIntercept = applyRequestAfterAuthInterceptor(ctx, executor, provider, execReq, execOpts, requestedModelAliasFromOptions(execOpts, routeModel))
		if errIntercept != nil {
			return nil, errIntercept
		}
		if executionModel == "" {
			execReq = attachResolvedAPIKeyModelInfo(routing, execReq, auth, routeModel, execModel)
		}
		if errCtx := ctx.Err(); errCtx != nil {
			return nil, errCtx
		}
		entry := logEntryWithRequestID(ctx)
		startStream := time.Now()
		streamResult, errStream := executor.ExecuteStream(ctx, auth, execReq, execOpts)
		durationStream := time.Since(startStream)
		if errStream != nil {
			if errCtx := ctx.Err(); errCtx != nil {
				return nil, errCtx
			}
			if allowRetry {
				alreadyTried := didRefreshOnUnauthorized
				willAttemptHomeRefresh := ephemeralResult && !alreadyTried && auth != nil && auth.AuthKind() == AuthKindOAuth && isUnauthorizedError(errStream)
				refreshCtx := newUpstreamAttemptContext(ctx)
				refreshed, okRefresh, errRefresh := m.tryRefreshExecutionAuthAfterUnauthorized(refreshCtx, executor, auth, errStream, alreadyTried, ephemeralResult)
				if willAttemptHomeRefresh {
					didRefreshOnUnauthorized = true
					if unauthorizedRefreshTried != nil {
						unauthorizedRefreshTried[auth.ID] = struct{}{}
					}
				}
				if errRefresh != nil {
					errStream = errRefresh
					warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, durationStream, errStream)
				} else if okRefresh {
					auth = refreshed
					m.replaceHomeExecutionLifecycleAuth(execOpts.ExecutionLifecycle, auth)
					publishSelectedAuthMetadata(execOpts.Metadata, auth)
					didRefreshOnUnauthorized = true
					ctx = newUpstreamAttemptContext(ctx)
					startRetry := time.Now()
					streamResult, errStream = executor.ExecuteStream(ctx, auth, execReq, execOpts)
					durationRetry := time.Since(startRetry)
					if errStream != nil {
						warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, durationRetry, errStream)
						if errCtx := ctx.Err(); errCtx != nil {
							return nil, errCtx
						}
					}
				} else {
					warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, durationStream, errStream)
				}
			} else {
				warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, durationStream, errStream)
			}
		}
		if !ephemeralResult {
			if errCancel := claudeOAuthRequestCancellation(ctx, auth, errStream); errCancel != nil {
				return nil, errCancel
			}
		}
		streamResult, errStream = validateStreamResult(streamResult, errStream)
		if errStream != nil {
			errStream = sanitizeErrorTextFields(errStream)
			rerr := resultErrorFromError(errStream)
			action, okAction := matchRequestScopedErrorAction(auth, errStream, m.runtimeConfigSnapshot())
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: execOpts}
			result.RetryAfter = retryAfterFromError(errStream)
			if isCredentialScopedError(errStream) {
				result.CredentialScope = true
			}
			applyRequestScopedActionToResult(action, okAction, &result)
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			if okAction {
				if isRequestScopedStop(action, okAction) {
					return nil, wrapRequestStopError(errStream)
				}
				lastErr = errStream
				if result.CredentialScope {
					return nil, errStream
				}
				continue
			}
			if isRequestInvalidError(errStream) {
				return nil, errStream
			}
			lastErr = errStream
			if result.CredentialScope {
				return nil, errStream
			}
			continue
		}

		buffered, closed, bootstrapErr := readStreamBootstrap(ctx, streamResult.Chunks, execReq.Payload, execOpts.OriginalRequest)
		if bootstrapErr != nil {
			if errCtx := ctx.Err(); errCtx != nil {
				discardStreamChunks(ctx, streamResult.Chunks)
				return nil, errCtx
			}
			if allowRetry {
				alreadyTried := didRefreshOnUnauthorized
				willAttemptHomeRefresh := ephemeralResult && !alreadyTried && auth != nil && auth.AuthKind() == AuthKindOAuth && isUnauthorizedError(bootstrapErr)
				refreshCtx := newUpstreamAttemptContext(ctx)
				refreshed, okRefresh, errRefresh := m.tryRefreshExecutionAuthAfterUnauthorized(refreshCtx, executor, auth, bootstrapErr, alreadyTried, ephemeralResult)
				if willAttemptHomeRefresh {
					didRefreshOnUnauthorized = true
					if unauthorizedRefreshTried != nil {
						unauthorizedRefreshTried[auth.ID] = struct{}{}
					}
				}
				if errRefresh != nil {
					discardStreamChunks(ctx, streamResult.Chunks)
					bootstrapErr = errRefresh
					warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startStream), bootstrapErr)
					streamResult = &cliproxyexecutor.StreamResult{}
				} else if okRefresh {
					discardStreamChunks(ctx, streamResult.Chunks)
					auth = refreshed
					m.replaceHomeExecutionLifecycleAuth(execOpts.ExecutionLifecycle, auth)
					publishSelectedAuthMetadata(execOpts.Metadata, auth)
					didRefreshOnUnauthorized = true
					ctx = newUpstreamAttemptContext(ctx)
					startRetry := time.Now()
					retryStream, retryErr := executor.ExecuteStream(ctx, auth, execReq, execOpts)
					retryStream, retryErr = validateStreamResult(retryStream, retryErr)
					if retryErr != nil {
						if errCtx := ctx.Err(); errCtx != nil {
							return nil, errCtx
						}
						bootstrapErr = retryErr
						warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startRetry), bootstrapErr)
						streamResult = &cliproxyexecutor.StreamResult{}
					} else {
						streamResult = retryStream
						buffered, closed, bootstrapErr = readStreamBootstrap(ctx, streamResult.Chunks, execReq.Payload, execOpts.OriginalRequest)
						if bootstrapErr != nil {
							warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startRetry), bootstrapErr)
						}
					}
				} else {
					warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startStream), bootstrapErr)
				}
			} else {
				warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startStream), bootstrapErr)
			}
		}
		if !ephemeralResult {
			if errCancel := claudeOAuthRequestCancellation(ctx, auth, bootstrapErr); errCancel != nil {
				discardStreamChunks(ctx, streamResult.Chunks)
				return nil, errCancel
			}
		}
		if bootstrapErr != nil {
			bootstrapErr = sanitizeErrorTextFields(bootstrapErr)
			action, okAction := matchRequestScopedErrorAction(auth, bootstrapErr, m.runtimeConfigSnapshot())
			if okAction {
				rerr := resultErrorFromError(bootstrapErr)
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: execOpts}
				result.RetryAfter = retryAfterFromError(bootstrapErr)
				if isCredentialScopedError(bootstrapErr) {
					result.CredentialScope = true
				}
				applyRequestScopedActionToResult(action, okAction, &result)
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
				discardStreamChunks(ctx, streamResult.Chunks)
				if isRequestScopedStop(action, okAction) {
					return nil, wrapRequestStopError(bootstrapErr)
				}
				lastErr = bootstrapErr
				if result.CredentialScope {
					return nil, newStreamBootstrapError(bootstrapErr, streamResult.Headers)
				}
				continue
			}
			if isRequestInvalidError(bootstrapErr) {
				rerr := resultErrorFromError(bootstrapErr)
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: execOpts}
				result.RetryAfter = retryAfterFromError(bootstrapErr)
				if isCredentialScopedError(bootstrapErr) {
					result.CredentialScope = true
				}
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
				discardStreamChunks(ctx, streamResult.Chunks)
				return nil, bootstrapErr
			}
			if idx < len(execModels)-1 {
				rerr := resultErrorFromError(bootstrapErr)
				result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: execOpts}
				result.RetryAfter = retryAfterFromError(bootstrapErr)
				if isCredentialScopedError(bootstrapErr) {
					result.CredentialScope = true
				}
				m.recordExecutionResult(ctx, result, auth, ephemeralResult)
				discardStreamChunks(ctx, streamResult.Chunks)
				lastErr = bootstrapErr
				if result.CredentialScope {
					return nil, newStreamBootstrapError(bootstrapErr, streamResult.Headers)
				}
				continue
			}
			rerr := resultErrorFromError(bootstrapErr)
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: rerr, Options: execOpts}
			result.RetryAfter = retryAfterFromError(bootstrapErr)
			if isCredentialScopedError(bootstrapErr) {
				result.CredentialScope = true
			}
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			discardStreamChunks(ctx, streamResult.Chunks)
			return nil, newStreamBootstrapError(bootstrapErr, streamResult.Headers)
		}

		payloadBytes := 0
		for _, chunk := range buffered {
			payloadBytes += len(chunk.Payload)
		}
		// Emptiness is decided on buffered payload bytes and on the aggregated
		// prefix, not on the chunk count: zero-payload chunks are dropped
		// downstream by wrapStreamResult, and a prefix of pure protocol frames
		// (a lone [DONE], a stop with zero completion tokens) is a completed
		// response that says nothing. Both surfaced as a successful empty
		// stream with no failover.
		if closed && (payloadBytes == 0 || isEmptyCompletion(buffered)) {
			emptyErr := errEmptyCompletion
			if payloadBytes == 0 {
				emptyErr = &Error{Code: "empty_stream", Message: "upstream stream closed before first payload", Retryable: true}
			}
			warnLogUpstreamFailure(ctx, entry, provider, execModel, auth, time.Since(startStream), emptyErr)
			result := Result{AuthID: auth.ID, Provider: provider, Model: resultModel, Success: false, Error: emptyErr, Options: execOpts}
			m.recordExecutionResult(ctx, result, auth, ephemeralResult)
			// The source may still be open here: a terminal-empty prefix ends
			// the read before the channel closes. Leaving it undrained blocks
			// the producer and holds the upstream connection.
			discardStreamChunks(ctx, streamResult.Chunks)
			if idx < len(execModels)-1 {
				lastErr = emptyErr
				continue
			}
			return nil, newStreamBootstrapError(emptyErr, streamResult.Headers)
		}

		remaining := streamResult.Chunks
		if closed {
			discardStreamChunks(ctx, streamResult.Chunks)
			closedCh := make(chan cliproxyexecutor.StreamChunk)
			close(closedCh)
			remaining = closedCh
		}
		attemptAliasResult := resolveAttemptAliasResult(routing, auth, routeModel, execModel, aliasResult)
		return m.wrapStreamResult(ctx, auth.Clone(), provider, resultModel, streamResult.Headers, buffered, remaining, attemptAliasResult, ephemeralResult, execOpts), nil
	}
	if lastErr == nil {
		lastErr = &Error{Code: "auth_not_found", Message: "no upstream model available"}
	}
	return nil, lastErr
}
