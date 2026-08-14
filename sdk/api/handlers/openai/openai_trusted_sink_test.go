package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// trustedSinkSecret is a value a plugin places in its direct-response body. The
// trusted sinks must preserve it verbatim; untrusted upstream termination must
// never surface it.
const trustedSinkSecret = "TSINK-SECRET-VALUE"

const trustedSinkModel = "trusted-sink-model"

var trustedSinkAuthCounterMu sync.Mutex
var trustedSinkAuthCounter int

func nextTrustedSinkAuthID() string {
	trustedSinkAuthCounterMu.Lock()
	defer trustedSinkAuthCounterMu.Unlock()
	trustedSinkAuthCounter++
	return fmt.Sprintf("trusted-sink-%d-%s", trustedSinkAuthCounter, strings.Repeat("x", 8))
}

func cloneTestHeader(src http.Header) http.Header {
	if src == nil {
		return nil
	}
	dst := make(http.Header, len(src))
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
	return dst
}

func cloneTestBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}

type trustedSinkExecutor struct {
	// trust controls RequestTerminatedError.Trusted (true) vs zero-value (false).
	trust bool
	// streamErr returns a termination error from ExecuteStream (stream-peek path).
	streamErr bool
	// streamThenErr emits one payload chunk, then a terminal untrusted chunk error.
	streamThenErr bool
}

func (e *trustedSinkExecutor) Identifier() string { return "trusted-sink" }

func (e *trustedSinkExecutor) terminatedError() *coreexecutor.RequestTerminatedError {
	return &coreexecutor.RequestTerminatedError{
		HTTPStatus: http.StatusTooManyRequests,
		Header:     http.Header{"X-Plugin-Response": {"true"}, "Retry-After": {"17"}},
		Body:       []byte(`{"error":{"message":"plugin direct ` + trustedSinkSecret + `"}}`),
		Trusted:    e.trust,
	}
}

func (e *trustedSinkExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.terminatedError()
}

func (e *trustedSinkExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	switch {
	case e.streamErr:
		return nil, e.terminatedError()
	case e.streamThenErr:
		chunks := make(chan coreexecutor.StreamChunk, 2)
		chunks <- coreexecutor.StreamChunk{Payload: []byte("event: data\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")}
		chunks <- coreexecutor.StreamChunk{Err: &coreexecutor.RequestTerminatedError{
			HTTPStatus: http.StatusBadGateway,
			Body:       []byte(`{"error":{"message":"leaked ` + trustedSinkSecret + `"}}`),
		}}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	default:
		chunks := make(chan coreexecutor.StreamChunk)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
}

func (e *trustedSinkExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *trustedSinkExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, &coreexecutor.RequestTerminatedError{HTTPStatus: http.StatusTooManyRequests}
}

func (e *trustedSinkExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, &coreauth.Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

type trustedSinkInterceptHost struct {
	interceptBefore func(context.Context, pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse
}

func (h *trustedSinkInterceptHost) InterceptRequestBeforeAuth(ctx context.Context, req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
	if h != nil && h.interceptBefore != nil {
		return h.interceptBefore(ctx, req)
	}
	return pluginapi.RequestInterceptResponse{Headers: cloneTestHeader(req.Headers), Body: cloneTestBytes(req.Body)}
}

func (h *trustedSinkInterceptHost) InterceptRequestAfterAuth(ctx context.Context, req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
	return pluginapi.RequestInterceptResponse{Headers: cloneTestHeader(req.Headers), Body: cloneTestBytes(req.Body)}
}

func (h *trustedSinkInterceptHost) InterceptResponse(ctx context.Context, req pluginapi.ResponseInterceptRequest) pluginapi.ResponseInterceptResponse {
	return pluginapi.ResponseInterceptResponse{Headers: cloneTestHeader(req.ResponseHeaders), Body: cloneTestBytes(req.Body)}
}

func (h *trustedSinkInterceptHost) InterceptStreamChunk(ctx context.Context, req pluginapi.StreamChunkInterceptRequest) pluginapi.StreamChunkInterceptResponse {
	return pluginapi.StreamChunkInterceptResponse{Headers: cloneTestHeader(req.ResponseHeaders), Body: cloneTestBytes(req.Body)}
}

func newTrustedSinkHandler(t *testing.T, executor *trustedSinkExecutor) *handlers.BaseAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := nextTrustedSinkAuthID()
	auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("manager.Register(): %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, auth.Provider, []*registry.ModelInfo{
		{ID: trustedSinkModel},
		{ID: "grok-imagine-image"},
		{ID: "grok-imagine-video"},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})
	return handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, manager)
}

func postToSink(t *testing.T, handler gin.HandlerFunc, path, body string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST(path, handler)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for key, values := range header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

// TestTrustedDirectResponsePreservedAcrossOpenAISinks drives a trusted local
// plugin/interceptor termination (DirectResponse && TrustedDirectResponse) through
// the real pre-output sinks for chat/completions, Responses, Images, and Videos.
// Each sink must reach writeDirectErrorResponse verbatim: exact trusted status,
// body, and safe plugin headers; the generic sanitized envelope must not replace
// the body.
func TestTrustedDirectResponsePreservedAcrossOpenAISinks(t *testing.T) {
	cases := []struct {
		name   string
		build  func(t *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc
		path   string
		body   string
		header http.Header
	}{
		{
			name: "chat-completions",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).ChatCompletions
			},
			path: "/v1/chat/completions",
			body: `{"model":"` + trustedSinkModel + `","messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "completions",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).Completions
			},
			path: "/v1/completions",
			body: `{"model":"` + trustedSinkModel + `","prompt":"hi"}`,
		},
		{
			name: "responses",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIResponsesAPIHandler(base).Responses
			},
			path: "/v1/responses",
			body: `{"model":"` + trustedSinkModel + `","input":"hi"}`,
		},
		{
			name: "images-generations",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).ImagesGenerations
			},
			path: "/v1/images/generations",
			body: `{"model":"grok-imagine-image","prompt":"draw a square","response_format":"b64_json"}`,
		},
		{
			name: "videos-create",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).VideosCreate
			},
			path: "/v1/videos",
			body: `{"model":"grok-imagine-video","prompt":"make it move"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := &trustedSinkExecutor{trust: true}
			base := newTrustedSinkHandler(t, executor)
			base.SetPluginHost(&trustedSinkInterceptHost{
				interceptBefore: func(_ context.Context, req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
					return pluginapi.RequestInterceptResponse{
						Terminate:       true,
						StatusCode:      http.StatusTooManyRequests,
						ResponseHeaders: http.Header{"X-Plugin-Response": {"true"}, "Retry-After": {"17"}},
						ResponseBody:    []byte(`{"error":{"message":"plugin direct ` + trustedSinkSecret + `"}}`),
					}
				},
			})
			recorder := postToSink(t, tc.build(t, base), tc.path, tc.body, tc.header)

			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Body.String(); got != `{"error":{"message":"plugin direct `+trustedSinkSecret+`"}}` {
				t.Fatalf("body replaced by sanitized envelope: %q", got)
			}
			if recorder.Header().Get("Retry-After") != "17" || recorder.Header().Get("X-Plugin-Response") != "true" {
				t.Fatalf("headers = %v, want safe plugin headers", recorder.Header())
			}
		})
	}
}

// TestChatStreamPeekPreservesTrustedDirectResponse drives a trusted plugin
// termination through the chat streaming-initial (peek) sink via
// ExecuteStreamWithAuthManager, proving the pre-output path preserves status,
// body, and safe headers before any SSE frame is committed.
func TestChatStreamPeekPreservesTrustedDirectResponse(t *testing.T) {
	executor := &trustedSinkExecutor{trust: true, streamErr: true}
	base := newTrustedSinkHandler(t, executor)
	base.SetPluginHost(&trustedSinkInterceptHost{
		interceptBefore: func(_ context.Context, req pluginapi.RequestInterceptRequest) pluginapi.RequestInterceptResponse {
			return pluginapi.RequestInterceptResponse{
				Terminate:       true,
				StatusCode:      http.StatusTooManyRequests,
				ResponseHeaders: http.Header{"Retry-After": {"17"}, "X-Plugin-Response": {"true"}},
				ResponseBody:    []byte(`{"error":{"message":"plugin direct ` + trustedSinkSecret + `"}}`),
			}
		},
	})
	recorder := postToSink(t, NewOpenAIAPIHandler(base).ChatCompletions, "/v1/chat/completions",
		`{"model":"`+trustedSinkModel+`","messages":[{"role":"user","content":"hi"}],"stream":true}`, nil)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Body.String(); got != `{"error":{"message":"plugin direct `+trustedSinkSecret+`"}}` {
		t.Fatalf("stream-peek body replaced by sanitized envelope: %q", got)
	}
	if recorder.Header().Get("Retry-After") != "17" || recorder.Header().Get("X-Plugin-Response") != "true" {
		t.Fatalf("stream-peek headers = %v, want safe plugin headers", recorder.Header())
	}
	if strings.Contains(recorder.Body.String(), "text/event-stream") {
		t.Fatalf("stream-peek wrote SSE headers for a direct JSON error: %q", recorder.Body.String())
	}
}

// TestUntrustedTerminationStrippedAcrossOpenAISinks proves the same real sinks
// strip an untrusted upstream RequestTerminatedError: the raw Body/headers never
// reach the client, the generic sanitized envelope replaces the body, and the
// secret is absent from the output.
func TestUntrustedTerminationStrippedAcrossOpenAISinks(t *testing.T) {
	cases := []struct {
		name   string
		build  func(t *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc
		path   string
		body   string
		header http.Header
	}{
		{
			name: "chat-completions",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).ChatCompletions
			},
			path: "/v1/chat/completions",
			body: `{"model":"` + trustedSinkModel + `","messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "responses",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIResponsesAPIHandler(base).Responses
			},
			path: "/v1/responses",
			body: `{"model":"` + trustedSinkModel + `","input":"hi"}`,
		},
		{
			name: "images-generations",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).ImagesGenerations
			},
			path: "/v1/images/generations",
			body: `{"model":"grok-imagine-image","prompt":"draw a square"}`,
		},
		{
			name: "videos-create",
			build: func(_ *testing.T, base *handlers.BaseAPIHandler) gin.HandlerFunc {
				return NewOpenAIAPIHandler(base).VideosCreate
			},
			path: "/v1/videos",
			body: `{"model":"grok-imagine-video","prompt":"make it move"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No plugin host: the executor's untrusted RequestTerminatedError is
			// converted by executionErrorMessage (DirectResponse=true,
			// TrustedDirectResponse=false) and must be sanitized by the sink.
			executor := &trustedSinkExecutor{trust: false}
			base := newTrustedSinkHandler(t, executor)
			recorder := postToSink(t, tc.build(t, base), tc.path, tc.body, tc.header)

			if recorder.Code == http.StatusTooManyRequests && recorder.Body.String() == `{"error":{"message":"plugin direct `+trustedSinkSecret+`"}}` {
				t.Fatalf("untrusted direct body/status preserved verbatim: status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Retry-After") != "" || recorder.Header().Get("X-Plugin-Response") != "" {
				t.Fatalf("untrusted plugin headers leaked: %v", recorder.Header())
			}
			if strings.Contains(recorder.Body.String(), trustedSinkSecret) {
				t.Fatalf("untrusted body leaked secret: %q", recorder.Body.String())
			}
			if recorder.Body.String() == `{"error":{"message":"plugin direct `+trustedSinkSecret+`"}}` {
				t.Fatalf("untrusted raw body echoed to client: %q", recorder.Body.String())
			}
		})
	}
}

// TestResponsesHandlerPostFrameTerminalErrorStaysStrict proves the post-frame
// (after streaming began) terminal error path in forwardResponsesStream remains
// strict even when the terminal error originates from a RequestTerminatedError:
// the secret is redacted and the frame is an SSE terminal event, not a raw body.
func TestResponsesHandlerPostFrameTerminalErrorStaysStrict(t *testing.T) {
	executor := &trustedSinkExecutor{streamThenErr: true}
	base := newTrustedSinkHandler(t, executor)
	recorder := postToSink(t, NewOpenAIResponsesAPIHandler(base).Responses, "/v1/responses",
		`{"model":"`+trustedSinkModel+`","input":"hi","stream":true}`,
		http.Header{"User-Agent": {"Codex Desktop/26.803.41515"}})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after stream start; body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_text.delta") {
		t.Fatalf("handler did not preserve the first frame: %q", body)
	}
	if strings.Contains(body, trustedSinkSecret) {
		t.Fatalf("post-frame terminal leaked secret: %q", body)
	}
	if strings.Contains(body, `{"error":{"message":"leaked`) {
		t.Fatalf("post-frame terminal wrote raw error body: %q", body)
	}
	if !strings.Contains(body, "event: response.failed") {
		t.Fatalf("post-frame terminal missing SSE failure event: %q", body)
	}
}
