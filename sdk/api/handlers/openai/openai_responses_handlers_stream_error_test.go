package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

const (
	prematureResponsesStreamModel       = "premature-responses-stream-model"
	initialFailureResponsesModel        = "initial-failure-responses-stream-model"
	emptyResponsesStreamModel           = "empty-responses-stream-model"
	incompleteFirstFrameResponsesModel  = "incomplete-first-frame-responses-model"
	dataOnlyFirstFrameResponsesModel    = "data-only-first-frame-responses-model"
	dataOnlyCleanCloseResponsesModel    = "data-only-clean-close-responses-model"
	sensitiveInitialErrorResponsesModel = "sensitive-initial-error-responses-model"
	directInitialErrorResponsesModel    = "direct-initial-error-responses-model"
	crossChunkMultilineResponsesModel   = "cross-chunk-multiline-responses-model"
	validThenMalformedResponsesModel    = "valid-then-malformed-responses-model"
)

type prematureResponsesStreamExecutor struct{}

func (*prematureResponsesStreamExecutor) Identifier() string { return "premature-responses-stream" }

func (*prematureResponsesStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*prematureResponsesStreamExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	if req.Model == directInitialErrorResponsesModel {
		return nil, &coreexecutor.RequestTerminatedError{
			HTTPStatus: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"17"}, "X-Plugin-Response": []string{"true"}},
			Body:       []byte(`{"error":{"message":"plugin direct response"}}`),
			Trusted:    true,
		}
	}
	chunks := make(chan coreexecutor.StreamChunk, 2)
	if req.Model == validThenMalformedResponsesModel {
		chunks <- coreexecutor.StreamChunk{Payload: []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"\n\n")}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == crossChunkMultilineResponsesModel {
		chunks <- coreexecutor.StreamChunk{Payload: []byte("event: response.completed\ndata: {\"type\":\"response.completed\",")}
		chunks <- coreexecutor.StreamChunk{Payload: []byte("data: \"response\":{\"id\":\"resp-1\",\"status\":\"completed\"}}\n\n")}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == sensitiveInitialErrorResponsesModel {
		chunks <- coreexecutor.StreamChunk{Err: errors.New(`{"error":{"type":"server_error","code":"upstream_failed","message":"initial upstream failure: {\"api_key\":\"initial-message-secret\"}"},"debug":{"token":"initial-debug-secret","trace":"` + strings.Repeat("x", 8192) + `"}}`)}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == dataOnlyFirstFrameResponsesModel || req.Model == dataOnlyCleanCloseResponsesModel {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"partial"}`)}
		if req.Model == dataOnlyFirstFrameResponsesModel {
			chunks <- coreexecutor.StreamChunk{Err: errors.New("upstream failed after data-only frame")}
		}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == incompleteFirstFrameResponsesModel {
		chunks <- coreexecutor.StreamChunk{Payload: []byte("event: response.created")}
		chunks <- coreexecutor.StreamChunk{Err: errors.New("upstream failed before first complete frame")}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == emptyResponsesStreamModel {
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	if req.Model == initialFailureResponsesModel {
		chunks <- coreexecutor.StreamChunk{Err: errors.New("upstream failed before first payload")}
		close(chunks)
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks <- coreexecutor.StreamChunk{Payload: []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")}
	chunks <- coreexecutor.StreamChunk{Err: errors.New("unexpected EOF")}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*prematureResponsesStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*prematureResponsesStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*prematureResponsesStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestResponsesHandlerEmitsFailureWhenExecutorStopsAfterPartialOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "premature-responses-stream-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: prematureResponsesStreamModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"premature-responses-stream-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after stream start; body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "event: response.failed") {
		t.Fatalf("handler did not preserve partial output and terminal failure: %q", body)
	}
	if !strings.Contains(body, "unexpected EOF") {
		t.Fatalf("handler terminal failure lost executor error: %q", body)
	}
}

func TestSanitizeResponsesStreamErrorMessageNormalizesSuccessStatus(t *testing.T) {
	got := sanitizeResponsesStreamErrorMessage(&interfaces.ErrorMessage{StatusCode: http.StatusOK, Error: errors.New("upstream failed")})
	if got == nil || got.StatusCode != http.StatusInternalServerError {
		t.Fatalf("sanitized status = %#v, want %d", got, http.StatusInternalServerError)
	}
}

func TestRedactResponsesStreamErrorTextSensitiveValues(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			name: "plain JSON",
			text: `{"api_key":"plain-secret"}`,
			want: `{"api_key":"[REDACTED]"}`,
		},
		{
			name: "escaped JSON",
			text: `{\"api_key\":\"escaped-secret\"}`,
			want: `{\"api_key\":\"[REDACTED]\"}`,
		},
		{
			name: "double-escaped JSON",
			text: `{\\\"api_key\\\":\\\"double-escaped-secret\\\"}`,
			want: `{\\\"api_key\\\":\\\"[REDACTED]\\\"}`,
		},
		{
			name: "equals separator",
			text: "api_" + "key=" + "equals-value",
			want: `api_key=[REDACTED]`,
		},
		{
			name: "similar keys remain",
			text: `not_api_key=keep api_key_hint=keep token_count=2 tokenizer=value secretariat=value`,
			want: `not_api_key=keep api_key_hint=keep token_count=2 tokenizer=value secretariat=value`,
		},
		{
			name: "client secret",
			text: `client_secret=cs-value`,
			want: `client_secret=[REDACTED]`,
		},
		{
			name: "api token",
			text: `api_token=at-value`,
			want: `api_token=[REDACTED]`,
		},
		{
			name: "refresh token",
			text: `refresh_token=rt-value`,
			want: `refresh_token=[REDACTED]`,
		},
		{
			name: "compound secret",
			text: `foo_bar_secret=fbs-value`,
			want: `foo_bar_secret=[REDACTED]`,
		},
		{
			name: "hyphen token key",
			text: `api-token=ht-value`,
			want: `api-token=[REDACTED]`,
		},
		{
			name: "hyphen compound secret",
			text: `foo-bar-secret=fbs-hyphen`,
			want: `foo-bar-secret=[REDACTED]`,
		},
		{
			name: "standalone basic outside auth key",
			text: `x_authorization: Basic dGVzdDox`,
			want: `x_authorization: Basic [REDACTED]`,
		},
		{
			name: "benign suffix keys unchanged",
			text: `not_api_key=keep token_count=2 tokenizer=value secretariat=value mytoken=keep`,
			want: `not_api_key=keep token_count=2 tokenizer=value secretariat=value mytoken=keep`,
		},
		{
			name: "bearer scheme preserved",
			text: `Authorization: Bearer abcd.efgh`,
			want: `Authorization: Bearer [REDACTED]`,
		},
		{
			name: "basic scheme preserved",
			text: `Authorization: Basic Zm9vOmJhcg==`,
			want: `Authorization: Basic [REDACTED]`,
		},
		{
			name: "escaped-quoted bearer value",
			text: `{\"authorization\":\"Bearer abcd.efgh\"}`,
			want: `{\"authorization\":\"Bearer [REDACTED]\"}`,
		},
		{
			name: "escaped tail does not leak",
			text: `token=val\"tail`,
			want: `token=[REDACTED]`,
		},
		{
			name: "escaped tail in quoted value",
			text: `{"token":"val\"tail` + `"}`,
			want: `{"token":"[REDACTED]"}`,
		},
		{
			name: "compound secret escaped quoted",
			text: `{\"client_secret\":\"cs-value\"}`,
			want: `{\"client_secret\":\"[REDACTED]\"}`,
		},
		{
			name: "api token json",
			text: `{"api_token":"at-value"}`,
			want: `{"api_token":"[REDACTED]"}`,
		},
		{
			name: "refresh token equals",
			text: `refresh_token=rt-value`,
			want: `refresh_token=[REDACTED]`,
		},
		{
			name: "mytoken unchanged json",
			text: `{"mytoken":"keep"}`,
			want: `{"mytoken":"keep"}`,
		},
		{
			name: "authorization key json basic",
			text: `{"authorization":"Basic dGVzdDox"}`,
			want: `{"authorization":"Basic [REDACTED]"}`,
		},
		{
			name: "compound secret no over-redaction",
			text: `not_api_key=keep foo_bar_secret=fbs token_count=1`,
			want: `not_api_key=keep foo_bar_secret=[REDACTED] token_count=1`,
		},
		{
			name: "client secret json escaped",
			text: `{\"client_secret\":\"cs\"}`,
			want: `{\"client_secret\":\"[REDACTED]\"}`,
		},
		{
			name: "api token json escaped",
			text: `{\"api_token\":\"at\"}`,
			want: `{\"api_token\":\"[REDACTED]\"}`,
		},
		{
			name: "F1 openai api key underscore",
			text: `openai_api_key=oak-1`,
			want: `openai_api_key=[REDACTED]`,
		},
		{
			name: "F1 uppercase openai api key",
			text: `OPENAI_API_KEY=oak-2`,
			want: `OPENAI_API_KEY=[REDACTED]`,
		},
		{
			name: "F1 anthropic api key",
			text: `anthropic_api_key=ak-1`,
			want: `anthropic_api_key=[REDACTED]`,
		},
		{
			name: "F1 github api key",
			text: `github_api_key=gh-1`,
			want: `github_api_key=[REDACTED]`,
		},
		{
			name: "F1 stripe api key",
			text: `stripe_api_key=sk-1`,
			want: `stripe_api_key=[REDACTED]`,
		},
		{
			name: "F1 x api key",
			text: `x_api_key=x-1`,
			want: `x_api_key=[REDACTED]`,
		},
		{
			name: "F1 foo bar key",
			text: `foo_bar_key=fbk-1`,
			want: `foo_bar_key=[REDACTED]`,
		},
		{
			name: "F1 my access key",
			text: `my_access_key=mak-1`,
			want: `my_access_key=[REDACTED]`,
		},
		{
			name: "F1 my api key",
			text: `my_api_key=my-1`,
			want: `my_api_key=[REDACTED]`,
		},
		{
			name: "F1 hyphen api key",
			text: `github-api-key=ghk-1`,
			want: `github-api-key=[REDACTED]`,
		},
		{
			name: "F1 not api key benign",
			text: `not_api_key=keep`,
			want: `not_api_key=keep`,
		},
		{
			name: "F1 token count benign",
			text: `token_count=5`,
			want: `token_count=5`,
		},
		{
			name: "F1 tokenizer benign",
			text: `tokenizer=value`,
			want: `tokenizer=value`,
		},
		{
			name: "F1 secretariat benign",
			text: `secretariat=value`,
			want: `secretariat=value`,
		},
		{
			name: "F1 mytoken benign",
			text: `mytoken=value`,
			want: `mytoken=value`,
		},
		{
			name: "F2 digest auth fully redacted",
			text: `Authorization: Digest username="u", realm="r", nonce="n", uri="/x", response="deadbeef"`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "F2 aws sig auth fully redacted",
			text: `Authorization: AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "F2 oauth auth fully redacted",
			text: `Authorization: OAuth oauth_consumer_key="ck", oauth_signature="sig"`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "F2 custom scheme auth fully redacted",
			text: `Authorization: X-Custom abcdef`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "F2 opaque auth value fully redacted",
			text: `Authorization: ABCDEFGHIJKLMNOP`,
			want: `Authorization: [REDACTED]`,
		},
		{
			name: "F2 digest auth quoted",
			text: `{"authorization":"Digest username=\"u\", realm=\"r\", nonce=\"n\", uri=\"/x\", response=\"deadbeef\""}`,
			want: `{"authorization":"[REDACTED]"}`,
		},
		{
			name: "F2 digest auth escaped quoted",
			text: `{\"authorization\":\"Digest username=\\\"u\\\", realm=\\\"r\\\", nonce=\\\"n\\\", uri=\\\"/x\\\", response=\\\"deadbeef\\\"\"}`,
			want: `{\"authorization\":\"[REDACTED]\"}`,
		},
		{
			name: "F2 digest auth double escaped",
			text: `{\\\"authorization\\\":\\\"Digest username=\\\\\\\"u\\\\\\\", realm=\\\\\\\"r\\\\\\\", nonce=\\\\\\\"n\\\\\\\", uri=\\\\\\\"/x\\\\\\\", response=\\\\\\\"deadbeef\\\\\\\"\\\"}`,
			want: `{\\\"authorization\\\":\\\"[REDACTED]\\\"}`,
		},
		{
			name: "F3 x-api-key bearer",
			text: `x-api-key: Bearer abc123`,
			want: `x-api-key: Bearer [REDACTED]`,
		},
		{
			name: "F3 x-api-key bare value",
			text: `x-api-key: abc123`,
			want: `x-api-key: [REDACTED]`,
		},
		{
			name: "F3 x-api-key bearer no residual",
			text: `x-api-key: Bearer abc123 def456`,
			want: `x-api-key: Bearer [REDACTED]`,
		},
		{
			name: "F4 rate limit token prose",
			text: `the rate limit token: expired`,
			want: `the rate limit token: expired`,
		},
		{
			name: "F4 secret is out prose",
			text: `the secret: is out`,
			want: `the secret: is out`,
		},
		{
			name: "F4 count token benign",
			text: `count_token=5`,
			want: `count_token=5`,
		},
		{
			name: "F4 token json still redacts",
			text: `{"token":"tok-1"}`,
			want: `{"token":"[REDACTED]"}`,
		},
		{
			name: "F4 token equals still redacts",
			text: `token=tok-2`,
			want: `token=[REDACTED]`,
		},
		{
			name: "F4 secret json still redacts",
			text: `{"secret":"sec-1"}`,
			want: `{"secret":"[REDACTED]"}`,
		},
		{
			name: "F5 double escaped bearer",
			text: `{\\\"authorization\\\":\\\"Bearer abc123\\\"}`,
			want: `{\\\"authorization\\\":\\\"Bearer [REDACTED]\\\"}`,
		},
		{
			name: "F5 double escaped basic",
			text: `{\\\"authorization\\\":\\\"Basic dGVzdDox\\\"}`,
			want: `{\\\"authorization\\\":\\\"Basic [REDACTED]\\\"}`,
		},
		{
			name: "v4 aws access key id",
			text: `aws_access_key_id=AKIAIOSFODNN7EXAMPLE`,
			want: `aws_access_key_id=[REDACTED]`,
		},
		{
			name: "v4 uppercase aws access key id",
			text: `AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE`,
			want: `AWS_ACCESS_KEY_ID=[REDACTED]`,
		},
		{
			name: "v4 api key id",
			text: `api_key_id=kid-123`,
			want: `api_key_id=[REDACTED]`,
		},
		{
			name: "v4 access key id",
			text: `access_key_id=aki-456`,
			want: `access_key_id=[REDACTED]`,
		},
		{
			name: "v4 aws credential",
			text: `aws_credential=aws-cred-1`,
			want: `aws_credential=[REDACTED]`,
		},
		{
			name: "v4 compound key id",
			text: `foo_bar_key_id=fkid-1`,
			want: `foo_bar_key_id=[REDACTED]`,
		},
		{
			name: "v4 vendor credential json",
			text: `{"vendor_credential":"vc-1"}`,
			want: `{"vendor_credential":"[REDACTED]"}`,
		},
		{
			name: "v4 benign key count",
			text: `key_count=3 access_key_ids_total=5`,
			want: `key_count=3 access_key_ids_total=5`,
		},
		{
			name: "v4 token count benign",
			text: `token_count=5`,
			want: `token_count=5`,
		},
		{
			name: "v4 single quoted token",
			text: `{'token':'tok-single'}`,
			want: `{'token':'[REDACTED]'}`,
		},
		{
			name: "v4 single quoted api key",
			text: `{'api_key':'sk-single'}`,
			want: `{'api_key':'[REDACTED]'}`,
		},
		{
			name: "v4 single quoted auth bearer",
			text: `{'authorization':'Bearer abc-xyz'}`,
			want: `{'authorization':'Bearer [REDACTED]'}`,
		},
		{
			name: "v4 space api key assignment",
			text: `api key=sp-key-1`,
			want: `api key=[REDACTED]`,
		},
		{
			name: "v4 space api key colon",
			text: `api key: sp-key-2`,
			want: `api key: [REDACTED]`,
		},
		{
			name: "v4 space api key json",
			text: `{"api key":"sp-key-3"}`,
			want: `{"api key":"[REDACTED]"}`,
		},
		{
			name: "v4 apikey redacts",
			text: `apikey=ak-1`,
			want: `apikey=[REDACTED]`,
		},
		{
			name: "v4 apikey bearer",
			text: `apikey: Bearer tok-1`,
			want: `apikey: Bearer [REDACTED]`,
		},
		{
			name: "v4 double escaped aws key",
			text: `{\\\"aws_access_key_id\\\":\\\"AKIAIOSFODNN7EXAMPLE\\\"}`,
			want: `{\\\"aws_access_key_id\\\":\\\"[REDACTED]\\\"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactResponsesStreamErrorText(tc.text); got != tc.want {
				t.Fatalf("redactResponsesStreamErrorText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedactResponsesStreamErrorTextCamelCase(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			name: "camelCase refreshToken assignment",
			text: "refreshToken=abc",
			want: "refreshToken=[REDACTED]",
		},
		{
			name: "camelCase clientSecret colon",
			text: "clientSecret: xyz",
			want: "clientSecret: [REDACTED]",
		},
		{
			name: "camelCase apiKey assignment",
			text: "apiKey=secret123",
			want: "apiKey=[REDACTED]",
		},
		{
			name: "camelCase accessToken colon",
			text: "accessToken: tok456",
			want: "accessToken: [REDACTED]",
		},
		{
			name: "non-sensitive camelCase words untouched",
			text: "userProfile=safe statusCode: 200 maxRetries=3 donkey=safe",
			want: "userProfile=safe statusCode: 200 maxRetries=3 donkey=safe",
		},
		{
			name: "acronym-prefixed IDToken assignment",
			text: "IDToken=abc",
			want: "IDToken=[REDACTED]",
		},
		{
			name: "acronym-prefixed JWTToken colon",
			text: "JWTToken: xyz",
			want: "JWTToken: [REDACTED]",
		},
		{
			name: "acronym-prefixed XApiKey assignment",
			text: "XApiKey=secret",
			want: "XApiKey=[REDACTED]",
		},
		{
			name: "acronym-prefixed XAPIKey assignment",
			text: "XAPIKey=secret",
			want: "XAPIKey=[REDACTED]",
		},
		{
			name: "acronym-prefixed OAuthToken assignment",
			text: "OAuthToken=secret",
			want: "OAuthToken=[REDACTED]",
		},
		{
			name: "Keyboard assignment untouched",
			text: "Keyboard=mechanical",
			want: "Keyboard=mechanical",
		},
		{
			name: "prose mentioning API key untouched",
			text: "the API key rotated recently",
			want: "the API key rotated recently",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactResponsesStreamErrorText(tc.text); got != tc.want {
				t.Fatalf("redactResponsesStreamErrorText() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRedactResponsesStreamErrorTextNoPanicOnTrailingBackslash(t *testing.T) {
	inputs := []string{
		`token=abc\`,
		`token=abc\def`,
		`api_key=sk-1234\`,
		`Authorization: Bearer tok\`,
		`Authorization: Bearer tok\\`,
		`Authorization: Basic dGVzdDox\`,
		`Authorization: Digest username="u\", realm="r"\`,
		`api_key="sk-\`,
		`api key: sk-1\`,
		`{'api_key':'sk-\`,
		`aws_access_key_id=AKIA\`,
		`token="val\"tail\`,
	}
	for _, in := range inputs {
		if got := redactResponsesStreamErrorText(in); got == "" {
			t.Fatalf("redactResponsesStreamErrorText(%q) returned empty", in)
		}
	}
	// Large/bounded input must not panic and must stay bounded.
	big := "token=" + strings.Repeat("x", 1<<16) + `\`
	got := redactResponsesStreamErrorText(big)
	if len(got) > len(big)+64 {
		t.Fatalf("large input grew unexpectedly: len=%d", len(got))
	}
}

func TestSanitizeOpenAIErrorMessageStrictTrustBoundary(t *testing.T) {
	const secret = "fixture-body-secret"
	errMsg := &interfaces.ErrorMessage{
		StatusCode:     http.StatusBadGateway,
		Error:          errors.New(`provider failed: {"api_key":"` + secret + `"}`),
		DirectResponse: true,
		Body:           []byte(`{"raw":"` + secret + `"}`),
	}
	got := sanitizeOpenAIErrorMessage(errMsg)
	if got == nil {
		t.Fatal("sanitizeOpenAIErrorMessage returned nil")
	}
	if got.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want %d", got.StatusCode, http.StatusBadGateway)
	}
	if got.DirectResponse {
		t.Fatal("DirectResponse must be cleared")
	}
	if got.Body != nil {
		t.Fatalf("Body must be nil, got %q", got.Body)
	}
	if strings.Contains(got.Error.Error(), secret) {
		t.Fatalf("sanitized error leaked %q: %q", secret, got.Error.Error())
	}
	if sanitizeOpenAIErrorMessage(nil) != nil {
		t.Fatal("sanitizeOpenAIErrorMessage(nil) must be nil")
	}
}

func TestSanitizeResponsesStreamErrorMessageRedactsNestedJSONWithRouteSummary(t *testing.T) {
	const secret = "fixture-api-key"
	rawError := `{"error":{"type":"server_error","code":"upstream_failed","message":"provider failed: {\"api_key\":\"` + secret + `\"}"}}; attempted routes: [openai:401, codex:429]`

	got := sanitizeResponsesStreamErrorMessage(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New(rawError)})
	if got == nil || got.StatusCode != http.StatusBadGateway || got.Error == nil {
		t.Fatalf("sanitized error = %#v, want status %d and error", got, http.StatusBadGateway)
	}
	text := got.Error.Error()
	if strings.Contains(text, secret) {
		t.Fatalf("sanitized error leaked sensitive value: %q", text)
	}
	for _, want := range []string{`\"api_key\":\"[REDACTED]\"`, "attempted routes: [openai:401, codex:429]"} {
		if !strings.Contains(text, want) {
			t.Fatalf("sanitized error lost %q: %q", want, text)
		}
	}
}

func TestSanitizeResponsesStreamErrorMessageRedactsNestedCompoundAndAuth(t *testing.T) {
	cases := []struct {
		name    string
		secret  string
		raw     string
		wantSub string
	}{
		{
			name:    "compound secret",
			secret:  "fixture-compound-secret",
			raw:     `provider failed: {\"client_secret\":\"` + "fixture-compound-secret" + `\"}`,
			wantSub: `\"client_secret\":\"[REDACTED]\"`,
		},
		{
			name:    "api token",
			secret:  "fixture-api-token",
			raw:     `provider failed: {\"api_token\":\"` + "fixture-api-token" + `\"}`,
			wantSub: `\"api_token\":\"[REDACTED]\"`,
		},
		{
			name:    "basic auth scheme",
			secret:  "Zm9vOmJhcg==",
			raw:     `provider failed: {"authorization":"Basic Zm9vOmJhcg=="}`,
			wantSub: `{"authorization":"Basic [REDACTED]"}`,
		},
		{
			name:    "bearer auth scheme",
			secret:  "abcd.efgh.ijkl",
			raw:     `provider failed: {"authorization":"Bearer abcd.efgh.ijkl"}`,
			wantSub: `{"authorization":"Bearer [REDACTED]"}`,
		},
		{
			name:    "digest auth fully redacted",
			secret:  "deadbeefdigest",
			raw:     `provider failed: Authorization: Digest username="u", realm="r", nonce="n", uri="/x", response="deadbeefdigest"`,
			wantSub: `Authorization: [REDACTED]`,
		},
		{
			name:    "aws sig auth fully redacted",
			secret:  "AKIDEXAMPLE",
			raw:     `provider failed: Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef`,
			wantSub: `Authorization: [REDACTED]`,
		},
		{
			name:    "oauth auth fully redacted",
			secret:  "oauth-sig-123",
			raw:     `provider failed: Authorization: OAuth oauth_consumer_key="ck", oauth_signature="oauth-sig-123"`,
			wantSub: `Authorization: [REDACTED]`,
		},
		{
			name:    "double escaped bearer",
			secret:  "abc123",
			raw:     `provider failed: {\\\"authorization\\\":\\\"Bearer abc123\\\"}`,
			wantSub: `\\\"authorization\\\":\\\"Bearer [REDACTED]\\\"`,
		},
		{
			name:    "double escaped basic",
			secret:  "dGVzdDox",
			raw:     `provider failed: {\\\"authorization\\\":\\\"Basic dGVzdDox\\\"}`,
			wantSub: `\\\"authorization\\\":\\\"Basic [REDACTED]\\\"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"error":{"type":"server_error","code":"upstream_failed","message":"` + tc.raw + `"}}; attempted routes: [openai:401]`
			got := sanitizeResponsesStreamErrorMessage(&interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New(raw)})
			if got == nil || got.StatusCode != http.StatusBadGateway || got.Error == nil {
				t.Fatalf("sanitized error = %#v, want status %d and error", got, http.StatusBadGateway)
			}
			text := got.Error.Error()
			if strings.Contains(text, tc.secret) {
				t.Fatalf("sanitized error leaked %q: %q", tc.secret, text)
			}
			if !strings.Contains(text, tc.wantSub) {
				t.Fatalf("sanitized error lost %q: %q", tc.wantSub, text)
			}
		})
	}
}

// TestSinksForOpenAIResponseError asserts the strict shared sanitizer is applied
// to the non-stream trust-boundary sinks: the client error body and the captured
// request log must never carry a raw upstream secret, DirectResponse must be
// disabled, and Body must be nil. It drives WriteErrorResponse via a real
// handler so the client/log plumbing is exercised end to end.
func TestSinksForOpenAIResponseError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const secret = "fixture-sink-secret"
	rawSecret := strings.Repeat(secret, 2)

	for name, setup := range map[string]func(h *OpenAIAPIHandler, c *gin.Context){
		"chat non-stream": func(h *OpenAIAPIHandler, c *gin.Context) {
			errMsg := &interfaces.ErrorMessage{
				StatusCode:     http.StatusBadGateway,
				Error:          errors.New(`upstream provider failed: {"api_key":"` + rawSecret + `"}`),
				DirectResponse: false,
				Body:           []byte(`{"raw":"` + rawSecret + `"}`),
			}
			h.WriteErrorResponse(c, sanitizeOpenAIErrorMessage(errMsg))
		},
		"routed upstream direct": func(h *OpenAIAPIHandler, c *gin.Context) {
			errMsg := &interfaces.ErrorMessage{
				StatusCode:     http.StatusBadGateway,
				Error:          errors.New(`provider failed: Bearer ` + secret),
				DirectResponse: true,
				Body:           []byte(`{"raw":"` + rawSecret + `"}`),
			}
			h.WriteErrorResponse(c, sanitizeOpenAIErrorMessage(errMsg))
		},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			h := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil))
			setup(h, c)

			body := recorder.Body.String()
			if strings.Contains(body, rawSecret) || strings.Contains(body, secret) {
				t.Fatalf("client body leaked secret: %q", body)
			}
			// The request-log sink must also be sanitized.
			if logged, exists := c.Get("API_RESPONSE"); exists {
				if loggedBytes, ok := logged.([]byte); ok && strings.Contains(string(loggedBytes), secret) {
					t.Fatalf("API_RESPONSE log leaked secret: %q", string(loggedBytes))
				}
			}
			if errors_, exists := c.Get("API_RESPONSE_ERROR"); exists {
				if list, ok := errors_.([]*interfaces.ErrorMessage); ok {
					for _, e := range list {
						if e != nil && e.Error != nil && strings.Contains(e.Error.Error(), secret) {
							t.Fatalf("API_RESPONSE_ERROR log leaked secret: %q", e.Error.Error())
						}
						if e != nil && e.Body != nil {
							t.Fatalf("logged error carried Body: %q", e.Body)
						}
					}
				}
			}
		})
	}
}

// TestStreamingChatTerminalErrorSanitizesRawUpstream exercises the mid-stream
// terminal path used by handleStreamResult (OpenAIAPIHandler chat/completions
// streaming): an upstream error delivered after headers are committed must be
// sanitized by NormalizeTerminalError before WriteTerminalError emits the SSE
// payload, so no raw secret or Body/DirectResponse leaks, and exactly one
// terminal emission happens with a well-formed SSE data frame.
func TestStreamingChatTerminalErrorSanitizesRawUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const secret = "fixture-chat-terminal-secret"
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}
	h := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil))

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{
		StatusCode:     http.StatusBadGateway,
		Error:          errors.New(`provider failed: aws_access_key_id=AKIAEXAMPLE, Authorization: Digest username="u", api key: Bearer ` + secret),
		DirectResponse: true,
		Body:           []byte(`{"secret":"` + secret + `"}`),
	}
	close(errs)

	var terminalErr error
	h.ForwardStream(c, flusher, func(err error) { terminalErr = err }, data, errs, handlers.StreamForwardOptions{
		NormalizeTerminalError: sanitizeOpenAIErrorMessage,
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			status := http.StatusInternalServerError
			if errMsg != nil && errMsg.StatusCode > 0 {
				status = errMsg.StatusCode
			}
			errText := http.StatusText(status)
			if errMsg != nil && errMsg.Error != nil && errMsg.Error.Error() != "" {
				errText = errMsg.Error.Error()
			}
			body := handlers.BuildErrorResponseBody(status, errText)
			_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", string(body))
		},
	})
	_ = terminalErr

	body := recorder.Body.String()
	if strings.Contains(body, secret) || strings.Contains(body, "AKIAEXAMPLE") {
		t.Fatalf("streaming terminal body leaked secret: %q", body)
	}
	if !strings.Contains(body, "[REDACTED]") {
		t.Fatalf("streaming terminal body lacks redaction: %q", body)
	}
	if terminalErr != nil && strings.Contains(terminalErr.Error(), secret) {
		t.Fatalf("cancel() received raw secret: %q", terminalErr.Error())
	}
}

// TestSinksRejectFixtureVariants asserts that sink sanitization fails closed for
// the concrete remediation fixture shapes: trailing backslash, single-quoted
// values, AWS/access/api key variants, Authorization Bearer/Basic/Digest/AWS/
// OAuth/custom, and multiline raw Body. No fixture substring may reach the client
// body or the API_RESPONSE/API_RESPONSE_ERROR logs; Body must be nil and
// DirectResponse false.
func TestSinksRejectFixtureVariants(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const secret = "FIXTUREV4SECRET"
	_ = secret
	fixtures := []string{
		`token=abc\`,
		`api_key=sk-1234\`,
		`{'authorization':'Bearer xyz'}`,
		`aws_access_key_id=AKIAIOSFODNN7EXAMPLE`,
		`api_key_id=kid-1`,
		`Authorization: Basic dGVzdDox`,
		`Authorization: AWS4-HMAC-SHA256 Credential=AKID/x`,
		`Authorization: OAuth oauth_signature="sig"`,
		`Authorization: X-Custom abc`,
		"multi\nline\napi_key=" + "sk-multi",
	}
	for _, raw := range fixtures {
		errMsg := &interfaces.ErrorMessage{
			StatusCode: http.StatusServiceUnavailable,
			Error:      errors.New(raw),
			Body:       []byte(raw),
		}
		got := sanitizeOpenAIErrorMessage(errMsg)
		if got == nil {
			t.Fatalf("sanitizeOpenAIErrorMessage(%q) nil", raw)
		}
		if got.Body != nil {
			t.Fatalf("Body not nil for %q", raw)
		}
		if got.DirectResponse {
			t.Fatalf("DirectResponse not cleared for %q", raw)
		}
	}
}

func TestResponsesHandlerCommitsValidFrameBeforeMalformedFrameInSameChunk(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "valid-then-malformed-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: validThenMalformedResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"valid-then-malformed-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "response.output_text.delta") || !strings.Contains(recorder.Body.String(), "event: response.failed") {
		t.Fatalf("valid then malformed response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestResponsesHandlerAcceptsMultilineDataAcrossExecutorChunks(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "cross-chunk-multiline-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: crossChunkMultilineResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"cross-chunk-multiline-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event: response.completed") {
		t.Fatalf("cross-chunk multiline response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestSanitizeOpenAIErrorMessagePreservesTrustedDirect(t *testing.T) {
	const secret = "fixture-trusted-openai-secret"
	trusted := &interfaces.ErrorMessage{
		StatusCode:            http.StatusTooManyRequests,
		Error:                 errors.New("plugin"),
		DirectResponse:        true,
		TrustedDirectResponse: true,
		Body:                  []byte(`{"raw":"` + secret + `"}`),
		Headers:               http.Header{"X-Plugin": {"yes"}},
	}
	if got := sanitizeOpenAIErrorMessage(trusted); got != trusted {
		t.Fatalf("trusted direct response must be preserved verbatim, got %#v", got)
	}

	untrusted := &interfaces.ErrorMessage{
		StatusCode:     http.StatusBadGateway,
		Error:          errors.New(`provider failed: {"api_key":"` + secret + `"}`),
		DirectResponse: true,
		Body:           []byte(`{"raw":"` + secret + `"}`),
	}
	got := sanitizeOpenAIErrorMessage(untrusted)
	if got == nil || got.DirectResponse || got.Body != nil {
		t.Fatalf("untrusted direct response not strict, got %#v", got)
	}
	if strings.Contains(got.Error.Error(), secret) {
		t.Fatalf("untrusted sanitized error leaked %q: %q", secret, got.Error.Error())
	}
}

func TestSanitizeResponsesErrorMessageUnwrapNil(t *testing.T) {
	rawErr := errors.New("upstream-sensitive-internal-cause")
	errMsg := &interfaces.ErrorMessage{
		StatusCode: http.StatusBadGateway,
		Error:      rawErr,
	}
	sanitized := sanitizeOpenAIErrorMessage(errMsg)
	if sanitized == nil || sanitized.Error == nil {
		t.Fatalf("expected sanitized error, got %#v", sanitized)
	}
	if errors.Unwrap(sanitized.Error) != nil {
		t.Fatalf("sanitized error must return nil on errors.Unwrap, got %v", errors.Unwrap(sanitized.Error))
	}
	if sanitized.StatusCode != http.StatusBadGateway {
		t.Fatalf("status code = %d, want %d", sanitized.StatusCode, http.StatusBadGateway)
	}
}

func TestSanitizeResponsesInitialErrorMessageTrustedSplit(t *testing.T) {
	const secret = "fixture-trusted-secret"
	trusted := &interfaces.ErrorMessage{
		StatusCode:            http.StatusBadGateway,
		Error:                 errors.New("plugin"),
		DirectResponse:        true,
		TrustedDirectResponse: true,
		Body:                  []byte(`{"raw":"` + secret + `"}`),
		Headers:               http.Header{"X-Plugin": {"yes"}},
	}
	if got := sanitizeOpenAIErrorMessage(trusted); got != trusted {
		t.Fatalf("trusted direct response must be preserved verbatim, got %#v", got)
	}

	untrusted := &interfaces.ErrorMessage{
		StatusCode:            http.StatusBadGateway,
		Error:                 errors.New(`{"error":{"type":"server_error","code":"upstream_failed","message":"provider failed: {\"api_key\":\"` + secret + `\"}"}}`),
		DirectResponse:        true,
		TrustedDirectResponse: false,
		Body:                  []byte(`{"raw":"` + secret + `"}`),
	}
	got := sanitizeOpenAIErrorMessage(untrusted)
	if got == nil || got.DirectResponse || got.TrustedDirectResponse || got.Body != nil {
		t.Fatalf("untrusted initial error must be strict, got %#v", got)
	}
	if strings.Contains(got.Error.Error(), secret) {
		t.Fatalf("untrusted initial error leaked %q: %q", secret, got.Error.Error())
	}

	plain := &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("upstream")}
	if got := sanitizeOpenAIErrorMessage(plain); got == nil || got.DirectResponse {
		t.Fatalf("plain initial error must be sanitized, got %#v", got)
	}
}

func TestSanitizeResponsesHandlerPreservesTrustedDirectBeforeFirstFrame(t *testing.T) {
	t.Run("trusted", func(t *testing.T) {
		errMsg := &interfaces.ErrorMessage{
			StatusCode:            http.StatusTooManyRequests,
			Error:                 errors.New("plugin direct response"),
			DirectResponse:        true,
			TrustedDirectResponse: true,
			Body:                  []byte(`{"error":{"message":"plugin direct response"}}`),
			Headers:               http.Header{"Retry-After": {"17"}, "X-Plugin-Response": {"true"}},
		}
		got := sanitizeOpenAIErrorMessage(errMsg)
		if got == nil || !got.DirectResponse || !got.TrustedDirectResponse {
			t.Fatalf("trusted direct response not preserved: %#v", got)
		}
	})
	t.Run("untrusted", func(t *testing.T) {
		errMsg := &interfaces.ErrorMessage{
			StatusCode:     http.StatusBadGateway,
			Error:          errors.New("upstream failed"),
			DirectResponse: true,
			Body:           []byte(`{"raw":"secret"}`),
		}
		got := sanitizeOpenAIErrorMessage(errMsg)
		if got == nil || got.DirectResponse || got.Body != nil {
			t.Fatalf("untrusted direct response not strict: %#v", got)
		}
	})
}

func TestResponsesHandlerPreservesDirectResponseBeforeFirstFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "direct-initial-error-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: directInitialErrorResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"direct-initial-error-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests || recorder.Header().Get("Retry-After") != "17" || recorder.Header().Get("X-Plugin-Response") != "true" {
		t.Fatalf("direct response status=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	if recorder.Body.String() != `{"error":{"message":"plugin direct response"}}` {
		t.Fatalf("direct response body = %q", recorder.Body.String())
	}
}

func TestResponsesHandlerSanitizesErrorBeforeFirstFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "sensitive-initial-error-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: sensitiveInitialErrorResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"sensitive-initial-error-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	body := recorder.Body.String()
	if recorder.Code == http.StatusOK || !strings.Contains(body, "upstream_failed") || !strings.Contains(body, "initial upstream failure") {
		t.Fatalf("initial error response = status %d body %q", recorder.Code, body)
	}
	for _, secret := range []string{"initial-message-secret", "initial-debug-secret"} {
		if strings.Contains(body, secret) {
			t.Fatalf("initial error leaked %q: %q", secret, body)
		}
	}
	if len(body) > 4096 {
		t.Fatalf("initial error response remained unbounded: len=%d", len(body))
	}
}

func TestResponsesHandlerFlushesDataOnlyFrameBeforeStreamingError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "data-only-first-frame-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: dataOnlyFirstFrameResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"data-only-first-frame-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after complete data frame; body=%q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "event: response.failed") {
		t.Fatalf("data-only frame or terminal failure was lost: %q", body)
	}
}

func TestResponsesHandlerEmitsFailureWhenDataOnlyStreamClosesCleanly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "data-only-clean-close-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: dataOnlyCleanCloseResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"data-only-clean-close-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after complete data frame; body=%q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "event: response.failed") {
		t.Fatalf("clean close did not retain data and emit terminal failure: %q", body)
	}
	if strings.Contains(body, "event: response.completed") {
		t.Fatalf("clean close synthesized completion: %q", body)
	}
}

func TestResponsesHandlerDoesNotCommitHeadersForIncompleteFirstFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "incomplete-first-frame-responses-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: incompleteFirstFrameResponsesModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"incomplete-first-frame-responses-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code == http.StatusOK {
		t.Fatalf("incomplete first SSE frame committed HTTP 200: %q", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "upstream failed before first complete frame") {
		t.Fatalf("initial frame error was lost: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestResponsesHandlerRejectsStreamClosedBeforeFirstPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	executor := &prematureResponsesStreamExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "empty-responses-stream-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: emptyResponsesStreamModel}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"empty-responses-stream-model","input":"hi","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code == http.StatusOK {
		t.Fatalf("empty upstream stream returned HTTP 200: %q", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "closed before first payload") {
		t.Fatalf("empty upstream stream error is unclear: status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestResponsesHandlerDoesNotLoseErrorBeforeFirstPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for i := 0; i < 100; i++ {
		executor := &prematureResponsesStreamExecutor{}
		manager := coreauth.NewManager(nil, nil, nil)
		manager.RegisterExecutor(executor)
		auth := &coreauth.Auth{ID: fmt.Sprintf("initial-failure-responses-stream-auth-%d", i), Provider: executor.Identifier(), Status: coreauth.StatusActive}
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %d: %v", i, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: initialFailureResponsesModel}})

		base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
		h := NewOpenAIResponsesAPIHandler(base)
		router := gin.New()
		router.POST("/v1/responses", h.Responses)

		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"initial-failure-responses-stream-model","input":"hi","stream":true}`))
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)

		if recorder.Code == http.StatusOK {
			t.Fatalf("request %d lost the buffered initial error and returned HTTP 200: %q", i, recorder.Body.String())
		}
		if !strings.Contains(recorder.Body.String(), "upstream failed before first payload") {
			t.Fatalf("request %d lost the initial upstream error: status=%d body=%q", i, recorder.Code, recorder.Body.String())
		}
	}
}

// TestForwardResponsesStreamExposesTerminalErrors pins the SSE side: once a
// Responses stream has started, every terminal upstream error reaches the client.
func TestForwardResponsesStreamExposesTerminalErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name        string
		status      int
		message     string
		wantExposed bool
	}{
		{
			name:        "bad request",
			status:      http.StatusBadRequest,
			message:     `{"error":{"type":"invalid_request","code":"cyber_policy","message":"blocked"}}`,
			wantExposed: true,
		},
		{
			// Observed in production: the same cyber_policy rejection arrives with 502
			// when it is surfaced through the websocket disconnect channel.
			name:        "cyber policy behind bad gateway status",
			status:      http.StatusBadGateway,
			message:     `{"error":{"type":"invalid_request","code":"cyber_policy","message":"This content was flagged for possible cybersecurity risk.","param":null}}`,
			wantExposed: true,
		},
		{
			name:        "context length exceeded behind bad gateway status",
			status:      http.StatusBadGateway,
			message:     `{"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"Your input exceeds the context window."}}`,
			wantExposed: true,
		},
		{name: "conflict", status: http.StatusConflict, message: "conflict", wantExposed: true},
		{name: "message too big", status: http.StatusRequestEntityTooLarge, message: "too large", wantExposed: true},
		{name: "unprocessable entity", status: http.StatusUnprocessableEntity, message: "invalid input", wantExposed: true},
		{name: "authentication", status: http.StatusUnauthorized, message: "invalid credential", wantExposed: true},
		{name: "payment required", status: http.StatusPaymentRequired, message: "insufficient credits", wantExposed: true},
		{name: "quota error", status: http.StatusTooManyRequests, message: "usage limit reached", wantExposed: true},
		{name: "request timeout", status: http.StatusRequestTimeout, message: "upstream timeout", wantExposed: true},
		{name: "transport error", status: http.StatusInternalServerError, message: "unexpected EOF", wantExposed: true},
		{name: "upstream websocket drop", status: http.StatusInternalServerError,
			message: `{"error":{"message":"websocket: close 1006 (abnormal closure): unexpected EOF","type":"server_error","code":"internal_server_error"}}`, wantExposed: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
			h := NewOpenAIResponsesAPIHandler(base)

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

			flusher, ok := c.Writer.(http.Flusher)
			if !ok {
				t.Fatal("expected gin writer to implement http.Flusher")
			}

			data := make(chan []byte)
			errs := make(chan *interfaces.ErrorMessage, 1)
			errs <- &interfaces.ErrorMessage{StatusCode: tc.status, Error: errors.New(tc.message)}
			close(errs)

			h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)
			body := recorder.Body.String()
			exposed := strings.Contains(body, `"type":"error"`)
			if exposed != tc.wantExposed {
				t.Fatalf("error exposed = %t, want %t: %q", exposed, tc.wantExposed, body)
			}
			if exposed && strings.Contains(body, `"error":{`) {
				t.Fatalf("expected streaming error chunk, got HTTP error body: %q", body)
			}
		})
	}
}

func TestForwardResponsesStreamUsesResponseFailedForCodex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"type":"invalid_request","code":"cyber_policy","message":"blocked"}}`),
	}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") {
		t.Fatalf("missing response.failed event: %q", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("unexpected legacy error event for Codex: %q", body)
	}
	if !strings.Contains(body, `"type":"invalid_request"`) || !strings.Contains(body, `"code":"cyber_policy"`) {
		t.Fatalf("missing nested Codex error detail: %q", body)
	}
}

func TestForwardResponsesStreamExposesTransportErrorAfterOutputForCodex(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"))
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("unexpected EOF")}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") {
		t.Fatalf("transport failure ended without response.failed: %q", body)
	}
	if !strings.Contains(body, "unexpected EOF") {
		t.Fatalf("response.failed lost the upstream error: %q", body)
	}

	loggedValue, ok := c.Get("API_RESPONSE_ERROR")
	if !ok {
		t.Fatal("request log did not retain the stream error")
	}
	loggedErrors, ok := loggedValue.([]*interfaces.ErrorMessage)
	if !ok || len(loggedErrors) != 1 || loggedErrors[0] == nil || loggedErrors[0].Error == nil {
		t.Fatalf("unexpected request-log errors: %#v", loggedValue)
	}
	diagnostic := loggedErrors[0].Error.Error()
	if !strings.Contains(diagnostic, "response.output_text.delta") || !strings.Contains(diagnostic, "unexpected EOF") {
		t.Fatalf("request-log diagnostic lacks last event or upstream error: %q", diagnostic)
	}
}

func TestForwardResponsesStreamSanitizesDiagnosticErrorDetails(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	debugSecret := "super-secret-provider-debug-value"
	messageSecret := "super-secret-provider-message-value"
	rawError := `{"error":{"type":"server_error","code":"upstream_failed","message":"upstream failed: {\"api_key\":\"` + messageSecret + `\"}"},"debug":{"api_key":"` + debugSecret + `","trace":"` + strings.Repeat("x", 8192) + `"}}`
	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"))
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New(rawError)}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	body := recorder.Body.String()
	if !strings.Contains(body, "upstream failed") || !strings.Contains(body, "upstream_failed") {
		t.Fatalf("client error lost safe structured fields: %q", body)
	}
	if strings.Contains(body, debugSecret) || strings.Contains(body, messageSecret) {
		t.Fatalf("client error leaked provider secret: %q", body)
	}

	loggedValue, ok := c.Get("API_RESPONSE_ERROR")
	if !ok {
		t.Fatal("request log did not retain the sanitized stream error")
	}
	loggedErrors, ok := loggedValue.([]*interfaces.ErrorMessage)
	if !ok || len(loggedErrors) != 1 || loggedErrors[0] == nil || loggedErrors[0].Error == nil {
		t.Fatalf("unexpected request-log errors: %#v", loggedValue)
	}
	diagnostic := loggedErrors[0].Error.Error()
	if strings.Contains(diagnostic, debugSecret) || strings.Contains(diagnostic, messageSecret) || len(diagnostic) > 4096 {
		t.Fatalf("request-log diagnostic leaked or retained an unbounded upstream body: len=%d diagnostic=%q", len(diagnostic), diagnostic)
	}
	if !strings.Contains(diagnostic, "upstream failed") {
		t.Fatalf("sanitized request-log diagnostic lost upstream message: %q", diagnostic)
	}
}

func TestForwardResponsesStreamPreservesNestedResponseError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"))
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New(`{"type":"response.failed","response":{"error":{"type":"server_error","code":"upstream_failed","message":"nested response failure","param":"input"}}}`)}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	body := recorder.Body.String()
	for _, want := range []string{"nested response failure", "upstream_failed", "server_error"} {
		if !strings.Contains(body, want) {
			t.Fatalf("response.failed lost nested response error field %q: %q", want, body)
		}
	}
}

func TestForwardResponsesStreamSanitizesLastEventDiagnostic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	eventSecret := "event-secret-value"
	eventName := "custom-event-Bearer " + eventSecret + strings.Repeat("x", 1024)
	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: "+eventName+"\ndata: {\"message\":\"partial\"}\n\n"))
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("unexpected EOF")}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	loggedValue, ok := c.Get("API_RESPONSE_ERROR")
	if !ok {
		t.Fatal("request log did not retain the stream error")
	}
	loggedErrors, ok := loggedValue.([]*interfaces.ErrorMessage)
	if !ok || len(loggedErrors) != 1 || loggedErrors[0] == nil || loggedErrors[0].Error == nil {
		t.Fatalf("unexpected request-log errors: %#v", loggedValue)
	}
	diagnostic := loggedErrors[0].Error.Error()
	if strings.Contains(diagnostic, eventSecret) || len(diagnostic) > 1024 {
		t.Fatalf("last-event diagnostic leaked or remained unbounded: len=%d diagnostic=%q", len(diagnostic), diagnostic)
	}
}

func TestForwardResponsesStreamSanitizesPayloadErrorsAndStopsAtFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
	}{
		{
			name:  "event error with payload type",
			frame: "event: error\ndata: {\"type\":\"provider.error\",\"error\":{\"code\":\"failed\",\"message\":\"token=payload-secret\"}}\n\n",
		},
		{
			name:  "typed nested error",
			frame: "data: {\"type\":\"provider.error\",\"error\":{\"code\":\"failed\",\"message\":\"token=payload-secret\"}}\n\n",
		},
		{
			name:  "top level error fields",
			frame: "data: {\"code\":\"failed\",\"message\":\"token=payload-secret\"}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
			h := NewOpenAIResponsesAPIHandler(base)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
			flusher, ok := c.Writer.(http.Flusher)
			if !ok {
				t.Fatal("expected gin writer to implement http.Flusher")
			}

			data := make(chan []byte, 1)
			data <- []byte(tc.frame + "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")
			close(data)
			errs := make(chan *interfaces.ErrorMessage)
			close(errs)
			var canceled error

			h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, &responsesSSEFramer{})
			body := recorder.Body.String()
			if canceled == nil {
				t.Fatalf("payload error canceled with nil: %q", body)
			}
			if strings.Contains(body, "payload-secret") || strings.Contains(body, "event: response.completed") {
				t.Fatalf("payload error leaked or accepted later completion: %q", body)
			}
			if strings.Count(body, "event: response.failed") != 1 || !strings.Contains(body, "[REDACTED]") {
				t.Fatalf("payload error was not converted to one sanitized response.failed: %q", body)
			}
		})
	}
}

func TestForwardResponsesStreamReportsDataOnlyErrorFlushedAtEOF(t *testing.T) {
	gin.SetMode(gin.TestMode)
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	data := make(chan []byte, 1)
	data <- []byte(`data: {"type":"error","error":{"message":"failed at EOF"}}`)
	close(data)
	errs := make(chan *interfaces.ErrorMessage)
	close(errs)
	var canceled error
	h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, &responsesSSEFramer{})

	if canceled == nil || !strings.Contains(canceled.Error(), "failed at EOF") {
		t.Fatalf("EOF error cancel = %v, body=%q", canceled, recorder.Body.String())
	}
	if strings.Count(recorder.Body.String(), "event: response.failed") != 1 {
		t.Fatalf("EOF error terminal output = %q", recorder.Body.String())
	}
	if _, okLog := c.Get("API_RESPONSE_ERROR"); !okLog {
		t.Fatal("EOF error was not retained in request diagnostics")
	}
}

func TestForwardResponsesStreamDoesNotAppendFailureAfterTerminalEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"status\":\"completed\"}}\n\n"))
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("unexpected EOF after completion")}
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	body := recorder.Body.String()
	if strings.Contains(body, "event: response.failed") || strings.Contains(body, "event: error") {
		t.Fatalf("stream appended a second terminal event after response.completed: %q", body)
	}

	loggedValue, ok := c.Get("API_RESPONSE_ERROR")
	if !ok {
		t.Fatal("request log did not retain the post-terminal upstream error")
	}
	loggedErrors, ok := loggedValue.([]*interfaces.ErrorMessage)
	if !ok || len(loggedErrors) != 1 || loggedErrors[0] == nil || loggedErrors[0].Error == nil {
		t.Fatalf("unexpected request-log errors: %#v", loggedValue)
	}
	diagnostic := loggedErrors[0].Error.Error()
	if !strings.Contains(diagnostic, "response.completed") || !strings.Contains(diagnostic, "unexpected EOF after completion") {
		t.Fatalf("request-log diagnostic lacks terminal event or upstream error: %q", diagnostic)
	}
}

func TestForwardResponsesStreamFailsWhenUpstreamClosesWithoutTerminalEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatal("expected gin writer to implement http.Flusher")
	}

	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"))
	data := make(chan []byte)
	close(data)
	errs := make(chan *interfaces.ErrorMessage)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, framer)
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") {
		t.Fatalf("unterminated stream ended without response.failed: %q", body)
	}
}

// peekStreamExecutor feeds a fake executor stream that closes the chunk channel
// immediately. Both initial streaming peek loops (chat and legacy completions)
// then race a closed dataChan against any buffered pending error on errChan.
type peekStreamExecutor struct {
	// secret, when non-empty, is emitted as a chunk error so the producer
	// buffers it on errChan before closing dataChan.
	secret string
	// payload, when non-empty, is emitted as a valid content chunk so the
	// stream is a clean deterministic completion (bootstrap forwards it).
	payload string
}

func (*peekStreamExecutor) Identifier() string { return "peek-stream" }

func (*peekStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *peekStreamExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	if e.payload != "" {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(e.payload)}
	}
	if e.secret != "" {
		chunks <- coreexecutor.StreamChunk{Err: errors.New("upstream failure: api_key=" + e.secret)}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*peekStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*peekStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*peekStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

// sendPeekRequest drives the given handler method through a registered executor.
func sendPeekRequest(t *testing.T, route, body string, executor *peekStreamExecutor) *httptest.ResponseRecorder {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := "peek-stream-auth"
	auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, executor.Identifier(), []*registry.ModelInfo{{ID: "peek-stream-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	h := NewOpenAIAPIHandler(base)
	router := gin.New()
	router.POST("/v1/chat/completions", h.ChatCompletions)
	router.POST("/v1/completions", h.Completions)

	request := httptest.NewRequest(http.MethodPost, route, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

// TestStreamingPeekConsumesBufferedPendingError covers both chat completions and
// legacy completions: when the peek loop sees the data channel close while a
// buffered pending error is already queued on errChan, the handler MUST return
// an error status (never 200) and MUST NOT commit `data: [DONE]`.
func TestStreamingPeekConsumesBufferedPendingError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const secret = "upstream-secret-fx-8849"
	cases := []struct {
		name  string
		route string
		body  string
	}{
		{
			name:  "chat",
			route: "/v1/chat/completions",
			body:  `{"model":"peek-stream-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name:  "legacy",
			route: "/v1/completions",
			body:  `{"model":"peek-stream-model","stream":true,"prompt":"hi"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := sendPeekRequest(t, tc.route, tc.body, &peekStreamExecutor{secret: secret})
			body := recorder.Body.String()
			if recorder.Code == http.StatusOK {
				t.Fatalf("handler returned 200 despite buffered pending error: %q", body)
			}
			if recorder.Code < http.StatusBadRequest {
				t.Fatalf("status = %d, want error status; body=%q", recorder.Code, body)
			}
			if strings.Contains(body, "data: [DONE]") {
				t.Fatalf("stream emitted [DONE] despite pending error: %q", body)
			}
			if strings.Contains(body, secret) {
				t.Fatalf("stream body leaked upstream secret: %q", body)
			}
			if !strings.Contains(body, "[REDACTED]") {
				t.Fatalf("stream body did not redact upstream error: %q", body)
			}
		})
	}
}

// TestStreamingPeekCleanCloseStillEmitsDone is the control: a clean data-channel
// close with no pending error must still emit SSE [DONE].
func TestStreamingPeekCleanCloseStillEmitsDone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name  string
		route string
		body  string
	}{
		{
			name:  "chat",
			route: "/v1/chat/completions",
			body:  `{"model":"peek-stream-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name:  "legacy",
			route: "/v1/completions",
			body:  `{"model":"peek-stream-model","stream":true,"prompt":"hi"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A single valid content chunk then a clean close: bootstrap forwards
			// the completion and ForwardStream must emit [DONE] with no error.
			recorder := sendPeekRequest(t, tc.route, tc.body, &peekStreamExecutor{
				payload: `data: {"id":"cmpl-1","object":"chat.completion.chunk","created":1,"model":"peek-stream-model","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}`,
			})
			body := recorder.Body.String()
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%q", recorder.Code, body)
			}
			if !strings.Contains(body, "data: [DONE]") {
				t.Fatalf("clean close did not emit [DONE]: %q", body)
			}
		})
	}
}

func TestRedactResponsesStreamErrorTextBearerTokens(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{
			name: "lowercase-only standalone bearer token",
			text: "upstream error: Bearer abcdef",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "lowercase-only bare bearer token",
			text: "Bearer abcdef",
			want: "Bearer [REDACTED]",
		},
		{
			name: "lowercase-only bearer in sentence",
			text: "upstream error with Bearer abcdef in request",
			want: "upstream error with Bearer [REDACTED] in request",
		},
		{
			name: "lowercase-only bearer comma delimited",
			text: "upstream error: Bearer abcdef, request failed",
			want: "upstream error: Bearer [REDACTED], request failed",
		},
		{
			name: "mixed case and digit bearer token",
			text: "upstream error: Bearer abc123XYZ",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "numeric bearer token",
			text: "upstream error: Bearer 123456",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "RFC6750 b64token characters",
			text: "upstream error: Bearer abc-def_123.xyz~456+789/0==",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "lowercase-only basic token",
			text: "upstream error: Basic abcdef",
			want: "upstream error: Basic [REDACTED]",
		},
		{
			name: "json embedded standalone bearer",
			text: `{"error":"Bearer abcdef"}`,
			want: `{"error":"Bearer [REDACTED]"}`,
		},
		{
			name: "short 2-char bearer token",
			text: "upstream error: Bearer ab",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "short 1-char bearer token",
			text: "upstream error: Bearer a",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "short 2-char basic token",
			text: "upstream error: Basic ab",
			want: "upstream error: Basic [REDACTED]",
		},
		{
			name: "short 1-char basic token",
			text: "upstream error: Basic a",
			want: "upstream error: Basic [REDACTED]",
		},
		{
			name: "bearer of standalone at end",
			text: "upstream error: Bearer of",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "bearer to standalone at end",
			text: "upstream error: Bearer to",
			want: "upstream error: Bearer [REDACTED]",
		},
		{
			name: "bearer of bare token",
			text: "Bearer of",
			want: "Bearer [REDACTED]",
		},
		{
			name: "bearer to bare token",
			text: "Bearer to",
			want: "Bearer [REDACTED]",
		},
		{
			name: "bearer of in json quotes",
			text: `{"error":"Bearer of"}`,
			want: `{"error":"Bearer [REDACTED]"}`,
		},
		{
			name: "bearer to in json quotes",
			text: `{"error":"Bearer to"}`,
			want: `{"error":"Bearer [REDACTED]"}`,
		},
		{
			name: "bearer of with comma punctuation",
			text: "upstream error: Bearer of, please retry",
			want: "upstream error: Bearer [REDACTED], please retry",
		},
		{
			name: "authorization header bearer of",
			text: "Authorization: Bearer of",
			want: "Authorization: Bearer [REDACTED]",
		},
		{
			name: "lowercase bearer token followed by prose word",
			text: "upstream error: bearer abc expired",
			want: "upstream error: bearer [REDACTED] expired",
		},
		{
			name: "lowercase basic token followed by prose word",
			text: "upstream error: basic dGVzdA== rejected",
			want: "upstream error: basic [REDACTED] rejected",
		},
		// Prose controls: collateral prose masking is accepted in exchange for no credential leaks (reviewer trade-off).
		{
			name: "control: bearer of bad news partially masked per reviewer trade-off",
			text: "bearer of bad news",
			want: "bearer [REDACTED] bad news",
		},
		{
			name: "control: the bearer of good news partially masked per reviewer trade-off",
			text: "the bearer of good news",
			want: "the bearer [REDACTED] good news",
		},
		{
			name: "control: bearer to the manager partially masked per reviewer trade-off",
			text: "the bearer to the manager",
			want: "the bearer [REDACTED] the manager",
		},
		{
			name: "control: bearer in header partially masked per reviewer trade-off",
			text: "the bearer in header",
			want: "the bearer [REDACTED] header",
		},
		{
			name: "control: bearer is invalid partially masked per reviewer trade-off",
			text: "the bearer is invalid",
			want: "the bearer [REDACTED] invalid",
		},
		{
			name: "json password field",
			text: `{"password":"secret-pass-123"}`,
			want: `{"password":"[REDACTED]"}`,
		},
		{
			name: "escaped json password field",
			text: `{\"password\":\"secret-pass-123\"}`,
			want: `{\"password\":\"[REDACTED]\"}`,
		},
		{
			name: "password assignment key-value",
			text: "password=super-secret-pw",
			want: "password=[REDACTED]",
		},
		{
			name: "snake_case db_password",
			text: "db_password=db-secret-pw",
			want: "db_password=[REDACTED]",
		},
		{
			name: "kebab-case db-password",
			text: "db-password: db-secret-pw",
			want: "db-password: [REDACTED]",
		},
		{
			name: "camelCase userPassword",
			text: `{"userPassword":"camel-secret-pw"}`,
			want: `{"userPassword":"[REDACTED]"}`,
		},
		{
			name: "control: bare password in prose not redacted",
			text: "the password: is secret",
			want: "the password: is secret",
		},
		{
			name: "control: password_count not redacted",
			text: "password_count=5",
			want: "password_count=5",
		},
		{
			name: "url query param with leading question mark",
			text: "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro?key=AIzaSy1234567890abcdef",
			want: "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro?key=[REDACTED]",
		},
		{
			name: "url query param with leading ampersand and trailing param",
			text: "https://generativelanguage.googleapis.com/v1beta/models?version=v1&key=AIzaSy1234567890abcdef&alt=json",
			want: "https://generativelanguage.googleapis.com/v1beta/models?version=v1&key=[REDACTED]&alt=json",
		},
		{
			name: "plain key query param standalone",
			text: "?key=AIzaSy1234567890abcdef",
			want: "?key=[REDACTED]",
		},
		{
			name: "bare key assignment",
			text: "key=AIzaSy1234567890abcdef",
			want: "key=[REDACTED]",
		},
		{
			name: "json bare key field",
			text: `{"key":"AIzaSy1234567890abcdef"}`,
			want: `{"key":"[REDACTED]"}`,
		},
		{
			name: "control: bare key in prose not redacted",
			text: "the key point is that the request failed",
			want: "the key point is that the request failed",
		},
		{
			name: "control: bare key with colon in prose not redacted",
			text: "the key: is a secret token",
			want: "the key: is a secret token",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactResponsesStreamErrorText(tc.text)
			if got != tc.want {
				t.Fatalf("redactResponsesStreamErrorText(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}
