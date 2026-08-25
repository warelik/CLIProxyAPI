package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// tokenCount is a tolerant usage count that accepts any valid JSON number
// (integer, decimal, or exponent) and treats every other JSON value (null,
// string, object, array, or malformed) as unset, absorbing it without failing
// the enclosing frame. positive reports whether the count is a finite number
// greater than zero, the only property the empty-completion logic needs.
type tokenCount json.Number

func (t *tokenCount) UnmarshalJSON(b []byte) error {
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		*t = ""
		return nil
	}
	*t = tokenCount(n)
	return nil
}

// positive reports whether c is a finite JSON number greater than zero.
func (c tokenCount) positive() bool {
	n := json.Number(c)
	if n == "" {
		return false
	}
	f, err := n.Float64()
	if err != nil {
		return false
	}
	return !math.IsNaN(f) && !math.IsInf(f, 0) && f > 0
}

// addUsage folds a positive usage count into the accumulator's token total.
// Exact integer counts are summed; fractional, huge, or otherwise non-integer
// positive values still count as output evidence so the >0 check holds.
func (a *emptyCompletionAccum) addUsage(c tokenCount) {
	if !c.positive() {
		return
	}
	if n, err := json.Number(c).Int64(); err == nil && n > 0 {
		a.completionTokens += int(n)
	} else {
		a.completionTokens = max(a.completionTokens, 1)
	}
}

// errEmptyCompletion indicates the upstream returned a terminal but empty
// completion (no content, no tool calls, zero completion tokens). It is
// retriable so the conductor marks the auth as failed, cools it down, and
// rotates to the next auth/model.
var errEmptyCompletion = &Error{
	Code:       "empty_completion",
	Message:    "upstream returned an empty completion",
	Retryable:  true,
	HTTPStatus: http.StatusServiceUnavailable,
}

// maxStreamBootstrapBytes caps how much of a stream prefix the bootstrap
// detector buffers before it gives up and forwards. It bounds memory per
// in-flight stream and keeps a long metadata preamble from delaying the
// client's first byte indefinitely.
const maxStreamBootstrapBytes = 1 << 20

// openAIChunk is the minimal OpenAI-style SSE/JSON shape used to detect empty
// completions.
type openAIChunk struct {
	Choices []struct {
		Index *int   `json:"index"`
		Text  string `json:"text"`
		Delta struct {
			Content          string            `json:"content"`
			ReasoningContent string            `json:"reasoning_content"`
			Reasoning        string            `json:"reasoning"`
			Refusal          *string           `json:"refusal"`
			ToolCalls        []json.RawMessage `json:"tool_calls"`
			FunctionCall     json.RawMessage   `json:"function_call"`
			Audio            json.RawMessage   `json:"audio"`
			Images           []json.RawMessage `json:"images"`
		} `json:"delta"`
		Message struct {
			Content          string            `json:"content"`
			ReasoningContent string            `json:"reasoning_content"`
			Reasoning        string            `json:"reasoning"`
			Refusal          *string           `json:"refusal"`
			ToolCalls        []json.RawMessage `json:"tool_calls"`
			FunctionCall     json.RawMessage   `json:"function_call"`
			Audio            json.RawMessage   `json:"audio"`
			Images           []json.RawMessage `json:"images"`
		} `json:"message"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		CompletionTokens *tokenCount `json:"completion_tokens"`
	} `json:"usage"`
}

// nonEmptyJSONPayload reports whether raw holds a payload beyond an empty
// null, empty string, empty object, or empty array.
func nonEmptyJSONPayload(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var val any
	if err := json.Unmarshal(trimmed, &val); err != nil {
		return false
	}
	switch v := val.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(v) != ""
	case map[string]any:
		return len(v) > 0
	case []any:
		return len(v) > 0
	default:
		return true
	}
}

func hasMeaningfulJSONArguments(args string) bool {
	trimmed := strings.TrimSpace(args)
	if trimmed == "" || trimmed == "null" {
		return false
	}
	var val any
	if err := json.Unmarshal([]byte(trimmed), &val); err == nil {
		switch v := val.(type) {
		case nil:
			return false
		case string:
			return strings.TrimSpace(v) != ""
		case map[string]any:
			return len(v) > 0
		case []any:
			return len(v) > 0
		default:
			return true
		}
	}
	return true
}

func nonEmptyAudioPayload(raw json.RawMessage) bool {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return false
	}
	return nonEmptyAudioValue(value)
}

func nonEmptyAudioValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case bool:
		return typed
	case json.Number:
		number, err := typed.Float64()
		return err == nil && !math.IsNaN(number) && !math.IsInf(number, 0) && number != 0
	case []any:
		for _, item := range typed {
			if nonEmptyAudioValue(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range typed {
			if nonEmptyAudioValue(item) {
				return true
			}
		}
	}
	return false
}

// nonEmptyFunctionCall reports whether a legacy OpenAI function_call object
// carries a non-empty name and/or non-empty arguments.
func nonEmptyFunctionCall(raw json.RawMessage) bool {
	var fc struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &fc); err != nil {
		return false
	}
	return strings.TrimSpace(fc.Name) != "" || hasMeaningfulJSONArguments(fc.Arguments)
}

func hasMeaningfulImages(rawImages []json.RawMessage) bool {
	for _, raw := range rawImages {
		if nonEmptyJSONPayload(raw) {
			return true
		}
	}
	return false
}

func hasMeaningfulToolCalls(rawCalls []json.RawMessage) bool {
	for _, raw := range rawCalls {
		if isMeaningfulToolCall(raw) {
			return true
		}
	}
	return false
}

func isMeaningfulToolCall(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var call struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
		Name      string          `json:"name"`
		Arguments string          `json:"arguments"`
		Custom    json.RawMessage `json:"custom"`
	}
	if err := json.Unmarshal(trimmed, &call); err != nil {
		var m map[string]any
		if err := json.Unmarshal(trimmed, &m); err == nil && len(m) > 0 {
			for _, v := range m {
				if v != nil && v != "" {
					return true
				}
			}
		}
		return false
	}
	if strings.TrimSpace(call.Function.Name) != "" || hasMeaningfulJSONArguments(call.Function.Arguments) {
		return true
	}
	if strings.TrimSpace(call.Name) != "" || hasMeaningfulJSONArguments(call.Arguments) {
		return true
	}
	if nonEmptyJSONPayload(call.Custom) {
		return true
	}
	return false
}

type claudeContentBlock struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Signature string          `json:"signature"`
	Data      string          `json:"data"`
	Input     json.RawMessage `json:"input"`
	Citation  json.RawMessage `json:"citation"`
}

type claudeChunk struct {
	Type       string               `json:"type"`
	StopReason *string              `json:"stop_reason"`
	Content    []claudeContentBlock `json:"content"`
	Usage      *struct {
		OutputTokens *tokenCount `json:"output_tokens"`
	} `json:"usage"`
	Message *struct {
		Type       string               `json:"type"`
		StopReason *string              `json:"stop_reason"`
		Content    []claudeContentBlock `json:"content"`
		Usage      *struct {
			OutputTokens *tokenCount `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
	ContentBlock *claudeContentBlock `json:"content_block"`
	Delta        *struct {
		Type        string          `json:"type"`
		Text        string          `json:"text"`
		Thinking    string          `json:"thinking"`
		Signature   string          `json:"signature"`
		Citation    json.RawMessage `json:"citation"`
		PartialJSON string          `json:"partial_json"`
		StopReason  *string         `json:"stop_reason"`
	} `json:"delta"`
}

type geminiPart struct {
	Text                string          `json:"text"`
	FunctionCall        json.RawMessage `json:"functionCall"`
	InlineData          json.RawMessage `json:"inlineData"`
	FileData            json.RawMessage `json:"fileData"`
	FunctionResponse    json.RawMessage `json:"functionResponse"`
	ExecutableCode      json.RawMessage `json:"executableCode"`
	CodeExecutionResult json.RawMessage `json:"codeExecutionResult"`
	ThoughtSignature    string          `json:"thoughtSignature"`
	Thought_Signature   string          `json:"thought_signature"`
}

type geminiCandidate struct {
	Index   *int `json:"index"`
	Content *struct {
		Parts []geminiPart `json:"parts"`
	} `json:"content"`
	FinishReason      *string         `json:"finishReason"`
	GroundingMetadata json.RawMessage `json:"groundingMetadata"`
}

type geminiUsageMetadata struct {
	CandidatesTokenCount *tokenCount `json:"candidatesTokenCount"`
}

type geminiPromptFeedback struct {
	BlockReason string `json:"blockReason"`
}

type geminiChunk struct {
	Candidates        []geminiCandidate     `json:"candidates"`
	UsageMetadata     *geminiUsageMetadata  `json:"usageMetadata"`
	PromptFeedback    *geminiPromptFeedback `json:"promptFeedback"`
	GroundingMetadata json.RawMessage       `json:"groundingMetadata"`
	Response          *struct {
		Candidates        []geminiCandidate     `json:"candidates"`
		UsageMetadata     *geminiUsageMetadata  `json:"usageMetadata"`
		PromptFeedback    *geminiPromptFeedback `json:"promptFeedback"`
		GroundingMetadata json.RawMessage       `json:"groundingMetadata"`
	} `json:"response"`
}

type geminiGroundingChunk struct {
	Web *struct {
		URI   string `json:"uri"`
		Title string `json:"title"`
	} `json:"web"`
	RetrievedContext *struct {
		URI   string `json:"uri"`
		Title string `json:"title"`
		Text  string `json:"text"`
	} `json:"retrievedContext"`
}

type geminiGroundingMetadata struct {
	WebSearchQueries []string               `json:"webSearchQueries"`
	GroundingChunks  []geminiGroundingChunk `json:"groundingChunks"`
	SearchEntryPoint *struct {
		RenderedContent string `json:"renderedContent"`
	} `json:"searchEntryPoint"`
	RetrievalQueries []string `json:"retrievalQueries"`
}

// hasMeaningfulGeminiMediaPayload reports whether a Gemini media part contains
// usable content. Matching translator semantics, inlineData counts only when
// data is non-blank and fileData only when fileUri is non-blank; a scaffold
// object with only a mimeType carries no media.
func hasMeaningfulGeminiMediaPayload(inlineData, fileData json.RawMessage) bool {
	if len(bytes.TrimSpace(inlineData)) > 0 {
		var v struct {
			Data string `json:"data"`
		}
		if json.Unmarshal(inlineData, &v) == nil && strings.TrimSpace(v.Data) != "" {
			return true
		}
	}
	if len(bytes.TrimSpace(fileData)) > 0 {
		var v struct {
			FileURI string `json:"fileUri"`
		}
		if json.Unmarshal(fileData, &v) == nil && strings.TrimSpace(v.FileURI) != "" {
			return true
		}
	}
	return false
}

func isMeaningfulGeminiFunctionCall(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var call struct {
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	}
	if err := json.Unmarshal(trimmed, &call); err != nil {
		return false
	}
	if strings.TrimSpace(call.Name) != "" {
		return true
	}
	return nonEmptyJSONPayload(call.Args)
}

func hasMeaningfulGroundingMetadata(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("{}")) || bytes.Equal(trimmed, []byte("[]")) {
		return false
	}
	var meta geminiGroundingMetadata
	if err := json.Unmarshal(trimmed, &meta); err == nil {
		for _, q := range meta.WebSearchQueries {
			if strings.TrimSpace(q) != "" {
				return true
			}
		}
		for _, q := range meta.RetrievalQueries {
			if strings.TrimSpace(q) != "" {
				return true
			}
		}
		for _, chunk := range meta.GroundingChunks {
			if chunk.Web != nil {
				if strings.TrimSpace(chunk.Web.URI) != "" || strings.TrimSpace(chunk.Web.Title) != "" {
					return true
				}
			}
			if chunk.RetrievedContext != nil {
				if strings.TrimSpace(chunk.RetrievedContext.URI) != "" ||
					strings.TrimSpace(chunk.RetrievedContext.Title) != "" ||
					strings.TrimSpace(chunk.RetrievedContext.Text) != "" {
					return true
				}
			}
		}
		if meta.SearchEntryPoint != nil && strings.TrimSpace(meta.SearchEntryPoint.RenderedContent) != "" {
			return true
		}
	}
	var generic map[string]any
	if err := json.Unmarshal(trimmed, &generic); err == nil {
		for k, v := range generic {
			if k == "groundingChunks" || k == "webSearchQueries" || k == "retrievalQueries" || k == "searchEntryPoint" {
				continue
			}
			switch val := v.(type) {
			case nil:
			case string:
				if strings.TrimSpace(val) != "" {
					return true
				}
			case map[string]any:
				if len(val) > 0 {
					for _, mv := range val {
						if s, ok := mv.(string); ok && strings.TrimSpace(s) != "" {
							return true
						}
					}
				}
			case []any:
				if len(val) > 0 {
					for _, ev := range val {
						if s, ok := ev.(string); ok && strings.TrimSpace(s) != "" {
							return true
						}
					}
				}
			default:
				return true
			}
		}
	}
	return false
}

// emptyCompletionAccum accumulates the properties relevant to deciding whether
// an OpenAI-, Claude-, or Gemini-style completion is empty. Later slices add
// Responses/Interactions evaluators onto the same accum.
type emptyCompletionAccum struct {
	expectedChoices          int
	recognized               bool
	sawUnknownData           bool
	terminal                 bool
	hasContent               bool
	hasToolCalls             bool
	completionTokens         int
	sawUsage                 bool
	blocked                  bool
	sawMetadataOnly          bool
	sawMessageData           bool
	openAITerminal           bool
	claudeTerminal           bool
	geminiTerminal           bool
	openAIChoicesSeen        map[int]bool
	openAIChoicesFinished    map[int]bool
	geminiCandidatesSeen     map[int]bool
	geminiCandidatesFinished map[int]bool
}

func (a *emptyCompletionAccum) evalJSON(data []byte) bool {
	values, err := decodeJSONValues(data)
	if err != nil {
		return false
	}
	recognized := false
	for _, v := range values {
		if a.evalOpenAI(v) || a.evalClaude(v) || a.evalGemini(v) {
			recognized = true
		} else {
			a.sawUnknownData = true
		}
	}
	return recognized
}

// decodeJSONValues decodes every top-level JSON value in payload with the
// stdlib decoder until io.EOF, supporting pretty JSON, NDJSON, whitespace
// separated, and directly concatenated values. It requires at least one value
// and a clean EOF; malformed or trailing garbage returns an error.
func decodeJSONValues(payload []byte) ([]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	var values []json.RawMessage
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		values = append(values, raw)
	}
	if len(values) == 0 {
		return nil, io.EOF
	}
	return values, nil
}

func (a *emptyCompletionAccum) evalOpenAI(data []byte) bool {
	// Recognize the OpenAI shape by the presence of a "choices" key, even when
	// the array is empty (e.g. {"choices":[]}). Such prefixes must still be
	// judged at stream close instead of being forwarded immediately.
	if !hasJSONKey(data, "choices") {
		return false
	}
	a.recognized = true
	a.sawMessageData = true
	var chunk openAIChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		// A recognized choices-bearing payload whose shape does not decode
		// (for example message.content as an array of content parts) carries
		// forward-compatible output we cannot inspect. Treat it as unknown
		// data so it passes through instead of being misjudged as an empty
		// completion.
		a.sawUnknownData = true
		return true
	}
	if chunk.Usage != nil && chunk.Usage.CompletionTokens != nil {
		a.sawUsage = true
		a.addUsage(*chunk.Usage.CompletionTokens)
	}
	if a.openAIChoicesSeen == nil {
		a.openAIChoicesSeen = make(map[int]bool)
		a.openAIChoicesFinished = make(map[int]bool)
	}
	for i, ch := range chunk.Choices {
		idx := i
		if ch.Index != nil {
			idx = *ch.Index
		}
		a.openAIChoicesSeen[idx] = true
		if ch.FinishReason != nil {
			reason := strings.TrimSpace(*ch.FinishReason)
			if strings.EqualFold(reason, "stop") || strings.EqualFold(reason, "tool_calls") || strings.EqualFold(reason, "function_call") {
				a.openAIChoicesFinished[idx] = true
				a.terminal = true
			} else if reason != "" {
				// content_filter, length, and other non-stop terminal reasons
				// are not empty completions: the client must see the reason
				// rather than a silent auth rotation.
				a.blocked = true
				a.terminal = true
			}
		}
		content := ch.Text + ch.Delta.Content + ch.Message.Content + ch.Delta.ReasoningContent + ch.Message.ReasoningContent + ch.Delta.Reasoning + ch.Message.Reasoning
		if strings.TrimSpace(content) != "" {
			a.hasContent = true
		}
		if (ch.Delta.Refusal != nil && strings.TrimSpace(*ch.Delta.Refusal) != "") ||
			(ch.Message.Refusal != nil && strings.TrimSpace(*ch.Message.Refusal) != "") {
			a.hasContent = true
		}
		if hasMeaningfulToolCalls(ch.Delta.ToolCalls) || hasMeaningfulToolCalls(ch.Message.ToolCalls) {
			a.hasToolCalls = true
		}
		if nonEmptyFunctionCall(ch.Delta.FunctionCall) || nonEmptyFunctionCall(ch.Message.FunctionCall) {
			a.hasToolCalls = true
		}
		if nonEmptyAudioPayload(ch.Delta.Audio) || nonEmptyAudioPayload(ch.Message.Audio) {
			a.hasContent = true
		}
		if hasMeaningfulImages(ch.Delta.Images) || hasMeaningfulImages(ch.Message.Images) {
			a.hasContent = true
		}
	}
	expected := a.expectedChoices
	if expected <= 0 {
		expected = 1
	}
	targetChoices := expected
	if len(a.openAIChoicesSeen) > targetChoices {
		targetChoices = len(a.openAIChoicesSeen)
	}
	if len(a.openAIChoicesFinished) >= targetChoices && len(a.openAIChoicesFinished) >= len(a.openAIChoicesSeen) && !a.blocked {
		a.openAITerminal = true
	} else {
		a.openAITerminal = false
	}
	if len(chunk.Choices) == 0 && chunk.Usage != nil {
		// A completed non-streaming payload with zero choices
		// ({"choices":[], "usage":...}) never enters the loop above, so
		// terminal would never be set and the payload would be accepted as a
		// successful response. With usage present the response is complete, so
		// the empty judgment can run. (Streamed zero-choices chunks without
		// usage are mid-stream signals and must not mark terminal here.)
		a.terminal = true
	}
	return true
}

func (a *emptyCompletionAccum) evalClaude(data []byte) bool {
	var chunk claudeChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return false
	}

	isClaude := false
	switch chunk.Type {
	case "message", "message_start", "content_block_start", "content_block_delta", "message_delta", "message_stop", "ping":
		isClaude = true
	default:
		if chunk.StopReason != nil || (chunk.Message != nil && (chunk.Message.Type == "message" || chunk.Message.StopReason != nil)) {
			isClaude = true
		}
	}

	if !isClaude {
		return false
	}

	a.recognized = true
	if chunk.Type == "ping" {
		a.sawMetadataOnly = true
	} else {
		a.sawMessageData = true
	}
	if chunk.Type == "message_stop" {
		a.terminal = true
		a.claudeTerminal = true
	}

	a.evalClaudeStopReason(chunk.StopReason)
	if chunk.Message != nil {
		a.evalClaudeStopReason(chunk.Message.StopReason)
	}
	if chunk.Delta != nil {
		a.evalClaudeStopReason(chunk.Delta.StopReason)
	}

	if chunk.Usage != nil && chunk.Usage.OutputTokens != nil {
		a.sawUsage = true
		a.addUsage(*chunk.Usage.OutputTokens)
	}
	if chunk.Message != nil && chunk.Message.Usage != nil && chunk.Message.Usage.OutputTokens != nil {
		a.sawUsage = true
		a.addUsage(*chunk.Message.Usage.OutputTokens)
	}

	a.evalClaudeBlocks(chunk.Content)
	if chunk.Message != nil {
		a.evalClaudeBlocks(chunk.Message.Content)
	}
	if chunk.ContentBlock != nil {
		a.evalClaudeBlocks([]claudeContentBlock{*chunk.ContentBlock})
	}
	if chunk.Delta != nil {
		switch chunk.Delta.Type {
		case "text_delta":
			if strings.TrimSpace(chunk.Delta.Text) != "" {
				a.hasContent = true
			}
		case "thinking_delta":
			if strings.TrimSpace(chunk.Delta.Thinking) != "" {
				a.hasContent = true
			}
		case "signature_delta":
			// A lonely signature is not visible content and does not spend
			// completion tokens; do not treat it as a live completion.
		case "citations_delta":
			if nonEmptyJSONPayload(chunk.Delta.Citation) {
				a.hasContent = true
			}
		case "input_json_delta":
			if hasMeaningfulJSONArguments(chunk.Delta.PartialJSON) {
				a.hasToolCalls = true
			}
		default:
			if strings.TrimSpace(chunk.Delta.Text) != "" || strings.TrimSpace(chunk.Delta.Thinking) != "" || nonEmptyJSONPayload(chunk.Delta.Citation) {
				a.hasContent = true
			}
		}
	}

	return true
}

func (a *emptyCompletionAccum) evalClaudeStopReason(stopReason *string) {
	if stopReason == nil {
		return
	}
	reason := strings.TrimSpace(*stopReason)
	if strings.EqualFold(reason, "end_turn") || strings.EqualFold(reason, "tool_use") {
		a.terminal = true
		a.claudeTerminal = true
	} else if reason != "" {
		// Request/output limits, refusals, and control stop reasons must reach the
		// client instead of being converted into a credential failure.
		a.blocked = true
	}
}

func (a *emptyCompletionAccum) evalClaudeBlocks(blocks []claudeContentBlock) {
	for _, b := range blocks {
		if b.Type == "tool_use" || b.Type == "server_tool_use" || b.Type == "mcp_tool_use" {
			if (strings.TrimSpace(b.ID) != "" && strings.TrimSpace(b.Name) != "") || nonEmptyJSONPayload(b.Input) {
				a.hasToolCalls = true
			}
			continue
		}
		if nonEmptyJSONPayload(b.Input) {
			a.hasToolCalls = true
			continue
		}
		if b.Type == "thinking" || b.Type == "redacted_thinking" || b.Type == "reasoning" || strings.TrimSpace(b.Thinking) != "" || strings.TrimSpace(b.Data) != "" {
			if strings.TrimSpace(b.Thinking) != "" || strings.TrimSpace(b.Data) != "" {
				a.hasContent = true
			}
			continue
		}
		if b.Type == "text" || strings.TrimSpace(b.Text) != "" {
			if strings.TrimSpace(b.Text) != "" {
				a.hasContent = true
			}
			continue
		}
		if nonEmptyJSONPayload(b.Citation) {
			a.hasContent = true
			continue
		}
	}
}

// hasNestedResponseCandidates reports whether the payload's response object
// contains a candidates key (the Gemini streaming wrapper shape).
func hasNestedResponseCandidates(data []byte) bool {
	var probe struct {
		Response map[string]json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	_, ok := probe.Response["candidates"]
	return ok
}

func (a *emptyCompletionAccum) evalGemini(data []byte) bool {
	var chunk geminiChunk
	if err := json.Unmarshal(data, &chunk); err != nil {
		return false
	}

	candidates := chunk.Candidates
	usage := chunk.UsageMetadata
	promptFeedback := chunk.PromptFeedback

	if chunk.Response != nil {
		if len(candidates) == 0 {
			candidates = chunk.Response.Candidates
		}
		if usage == nil {
			usage = chunk.Response.UsageMetadata
		}
		if promptFeedback == nil {
			promptFeedback = chunk.Response.PromptFeedback
		}
	}
	promptBlocked := promptFeedback != nil && strings.TrimSpace(promptFeedback.BlockReason) != ""

	if len(candidates) == 0 {
		// Only treat an empty candidates array as a recognized empty completion
		// when the candidates key is actually present. An absent candidates key
		// is not a Gemini shape at all.
		if hasJSONKey(data, "candidates") || hasNestedResponseCandidates(data) {
			a.recognized = true
			a.sawMessageData = true
			a.terminal = true
			a.blocked = promptBlocked
			if !promptBlocked {
				a.geminiTerminal = true
			}
			if usage != nil && usage.CandidatesTokenCount != nil {
				a.sawUsage = true
				a.addUsage(*usage.CandidatesTokenCount)
			}
			return true
		}
		return false
	}

	a.recognized = true
	a.sawMessageData = true
	if promptBlocked {
		a.blocked = true
	}

	if usage != nil {
		if usage.CandidatesTokenCount != nil {
			a.sawUsage = true
			a.addUsage(*usage.CandidatesTokenCount)
		}
	}
	if hasMeaningfulGroundingMetadata(chunk.GroundingMetadata) {
		a.hasContent = true
	}
	if chunk.Response != nil && hasMeaningfulGroundingMetadata(chunk.Response.GroundingMetadata) {
		a.hasContent = true
	}

	if a.geminiCandidatesSeen == nil {
		a.geminiCandidatesSeen = make(map[int]bool)
		a.geminiCandidatesFinished = make(map[int]bool)
	}

	blocked := false
	for i, cand := range candidates {
		idx := i
		if cand.Index != nil {
			idx = *cand.Index
		}
		a.geminiCandidatesSeen[idx] = true
		if cand.FinishReason != nil {
			reason := strings.TrimSpace(*cand.FinishReason)
			if reason != "" {
				if strings.EqualFold(reason, "STOP") {
					a.geminiCandidatesFinished[idx] = true
					a.terminal = true
				} else {
					// SAFETY, RECITATION, MAX_TOKENS, BLOCKLIST,
					// PROHIBITED_CONTENT, OTHER: the client must see the
					// stop/block reason rather than a silent auth rotation.
					blocked = true
					a.terminal = true
				}
			}
		}
		if hasMeaningfulGroundingMetadata(cand.GroundingMetadata) {
			a.hasContent = true
		}
		if cand.Content != nil {
			for _, part := range cand.Content.Parts {
				if isMeaningfulGeminiFunctionCall(part.FunctionCall) {
					a.hasToolCalls = true
				}
				if hasMeaningfulGeminiMediaPayload(part.InlineData, part.FileData) ||
					nonEmptyJSONPayload(part.FunctionResponse) {
					a.hasContent = true
				}
				if nonEmptyJSONPayload(part.ExecutableCode) || nonEmptyJSONPayload(part.CodeExecutionResult) {
					a.hasContent = true
				}
				if strings.TrimSpace(part.Text) != "" {
					a.hasContent = true
				}
				// thoughtSignature / thought_signature is not visible content
				// and does not spend tokens; a lonely signature is empty.
			}
		}
	}

	if blocked {
		a.blocked = true
	}

	expected := a.expectedChoices
	if expected <= 0 {
		expected = 1
	}
	targetCandidates := expected
	if len(a.geminiCandidatesSeen) > targetCandidates {
		targetCandidates = len(a.geminiCandidatesSeen)
	}
	if len(a.geminiCandidatesFinished) >= targetCandidates && len(a.geminiCandidatesFinished) >= len(a.geminiCandidatesSeen) && !a.blocked {
		a.geminiTerminal = true
	} else {
		a.geminiTerminal = false
	}

	return true
}

// hasJSONKey reports whether the given JSON object contains name as a top-level
// key. It returns false for non-object or malformed input.
func hasJSONKey(data []byte, name string) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	_, ok := probe[name]
	return ok
}

// empty reports whether the accumulated stream is an empty completion.
func (a *emptyCompletionAccum) empty() bool {
	if a.sawUnknownData || a.blocked || a.hasContent || a.hasToolCalls || (a.sawUsage && a.completionTokens > 0) {
		return false
	}
	if a.recognized && a.terminal {
		return true
	}
	if a.recognized {
		return true
	}
	if a.sawMetadataOnly && !a.sawMessageData {
		return true
	}
	return false
}

// isEmptyCompletion reports whether the buffered stream prefix aggregates to an
// empty completion. The prefix is replayed through a fresh state and finalized,
// so a trailing frame that was never closed by a blank separator line is still
// judged instead of silently passing as output.
func isEmptyCompletion(chunks []cliproxyexecutor.StreamChunk) bool {
	if len(chunks) == 0 {
		return false
	}
	var state streamBootstrapState
	for _, c := range chunks {
		if state.observe(c.Payload) {
			return false
		}
	}
	state.finish()
	return state.isEmptyCompletion()
}

// streamBootstrapState incrementally evaluates stream chunks so a metadata-heavy
// prefix is processed once instead of reparsing the whole prefix per chunk. The
// streaming path cannot wait for a complete body: forward-or-rotate has to be
// decided on the prefix, so the state tracks just enough to answer two
// questions - is there meaningful output yet (forward), and has the stream
// already reached a terminal marker with nothing in it (rotate).
type streamBootstrapState struct {
	acc       emptyCompletionAccum
	bytes     int
	pending   []byte
	dataLines [][]byte
	forward   bool
	sawSSE    bool
	sawDone   bool
}

func (s *streamBootstrapState) flushData() {
	if len(s.dataLines) == 0 {
		return
	}
	data := bytes.Join(s.dataLines, []byte("\n"))
	s.dataLines = s.dataLines[:0]
	if bytes.Equal(data, []byte("[DONE]")) {
		s.acc.recognized = true
		s.acc.terminal = true
		s.acc.sawMessageData = true
		s.sawDone = true
		return
	}
	if len(data) == 0 {
		s.acc.sawMetadataOnly = true
		return
	}
	if !s.acc.evalJSON(data) {
		s.acc.sawUnknownData = true
	}
}

func (s *streamBootstrapState) processLine(line []byte) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		if len(s.dataLines) > 0 && classifyJSONBuffer(bytes.Join(s.dataLines, []byte("\n"))) == jsonBufIncomplete {
			// Pretty-printed raw JSON may contain blank lines; keep them in the
			// buffer and do not treat them as an SSE event boundary.
			s.dataLines = append(s.dataLines, []byte(""))
			return
		}
		s.flushData()
		return
	}
	s.processSingleLine(line)
}

func (s *streamBootstrapState) processSingleLine(line []byte) {
	switch {
	case bytes.HasPrefix(line, []byte("event:")):
		s.sawSSE = true
		if bytes.Equal(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("event:"))), []byte("message_stop")) {
			s.acc.recognized = true
			s.acc.terminal = true
			s.acc.sawMessageData = true
			s.sawDone = true
		} else {
			s.acc.sawMetadataOnly = true
		}
	case bytes.Equal(line, []byte("event")):
		s.sawSSE = true
		s.acc.sawMetadataOnly = true
	case bytes.HasPrefix(line, []byte("id:")), bytes.HasPrefix(line, []byte("retry:")), bytes.HasPrefix(line, []byte(":")):
		s.sawSSE = true
		s.acc.sawMetadataOnly = true
	case bytes.Equal(line, []byte("id")), bytes.Equal(line, []byte("retry")):
		s.sawSSE = true
		s.acc.sawMetadataOnly = true
	case bytes.HasPrefix(line, []byte("data:")):
		s.sawSSE = true
		s.dataLines = append(s.dataLines, parseSSEDataLine(line))
	case bytes.Equal(line, []byte("data")):
		s.sawSSE = true
		s.dataLines = append(s.dataLines, []byte(""))
	case bytes.HasPrefix(line, []byte("{")), bytes.HasPrefix(line, []byte("[")):
		s.sawSSE = true
		// Raw JSONL/NDJSON frames are newline-terminated and never followed by a
		// blank separator line, so buffering them would defer evaluation until
		// the bootstrap byte cap is hit. Evaluate a self-contained frame
		// immediately; keep buffering only when a multi-line JSON value is
		// already in progress.
		if len(s.dataLines) == 0 && classifyJSONBuffer(line) == jsonBufComplete {
			if !s.acc.evalJSON(line) {
				s.acc.sawUnknownData = true
			}
			return
		}
		s.dataLines = append(s.dataLines, line)
	default:
		// A pretty-printed raw JSON frame arrives one line at a time: the
		// opening brace lands in dataLines and every continuation line looks
		// like an isolated, invalid JSON value on its own. Append the line
		// first, then classify the joined buffer; evaluate as soon as it
		// becomes complete instead of waiting for a blank line that may never
		// come.
		if len(s.dataLines) > 0 {
			s.dataLines = append(s.dataLines, line)
			joined := bytes.Join(s.dataLines, []byte("\n"))
			s.dataLines = s.dataLines[:0]
			switch classifyJSONBuffer(joined) {
			case jsonBufComplete:
				if !s.acc.evalJSON(joined) {
					s.acc.sawUnknownData = true
				}
			case jsonBufIncomplete:
				// Still incomplete; restore the accumulated lines and wait for
				// the next continuation.
				s.dataLines = append([][]byte(nil), joined)
			default:
				s.acc.sawUnknownData = true
			}
			return
		}
		if classify := classifyJSONBuffer(line); classify == jsonBufComplete || classify == jsonBufIncomplete {
			if !s.acc.evalJSON(line) {
				s.acc.sawUnknownData = true
			}
		} else {
			s.acc.sawUnknownData = true
		}
	}
}

// observe folds the next chunk into the state and reports whether the stream
// must be forwarded to the client from here on. Once it returns true the state
// stops inspecting: meaningful output has been seen and no later frame can turn
// the response back into an empty completion.
func (s *streamBootstrapState) observe(fragment []byte) bool {
	if s.forward {
		return true
	}
	s.bytes += len(fragment)
	if s.bytes > maxStreamBootstrapBytes {
		// A prefix this long is not a metadata preamble. Stop buffering and let
		// the stream through rather than holding the client's first byte back.
		s.forward = true
		return true
	}
	s.pending = append(s.pending, fragment...)
	for {
		newline := bytes.IndexByte(s.pending, '\n')
		if newline < 0 {
			break
		}
		line := bytes.TrimSpace(s.pending[:newline])
		s.pending = s.pending[newline+1:]
		s.processLine(line)
		if s.shouldForward() {
			s.forward = true
			return true
		}
	}

	trimmed := bytes.TrimSpace(s.pending)
	if len(trimmed) == 0 {
		return false
	}

	if bytes.HasPrefix(trimmed, []byte("data:")) {
		payload := bytes.TrimSpace(trimmed[len("data:"):])
		if len(s.dataLines) == 0 && (bytes.Equal(payload, []byte("[DONE]")) || classifyJSONBuffer(payload) == jsonBufComplete) {
			// A complete data frame that arrived without its trailing newline:
			// evaluate it now, otherwise a provider that omits the final blank
			// line keeps the terminal marker invisible until EOF.
			s.sawSSE = true
			s.dataLines = append(s.dataLines, parseSSEDataLine(trimmed))
			s.flushData()
			s.pending = s.pending[:0]
			s.forward = s.shouldForward()
			return s.forward
		}
		return false
	}

	if couldBeSSEPrefix(trimmed) {
		return false
	}
	switch classifyJSONBuffer(trimmed) {
	case jsonBufComplete:
		if !s.acc.evalJSON(trimmed) {
			s.acc.sawUnknownData = true
		}
		s.pending = s.pending[:0]
	case jsonBufEmpty, jsonBufIncomplete:
		return false
	case jsonBufInvalid:
		s.acc.sawUnknownData = true
	}
	s.forward = s.shouldForward()
	return s.forward
}

// finish flushes whatever is still buffered. A provider may close the stream
// right after the last frame without the blank separator line, and only this
// call turns that tail into a verdict.
func (s *streamBootstrapState) finish() {
	if len(s.pending) > 0 {
		trimmed := bytes.TrimSpace(s.pending)
		s.pending = s.pending[:0]
		if len(trimmed) > 0 {
			s.processLine(trimmed)
		}
	}
	s.flushData()
}

// isEmptyCompletion reports the verdict for the observed prefix. A prefix that
// produced no frame this slice can read is never empty: the streaming detector
// knows OpenAI chat completions, Claude, and Gemini, and a Responses/
// Interactions stream - or one whose framing it failed to split into lines at
// all - must pass through untouched rather than be buried as an empty
// completion.
func (s *streamBootstrapState) isEmptyCompletion() bool {
	if !s.acc.recognized {
		return false
	}
	return s.acc.empty()
}

// isTerminalEmpty reports whether the prefix already reached a terminal marker
// with nothing meaningful in it, so the conductor can stop reading and rotate
// instead of waiting for a close that a stalled upstream may never send.
func (s *streamBootstrapState) isTerminalEmpty() bool {
	if s.acc.openAITerminal && !s.acc.sawUsage {
		// OpenAI streams may emit finish_reason=stop before the final usage
		// frame; do not judge the stream empty until usage arrives.
		return false
	}
	return (s.sawDone || s.acc.openAITerminal || s.acc.claudeTerminal || s.acc.geminiTerminal) && s.isEmptyCompletion()
}

// setExpectedChoices tells the state how many choices the request asked for, so
// a stream is not judged terminal after the first choice finishes while the
// remaining ones are still to come.
func (s *streamBootstrapState) setExpectedChoices(n int) {
	if n <= 0 {
		n = 1
	}
	s.acc.expectedChoices = n
}

// hasMeaningfulOutput reports whether the prefix already holds bytes the client
// would lose. It is deliberately more permissive than shouldForward: holding a
// prefix back is a detection decision, and it must not silently reclassify an
// error that arrives mid-answer into a bootstrap failure the conductor would
// retry on another credential.
func (s *streamBootstrapState) hasMeaningfulOutput() bool {
	if s.forward {
		return true
	}
	if s.acc.hasContent || s.acc.hasToolCalls || s.acc.blocked ||
		(s.acc.sawUsage && s.acc.completionTokens > 0) || s.acc.sawUnknownData {
		return true
	}
	// Bytes that matched no protocol frame at all are opaque output as far as
	// this detector is concerned; only a recognized or SSE-framed prefix is
	// safe to treat as "nothing was delivered yet".
	return !s.acc.recognized && !s.sawSSE && s.bytes > 0
}

func (s *streamBootstrapState) shouldForward() bool {
	return s.acc.hasContent || s.acc.hasToolCalls || s.acc.blocked ||
		(s.acc.sawUsage && s.acc.completionTokens > 0) || s.acc.sawUnknownData ||
		(!s.acc.recognized && !s.sawSSE)
}

type jsonBufferStatus int

const (
	jsonBufEmpty jsonBufferStatus = iota
	jsonBufComplete
	jsonBufIncomplete
	jsonBufInvalid
)

// classifyJSONBuffer classifies an accumulated raw-JSON stream tail as holding
// one or more complete values (jsonBufComplete), a truncated prefix of a value
// (jsonBufIncomplete), malformed or trailing garbage (jsonBufInvalid), or no
// value (jsonBufEmpty). It inspects only the given buffer, so it can be called
// again on each growing chunk without keeping a persistent decoder.
func classifyJSONBuffer(buf []byte) jsonBufferStatus {
	if hasTruncatedUTF8Suffix(buf) {
		return jsonBufIncomplete
	}
	dec := json.NewDecoder(bytes.NewReader(buf))
	count := 0
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				if count == 0 {
					return jsonBufEmpty
				}
				return jsonBufComplete
			}
			if isTruncatedJSON(err) {
				return jsonBufIncomplete
			}
			return jsonBufInvalid
		}
		count++
	}
}

// isTruncatedJSON reports whether a json decoding error is caused by the input
// ending mid-value (a truncated prefix) rather than by malformed contents.
func isTruncatedJSON(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var syn *json.SyntaxError
	if errors.As(err, &syn) {
		return strings.Contains(syn.Error(), "unexpected end of JSON input")
	}
	return false
}

// hasTruncatedUTF8Suffix reports whether buf ends in the middle of a multi-byte
// UTF-8 sequence, which happens when a raw JSON value is split at a chunk
// boundary inside a string literal.
func hasTruncatedUTF8Suffix(buf []byte) bool {
	n := len(buf)
	if n == 0 {
		return false
	}
	i := n - 1
	for i >= 0 && buf[i]&0xC0 == 0x80 {
		i--
	}
	if i < 0 {
		return false
	}
	lead := buf[i]
	var need int
	switch {
	case lead&0xE0 == 0xC0:
		need = 1
	case lead&0xF0 == 0xE0:
		need = 2
	case lead&0xF8 == 0xF0:
		need = 3
	default:
		return false
	}
	return n-i-1 < need
}

// couldBeSSEPrefix reports whether the buffered tail may still grow into an SSE
// field line, so a chunk boundary inside "eve|nt: foo" is not mistaken for
// unknown data.
func couldBeSSEPrefix(payload []byte) bool {
	const dataPrefix = "data:"
	const eventPrefix = "event:"
	const idPrefix = "id:"
	const retryPrefix = "retry:"
	value := string(payload)
	return strings.HasPrefix(value, ":") ||
		strings.HasPrefix(dataPrefix, value) || strings.HasPrefix(eventPrefix, value) ||
		strings.HasPrefix(idPrefix, value) || strings.HasPrefix(retryPrefix, value) ||
		strings.HasPrefix(value, dataPrefix) || strings.HasPrefix(value, eventPrefix) ||
		strings.HasPrefix(value, idPrefix) || strings.HasPrefix(value, retryPrefix) ||
		value == "data" || value == "event" || value == "id" || value == "retry"
}

// extractExpectedChoices reports how many OpenAI choices or Gemini candidates
// the request asked for (top-level "n", or generationConfig.candidateCount).
// Without it a two-candidate stream whose first candidate finishes empty looks
// terminal while the second is still to come, and a live credential is rotated
// away. Anything unparseable or absent means one choice.
func extractExpectedChoices(payload []byte) int {
	if len(payload) == 0 {
		return 1
	}
	var req struct {
		N                *int `json:"n"`
		CandidateCount   *int `json:"candidateCount"`
		Candidate_Count  *int `json:"candidate_count"`
		GenerationConfig *struct {
			CandidateCount  *int `json:"candidateCount"`
			Candidate_Count *int `json:"candidate_count"`
		} `json:"generationConfig"`
		Generation_Config *struct {
			CandidateCount  *int `json:"candidateCount"`
			Candidate_Count *int `json:"candidate_count"`
		} `json:"generation_config"`
		Request *struct {
			N                *int `json:"n"`
			CandidateCount   *int `json:"candidateCount"`
			Candidate_Count  *int `json:"candidate_count"`
			GenerationConfig *struct {
				CandidateCount  *int `json:"candidateCount"`
				Candidate_Count *int `json:"candidate_count"`
			} `json:"generationConfig"`
			Generation_Config *struct {
				CandidateCount  *int `json:"candidateCount"`
				Candidate_Count *int `json:"candidate_count"`
			} `json:"generation_config"`
		} `json:"request"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return 1
	}
	maxChoices := 1
	updateMax := func(ptr *int) {
		if ptr != nil && *ptr > maxChoices {
			maxChoices = *ptr
		}
	}
	updateMax(req.N)
	updateMax(req.CandidateCount)
	updateMax(req.Candidate_Count)
	if req.GenerationConfig != nil {
		updateMax(req.GenerationConfig.CandidateCount)
		updateMax(req.GenerationConfig.Candidate_Count)
	}
	if req.Generation_Config != nil {
		updateMax(req.Generation_Config.CandidateCount)
		updateMax(req.Generation_Config.Candidate_Count)
	}
	if req.Request != nil {
		updateMax(req.Request.N)
		updateMax(req.Request.CandidateCount)
		updateMax(req.Request.Candidate_Count)
		if req.Request.GenerationConfig != nil {
			updateMax(req.Request.GenerationConfig.CandidateCount)
			updateMax(req.Request.GenerationConfig.Candidate_Count)
		}
		if req.Request.Generation_Config != nil {
			updateMax(req.Request.Generation_Config.CandidateCount)
			updateMax(req.Request.Generation_Config.Candidate_Count)
		}
	}
	return maxChoices
}

// isEmptyCompletionPayload reports whether a payload (aggregated SSE chunks or
// a single non-stream JSON response) represents an empty completion.
func isEmptyCompletionPayload(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		// A zero-length or whitespace-only body on an HTTP success is the
		// canonical empty completion: without this, Execute and plugin
		// executors returned it as a successful response and never rotated
		// credentials. A literal JSON null is equally empty.
		return true
	}

	var jsonAcc emptyCompletionAccum
	if jsonAcc.evalJSON(trimmed) {
		var probe struct {
			Choices json.RawMessage `json:"choices"`
		}
		if json.Unmarshal(trimmed, &probe) == nil && probe.Choices != nil {
			jsonAcc.terminal = true
		}
		return jsonAcc.empty()
	}

	var acc emptyCompletionAccum

	if isSSEPayload(trimmed) {
		acc.evalSSE(trimmed)
		return acc.empty()
	}

	acc.evalJSON(trimmed)
	// A complete non-SSE OpenAI chat completion body is terminal by
	// construction: zero-choice payloads such as {"choices":[]} or
	// {"choices":[],"usage":null} never enter the per-choice terminal paths,
	// so without this they would be accepted as successful responses instead
	// of being judged as empty completions. Other recognized shapes (for
	// example Claude messages) keep their per-shape terminal rules.
	var probe struct {
		Choices json.RawMessage `json:"choices"`
	}
	if json.Unmarshal(trimmed, &probe) == nil && probe.Choices != nil {
		acc.terminal = true
	}
	return acc.empty()
}

func isSSEPayload(trimmed []byte) bool {
	for _, line := range bytes.Split(trimmed, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if isSSEPrefix(line) {
			return true
		}
	}
	return false
}

func parseSSEDataLine(line []byte) []byte {
	data := bytes.TrimPrefix(line, []byte("data:"))
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	return data
}

func isSSEPrefix(b []byte) bool {
	return bytes.HasPrefix(b, []byte("data:")) ||
		bytes.HasPrefix(b, []byte("event:")) ||
		bytes.HasPrefix(b, []byte("id:")) ||
		bytes.HasPrefix(b, []byte("retry:")) ||
		bytes.HasPrefix(b, []byte(":")) ||
		bytes.Equal(b, []byte("data")) ||
		bytes.Equal(b, []byte("event")) ||
		bytes.Equal(b, []byte("id")) ||
		bytes.Equal(b, []byte("retry"))
}

func (a *emptyCompletionAccum) evalSSE(payload []byte) {
	var dataLines [][]byte
	flush := func() {
		if len(dataLines) == 0 {
			return
		}
		data := bytes.Join(dataLines, []byte("\n"))
		dataLines = dataLines[:0]
		if bytes.Equal(data, []byte("[DONE]")) {
			a.recognized = true
			a.terminal = true
			a.sawMessageData = true
			return
		}
		if len(data) == 0 {
			a.sawMetadataOnly = true
			return
		}
		if !a.evalJSON(data) {
			a.sawUnknownData = true
		}
	}

	processSingle := func(line []byte) {
		if bytes.HasPrefix(line, []byte("event:")) {
			event := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("event:")))
			if bytes.Equal(event, []byte("message_stop")) {
				a.recognized = true
				a.terminal = true
				a.sawMessageData = true
			} else {
				a.sawMetadataOnly = true
			}
			return
		}
		if bytes.Equal(line, []byte("event")) {
			a.sawMetadataOnly = true
			return
		}
		if bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) || bytes.HasPrefix(line, []byte(":")) {
			a.sawMetadataOnly = true
			return
		}
		if bytes.Equal(line, []byte("id")) || bytes.Equal(line, []byte("retry")) {
			a.sawMetadataOnly = true
			return
		}
		switch {
		case bytes.HasPrefix(line, []byte("data:")):
			dataLines = append(dataLines, parseSSEDataLine(line))
		case bytes.Equal(line, []byte("data")):
			dataLines = append(dataLines, []byte(""))
		case bytes.HasPrefix(line, []byte("{")), bytes.HasPrefix(line, []byte("[")):
			// Some executors translate upstream SSE into the client format and
			// emit raw JSON payloads without SSE framing (the HTTP handler adds
			// the data: prefix later). Treat bare JSON lines as chunk data.
			dataLines = append(dataLines, line)
		default:
			a.sawUnknownData = true
		}
	}

	processLine := func(line []byte) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			flush()
			return
		}
		processSingle(line)
	}

	for _, line := range bytes.Split(payload, []byte("\n")) {
		processLine(line)
	}
	flush()
}

// markEmptyCompletion records a failed retriable empty-completion result and
// returns the error to propagate. The mixed duty execution path rotates on an
// empty completion; the home (credits) path reports it via reportHomeResult.
func (m *Manager) markEmptyCompletion(ctx context.Context, result *Result) error {
	result.Success = false
	result.Error = errEmptyCompletion
	m.MarkResult(ctx, *result)
	return errEmptyCompletion
}
