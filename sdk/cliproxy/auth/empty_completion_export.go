package auth

// IsEmptyCompletionPayload reports whether a payload (aggregated SSE chunks or
// a single non-stream JSON response) represents a terminal but empty
// completion. It is the exported form of the internal predicate used by the
// conductor, exposed so the plugin-executor path can reject empty completions
// before they reach the client.
func IsEmptyCompletionPayload(payload []byte) bool {
	return isEmptyCompletionPayload(payload)
}

// EmptyCompletionError returns the retriable error used when upstream returns
// a terminal but empty completion. The plugin-executor path returns it so the
// client receives an error instead of a silent empty response, matching how
// the conductor surfaces empty completions.
func EmptyCompletionError() error {
	return errEmptyCompletion
}

// ExtractExpectedChoices parses the request payload to extract the choice count
// parameter (top-level "n" for OpenAI or "generationConfig.candidateCount" for
// Gemini). Returns 1 if payload is empty, invalid, or choice count is omitted
// or <= 0. Thin wrapper over extractExpectedChoices: pluginhost must not carry
// a second copy of the parser.
func ExtractExpectedChoices(payload []byte) int {
	return extractExpectedChoices(payload)
}

// StreamBootstrapDetector incrementally classifies a stream prefix without
// reparsing previously observed chunks. Its zero value is ready for use.
type StreamBootstrapDetector struct {
	state streamBootstrapState
}

// SetExpectedChoices sets the number of expected choices for multi-choice streams.
// When n <= 0, it defaults to 1.
func (d *StreamBootstrapDetector) SetExpectedChoices(n int) {
	if d != nil {
		d.state.setExpectedChoices(n)
	}
}

// SetRequestPayload parses the request payload to configure expected choice count.
func (d *StreamBootstrapDetector) SetRequestPayload(payload []byte) {
	if d != nil {
		d.state.setExpectedChoices(ExtractExpectedChoices(payload))
	}
}

// Observe records an arbitrary stream byte fragment and reports whether
// buffered data should now be forwarded. It retains incomplete SSE lines across
// calls and forwards conservatively at the bootstrap byte limit.
func (d *StreamBootstrapDetector) Observe(payload []byte) bool {
	if d == nil {
		return true
	}
	return d.state.observe(payload)
}

// HasMeaningfulOutput reports whether any client-visible meaningful output
// (content, tool calls, blocked state, or non-scaffolding data) has been observed.
func (d *StreamBootstrapDetector) HasMeaningfulOutput() bool {
	if d == nil {
		return false
	}
	return d.state.hasMeaningfulOutput()
}

// Finish flushes any trailing pending fragment at EOF and reports whether the
// accumulated stream chunks represent a terminal empty completion.
func (d *StreamBootstrapDetector) Finish() bool {
	if d == nil {
		return false
	}
	d.state.finish()
	return d.state.isEmptyCompletion()
}

// StreamError returns the parsed in-band provider error detected so far, if any.
func (d *StreamBootstrapDetector) StreamError() error {
	if d == nil {
		return nil
	}
	return d.state.streamError()
}

// IsTerminalEmpty reports whether the accumulated stream has reached a terminal
// marker without any meaningful output.
func (d *StreamBootstrapDetector) IsTerminalEmpty() bool {
	if d == nil {
		return false
	}
	return d.state.isTerminalEmpty()
}

// RedactSecrets redacts credential-shaped values using the same recognition
// set as conductor logging and *Error sanitization. Plugin stream wrappers
// must apply it before emitting an error-path payload to a caller.
func RedactSecrets(s string) string {
	return redactSecretsForLog(s)
}

// SanitizeError redacts every exported string field of an *Error. It is the
// exported form of sanitizeErrorTextFields so pluginhost uses the same
// mechanism as wrapStreamResult rather than a second copy.
func SanitizeError(err error) error {
	return sanitizeErrorTextFields(err)
}

// StreamPayloadErrorDetector incrementally detects in-band provider errors in
// stream bytes after bootstrap has already forwarded meaningful output.
type StreamPayloadErrorDetector struct {
	state streamPayloadErrorDetector
}

// Observe records a stream fragment and returns a sanitized in-band error
// when one has been detected. A nil return is not a successful completion.
func (d *StreamPayloadErrorDetector) Observe(payload []byte) error {
	if d == nil {
		return nil
	}
	if err := d.state.Observe(payload); err != nil {
		return err
	}
	return nil
}

// Finish flushes a trailing unterminated fragment and returns a sanitized
// in-band error when one is present.
func (d *StreamPayloadErrorDetector) Finish() error {
	if d == nil {
		return nil
	}
	if err := d.state.Finish(); err != nil {
		return err
	}
	return nil
}
