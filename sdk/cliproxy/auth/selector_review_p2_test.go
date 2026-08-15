package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

// Regression tests for codex pullrequestreview-4943660625 on PR #4881
// (three P2 findings in selector.go).

// TestCooldownErrorCountsOnlyEligibleAuths is a regression guard for the
// first finding: cooldownCount used to be compared against len(auths),
// including request-excluded entries, so a pool where every pickable auth was
// cooling reported the non-retryable auth_unavailable instead of
// model_cooldown with Retry-After.
func TestCooldownErrorCountsOnlyEligibleAuths(t *testing.T) {
	t.Parallel()

	model := "test-model"
	now := time.Now()
	next := now.Add(60 * time.Second)
	cooled := &Auth{
		ID: "auth-cooled",
		ModelStates: map[string]*ModelState{
			model: {
				Status:         StatusActive,
				Unavailable:    true,
				NextRetryAfter: next,
				Quota: QuotaState{
					Exceeded:      true,
					NextRecoverAt: next,
				},
			},
		},
	}
	excluded := &Auth{
		ID: "auth-excluded",
		ModelStates: map[string]*ModelState{
			model: {Status: StatusActive},
		},
	}

	_, err := getAvailableAuths([]*Auth{cooled, excluded}, "gemini", model, now, map[string]struct{}{"auth-excluded": {}})
	if err == nil {
		t.Fatal("getAvailableAuths() error = nil")
	}
	var mce *modelCooldownError
	if !errors.As(err, &mce) {
		t.Fatalf("getAvailableAuths() error = %T (%v), want *modelCooldownError: excluded auths must not count toward the cooldown decision", err, err)
	}
	if mce.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("StatusCode() = %d, want %d", mce.StatusCode(), http.StatusTooManyRequests)
	}
	if got := mce.Headers().Get("Retry-After"); got == "" {
		t.Fatal("Headers().Get(Retry-After) = empty, want a value")
	}
}

// TestGetAvailableAuthsSkipsNilCandidates is a regression guard for the
// second finding: a nil entry in the auth list used to panic on candidate.ID
// when consulting the exclusion map.
func TestGetAvailableAuthsSkipsNilCandidates(t *testing.T) {
	t.Parallel()

	model := "test-model"
	active := &Auth{
		ID: "auth-active",
		ModelStates: map[string]*ModelState{
			model: {Status: StatusActive},
		},
	}

	got, err := getAvailableAuths([]*Auth{nil, active}, "gemini", model, time.Now())
	if err != nil {
		t.Fatalf("getAvailableAuths() error = %v, want nil", err)
	}
	if len(got) != 1 || got[0] != active {
		t.Fatalf("getAvailableAuths() = %v, want [auth-active]", got)
	}

	_, err = getAvailableAuths([]*Auth{nil}, "gemini", model, time.Now(), map[string]struct{}{"anything": {}})
	if err == nil {
		t.Fatal("getAvailableAuths() with only a nil candidate: error = nil, want auth_unavailable")
	}
	var mce *modelCooldownError
	if errors.As(err, &mce) {
		t.Fatalf("getAvailableAuths() with only a nil candidate: error = %v, must not be modelCooldownError", err)
	}
}

// TestRebindGroupCASRetriesAfterConcurrentWrite is a regression guard for the
// third finding: when CompareAndReplaceGroup loses to a concurrent writer
// between Observe and the rebind, the binding is retried with a fresh
// observation instead of being silently dropped.
func TestRebindGroupCASRetriesAfterConcurrentWrite(t *testing.T) {
	t.Parallel()

	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	cacheKey := "pck:test"
	keys := []string{cacheKey}

	selector.cache.Set(cacheKey, "auth-old")
	gen, authID, aliases, ok := selector.cache.Observe(cacheKey)
	if !ok {
		t.Fatal("Observe() ok = false after Set")
	}

	// Simulate a concurrent writer changing the group between Observe and the
	// compare-and-replace: the first CAS attempt now loses the race.
	selector.cache.Set(cacheKey, "auth-other")

	if !selector.rebindGroupCAS(cacheKey, gen, authID, aliases, "auth-new", keys) {
		t.Fatal("rebindGroupCAS() = false, want success after re-observe and retry")
	}
	_, finalAuth, _, ok := selector.cache.Observe(cacheKey)
	if !ok || finalAuth != "auth-new" {
		t.Fatalf("Observe() authID = %q, ok = %v; want auth-new bound", finalAuth, ok)
	}
}

// TestPickRebindsSplitAffinityGroupsOnFailover is a regression guard for the
// codex P2 finding on PR #4881: when the prompt-cache and conversation
// aliases were previously bound to different auths (split groups) and both
// cached credentials are unavailable, pickCached missed and the splitConflict
// branch skipped the rebind, leaving the session pinned to the dead split
// bindings. The fallback auth must now be reconciled onto both groups.
func TestPickRebindsSplitAffinityGroupsOnFailover(t *testing.T) {
	t.Parallel()

	model := "test-model"
	provider := "gemini"
	primaryKey := provider + "::pck:pk1::" + model
	fallbackKey := provider + "::conv:c1::" + model

	cooled := func(id string) *Auth {
		return &Auth{
			ID: id,
			ModelStates: map[string]*ModelState{
				model: {
					Status:         StatusActive,
					Unavailable:    true,
					NextRetryAfter: time.Now().Add(60 * time.Second),
					Quota: QuotaState{
						Exceeded:      true,
						NextRecoverAt: time.Now().Add(60 * time.Second),
					},
				},
			},
		}
	}
	authA := cooled("auth-a")
	authB := cooled("auth-b")
	authC := &Auth{
		ID: "auth-c",
		ModelStates: map[string]*ModelState{
			model: {Status: StatusActive},
		},
	}

	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	selector.cache.SetAliases("auth-a", primaryKey)
	selector.cache.SetAliases("auth-b", fallbackKey)

	payload := []byte(`{"prompt_cache_key":"pk1","conversation":{"id":"c1"}}`)
	opts := cliproxyexecutor.Options{OriginalRequest: payload, Metadata: map[string]any{}}
	auth, err := selector.Pick(context.Background(), provider, model, opts, []*Auth{authA, authB, authC})
	if err != nil {
		t.Fatalf("Pick() error = %v, want nil", err)
	}
	if auth != authC {
		t.Fatalf("Pick() = %v, want auth-c (only available auth)", auth.ID)
	}

	_, gotPrimary, _, okPrimary := selector.cache.Observe(primaryKey)
	if !okPrimary || gotPrimary != "auth-c" {
		t.Fatalf("primary group after failover = %q (ok=%v), want auth-c", gotPrimary, okPrimary)
	}
	_, gotFallback, _, okFallback := selector.cache.Observe(fallbackKey)
	if !okFallback || gotFallback != "auth-c" {
		t.Fatalf("fallback group after failover = %q (ok=%v), want auth-c", gotFallback, okFallback)
	}
}
