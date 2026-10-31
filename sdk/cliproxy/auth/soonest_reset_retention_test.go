package auth

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// Sparse 5h snapshots must not turn a known weekly account into a 5h-only plan.
func TestSoonestResetSparseShortSnapshotKeepsWeeklyOrder(t *testing.T) {
	now := soonestResetTestNow
	a := claudeSubscription("a", now, claudeWeeklySignals(now, now.Add(24*time.Hour)))
	b := claudeSubscription("b", now, claudeWeeklySignals(now, now.Add(72*time.Hour)))
	short := claudeSubscription("a", now.Add(time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Status": "allowed",
		"Anthropic-Ratelimit-Unified-5h-Reset":  epochSignal(now.Add(time.Hour)),
	})
	carried := soonestResetFactsFor(short).carry(soonestResetFactsFor(a))
	if got := pickSoonestResetAuth([]*Auth{short, b}, now.Add(time.Minute), map[string]soonestResetFacts{"a": carried}, nil, false); got.ID != "a" {
		t.Fatalf("sparse 5h winner = %s, want a (known earlier weekly reset)", got.ID)
	}
	// Once that weekly window passes, it must be relearned, not become a 5h-only plan.
	short.Quota.ObservedAt = now.Add(25 * time.Hour)
	expired := soonestResetFactsFor(short).carry(carried)
	if rank := expired.rank(now.Add(25*time.Hour), false); rank.class != soonestResetClassProbe || !rank.weeklyReset.IsZero() {
		t.Fatalf("expired sparse weekly rank = %+v, want an unknown probe", rank)
	}
}

// A weekly-only snapshot must not clear a still-exhausted short window.
func TestSoonestResetSparseWeeklySnapshotKeepsShortExhaustion(t *testing.T) {
	now := soonestResetTestNow
	signals := claudeWeeklySignals(now, now.Add(24*time.Hour))
	signals["Anthropic-Ratelimit-Unified-5h-Status"] = "rejected"
	signals["Anthropic-Ratelimit-Unified-5h-Reset"] = epochSignal(now.Add(time.Hour))
	a := claudeSubscription("a", now, signals)
	b := claudeSubscription("b", now, claudeWeeklySignals(now, now.Add(72*time.Hour)))
	weekly := claudeSubscription("a", now.Add(time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status": "allowed",
		"Anthropic-Ratelimit-Unified-7d-Reset":  epochSignal(now.Add(24 * time.Hour)),
	})
	carried := soonestResetFactsFor(weekly).carry(soonestResetFactsFor(a))
	if got := pickSoonestResetAuth([]*Auth{weekly, b}, now.Add(time.Minute), map[string]soonestResetFacts{"a": carried}, nil, false); got.ID != "b" {
		t.Fatalf("weekly-only winner = %s, want b (a's short window is exhausted)", got.ID)
	}
	// An explicit observation of recovery in that same window clears the penalty.
	recovered := claudeSubscription("a", now.Add(2*time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Status": "allowed",
		"Anthropic-Ratelimit-Unified-5h-Reset":  epochSignal(now.Add(time.Hour)),
	})
	fresh := soonestResetFactsFor(recovered).carry(carried)
	if got := pickSoonestResetAuth([]*Auth{recovered, b}, now.Add(2*time.Minute), map[string]soonestResetFacts{"a": fresh}, nil, false); got.ID != "a" {
		t.Fatalf("recovered winner = %s, want a", got.ID)
	}
	if rank := carried.rank(now.Add(2*time.Hour), false); rank.exhausted {
		t.Fatal("expired short exhaustion remained active")
	}
}

// An explicit short-window recovery says nothing about weekly exhaustion.
func TestSoonestResetWeeklyExhaustionNeedsWeeklyRecovery(t *testing.T) {
	now := soonestResetTestNow
	previous := soonestResetFactsFor(claudeSubscription("a", now, map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status": "rejected",
		"Anthropic-Ratelimit-Unified-7d-Reset":  epochSignal(now.Add(24 * time.Hour)),
	}))
	short := soonestResetFactsFor(claudeSubscription("a", now.Add(time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Status": "allowed",
	}))
	carried := short.carry(previous)
	if !carried.rank(now.Add(time.Minute), false).exhausted {
		t.Fatal("5h recovery cleared a still-exhausted weekly window")
	}
	weekly := soonestResetFactsFor(claudeSubscription("a", now.Add(2*time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status": "allowed",
	}))
	if weekly.carry(carried).rank(now.Add(2*time.Minute), false).exhausted {
		t.Fatal("explicit weekly recovery retained predecessor exhaustion")
	}
}

// A unified five_hour rejection remains a short-window fact on weekly-only responses.
func TestSoonestResetSparseWeeklyKeepsWindowlessShortRejection(t *testing.T) {
	now := soonestResetTestNow
	known := soonestResetFactsFor(claudeSubscription("a", now, claudeWeeklySignals(now, now.Add(24*time.Hour))))
	rejection := soonestResetFactsFor(claudeSubscription("a", now.Add(time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-Status":               "rejected",
		"Anthropic-Ratelimit-Unified-Representative-Claim": "five_hour",
		"Anthropic-Ratelimit-Unified-Reset":                epochSignal(now.Add(time.Hour)),
	}))
	weekly := soonestResetFactsFor(claudeSubscription("a", now.Add(2*time.Minute), map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status": "allowed",
		"Anthropic-Ratelimit-Unified-7d-Reset":  epochSignal(now.Add(24 * time.Hour)),
	}))
	for name, previous := range map[string]soonestResetFacts{"known weekly": known, "cold account": {}} {
		if !weekly.carry(rejection.carry(previous)).rank(now.Add(2*time.Minute), false).exhausted {
			t.Fatalf("%s: weekly-only snapshot cleared a windowless five_hour rejection", name)
		}
	}
}

// Reset metadata cannot establish recovery from either shared-window rejection.
func TestSoonestResetResetOnlyKeepsExhaustion(t *testing.T) {
	now := soonestResetTestNow
	for _, label := range []string{"5h", "7d"} {
		t.Run(label, func(t *testing.T) {
			reset := now.Add(time.Hour)
			if label == "7d" {
				reset = now.Add(24 * time.Hour)
			}
			prefix := "Anthropic-Ratelimit-Unified-" + label + "-"
			signals := claudeWeeklySignals(now, now.Add(24*time.Hour))
			signals[prefix+"Status"] = "rejected"
			signals[prefix+"Reset"] = epochSignal(reset)
			a := claudeSubscription("a", now, signals)
			b := claudeSubscription("b", now, claudeWeeklySignals(now, now.Add(72*time.Hour)))
			resetOnly := claudeSubscription("a", now.Add(time.Minute), map[string]string{
				prefix + "Reset": epochSignal(reset),
			})
			carried := soonestResetFactsFor(resetOnly).carry(soonestResetFactsFor(a))
			if got := pickSoonestResetAuth([]*Auth{resetOnly, b}, now.Add(time.Minute), map[string]soonestResetFacts{"a": carried}, nil, false); got.ID != "b" {
				t.Fatalf("reset-only winner = %s, want b (a's %s window is still exhausted)", got.ID, label)
			}
		})
	}
}

// Unknown status and invalid utilization must not act as recovery signals.
func TestSoonestResetClaudeRecoveryNeedsUsageEvidence(t *testing.T) {
	now := soonestResetTestNow
	previous := soonestResetFactsFor(claudeSubscription("a", now, map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Status": "rejected",
		"Anthropic-Ratelimit-Unified-7d-Reset":  epochSignal(now.Add(24 * time.Hour)),
	}))
	for _, test := range []struct {
		name      string
		key       string
		value     string
		exhausted bool
	}{
		{"unknown status", "Status", "unknown", true},
		{"invalid negative utilization", "Utilization", "-0.1", true},
		{"allowed warning recovers", "Status", "allowed_warning", false},
		{"valid utilization recovers", "Utilization", "0.2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fresh := soonestResetFactsFor(claudeSubscription("a", now.Add(time.Minute), map[string]string{
				"Anthropic-Ratelimit-Unified-7d-Reset":       epochSignal(now.Add(24 * time.Hour)),
				"Anthropic-Ratelimit-Unified-7d-" + test.key: test.value,
			}))
			if got := fresh.carry(previous).rank(now.Add(time.Minute), false).exhausted; got != test.exhausted {
				t.Fatalf("exhausted = %v, want %v", got, test.exhausted)
			}
		})
	}
}

// Same-ID account replacement must not inherit its predecessor's passive observations.
func TestManagerSoonestResetAccountReplacementDropsRetainedFacts(t *testing.T) {
	for _, affinity := range []bool{false, true} {
		name := "scheduler"
		var selector Selector = &SoonestResetSelector{}
		if affinity {
			name = "affinity fallback"
			selector = NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: selector, TTL: time.Hour})
		}
		t.Run(name, func(t *testing.T) {
			if stoppable, ok := selector.(StoppableSelector); ok {
				t.Cleanup(stoppable.Stop)
			}
			now := time.Now()
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(&soonestResetQuotaExecutor{})
			a := claudeSubscription("identity-a", now, claudeWeeklySignals(now, now.Add(24*time.Hour)))
			a.Metadata["account_uuid"] = "account-a"
			a.Metadata["email"] = "shared@example.test"
			b := claudeSubscription("identity-b", now, claudeWeeklySignals(now, now.Add(72*time.Hour)))
			registered, err := manager.Register(context.Background(), a)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = manager.Register(context.Background(), b); err != nil {
				t.Fatal(err)
			}
			// Token rotation of the same account keeps its known reset even with a sparse snapshot.
			refreshed := registered.Clone()
			refreshed.Metadata["access_token"] = "rotated-same-account-fixture"
			refreshed.Quota = QuotaState{ObservedAt: now.Add(time.Minute), Signals: claudeFastModeRefusalSignals(now)}
			refreshed, err = manager.Update(context.Background(), refreshed)
			if err != nil {
				t.Fatal(err)
			}
			pick := func(want string) {
				t.Helper()
				var got *Auth
				var err error
				if affinity {
					got, err = manager.SelectAuth(context.Background(), "claude", "", cliproxyexecutor.Options{})
				} else {
					got, err = manager.scheduler.pickSingle(context.Background(), "claude", "", cliproxyexecutor.Options{}, nil)
				}
				if err != nil || got == nil || got.ID != want {
					t.Fatalf("winner = %v, %v; want %s", got, err, want)
				}
			}
			pick("identity-a")
			replacement := refreshed.Clone()
			replacement.Metadata["account_uuid"] = "account-c"
			// The token is unchanged: identity protection must not be a token-change heuristic.
			// A watcher snapshot has no quota data; Update must not re-import the predecessor's.
			replacement.Quota = QuotaState{}
			data := []byte(`{"account_uuid":"account-c"}`)
			path := filepath.Join(t.TempDir(), "replacement.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			replacement.BindFileSnapshot(path, data)
			replacement, err = manager.Update(context.Background(), replacement)
			if err != nil {
				t.Fatal(err)
			}
			if !replacement.Quota.ObservedAt.IsZero() || len(replacement.Quota.Signals) != 0 {
				t.Error("replacement inherited predecessor's passive snapshot")
			}
			// A delayed successful response from the old account must not repopulate that snapshot.
			ctx := internallogging.WithResponseHeadersHolder(context.Background())
			internallogging.SetResponseHeaders(ctx, http.Header{"Anthropic-Ratelimit-Unified-7d-Reset": {epochSignal(now.Add(time.Hour))}})
			manager.MarkResult(ctx, Result{AuthID: refreshed.ID, Provider: "claude", Success: true, AccessToken: authAccessToken(refreshed), CredentialVersion: refreshed.CredentialVersion, RegistrationEpoch: refreshed.RegistrationEpoch})
			if current, _ := manager.GetByID(refreshed.ID); !current.Quota.ObservedAt.IsZero() {
				t.Error("delayed predecessor response repopulated replacement quota")
			}
			// A real request would probe the replacement once. Reserve that probe so the winner
			// distinguishes unknown replacement state from the predecessor's known early reset.
			manager.scheduler.mu.Lock()
			manager.scheduler.providers["claude"].soonestResetProbes.record("identity-a", now)
			manager.scheduler.mu.Unlock()
			if direct, ok := selector.(*SoonestResetSelector); ok {
				direct.probes.record("identity-a", now)
			}
			if wrapper, ok := selector.(*SessionAffinitySelector); ok {
				wrapper.fallback.(*SoonestResetSelector).probes.record("identity-a", now)
			}
			pick("identity-b")
		})
	}
}
