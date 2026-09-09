package aggregator

import "testing"

func TestShouldAlert(t *testing.T) {
	tests := []struct {
		name    string
		prev    string
		overall string
		want    bool
	}{
		{"first ever observation is up: no news, no alert", "", "up", false},
		{"first ever observation is down: worth knowing immediately", "", "down", true},
		{"first ever observation is degraded: worth knowing immediately", "", "degraded", true},
		{"up staying up: no alert", "up", "up", false},
		{"up transitioning to down: alert", "up", "down", true},
		{"down transitioning to up: alert (recovery)", "down", "up", true},
		{"up transitioning to degraded: alert", "up", "degraded", true},
		{"degraded transitioning to down: alert", "degraded", "down", true},
		{"down staying down: no repeat alert", "down", "down", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldAlert(tt.prev, tt.overall); got != tt.want {
				t.Errorf("shouldAlert(%q, %q) = %v, want %v", tt.prev, tt.overall, got, tt.want)
			}
		})
	}
}

func TestConfirmTransition(t *testing.T) {
	// A single flip should NOT reach the threshold with threshold=2.
	reached, pending, count := confirmTransition("", 0, "down", 2)
	if reached {
		t.Fatal("first observation of a candidate should not confirm at threshold 2")
	}
	if pending != "down" || count != 1 {
		t.Errorf("pending = %q/%d, want down/1", pending, count)
	}

	// A second, agreeing observation SHOULD reach the threshold.
	reached, pending, count = confirmTransition(pending, count, "down", 2)
	if !reached {
		t.Fatal("second agreeing observation should confirm at threshold 2")
	}
	if pending != "" || count != 0 {
		t.Errorf("pending state should reset after confirming, got %q/%d", pending, count)
	}
}

func TestConfirmTransitionResetsOnDisagreement(t *testing.T) {
	// Start building toward "down"...
	_, pending, count := confirmTransition("", 0, "down", 3)
	// ...then a single "degraded" result interrupts the run entirely,
	// rather than adding to a mixed count — flapping between two
	// different bad states shouldn't confirm faster than flapping between
	// good and bad.
	reached, pending, count := confirmTransition(pending, count, "degraded", 3)
	if reached {
		t.Fatal("a disagreeing observation should not confirm anything")
	}
	if pending != "degraded" || count != 1 {
		t.Errorf("disagreement should restart the count on the new candidate, got %q/%d", pending, count)
	}
}

func TestConfirmTransitionRequiresFullThreshold(t *testing.T) {
	pending, count := "", 0
	for i := 0; i < 4; i++ {
		reached, p, c := confirmTransition(pending, count, "down", 5)
		pending, count = p, c
		if reached {
			t.Fatalf("should not confirm before reaching threshold (confirmed early at observation %d)", i+1)
		}
	}
	reached, _, _ := confirmTransition(pending, count, "down", 5)
	if !reached {
		t.Fatal("should confirm on the 5th consecutive agreeing observation")
	}
}

func TestStatusHeadline(t *testing.T) {
	tests := []struct {
		name          string
		prev, overall string
		wantEmoji     string
	}{
		{"first observation down gets its own wording, not an arrow", "", "down", "🔴"},
		{"recovery is celebratory", "down", "up", "✅"},
		{"going down is a red alert", "up", "down", "🔴"},
		{"degrading is a yellow warning", "up", "degraded", "🟡"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emoji, headline := statusHeadline(tt.prev, tt.overall)
			if emoji != tt.wantEmoji {
				t.Errorf("emoji = %q, want %q", emoji, tt.wantEmoji)
			}
			if headline == "" {
				t.Error("headline should never be empty")
			}
		})
	}
}
