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
