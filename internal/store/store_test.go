package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/icelanced/witness/internal/models"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return New(mr.Addr())
}

func TestChecksCRUD(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	c := models.Check{ID: "c1", Name: "GitHub", Type: models.CheckHTTP, Target: "https://github.com", Interval: 30}
	if err := s.SaveCheck(ctx, c); err != nil {
		t.Fatalf("SaveCheck: %v", err)
	}

	got, err := s.GetCheck(ctx, "c1")
	if err != nil {
		t.Fatalf("GetCheck: %v", err)
	}
	if got == nil || got.Name != "GitHub" {
		t.Fatalf("GetCheck returned %+v, want a check named GitHub", got)
	}

	list, err := s.ListChecks(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListChecks = %v, %v; want 1 check", list, err)
	}

	if err := s.DeleteCheck(ctx, "c1"); err != nil {
		t.Fatalf("DeleteCheck: %v", err)
	}
	if got, _ := s.GetCheck(ctx, "c1"); got != nil {
		t.Fatal("check should be gone after DeleteCheck")
	}
}

func TestAgentTokenLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.RegisterAgentToken(ctx, "tok-abc", "frankfurt"); err != nil {
		t.Fatalf("RegisterAgentToken: %v", err)
	}

	region, err := s.ResolveAgentToken(ctx, "tok-abc")
	if err != nil || region != "frankfurt" {
		t.Fatalf("ResolveAgentToken = %q, %v; want frankfurt", region, err)
	}

	if _, err := s.ResolveAgentToken(ctx, "does-not-exist"); err == nil {
		t.Fatal("resolving an unknown token should return an error")
	}

	regions, err := s.ListRegions(ctx)
	if err != nil || len(regions) != 1 || regions[0] != "frankfurt" {
		t.Fatalf("ListRegions = %v, %v; want [frankfurt]", regions, err)
	}
}

func TestRevokeRegionCutsOffAccessEverywhere(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.RegisterAgentToken(ctx, "tok-1", "singapore")
	s.SaveCheck(ctx, models.Check{ID: "c1", Name: "Site", Type: models.CheckHTTP, Target: "https://example.com"})
	s.SetRegionStatus(ctx, "c1", models.RegionStatus{Region: "singapore", Up: true, CheckedAt: time.Now()})
	s.TouchRegionLastSeen(ctx, "singapore", time.Now())

	if err := s.RevokeRegion(ctx, "singapore"); err != nil {
		t.Fatalf("RevokeRegion: %v", err)
	}

	if _, err := s.ResolveAgentToken(ctx, "tok-1"); err == nil {
		t.Error("revoked token should no longer resolve")
	}
	statuses, _ := s.GetRegionStatuses(ctx, "c1")
	for _, rs := range statuses {
		if rs.Region == "singapore" {
			t.Error("revoked region's status should be wiped from every check")
		}
	}
	if _, seen := s.RegionLastSeen(ctx, "singapore"); seen {
		t.Error("revoked region's last-seen timestamp should be cleared")
	}
}

func TestUptimePercentAndDailyBuckets(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()

	// 3 up, 1 down in the last 24h.
	s.RecordUptimeSample(ctx, "c1", true, now)
	s.RecordUptimeSample(ctx, "c1", true, now.Add(-time.Minute))
	s.RecordUptimeSample(ctx, "c1", true, now.Add(-2*time.Minute))
	s.RecordUptimeSample(ctx, "c1", false, now.Add(-3*time.Minute))

	pct, err := s.UptimePercent(ctx, "c1", now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("UptimePercent: %v", err)
	}
	if pct != 75.0 {
		t.Errorf("UptimePercent = %.2f, want 75.00", pct)
	}

	// No samples at all for a check should read as 100% (nothing to blame it for yet).
	pctEmpty, _ := s.UptimePercent(ctx, "never-checked", now.Add(-24*time.Hour))
	if pctEmpty != 100.0 {
		t.Errorf("UptimePercent for a check with no samples = %.2f, want 100.00", pctEmpty)
	}

	bars, err := s.DailyBuckets(ctx, "c1", 7)
	if err != nil {
		t.Fatalf("DailyBuckets: %v", err)
	}
	if len(bars) != 7 {
		t.Fatalf("DailyBuckets returned %d entries, want 7", len(bars))
	}
	// Today (last element) should reflect the 75% we just recorded.
	if bars[6] != 75.0 {
		t.Errorf("today's bucket = %.2f, want 75.00", bars[6])
	}
	// Every earlier day has no data and should be the -1 sentinel.
	for i := 0; i < 6; i++ {
		if bars[i] != -1 {
			t.Errorf("bucket[%d] = %.2f, want -1 (no data)", i, bars[i])
		}
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if v, _ := s.GetSetting(ctx, "telegram_bot_token"); v != "" {
		t.Fatalf("unset setting should read as empty string, got %q", v)
	}

	if err := s.SetSetting(ctx, "telegram_bot_token", "abc123"); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	v, err := s.GetSetting(ctx, "telegram_bot_token")
	if err != nil || v != "abc123" {
		t.Fatalf("GetSetting = %q, %v; want abc123", v, err)
	}
}
