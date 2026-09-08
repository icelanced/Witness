package consensus

import (
	"testing"
	"time"

	"github.com/icelanced/witness/internal/models"
)

func rs(region string, up bool, age time.Duration, now time.Time) models.RegionStatus {
	return models.RegionStatus{Region: region, Up: up, CheckedAt: now.Add(-age)}
}

func TestEvaluate(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		regions    []models.RegionStatus
		wantStatus string
		wantUp     int
		wantActive int
	}{
		{
			name:       "no regions at all",
			regions:    nil,
			wantStatus: "unknown",
		},
		{
			name: "all regions agree up",
			regions: []models.RegionStatus{
				rs("a", true, 0, now),
				rs("b", true, 0, now),
			},
			wantStatus: "up", wantUp: 2, wantActive: 2,
		},
		{
			name: "all regions agree down",
			regions: []models.RegionStatus{
				rs("a", false, 0, now),
				rs("b", false, 0, now),
			},
			wantStatus: "down", wantUp: 0, wantActive: 2,
		},
		{
			name: "split vote is degraded, not down",
			regions: []models.RegionStatus{
				rs("a", true, 0, now),
				rs("b", false, 0, now),
			},
			wantStatus: "degraded", wantUp: 1, wantActive: 2,
		},
		{
			name: "a stale region is excluded from voting entirely",
			regions: []models.RegionStatus{
				rs("a", true, 0, now),
				rs("b", true, 0, now),
				rs("stale-liar", false, 5*time.Minute, now), // reported down, but ages ago
			},
			wantStatus: "up", wantUp: 2, wantActive: 2,
		},
		{
			name: "every region stale -> unknown, not up or down",
			regions: []models.RegionStatus{
				rs("a", true, 5*time.Minute, now),
				rs("b", false, 10*time.Minute, now),
			},
			wantStatus: "unknown", wantUp: 0, wantActive: 0,
		},
		{
			name: "a region exactly at the staleness boundary still counts",
			regions: []models.RegionStatus{
				rs("a", true, StaleAfter-time.Second, now),
			},
			wantStatus: "up", wantUp: 1, wantActive: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, up, active := Evaluate(tt.regions, now)
			if status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if up != tt.wantUp {
				t.Errorf("up = %d, want %d", up, tt.wantUp)
			}
			if active != tt.wantActive {
				t.Errorf("active = %d, want %d", active, tt.wantActive)
			}
		})
	}
}

func TestIsStale(t *testing.T) {
	now := time.Now()

	fresh := rs("a", true, 10*time.Second, now)
	if IsStale(fresh, now) {
		t.Error("a region checked 10s ago should not be stale")
	}

	old := rs("b", true, 5*time.Minute, now)
	if !IsStale(old, now) {
		t.Error("a region checked 5 minutes ago should be stale")
	}
}
