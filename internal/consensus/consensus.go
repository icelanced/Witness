package consensus

import (
	"time"

	"github.com/icelanced/witness/internal/models"
)

// StaleAfter is how long we trust a region's last reported status before
// treating it as "not currently voting". Without this, an agent that dies
// (VPS down, container stopped, network partition) leaves its last "up"
// frozen in Redis forever, and the check would never flip to down/degraded
// no matter how long the region has actually been silent.
const StaleAfter = 90 * time.Second

// Evaluate computes the overall status from per-region votes, ignoring any
// region that hasn't reported within StaleAfter. Returns the active
// (non-stale) region count alongside up-votes so callers can still show the
// total configured region count separately if they want to.
func Evaluate(regions []models.RegionStatus, now time.Time) (status string, up int, active int) {
	live := make([]models.RegionStatus, 0, len(regions))
	for _, r := range regions {
		if now.Sub(r.CheckedAt) <= StaleAfter {
			live = append(live, r)
		}
	}

	active = len(live)
	if active == 0 {
		return "unknown", 0, 0
	}
	for _, r := range live {
		if r.Up {
			up++
		}
	}
	switch {
	case up == active:
		return "up", up, active
	case up == 0:
		return "down", up, active
	default:
		return "degraded", up, active
	}
}

// IsStale reports whether a single region's last report is too old to trust.
func IsStale(r models.RegionStatus, now time.Time) bool {
	return now.Sub(r.CheckedAt) > StaleAfter
}
