package models

import "time"

// CheckType defines what kind of probe an agent performs.
type CheckType string

const (
	CheckHTTP CheckType = "http"
	CheckTCP  CheckType = "tcp"
)

// Check is a monitored target, configured on the control plane.
type Check struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Type     CheckType `json:"type"`
	Target   string    `json:"target"`   // URL for http, host:port for tcp
	Interval int       `json:"interval"` // seconds
}

// Result is a single probe outcome reported by one agent for one check.
type Result struct {
	CheckID    string    `json:"check_id"`
	Region     string    `json:"region"`
	Success    bool      `json:"success"`
	LatencyMs  int64     `json:"latency_ms"`
	StatusCode int       `json:"status_code,omitempty"`
	Error      string    `json:"error,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// RegionStatus is the last known state of a check from a specific region.
type RegionStatus struct {
	Region    string    `json:"region"`
	Up        bool      `json:"up"`
	LatencyMs int64     `json:"latency_ms"`
	CheckedAt time.Time `json:"checked_at"`
}

// CheckStatus is the aggregated, consensus view of a check across all regions.
type CheckStatus struct {
	Check      Check          `json:"check"`
	Overall    string         `json:"overall"` // "up", "down", "degraded"
	Regions    []RegionStatus `json:"regions"`
	UptimeDay  float64        `json:"uptime_day"`
	Uptime30d  float64        `json:"uptime_30d"`
	LastChange time.Time      `json:"last_change"`
}
