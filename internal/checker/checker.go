package checker

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/icelanced/witness/internal/models"
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
	},
}

// Run executes a single probe for the given check and returns a Result
// (region is filled in by the caller, this package only knows about the
// network side of things).
func Run(ctx context.Context, c models.Check) models.Result {
	start := time.Now()

	switch c.Type {
	case models.CheckTCP:
		return runTCP(ctx, c, start)
	default:
		return runHTTP(ctx, c, start)
	}
}

func runHTTP(ctx context.Context, c models.Check, start time.Time) models.Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Target, nil)
	if err != nil {
		return models.Result{CheckID: c.ID, Success: false, Error: err.Error(), Timestamp: start}
	}
	req.Header.Set("User-Agent", "witness-agent/1.0")

	resp, err := httpClient.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return models.Result{
			CheckID:   c.ID,
			Success:   false,
			LatencyMs: latency,
			Error:     err.Error(),
			Timestamp: start,
		}
	}
	defer resp.Body.Close()

	success := resp.StatusCode >= 200 && resp.StatusCode < 400
	return models.Result{
		CheckID:    c.ID,
		Success:    success,
		LatencyMs:  latency,
		StatusCode: resp.StatusCode,
		Timestamp:  start,
	}
}

func runTCP(ctx context.Context, c models.Check, start time.Time) models.Result {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.Target)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return models.Result{
			CheckID:   c.ID,
			Success:   false,
			LatencyMs: latency,
			Error:     err.Error(),
			Timestamp: start,
		}
	}
	defer conn.Close()

	return models.Result{
		CheckID:   c.ID,
		Success:   true,
		LatencyMs: latency,
		Timestamp: start,
	}
}
