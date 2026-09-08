package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/icelanced/witness/internal/checker"
	"github.com/icelanced/witness/internal/models"
)

func main() {
	serverURL := mustEnv("SERVER_URL")
	token := mustEnv("AGENT_TOKEN")
	pollInterval := envDuration("POLL_INTERVAL", 15*time.Second)

	client := &http.Client{Timeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("agent starting, server=%s poll=%s", serverURL, pollInterval)

	var mu sync.Mutex
	running := map[string]context.CancelFunc{} // checkID -> stop func for its own ticker

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return
		default:
		}

		checks, err := fetchChecks(client, serverURL, token)
		if err != nil {
			log.Printf("fetch checks failed: %v", err)
			time.Sleep(pollInterval)
			continue
		}

		mu.Lock()
		seen := map[string]bool{}
		for _, c := range checks {
			seen[c.ID] = true
			if _, ok := running[c.ID]; ok {
				continue // already probing this check
			}
			cctx, cancel := context.WithCancel(ctx)
			running[c.ID] = cancel
			go probeLoop(cctx, client, serverURL, token, c)
		}
		// stop probing checks that were removed on the server
		for id, cancel := range running {
			if !seen[id] {
				cancel()
				delete(running, id)
			}
		}
		mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
}

func probeLoop(ctx context.Context, client *http.Client, serverURL, token string, c models.Check) {
	interval := time.Duration(c.Interval) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		result := checker.Run(reqCtx, c)
		if err := submitResult(client, serverURL, token, result); err != nil {
			log.Printf("submit result for %s failed: %v", c.Name, err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

func fetchChecks(client *http.Client, serverURL, token string) ([]models.Check, error) {
	req, err := http.NewRequest(http.MethodGet, serverURL+"/api/v1/checks", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var checks []models.Check
	if err := json.NewDecoder(resp.Body).Decode(&checks); err != nil {
		return nil, err
	}
	return checks, nil
}

func submitResult(client *http.Client, serverURL, token string, r models.Result) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/results", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
