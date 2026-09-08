package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"

	"github.com/icelanced/witness/internal/models"
	"github.com/icelanced/witness/internal/store"
)

func TestRateLimiterAllow(t *testing.T) {
	rl := newRateLimiter()

	for i := 0; i < 5; i++ {
		if !rl.Allow("k", 5, time.Minute) {
			t.Fatalf("call %d should be allowed (under limit)", i+1)
		}
	}
	if rl.Allow("k", 5, time.Minute) {
		t.Fatal("6th call should be blocked once the limit is reached")
	}
	// A different key has its own independent budget.
	if !rl.Allow("other-key", 5, time.Minute) {
		t.Fatal("a different key should not be affected by another key's limit")
	}
}

func TestRateLimiterBlockedDoesNotConsume(t *testing.T) {
	rl := newRateLimiter()

	// Blocked() alone should never trip the limit — only Record() should.
	for i := 0; i < 100; i++ {
		if rl.Blocked("k", 10, time.Minute) {
			t.Fatalf("Blocked() should not itself count as an attempt (call %d)", i+1)
		}
	}
	for i := 0; i < 10; i++ {
		rl.Record("k")
	}
	if !rl.Blocked("k", 10, time.Minute) {
		t.Fatal("after 10 recorded failures, key should be blocked at limit=10")
	}
}

func TestRateLimiterWindowExpires(t *testing.T) {
	rl := newRateLimiter()
	rl.Record("k")
	rl.Record("k")
	// A window shorter than "now" means the recorded attempts have already aged out.
	if rl.Blocked("k", 1, time.Nanosecond) {
		t.Fatal("attempts older than the window should no longer count")
	}
}

func TestCSRFProtectBlocksMismatchedOrMissingToken(t *testing.T) {
	s := &server{limiter: newRateLimiter()}
	protected := s.csrfProtect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// No cookie, no form value at all -> forbidden.
	req := httptest.NewRequest(http.MethodPost, "/admin/checks", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("missing csrf token: got %d, want 403", rec.Code)
	}

	// Cookie present but form token doesn't match -> forbidden.
	req2 := httptest.NewRequest(http.MethodPost, "/admin/checks?csrf_token=wrong", nil)
	req2.AddCookie(&http.Cookie{Name: "csrf_token", Value: "right"})
	rec2 := httptest.NewRecorder()
	protected.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Errorf("mismatched csrf token: got %d, want 403", rec2.Code)
	}

	// Cookie and form token match -> allowed through.
	req3 := httptest.NewRequest(http.MethodPost, "/admin/checks?csrf_token=right", nil)
	req3.AddCookie(&http.Cookie{Name: "csrf_token", Value: "right"})
	rec3 := httptest.NewRecorder()
	protected.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Errorf("matching csrf token: got %d, want 200", rec3.Code)
	}
}

func TestCSRFProtectSkipsGET(t *testing.T) {
	s := &server{limiter: newRateLimiter()}
	protected := s.csrfProtect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET requests should never be CSRF-checked: got %d, want 200", rec.Code)
	}
}

func TestSubmitResultRejectsUnknownCheckID(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	st := store.New(mr.Addr())
	ctx := context.Background()
	st.SaveCheck(ctx, models.Check{ID: "real-check", Name: "Real", Type: models.CheckHTTP, Target: "https://example.com"})

	s := &server{st: st}
	router := chi.NewRouter()
	router.Post("/api/v1/results", func(w http.ResponseWriter, r *http.Request) {
		rctx := context.WithValue(r.Context(), regionCtxKey{}, "frankfurt")
		s.handleSubmitResult(w, r.WithContext(rctx))
	})

	post := func(checkID string) int {
		body, _ := json.Marshal(models.Result{CheckID: checkID, Success: true, Timestamp: time.Now()})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/results", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post("garbage-id-that-does-not-exist"); code != http.StatusBadRequest {
		t.Errorf("submitting a result for an unknown check_id: got %d, want 400", code)
	}
	if code := post("real-check"); code != http.StatusAccepted {
		t.Errorf("submitting a result for a real check_id: got %d, want 202", code)
	}
}

func TestBasicAuthRateLimitsOnlyFailures(t *testing.T) {
	s := &server{adminUser: "admin", adminPass: "secret", limiter: newRateLimiter()}
	protected := s.basicAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	doReq := func(user, pass string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.SetBasicAuth(user, pass)
		req.RemoteAddr = "1.2.3.4:5555"
		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, req)
		return rec.Code
	}

	// Many successful logins in a row must never trip the limiter.
	for i := 0; i < 20; i++ {
		if code := doReq("admin", "secret"); code != http.StatusOK {
			t.Fatalf("successful login %d got %d, want 200 (limiter should ignore successes)", i+1, code)
		}
	}

	// 10 failures are allowed (as 401s); the 11th should be rate-limited.
	for i := 0; i < 10; i++ {
		if code := doReq("admin", "wrong"); code != http.StatusUnauthorized {
			t.Fatalf("failed login %d got %d, want 401", i+1, code)
		}
	}
	if code := doReq("admin", "wrong"); code != http.StatusTooManyRequests {
		t.Errorf("11th failed login got %d, want 429", code)
	}
	// Even correct credentials are now blocked, since the limiter is
	// per-IP: this is intentional, it's what makes brute-forcing pointless.
	if code := doReq("admin", "secret"); code != http.StatusTooManyRequests {
		t.Errorf("correct login from a rate-limited IP got %d, want 429", code)
	}
}
