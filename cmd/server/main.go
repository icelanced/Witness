package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/icelanced/witness/internal/aggregator"
	"github.com/icelanced/witness/internal/alert"
	"github.com/icelanced/witness/internal/consensus"
	"github.com/icelanced/witness/internal/models"
	"github.com/icelanced/witness/internal/store"
)

type server struct {
	st        *store.Store
	tmpl      *template.Template
	adminUser string
	adminPass string
	limiter   *rateLimiter
}

func main() {
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	adminUser := getenv("ADMIN_USER", "admin")
	adminPass := getenv("ADMIN_PASSWORD", "changeme")
	port := getenv("PORT", "8080")

	st := store.New(redisAddr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := st.Ping(ctx); err != nil {
		log.Fatalf("cannot reach redis at %s: %v", redisAddr, err)
	}
	if err := st.EnsureConsumerGroup(ctx); err != nil {
		log.Fatalf("cannot set up consumer group: %v", err)
	}
	seedAgentTokens(ctx, st) // optional, for docker-compose demo: SEED_AGENT_TOKENS="region:token,region:token"
	seedDemoChecks(ctx, st)  // optional, for docker-compose demo: SEED_DEMO_CHECKS=1

	agg := aggregator.New(st)
	go agg.Run(ctx)
	go agg.WatchAgents(ctx)
	go agg.WatchTLSExpiry(ctx)

	tmpl := template.Must(template.New("").Funcs(template.FuncMap{
		"barColor": barColor,
		"dayLabel": dayLabel,
	}).ParseGlob("web/templates/*.html"))

	s := &server{st: st, tmpl: tmpl, adminUser: adminUser, adminPass: adminPass, limiter: newRateLimiter()}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Public status page — no auth, this is what you'd put on status.yourdomain.com
	r.Get("/status", s.handleStatusPage)
	r.Get("/status/partial", s.handleStatusPartial)

	// Agent-facing API, authenticated with a per-region bearer token.
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(s.agentAuth)
		r.Get("/checks", s.handleListChecksJSON)
		r.Post("/results", s.handleSubmitResult)
	})

	// Admin dashboard, protected with HTTP Basic Auth (ADMIN_USER/ADMIN_PASSWORD),
	// CSRF protection on state-changing requests, and a login-attempt rate limit.
	r.Group(func(r chi.Router) {
		r.Use(s.basicAuth)
		r.Use(s.csrfProtect)
		r.Get("/", s.handleDashboard)
		r.Get("/partial", s.handleDashboardPartial)
		r.Post("/admin/checks", s.handleCreateCheck)
		r.Post("/admin/checks/{id}/delete", s.handleDeleteCheck)
		r.Post("/admin/checks/{id}/toggle-mute", s.handleToggleMute)
		r.Post("/admin/regions", s.handleCreateRegion)
		r.Post("/admin/regions/revoke", s.handleRevokeRegion)
		r.Post("/admin/settings/telegram", s.handleSaveTelegramSettings)
		r.Post("/admin/settings/telegram/test", s.handleTestTelegram)
	})

	log.Printf("witness server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

// ---- rate limiting (in-memory sliding window, single-process — fine for a
// single self-hosted instance; doesn't survive a restart, which is fine
// since the point is slowing down a live brute-force, not perfect history) ----

type rateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{attempts: map[string][]time.Time{}}
}

// Allow returns false once `limit` calls for this key have happened within
// `window`; the caller should reject the request when it does. Every call
// counts, whether or not the caller ends up rejecting the request — use
// this when you want to cap total attempts regardless of outcome (e.g. "no
// more than 5 test-alert clicks a minute").
func (rl *rateLimiter) Allow(key string, limit int, window time.Duration) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-window)
	kept := rl.attempts[key][:0]
	for _, t := range rl.attempts[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= limit {
		rl.attempts[key] = kept
		return false
	}
	rl.attempts[key] = append(kept, now)
	return true
}

// Blocked checks whether `limit` failures have already been recorded for
// this key within `window`, without recording a new attempt itself. Pair
// with Record so that only failures count against the limit — successful
// requests (like a legitimately logged-in admin's polling) never do.
func (rl *rateLimiter) Blocked(key string, limit int, window time.Duration) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-window)
	kept := rl.attempts[key][:0]
	for _, t := range rl.attempts[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	rl.attempts[key] = kept
	return len(kept) >= limit
}

func (rl *rateLimiter) Record(key string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.attempts[key] = append(rl.attempts[key], time.Now())
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---- auth middleware ----

func (s *server) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := "login:" + clientIP(r)
		if s.limiter.Blocked(key, 10, 5*time.Minute) {
			http.Error(w, "too many failed attempts, try again later", http.StatusTooManyRequests)
			return
		}

		user, pass, ok := r.BasicAuth()
		// Constant-time comparisons so a login attempt can't be timed to
		// leak how many leading characters of the password were correct.
		validUser := subtle.ConstantTimeCompare([]byte(user), []byte(s.adminUser)) == 1
		validPass := subtle.ConstantTimeCompare([]byte(pass), []byte(s.adminPass)) == 1
		if !ok || !validUser || !validPass {
			s.limiter.Record(key) // only failures count against the limit
			w.Header().Set("WWW-Authenticate", `Basic realm="witness"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// csrfProtect implements the double-submit-cookie pattern: handleDashboard
// sets a random csrf_token cookie and embeds the same value in every form's
// hidden field when it renders the page. A cross-site page can trigger a
// POST with the browser's cached Basic Auth credentials attached, but it
// can't read or set our cookie, so its form won't carry a matching token.
func (s *server) csrfProtect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie("csrf_token")
		r.ParseForm()
		formToken := r.FormValue("csrf_token")
		if err != nil || cookie.Value == "" || formToken == "" ||
			!hmac.Equal([]byte(cookie.Value), []byte(formToken)) {
			http.Error(w, "invalid or missing csrf token — reload the page and try again", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) agentAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		token := strings.TrimPrefix(authz, "Bearer ")
		if token == "" || token == authz {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		region, err := s.st.ResolveAgentToken(r.Context(), token)
		if err != nil {
			http.Error(w, "invalid agent token", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), regionCtxKey{}, region)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type regionCtxKey struct{}

// ---- agent API handlers ----

func (s *server) handleListChecksJSON(w http.ResponseWriter, r *http.Request) {
	checks, err := s.st.ListChecks(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(checks)
}

func (s *server) handleSubmitResult(w http.ResponseWriter, r *http.Request) {
	region, _ := r.Context().Value(regionCtxKey{}).(string)

	var res models.Result
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, "bad payload", http.StatusBadRequest)
		return
	}

	// Reject results for a check_id that doesn't exist (anymore). Without
	// this, any valid agent token could submit results for arbitrary/garbage
	// check_ids and grow region_status:* keys in Redis without bound, or
	// resurrect a deleted check's status under its old id.
	check, err := s.st.GetCheck(r.Context(), res.CheckID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if check == nil {
		http.Error(w, "unknown check_id", http.StatusBadRequest)
		return
	}

	res.Region = region
	if res.Timestamp.IsZero() {
		res.Timestamp = time.Now()
	}
	if err := s.st.PublishResult(r.Context(), res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if res.TLSExpiresAt != nil {
		s.st.SetTLSExpiry(r.Context(), res.CheckID, *res.TLSExpiresAt)
	}
	s.st.TouchRegionLastSeen(r.Context(), region, res.Timestamp)
	w.WriteHeader(http.StatusAccepted)
}

// ---- dashboard (admin) handlers ----

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data.NewToken = r.URL.Query().Get("new_token")
	data.TelegramTest = r.URL.Query().Get("telegram_test")
	token, _ := s.st.GetSetting(r.Context(), "telegram_bot_token")
	chatID, _ := s.st.GetSetting(r.Context(), "telegram_chat_id")
	data.TelegramSet = token != "" && chatID != ""

	csrfToken := randomToken()
	http.SetCookie(w, &http.Cookie{
		Name:     "csrf_token",
		Value:    csrfToken,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	})
	data.CSRFToken = csrfToken

	s.render(w, "dashboard.html", data)
}

func (s *server) handleDashboardPartial(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Reuse whatever token the current page load already has — this partial
	// is polled by htmx every few seconds and must not invalidate the forms
	// still sitting in the surrounding (unrefreshed) page.
	if c, err := r.Cookie("csrf_token"); err == nil {
		data.CSRFToken = c.Value
	}
	s.render(w, "checks_partial.html", data)
}

func (s *server) handleCreateCheck(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	c := models.Check{
		ID:       uuid.NewString(),
		Name:     r.FormValue("name"),
		Type:     models.CheckType(r.FormValue("type")),
		Target:   r.FormValue("target"),
		Interval: 30,
	}
	if err := s.st.SaveCheck(r.Context(), c); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleDeleteCheck(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	s.st.DeleteCheck(r.Context(), id)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleToggleMute(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	c, err := s.st.GetCheck(r.Context(), id)
	if err != nil || c == nil {
		http.Error(w, "check not found", http.StatusNotFound)
		return
	}
	if err := s.st.SetCheckMuted(r.Context(), id, !c.Muted); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleCreateRegion(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	region := r.FormValue("region")
	token := randomToken()
	if err := s.st.RegisterAgentToken(r.Context(), token, region); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/?new_token="+token, http.StatusSeeOther)
}

func (s *server) handleRevokeRegion(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	region := r.FormValue("region")
	if err := s.st.RevokeRegion(r.Context(), region); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleSaveTelegramSettings(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	token := strings.TrimSpace(r.FormValue("bot_token"))
	chatID := strings.TrimSpace(r.FormValue("chat_id"))
	if token != "" {
		s.st.SetSetting(r.Context(), "telegram_bot_token", token)
	}
	if chatID != "" {
		s.st.SetSetting(r.Context(), "telegram_chat_id", chatID)
	}
	http.Redirect(w, r, "/?telegram_saved=1", http.StatusSeeOther)
}

func (s *server) handleTestTelegram(w http.ResponseWriter, r *http.Request) {
	if !s.limiter.Allow("tgtest:"+clientIP(r), 5, time.Minute) {
		http.Error(w, "too many test requests, slow down", http.StatusTooManyRequests)
		return
	}
	ctx := r.Context()
	token, _ := s.st.GetSetting(ctx, "telegram_bot_token")
	chatID, _ := s.st.GetSetting(ctx, "telegram_chat_id")
	notifier := alert.NewTelegram(token, chatID)

	if !notifier.Enabled() {
		http.Redirect(w, r, "/?telegram_test=not_configured", http.StatusSeeOther)
		return
	}
	if err := notifier.Send("🔔 Тестовое уведомление от witness. Если ты это видишь — алерты настроены правильно."); err != nil {
		log.Printf("telegram test failed: %v", err)
		http.Redirect(w, r, "/?telegram_test=failed", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/?telegram_test=ok", http.StatusSeeOther)
}

// ---- public status page ----

func (s *server) handleStatusPage(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.render(w, "status.html", data)
}

func (s *server) handleStatusPartial(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.render(w, "status_partial.html", data)
}

// ---- shared view-model building ----

type regionView struct {
	Region    string
	Up        bool
	Stale     bool
	LatencyMs int64
}

type checkView struct {
	Check     models.Check
	Overall   string
	Regions   []regionView
	UpCount   int
	Total     int
	Uptime24h float64
	Uptime30d float64
	DailyBars []float64
}

type dashboardData struct {
	Checks       []checkView
	Regions      []regionAgentView
	NewToken     string
	TelegramSet  bool
	TelegramTest string // "", "ok", "failed", "not_configured"
	CSRFToken    string
}

type regionAgentView struct {
	Name  string
	Stale bool
	Seen  bool // false if this region has never sent a single result yet
}

func (s *server) buildDashboardData(ctx context.Context) (dashboardData, error) {
	checks, err := s.st.ListChecks(ctx)
	if err != nil {
		return dashboardData{}, err
	}
	regionNames, _ := s.st.ListRegions(ctx)
	now := time.Now()

	agentViews := make([]regionAgentView, 0, len(regionNames))
	for _, name := range regionNames {
		lastSeen, seen := s.st.RegionLastSeen(ctx, name)
		stale := !seen || now.Sub(lastSeen) > consensus.StaleAfter
		agentViews = append(agentViews, regionAgentView{Name: name, Stale: stale, Seen: seen})
	}

	views := make([]checkView, 0, len(checks))
	for _, c := range checks {
		regionStatuses, _ := s.st.GetRegionStatuses(ctx, c.ID)
		overall, upCount, total := consensus.Evaluate(regionStatuses, now)
		uptime24h, _ := s.st.UptimePercent(ctx, c.ID, now.Add(-24*time.Hour))
		uptime30d, _ := s.st.UptimePercent(ctx, c.ID, now.Add(-30*24*time.Hour))
		bars, _ := s.st.DailyBuckets(ctx, c.ID, 30)

		regionViews := make([]regionView, 0, len(regionStatuses))
		for _, rs := range regionStatuses {
			regionViews = append(regionViews, regionView{
				Region:    rs.Region,
				Up:        rs.Up,
				Stale:     consensus.IsStale(rs, now),
				LatencyMs: rs.LatencyMs,
			})
		}

		views = append(views, checkView{
			Check:     c,
			Overall:   overall,
			Regions:   regionViews,
			UpCount:   upCount,
			Total:     total,
			Uptime24h: uptime24h,
			Uptime30d: uptime30d,
			DailyBars: bars,
		})
	}

	// Broken checks matter more than healthy ones — sort so down/degraded
	// checks float to the top instead of getting buried in a long list of
	// green ones. Stable sort keeps insertion order within the same
	// severity, so this doesn't reshuffle checks that share a status.
	sort.SliceStable(views, func(i, j int) bool {
		return severityRank(views[i].Overall) < severityRank(views[j].Overall)
	})

	return dashboardData{Checks: views, Regions: agentViews}, nil
}

func (s *server) render(w http.ResponseWriter, name string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		log.Printf("template error (%s): %v", name, err)
		http.Error(w, "render error", http.StatusInternalServerError)
	}
}

// severityRank orders check statuses worst-first, so buildDashboardData can
// sort broken checks to the top of the list.
func severityRank(status string) int {
	switch status {
	case "down":
		return 0
	case "degraded":
		return 1
	case "unknown":
		return 2
	default: // "up"
		return 3
	}
}

func barColor(pct float64) string {
	switch {
	case pct < 0:
		return "bar-empty"
	case pct >= 99.5:
		return "bar-up"
	case pct >= 95:
		return "bar-degraded"
	default:
		return "bar-down"
	}
}

// dayLabel turns a bar's index in the DailyBars slice (oldest first, today
// last) into a short human label for the hover tooltip.
func dayLabel(index, total int) string {
	daysAgo := total - 1 - index
	switch daysAgo {
	case 0:
		return "Today"
	case 1:
		return "Yesterday"
	default:
		return fmt.Sprintf("%d days ago", daysAgo)
	}
}

func randomToken() string {
	b := make([]byte, 20)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// seedAgentTokens lets docker-compose (or any first-boot script) pre-register
// known region/token pairs without clicking through the dashboard, e.g.
// SEED_AGENT_TOKENS="frankfurt:demo-token-1,singapore:demo-token-2".
// Real deployments generate tokens from the dashboard instead.
func seedAgentTokens(ctx context.Context, st *store.Store) {
	raw := os.Getenv("SEED_AGENT_TOKENS")
	if raw == "" {
		return
	}
	for _, pair := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) != 2 {
			continue
		}
		region, token := parts[0], parts[1]
		if err := st.RegisterAgentToken(ctx, token, region); err != nil {
			log.Printf("seed token for %s failed: %v", region, err)
		} else {
			log.Printf("seeded agent token for region %q", region)
		}
	}
}

func seedDemoChecks(ctx context.Context, st *store.Store) {
	if os.Getenv("SEED_DEMO_CHECKS") == "" {
		return
	}
	existing, _ := st.ListChecks(ctx)
	if len(existing) > 0 {
		return
	}
	demo := []models.Check{
		{ID: uuid.NewString(), Name: "GitHub", Type: models.CheckHTTP, Target: "https://github.com", Interval: 20},
		{ID: uuid.NewString(), Name: "Cloudflare DNS", Type: models.CheckTCP, Target: "1.1.1.1:53", Interval: 20},
	}
	for _, c := range demo {
		_ = st.SaveCheck(ctx, c)
	}
	log.Println("seeded demo checks")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
