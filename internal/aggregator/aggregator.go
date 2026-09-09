package aggregator

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/icelanced/witness/internal/alert"
	"github.com/icelanced/witness/internal/consensus"
	"github.com/icelanced/witness/internal/models"
	"github.com/icelanced/witness/internal/store"
)

// Aggregator consumes probe results from the shared stream, decides overall
// up/down state per check using cross-region consensus, and persists both
// the live state and the uptime history used for the % and daily bars.
type Aggregator struct {
	st           *store.Store
	consumerName string

	mu           sync.Mutex
	lastStatus   map[string]string // checkID -> last known overall status, for change detection
	agentAliveMu sync.Mutex
	agentAlive   map[string]bool // region -> was it alive last time we checked
}

func New(st *store.Store) *Aggregator {
	return &Aggregator{
		st:           st,
		consumerName: newConsumerName(),
		lastStatus:   map[string]string{},
		agentAlive:   map[string]bool{},
	}
}

// newConsumerName gives each server process a unique identity within the
// Redis consumer group. Without this, running two server instances (e.g.
// for HA) would have them both claim the name "aggregator-1" — Redis would
// then treat later connections as the same consumer reconnecting, so the
// two processes would fight over the same pending-entries list instead of
// each getting their own share of the stream.
func newConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("aggregator-%s-%d", host, os.Getpid())
}

// Run blocks, continuously reading from the results stream. Intended to run
// as its own goroutine for the lifetime of the process.
func (a *Aggregator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := a.st.ReadResults(ctx, a.consumerName, 5*time.Second, 50)
		if err != nil {
			log.Printf("aggregator: read error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range msgs {
			raw, ok := m.Values["payload"].(string)
			if !ok {
				a.st.AckResult(ctx, m.ID)
				continue
			}
			var r models.Result
			if err := json.Unmarshal([]byte(raw), &r); err != nil {
				a.st.AckResult(ctx, m.ID)
				continue
			}
			a.handleResult(ctx, r)
			a.st.AckResult(ctx, m.ID)
		}
	}
}

func (a *Aggregator) handleResult(ctx context.Context, r models.Result) {
	// 1. Update this region's live status for the check.
	_ = a.st.SetRegionStatus(ctx, r.CheckID, models.RegionStatus{
		Region:    r.Region,
		Up:        r.Success,
		LatencyMs: r.LatencyMs,
		CheckedAt: r.Timestamp,
	})

	// 2. Recompute consensus across all regions that have reported for this check.
	regions, err := a.st.GetRegionStatuses(ctx, r.CheckID)
	if err != nil {
		log.Printf("aggregator: region status lookup failed: %v", err)
		return
	}
	overall, upCount, total := consensus.Evaluate(regions, time.Now())

	// 3. Record one uptime sample (consensus-level, not per-region) for history/graphs.
	_ = a.st.RecordUptimeSample(ctx, r.CheckID, overall == "up", r.Timestamp)

	// 4. Alert only on a genuine state transition, never on every single poll.
	a.mu.Lock()
	prev := a.lastStatus[r.CheckID]
	a.lastStatus[r.CheckID] = overall
	a.mu.Unlock()

	if shouldAlert(prev, overall) {
		a.sendTransitionAlert(ctx, r.CheckID, prev, overall, regions, upCount, total)
	}
}

// shouldAlert decides whether a status observation is worth notifying about.
// Pulled out as a pure function so the "don't spam on the very first
// observation unless it's already bad" rule can be unit-tested without
// spinning up Redis or a stream.
func shouldAlert(prev, overall string) bool {
	if prev == "" {
		return overall != "up" // first-ever observation: only news if it's bad
	}
	return prev != overall
}

// sendTransitionAlert looks up current Telegram settings (which may have
// been changed from the dashboard since this process started) and, if
// configured, sends a human-readable message naming the check and which
// regions currently disagree.
func (a *Aggregator) sendTransitionAlert(ctx context.Context, checkID, prev, overall string, regions []models.RegionStatus, upCount, total int) {
	token, _ := a.st.GetSetting(ctx, "telegram_bot_token")
	chatID, _ := a.st.GetSetting(ctx, "telegram_chat_id")
	notifier := alert.NewTelegram(token, chatID)
	if !notifier.Enabled() {
		return
	}

	name := checkID
	if c, err := a.st.GetCheck(ctx, checkID); err == nil && c != nil {
		name = c.Name
	}
	name = html.EscapeString(name)

	emoji, headline := statusHeadline(prev, overall)

	now := time.Now()
	var regionLines strings.Builder
	for _, r := range regions {
		mark := "🟢"
		note := "up"
		switch {
		case consensus.IsStale(r, now):
			mark, note = "⚪️", "no data"
		case !r.Up:
			mark, note = "🔴", "down"
		}
		fmt.Fprintf(&regionLines, "\n%s <code>%s</code> — %s", mark, html.EscapeString(r.Region), note)
	}

	fullMsg := fmt.Sprintf("%s <b>%s</b>\n%s (%d/%d regions confirm)%s",
		emoji, name, headline, upCount, total, regionLines.String())

	if err := notifier.Send(fullMsg); err != nil {
		log.Printf("aggregator: telegram send failed: %v", err)
	}
}

// statusHeadline picks the emoji and a short status sentence for a
// transition. The very first observation of a check (prev == "") gets its
// own wording instead of an awkward "unknown → X" arrow.
func statusHeadline(prev, overall string) (emoji, headline string) {
	if prev == "" {
		switch overall {
		case "down":
			return "🔴", "down since the very first check"
		default:
			return "🟡", "partially down since the very first check"
		}
	}
	switch overall {
	case "up":
		return "✅", "is back up"
	case "down":
		return "🔴", "is now down"
	default:
		return "🟡", "is now partially down"
	}
}

// WatchAgents periodically checks whether each registered region has
// reported recently, and alerts on the transition — unlike check status,
// a dead agent produces no events of its own to react to, so this has to
// poll on a timer instead of reacting to the results stream.
func (a *Aggregator) WatchAgents(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkAgentLiveness(ctx)
		}
	}
}

func (a *Aggregator) checkAgentLiveness(ctx context.Context) {
	regions, err := a.st.ListRegions(ctx)
	if err != nil {
		log.Printf("agent watcher: list regions failed: %v", err)
		return
	}
	now := time.Now()

	for _, region := range regions {
		lastSeen, seen := a.st.RegionLastSeen(ctx, region)
		alive := seen && now.Sub(lastSeen) <= consensus.StaleAfter

		a.agentAliveMu.Lock()
		prevAlive, known := a.agentAlive[region]
		a.agentAlive[region] = alive
		a.agentAliveMu.Unlock()

		if !known {
			// First time we've ever checked this region. Only alert if it's
			// already dead (mirrors the "first observation is down" rule
			// used for check-status alerts) — a freshly added region that
			// simply hasn't connected yet isn't news.
			if !alive && seen {
				a.sendAgentAlert(ctx, region, false, lastSeen)
			}
			continue
		}
		if prevAlive != alive {
			a.sendAgentAlert(ctx, region, alive, lastSeen)
		}
	}
}

func (a *Aggregator) sendAgentAlert(ctx context.Context, region string, alive bool, lastSeen time.Time) {
	token, _ := a.st.GetSetting(ctx, "telegram_bot_token")
	chatID, _ := a.st.GetSetting(ctx, "telegram_chat_id")
	notifier := alert.NewTelegram(token, chatID)
	if !notifier.Enabled() {
		return
	}

	safeRegion := html.EscapeString(region)
	var fullMsg string
	if alive {
		fullMsg = fmt.Sprintf("🔌 Agent <code>%s</code> is back online.", safeRegion)
	} else {
		fullMsg = fmt.Sprintf("🔌⚠️ Agent <code>%s</code> hasn't responded in over %d sec (last seen %s ago).\nThis is about your monitoring setup, not your sites — the region just stopped sending data.",
			safeRegion, int(consensus.StaleAfter.Seconds()), time.Since(lastSeen).Round(time.Second))
	}
	if err := notifier.Send(fullMsg); err != nil {
		log.Printf("agent watcher: telegram send failed: %v", err)
	}
}
