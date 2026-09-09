package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/icelanced/witness/internal/models"
)

const (
	StreamResults = "results_stream"
	ConsumerGroup = "server_group"
)

type Store struct {
	rdb *redis.Client
}

func New(addr string) *Store {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	return &Store{rdb: rdb}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}

// EnsureConsumerGroup creates the consumer group for the results stream if it
// doesn't already exist. Safe to call on every startup.
func (s *Store) EnsureConsumerGroup(ctx context.Context) error {
	err := s.rdb.XGroupCreateMkStream(ctx, StreamResults, ConsumerGroup, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// ---- Checks (config) ----

func (s *Store) SaveCheck(ctx context.Context, c models.Check) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, "checks", c.ID, b)
	pipe.SAdd(ctx, "checks:index", c.ID)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *Store) ListChecks(ctx context.Context) ([]models.Check, error) {
	vals, err := s.rdb.HGetAll(ctx, "checks").Result()
	if err != nil {
		return nil, err
	}
	checks := make([]models.Check, 0, len(vals))
	for _, v := range vals {
		var c models.Check
		if err := json.Unmarshal([]byte(v), &c); err == nil {
			checks = append(checks, c)
		}
	}
	return checks, nil
}

func (s *Store) DeleteCheck(ctx context.Context, id string) error {
	pipe := s.rdb.TxPipeline()
	pipe.HDel(ctx, "checks", id)
	pipe.SRem(ctx, "checks:index", id)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) GetCheck(ctx context.Context, id string) (*models.Check, error) {
	v, err := s.rdb.HGet(ctx, "checks", id).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c models.Check
	if err := json.Unmarshal([]byte(v), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// ---- Agent tokens ----

// RegisterAgentToken maps a bearer token to a region name so agents can
// authenticate without the server needing a full user/auth system.
func (s *Store) RegisterAgentToken(ctx context.Context, token, region string) error {
	return s.rdb.HSet(ctx, "agent_tokens", token, region).Err()
}

func (s *Store) ResolveAgentToken(ctx context.Context, token string) (string, error) {
	region, err := s.rdb.HGet(ctx, "agent_tokens", token).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("unknown or revoked agent token")
	}
	return region, err
}

// RevokeRegion fully removes a region: every agent token mapped to it, its
// liveness tracking, and its last-known status on every check. Used when a
// VPS is decommissioned or compromised — without this there was no way to
// cut off a leaked agent token except editing Redis by hand.
func (s *Store) RevokeRegion(ctx context.Context, region string) error {
	vals, err := s.rdb.HGetAll(ctx, "agent_tokens").Result()
	if err == nil {
		for token, r := range vals {
			if r == region {
				s.rdb.HDel(ctx, "agent_tokens", token)
			}
		}
	}
	s.rdb.HDel(ctx, "region_last_seen", region)

	checks, err := s.ListChecks(ctx)
	if err == nil {
		for _, c := range checks {
			s.rdb.HDel(ctx, "region_status:"+c.ID, region)
		}
	}
	return nil
}

func (s *Store) ListRegions(ctx context.Context) ([]string, error) {
	vals, err := s.rdb.HGetAll(ctx, "agent_tokens").Result()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(vals))
	for _, region := range vals {
		out = append(out, region)
	}
	return out, nil
}

// ---- Region liveness (independent of any single check) ----
//
// A region can be actively probing several checks, so "is this region
// alive" can't be read off any one check's region_status hash — that only
// updates when a result for *that* check comes in. This tracks the most
// recent result seen from each region across every check, so the sidebar
// list of agents reflects real liveness instead of just "a token exists".

func (s *Store) TouchRegionLastSeen(ctx context.Context, region string, at time.Time) error {
	return s.rdb.HSet(ctx, "region_last_seen", region, at.Unix()).Err()
}

func (s *Store) RegionLastSeen(ctx context.Context, region string) (time.Time, bool) {
	v, err := s.rdb.HGet(ctx, "region_last_seen", region).Result()
	if err != nil {
		return time.Time{}, false
	}
	var ts int64
	if _, err := fmt.Sscanf(v, "%d", &ts); err != nil {
		return time.Time{}, false
	}
	return time.Unix(ts, 0), true
}

// ---- Settings (small generic key/value store, e.g. Telegram bot config) ----

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	return s.rdb.HSet(ctx, "settings", key, value).Err()
}

func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	v, err := s.rdb.HGet(ctx, "settings", key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// ---- Results stream (agent -> server transport) ----

func (s *Store) PublishResult(ctx context.Context, r models.Result) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	// Cap the stream at ~10k entries (approximate trim, cheap) so it can't
	// grow forever — XAck only marks a message processed in the consumer
	// group, it never removes it from the stream itself. 10k is generous
	// headroom even if the aggregator falls behind for a while.
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamResults,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]interface{}{"payload": b},
	}).Err()
}

// ReadResults blocks (up to block duration) for new results via the shared
// consumer group, so multiple server replicas could share the work.
func (s *Store) ReadResults(ctx context.Context, consumerName string, block time.Duration, count int64) ([]redis.XMessage, error) {
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{StreamResults, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

func (s *Store) AckResult(ctx context.Context, id string) error {
	return s.rdb.XAck(ctx, StreamResults, ConsumerGroup, id).Err()
}

// ---- Live region status (per check, per region) ----

func (s *Store) SetRegionStatus(ctx context.Context, checkID string, rs models.RegionStatus) error {
	b, err := json.Marshal(rs)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, "region_status:"+checkID, rs.Region, b).Err()
}

func (s *Store) GetRegionStatuses(ctx context.Context, checkID string) ([]models.RegionStatus, error) {
	vals, err := s.rdb.HGetAll(ctx, "region_status:"+checkID).Result()
	if err != nil {
		return nil, err
	}
	out := make([]models.RegionStatus, 0, len(vals))
	for _, v := range vals {
		var rs models.RegionStatus
		if err := json.Unmarshal([]byte(v), &rs); err == nil {
			out = append(out, rs)
		}
	}
	return out, nil
}

// ---- Uptime history (minute-resolution buckets in a sorted set) ----
// score = unix timestamp (minute-aligned), member = "1" or "0" prefixed with ts to stay unique

func (s *Store) RecordUptimeSample(ctx context.Context, checkID string, up bool, at time.Time) error {
	minuteTs := at.Truncate(time.Minute).Unix()
	val := "0"
	if up {
		val = "1"
	}
	member := fmt.Sprintf("%d:%s", minuteTs, val)
	key := "uptime:" + checkID
	pipe := s.rdb.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(minuteTs), Member: member})
	// Trim anything older than 30 days to keep the set bounded.
	cutoff := at.Add(-30 * 24 * time.Hour).Unix()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", cutoff))
	_, err := pipe.Exec(ctx)
	return err
}

// UptimePercent returns the fraction of "up" samples recorded since `since`.
func (s *Store) UptimePercent(ctx context.Context, checkID string, since time.Time) (float64, error) {
	key := "uptime:" + checkID
	members, err := s.rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", since.Unix()),
		Max: "+inf",
	}).Result()
	if err != nil {
		return 100, err
	}
	if len(members) == 0 {
		return 100, nil
	}
	upCount := 0
	for _, m := range members {
		if len(m) > 0 && m[len(m)-1] == '1' {
			upCount++
		}
	}
	return float64(upCount) / float64(len(members)) * 100, nil
}

// DailyBuckets returns up/down ratio per day for the last `days` days, used
// to render the small green/red bar strip on the status page.
func (s *Store) DailyBuckets(ctx context.Context, checkID string, days int) ([]float64, error) {
	key := "uptime:" + checkID
	now := time.Now()
	out := make([]float64, days)
	for i := 0; i < days; i++ {
		dayStart := now.AddDate(0, 0, -i).Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)
		members, err := s.rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
			Min: fmt.Sprintf("%d", dayStart.Unix()),
			Max: fmt.Sprintf("%d", dayEnd.Unix()),
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(members) == 0 {
			out[days-1-i] = -1 // no data
			continue
		}
		up := 0
		for _, m := range members {
			if len(m) > 0 && m[len(m)-1] == '1' {
				up++
			}
		}
		out[days-1-i] = float64(up) / float64(len(members)) * 100
	}
	return out, nil
}
