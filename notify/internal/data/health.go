package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var _ biz.ChannelHealthRepo = (*healthRepo)(nil)

// healthRepo 渠道共享熔断状态（notify_channel_health）：单语句 upsert 在行锁下
// 原子重算，多实例同一视图。
type healthRepo struct {
	data *Data
}

// NewHealthRepo new a healthRepo.
func NewHealthRepo(data *Data) *healthRepo { return &healthRepo{data: data} }

// 熔断参数与 biz.Breaker 一致：60s 滑窗、≥10 样本、失败率 >50%、打开 60s；
// 探测名额被领取后 30s 无结果视为探测方失联，允许他人接管。
const (
	healthWindow    = "interval '60 seconds'"
	healthOpenFor   = "interval '60 seconds'"
	healthProbeTTL  = "interval '30 seconds'"
	healthMinSample = 10
)

// Record implements biz.ChannelHealthRepo。$2 为本次失败计数（0/1）。
// 分支：probing（本次是探测结果）→ 成功关闭 / 失败重开；打开未到期 → 迟到结果不改状态；
// 否则滑窗重置或累加，达阈值即打开并清零。
func (r *healthRepo) Record(ctx context.Context, channelID string, ok bool) (time.Time, error) {
	fail := 0
	if !ok {
		fail = 1
	}
	reset := "(h.window_start < now() - " + healthWindow + ")"
	samples := "(CASE WHEN " + reset + " THEN 1 ELSE h.samples + 1 END)"
	failures := "(CASE WHEN " + reset + " THEN $2::int ELSE h.failures + $2::int END)"
	trip := fmt.Sprintf("(%s >= %d AND %s * 2 > %s)", samples, healthMinSample, failures, samples)
	openNow := "(h.open_until IS NOT NULL AND h.open_until > now())"
	q := `INSERT INTO notify_channel_health AS h (channel_id, window_start, samples, failures, open_until, probing, update_time)
		VALUES ($1, now(), 1, $2::int, NULL, false, now())
		ON CONFLICT (channel_id) DO UPDATE SET
		  samples      = CASE WHEN h.probing THEN 0 WHEN ` + openNow + ` THEN h.samples WHEN ` + trip + ` THEN 0 ELSE ` + samples + ` END,
		  failures     = CASE WHEN h.probing THEN 0 WHEN ` + openNow + ` THEN h.failures WHEN ` + trip + ` THEN 0 ELSE ` + failures + ` END,
		  window_start = CASE WHEN ` + openNow + ` THEN h.window_start WHEN h.probing OR ` + trip + ` OR ` + reset + ` THEN now() ELSE h.window_start END,
		  open_until   = CASE WHEN h.probing THEN (CASE WHEN $2::int = 0 THEN NULL ELSE now() + ` + healthOpenFor + ` END)
		                      WHEN ` + openNow + ` THEN h.open_until
		                      WHEN ` + trip + ` THEN now() + ` + healthOpenFor + `
		                      ELSE NULL END,
		  probing      = false,
		  update_time  = now()
		RETURNING open_until`
	var until *time.Time
	if err := r.data.Pool.QueryRow(ctx, q, channelID, fail).Scan(&until); err != nil {
		return time.Time{}, fmt.Errorf("record channel health: %w", err)
	}
	if until == nil {
		return time.Time{}, nil
	}
	return *until, nil
}

// ClaimProbe implements biz.ChannelHealthRepo：条件 UPDATE 领取探测名额（到期且未被
// 领取，或领取方 30s 无结果）；0 行时读状态区分「已关闭」与「他人探测中」。
func (r *healthRepo) ClaimProbe(ctx context.Context, channelID string) (bool, bool, error) {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE notify_channel_health SET probing = true, update_time = now()
		 WHERE channel_id = $1 AND open_until IS NOT NULL AND open_until <= now()
		   AND (probing = false OR update_time < now() - `+healthProbeTTL+`)`, channelID)
	if err != nil {
		return false, false, fmt.Errorf("claim breaker probe: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, false, nil
	}
	var until *time.Time
	err = r.data.Pool.QueryRow(ctx, "SELECT open_until FROM notify_channel_health WHERE channel_id = $1", channelID).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && until == nil) {
		return false, true, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("read channel health: %w", err)
	}
	return false, false, nil
}

// ListOpen implements biz.ChannelHealthRepo：打开中及刚到期待探测（60s 内）的渠道。
func (r *healthRepo) ListOpen(ctx context.Context) (map[string]time.Time, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT channel_id::text, open_until FROM notify_channel_health WHERE open_until IS NOT NULL AND open_until > now() - "+healthOpenFor)
	if err != nil {
		return nil, fmt.Errorf("list open breakers: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var id string
		var until time.Time
		if err := rows.Scan(&id, &until); err != nil {
			return nil, err
		}
		out[id] = until
	}
	return out, rows.Err()
}
