package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.RateBucketSyncer = (*rateBucketRepo)(nil)
	_ biz.RateLimiter      = (*rateBucketRepo)(nil)
)

// rateBucketRepo 令牌桶（设计 §4.6）：PG 条件 UPDATE 原子消费，多实例共享配额；
// 桶参数随配置快照幂等同步（评审 #22），qps=0 渠道无行 = 不限流。
type rateBucketRepo struct {
	data *Data
}

// NewRateBucketRepo new a rateBucketRepo.
func NewRateBucketRepo(data *Data) *rateBucketRepo { return &rateBucketRepo{data: data} }

// SyncBuckets implements biz.RateBucketSyncer.
func (r *rateBucketRepo) SyncBuckets(ctx context.Context, channels []*biz.Channel) error {
	batch := &pgx.Batch{}
	n := 0
	for _, ch := range channels {
		if ch.RateLimit.QPS <= 0 {
			batch.Queue("DELETE FROM notify_rate_bucket WHERE channel_id = $1", ch.ID)
			n++
			continue
		}
		capacity := float64(ch.RateLimit.Burst)
		if capacity < 1 {
			capacity = float64(ch.RateLimit.QPS)
		}
		batch.Queue(
			`INSERT INTO notify_rate_bucket (channel_id, tokens, capacity, rate, last_refill) VALUES ($1, $2, $2, $3, now())
			 ON CONFLICT (channel_id) DO UPDATE SET capacity = EXCLUDED.capacity, rate = EXCLUDED.rate,
			   tokens = LEAST(notify_rate_bucket.tokens, EXCLUDED.capacity)`,
			ch.ID, capacity, float64(ch.RateLimit.QPS))
		n++
	}
	if n == 0 {
		return nil
	}
	res := r.data.Pool.SendBatch(ctx, batch)
	for i := 0; i < n; i++ {
		if _, err := res.Exec(); err != nil {
			_ = res.Close()
			return fmt.Errorf("sync rate bucket: %w", err)
		}
	}
	return res.Close()
}

// Take implements biz.RateLimiter：按经过时间补充后扣 1 令牌；0 行 = 无令牌或无桶。
// 无桶（qps=0）直接放行；有桶无令牌按 (1-tokens)/rate 给出等待建议。
func (r *rateBucketRepo) Take(ctx context.Context, ch *biz.Channel) (bool, time.Duration, error) {
	if ch.RateLimit.QPS <= 0 {
		return true, 0, nil
	}
	var tokens float64
	err := r.data.Pool.QueryRow(ctx,
		`UPDATE notify_rate_bucket
		 SET tokens = LEAST(capacity, tokens + extract(epoch FROM (now() - last_refill)) * rate) - 1, last_refill = now()
		 WHERE channel_id = $1 AND LEAST(capacity, tokens + extract(epoch FROM (now() - last_refill)) * rate) >= 1
		 RETURNING tokens`, ch.ID).Scan(&tokens)
	if err == nil {
		return true, 0, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, 0, fmt.Errorf("take token: %w", err)
	}
	// 无令牌：读当前水位估算等待；无行（尚未同步）按放行避免永久饿死。
	var cur, rate float64
	err = r.data.Pool.QueryRow(ctx,
		"SELECT LEAST(capacity, tokens + extract(epoch FROM (now() - last_refill)) * rate), rate FROM notify_rate_bucket WHERE channel_id = $1", ch.ID).Scan(&cur, &rate)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("read bucket: %w", err)
	}
	wait := time.Second
	if rate > 0 {
		wait = time.Duration((1-cur)/rate*float64(time.Second)) + 100*time.Millisecond
	}
	return false, wait, nil
}
