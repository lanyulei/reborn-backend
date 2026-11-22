package biz

import (
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// 运行参数缺省值（配置未设置时生效）。
const (
	defaultTaskTimeout        = 8 * time.Minute
	defaultMaxSteps           = 20
	defaultMaxConcurrentTasks = 3
	defaultDailyPromptBudget  = 2_000_000
	defaultDailyCompletion    = 300_000
	defaultRetention          = 90 * 24 * time.Hour
	defaultQueueTTL           = 10 * time.Minute
	// maxTaskTimeout 单任务时限上限（也约束机器令牌与租约 TTL）。
	maxTaskTimeout = 30 * time.Minute
)

// defaultRetryBackoff 失败重试退避序列（长度即最多重试次数）。
var defaultRetryBackoff = []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// Config 本域运行参数（由 conf.Agent 规整而来）。
type Config struct {
	Enabled            bool
	TaskTimeout        time.Duration
	MaxSteps           int
	MaxConcurrentTasks int
	DailyPromptBudget  int64
	DailyCompletion    int64
	Retention          time.Duration
	QueueTTL           time.Duration
	RetryBackoff       []time.Duration
}

// NewConfig 从配置规整运行参数；nil 配置视为停用。
func NewConfig(c *conf.Agent) Config {
	cfg := Config{
		TaskTimeout:        defaultTaskTimeout,
		MaxSteps:           defaultMaxSteps,
		MaxConcurrentTasks: defaultMaxConcurrentTasks,
		DailyPromptBudget:  defaultDailyPromptBudget,
		DailyCompletion:    defaultDailyCompletion,
		Retention:          defaultRetention,
		QueueTTL:           defaultQueueTTL,
		RetryBackoff:       defaultRetryBackoff,
	}
	if c == nil {
		return cfg
	}
	cfg.Enabled = c.GetEnabled()
	if d := c.GetTaskTimeout(); d != nil && d.AsDuration() > 0 {
		cfg.TaskTimeout = min(d.AsDuration(), maxTaskTimeout)
	}
	if n := int(c.GetMaxSteps()); n > 0 {
		cfg.MaxSteps = n
	}
	if n := int(c.GetMaxConcurrentTasks()); n > 0 {
		cfg.MaxConcurrentTasks = n
	}
	if n := c.GetDailyPromptTokenBudget(); n > 0 {
		cfg.DailyPromptBudget = n
	}
	if n := c.GetDailyCompletionTokenBudget(); n > 0 {
		cfg.DailyCompletion = n
	}
	if d := c.GetRetention(); d != nil && d.AsDuration() > 0 {
		cfg.Retention = d.AsDuration()
	}
	if d := c.GetQueueTtl(); d != nil && d.AsDuration() > 0 {
		cfg.QueueTTL = d.AsDuration()
	}
	if seq := c.GetRetryBackoff(); len(seq) > 0 {
		backoff := make([]time.Duration, 0, len(seq))
		for _, d := range seq {
			if d != nil && d.AsDuration() > 0 {
				backoff = append(backoff, d.AsDuration())
			}
		}
		if len(backoff) > 0 {
			cfg.RetryBackoff = backoff
		}
	}
	return cfg
}

// MaxAttempts 最多尝试次数 = 首次 + 退避序列长度。
func (c Config) MaxAttempts() int { return 1 + len(c.RetryBackoff) }

// Backoff 第 attempt 次失败（从 1 起）后的退避；超出序列取最后一档。
func (c Config) Backoff(attempt int) time.Duration {
	if len(c.RetryBackoff) == 0 {
		return time.Minute
	}
	i := attempt - 1
	if i < 0 {
		i = 0
	}
	if i >= len(c.RetryBackoff) {
		i = len(c.RetryBackoff) - 1
	}
	return c.RetryBackoff[i]
}
