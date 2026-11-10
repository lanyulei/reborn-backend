package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"
)

// 渠道类型常量（notify_channel.type）。
const (
	ChannelTypeEmail    = "email"
	ChannelTypeWebhook  = "webhook"
	ChannelTypeDingtalk = "dingtalk"
	ChannelTypeWecom    = "wecom"
	ChannelTypeFeishu   = "feishu"
)

// EmailConfig / WebhookConfig / RobotConfig / RateLimit：notify_channel.config /
// rate_limit 的 jsonb 结构，json 标签与 core biz 完全一致（契约）。
type (
	EmailConfig struct {
		Host     string `json:"host"`
		Port     uint32 `json:"port"`
		Username string `json:"username"`
		FromAddr string `json:"from_addr"`
		StartTLS bool   `json:"starttls"`
	}
	WebhookConfig struct {
		URL      string            `json:"url"`
		Method   string            `json:"method"`
		Headers  map[string]string `json:"headers"`
		TimeoutS int32             `json:"timeout_s"`
	}
	RobotConfig struct {
		AtMobiles []string `json:"at_mobiles"`
	}
	RateLimit struct {
		QPS   int64 `json:"qps"`
		Burst int64 `json:"burst"`
	}
)

// Channel 渠道实例快照（含已解密 secret；SecretErr 非 nil = 解密失败，
// 该实例不健康：发送侧延后不计尝试并告警）。
type Channel struct {
	ID         string
	BizID      string
	Type       string
	Name       string
	Email      *EmailConfig
	Webhook    *WebhookConfig
	Robot      *RobotConfig
	Secret     string
	SignSecret string // 机器人签名密钥（钉钉加签 / 飞书签名校验）；空 = 不签名
	SecretErr  error
	RateLimit  RateLimit
	Enabled    bool
}

// RouteRecipient 收件策略（notify_route.recipient_policy jsonb）。
type RouteRecipient struct {
	Fixed        []string `json:"fixed,omitempty"`
	Subscription bool     `json:"subscription,omitempty"`
}

// Route 路由快照。
type Route struct {
	ID                string
	BizID             string
	EventPattern      string
	SeverityMin       int32
	TagMatch          map[string]string
	Priority          int32
	ChannelIDs        []string
	Recipient         RouteRecipient
	AggregateWindowS  int32
	SuppressThreshold int32
	SuppressDurationM int32
	Enabled           bool
}

// SilenceWindow 静默时间窗（notify_silence.time_window jsonb；nil=全程）。
type SilenceWindow struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Tz    string `json:"tz,omitempty"`
}

// Silence 静默规则快照。
type Silence struct {
	ID           string
	BizID        string
	EventPattern string
	SeverityMax  int32
	Window       *SilenceWindow
	Enabled      bool
}

// Template 模板快照。
type Template struct {
	ID          string
	BizID       string
	EventType   string
	ChannelType string
	Subject     string
	Body        string
	Version     int32
	Enabled     bool
}

// Producer 生产者快照（ingress 鉴权用；TokenHash = sha256(token 明文)）。
type Producer struct {
	ID             string
	BizID          string
	Name           string
	EventWhitelist []string
	TokenHash      []byte
	ExpireTime     *time.Time
	Enabled        bool
	QPSLimit       int32 // 每秒接入上限，0 = 不限
	DailyLimit     int32 // 每日（UTC）接入上限，0 = 不限
}

// Subscriber 订阅者解析结果（notify_subscription ⋈ system_user，按事件实时查询）。
// Prefs 值为 bool（订阅开关）或 string（目标群 '$group:ops'）。
type Subscriber struct {
	UserID string
	Email  string
	Prefs  map[string]any
}

// ConfigVersions 是 notify_cfg_version 的 table_name → version 映射。
type ConfigVersions map[string]int64

// Snapshot 配置快照：一次 REPEATABLE READ 事务读取的全部配置表（含版本号），
// 构建后冻结只读；经 ConfigCache 原子交换。
type Snapshot struct {
	Versions  ConfigVersions
	Channels  map[string]*Channel
	routes    map[string][]*Route // biz → 按 (priority, 精确度, id) 排序
	silences  map[string][]*Silence
	templates map[templateKey]*Template
	Producers map[string]*Producer
	LoadedAt  time.Time
}

type templateKey struct{ biz, event, channel string }

// NewSnapshot 由原始行构建索引化快照（路由按优先级预排序，模板按三维键索引）。
func NewSnapshot(versions ConfigVersions, channels []*Channel, routes []*Route, silences []*Silence,
	templates []*Template, producers []*Producer, loadedAt time.Time,
) *Snapshot {
	s := &Snapshot{
		Versions:  versions,
		Channels:  make(map[string]*Channel, len(channels)),
		routes:    make(map[string][]*Route),
		silences:  make(map[string][]*Silence),
		templates: make(map[templateKey]*Template, len(templates)),
		Producers: make(map[string]*Producer, len(producers)),
		LoadedAt:  loadedAt,
	}
	for _, ch := range channels {
		s.Channels[ch.ID] = ch
	}
	for _, r := range routes {
		s.routes[r.BizID] = append(s.routes[r.BizID], r)
	}
	for _, list := range s.routes {
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].Priority != list[j].Priority {
				return list[i].Priority < list[j].Priority
			}
			if si, sj := patternSpecificity(list[i].EventPattern), patternSpecificity(list[j].EventPattern); si != sj {
				return si < sj
			}
			return list[i].ID < list[j].ID
		})
	}
	for _, sl := range silences {
		s.silences[sl.BizID] = append(s.silences[sl.BizID], sl)
	}
	for _, t := range templates {
		s.templates[templateKey{t.BizID, t.EventType, t.ChannelType}] = t
	}
	for _, p := range producers {
		s.Producers[p.ID] = p
	}
	return s
}

// Channel 按 ID 取渠道（nil = 不存在/已删除）。
func (s *Snapshot) Channel(id string) *Channel { return s.Channels[id] }

// Template 按解析链取模板：(biz,event,channel) > (biz,event,*) > (biz,*,channel) > (biz,*,*)；
// 停用模板跳过；全部缺失返回 nil（调用方用内置默认）。
func (s *Snapshot) Template(bizID, eventType, channelType string) *Template {
	for _, k := range []templateKey{
		{bizID, eventType, channelType},
		{bizID, eventType, "*"},
		{bizID, "*", channelType},
		{bizID, "*", "*"},
	} {
		if t, ok := s.templates[k]; ok && t.Enabled {
			return t
		}
	}
	return nil
}

// ConfigRepo 配置读取仓储（data 层实现）：版本号单查（10s 轮询）与全量快照
// （单只读事务，版本号与各表同一一致性视图）。
type ConfigRepo interface {
	LoadVersions(ctx context.Context) (ConfigVersions, error)
	LoadSnapshot(ctx context.Context) (*Snapshot, error)
}

// RateBucketSyncer 令牌桶参数同步（快照变更时幂等 upsert；qps=0 渠道无桶行）。
type RateBucketSyncer interface {
	SyncBuckets(ctx context.Context, channels []*Channel) error
}

// ConfigCache 进程内配置快照持有者：读端无锁（atomic.Pointer），
// Refresh 比对版本号变化后整体换入；初始加载失败即启动失败。
type ConfigCache struct {
	repo    ConfigRepo
	buckets RateBucketSyncer
	log     *slog.Logger
	current atomic.Pointer[Snapshot]
}

// NewConfigCache new a ConfigCache（不加载；由 worker 启动时 Load）。
func NewConfigCache(repo ConfigRepo, buckets RateBucketSyncer, logger *slog.Logger) *ConfigCache {
	return &ConfigCache{repo: repo, buckets: buckets, log: logger}
}

// Current 返回当前快照；未加载返回 nil（调用方应视为不可用）。
func (c *ConfigCache) Current() *Snapshot { return c.current.Load() }

// Load 强制全量加载并换入。
func (c *ConfigCache) Load(ctx context.Context) error {
	snap, err := c.repo.LoadSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("load config snapshot: %w", err)
	}
	c.install(ctx, snap)
	return nil
}

// Refresh 比对版本号：任一表版本变化即重载（≤10s 收敛）。
func (c *ConfigCache) Refresh(ctx context.Context) error {
	cur := c.current.Load()
	if cur == nil {
		return c.Load(ctx)
	}
	versions, err := c.repo.LoadVersions(ctx)
	if err != nil {
		return fmt.Errorf("load config versions: %w", err)
	}
	if versionsEqual(cur.Versions, versions) {
		return nil
	}
	return c.Load(ctx)
}

func (c *ConfigCache) install(ctx context.Context, snap *Snapshot) {
	channels := make([]*Channel, 0, len(snap.Channels))
	for _, ch := range snap.Channels {
		channels = append(channels, ch)
		if ch.SecretErr != nil {
			c.log.ErrorContext(ctx, "channel secret unavailable, channel marked unhealthy",
				"channel_id", ch.ID, "channel", ch.Name, "error", ch.SecretErr)
		}
	}
	if err := c.buckets.SyncBuckets(ctx, channels); err != nil {
		// 桶同步失败不阻塞配置换入：旧桶参数继续生效（无桶行=不限流），下轮重试。
		c.log.ErrorContext(ctx, "sync rate buckets failed", "error", err)
	}
	c.current.Store(snap)
	c.log.InfoContext(ctx, "config snapshot installed",
		"channels", len(snap.Channels), "producers", len(snap.Producers), "versions", snap.Versions)
}

func versionsEqual(a, b ConfigVersions) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
