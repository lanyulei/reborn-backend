// Package notifycontract 是 core 控制面与 notify 管道之间的数据契约（同库不同进程，
// 不跨模块 import 业务代码，只共享"落库长什么样"）：jsonb 列结构、状态机取值、
// 事件类型/模式的正则与匹配语义、两端都会执行的 SQL。任何一方改契约必须改这里，
// 由编译器把漂移点全部暴露。
package notifycontract

import (
	"regexp"
	"strings"
)

// DefaultBizID 默认业务命名空间。
const DefaultBizID = "default"

// 渠道类型（notify_channel.type）。
const (
	ChannelTypeEmail    = "email"
	ChannelTypeWebhook  = "webhook"
	ChannelTypeDingtalk = "dingtalk"
	ChannelTypeWecom    = "wecom"
	ChannelTypeFeishu   = "feishu"
)

// 模板维度通配值。
const (
	TemplateChannelAny   = "*"
	TemplateEventTypeAny = "*"
)

// 事件来源（notify_event.source）。
const (
	EventSourceAPI      = "api"      // 生产者经管道 ingress
	EventSourceInternal = "internal" // core 业务同事务直插
)

// 事件路由状态（notify_event.status）。
const (
	EventStatusReceived   int32 = 1
	EventStatusRouted     int32 = 2
	EventStatusSuppressed int32 = 3
	EventStatusFailed     int32 = 4
)

// 事件交付状态（notify_event.publish_state）：0 待 router 领取 / 1 已交付。
const (
	PublishPending   int32 = 0
	PublishPublished int32 = 1
)

// 发送任务状态（notify_dispatch.status）。
const (
	DispatchPending int32 = 1
	DispatchSending int32 = 2
	DispatchSent    int32 = 3
	DispatchFailed  int32 = 4
	DispatchDead    int32 = 5
)

// 死信重放状态（notify_dlq.replay_state）。
const (
	ReplayNone     int32 = 1
	ReplayPending  int32 = 2
	ReplayReplayed int32 = 3
)

// 聚合组状态（notify_agg_group.status）。
const (
	AggOpen   int32 = 1
	AggClosed int32 = 2
)

// jsonb 列结构（json 标签即落库 schema；管道以 DisallowUnknownFields 解码——
// 任何字段增删必须两端同时发布）。
type (
	// EmailConfig notify_channel.config（type=email）。
	EmailConfig struct {
		Host     string `json:"host"`
		Port     uint32 `json:"port"`
		Username string `json:"username"`
		FromAddr string `json:"from_addr"`
		StartTLS bool   `json:"starttls"`
	}
	// WebhookConfig notify_channel.config（type=webhook）。
	WebhookConfig struct {
		URL      string            `json:"url"`
		Method   string            `json:"method"`
		Headers  map[string]string `json:"headers"`
		TimeoutS int32             `json:"timeout_s"`
	}
	// RobotConfig notify_channel.config（type=dingtalk/wecom/feishu）。
	RobotConfig struct {
		AtMobiles []string `json:"at_mobiles"`
	}
	// RateLimit notify_channel.rate_limit；QPS=0 不限。
	RateLimit struct {
		QPS   int64 `json:"qps"`
		Burst int64 `json:"burst"`
	}
	// RouteRecipient notify_route.recipient_policy。
	RouteRecipient struct {
		Fixed        []string `json:"fixed,omitempty"`
		Subscription bool     `json:"subscription,omitempty"`
	}
	// SilenceWindow notify_silence.time_window（nil 列 = 全程；start>end 跨午夜；tz 缺省 UTC）。
	SilenceWindow struct {
		Start string `json:"start"`
		End   string `json:"end"`
		Tz    string `json:"tz,omitempty"`
	}
)

// 事件命名正则（proto 文件中的字面量须与此一致，由各模块守护测试比对）。
var (
	// EventTypePattern 事件类型：小写点分事件名，不允许通配。
	EventTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	// EventPatternPattern 事件匹配模式：'*' / 精确 / 尾部 '.*' 前缀通配（路由/静默/订阅/生产者白名单）。
	EventPatternPattern = regexp.MustCompile(`^(\*|[a-z0-9][a-z0-9._-]{0,127}|[a-z0-9][a-z0-9._-]{0,125}\.\*)$`)
	// TemplateEventTypePattern 模板事件维度：'*' 或精确事件名（无前缀通配）。
	TemplateEventTypePattern = regexp.MustCompile(`^(\*|[a-z0-9][a-z0-9._-]{0,127})$`)
)

// 正则字面量（供守护测试与 proto 比对）。
const (
	EventTypePatternLiteral         = `^[a-z0-9][a-z0-9._-]{0,127}$`
	EventPatternPatternLiteral      = `^(\*|[a-z0-9][a-z0-9._-]{0,127}|[a-z0-9][a-z0-9._-]{0,125}\.\*)$`
	TemplateEventTypePatternLiteral = `^(\*|[a-z0-9][a-z0-9._-]{0,127})$`
)

// MatchPattern 事件模式匹配：'*' 全匹配；'x.y.*' 等价前缀 'x.y.'；其余精确相等。
func MatchPattern(pattern, eventType string) bool {
	switch {
	case pattern == "*":
		return true
	case strings.HasSuffix(pattern, ".*"):
		return strings.HasPrefix(eventType, pattern[:len(pattern)-1])
	default:
		return pattern == eventType
	}
}

// PatternSpecificity 精确 0 < 前缀 1 < 全匹配 2（同优先级路由的决胜次序）。
func PatternSpecificity(pattern string) int {
	switch {
	case pattern == "*":
		return 2
	case strings.HasSuffix(pattern, ".*"):
		return 1
	default:
		return 0
	}
}

// InsertEventSQL 写 notify_event 一行（core 直插 / 测试事件 / 管道 ingress 共用）：
// 13 个实参按列序 id, biz_id, event_type, severity, title, payload, dedup_key, group_key,
// recipients, source, status, publish_state, publish_attempts；可空列（payload/dedup_key/
// group_key/recipients）空值须传 NULL；时间列取数据库 now()（与 router 的到期判定同一
// 时钟源，避免应用/DB 时钟偏差让刚插入的事件"在未来"）。
const InsertEventSQL = `INSERT INTO notify_event (id, biz_id, event_type, severity, title, payload, dedup_key, group_key, recipients,
	source, status, publish_state, publish_attempts, next_publish_time, create_time)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),now())`

// DedupGateSQL 去重门闩单语句 upsert：实参 biz_id, dedup_key, event_id, ttl(interval)。
// 有效期内命中返回首次 event_id；过期行被本次事件原地替换；返回值 == 本次 event_id
// 表示首次（应继续插事件）。DO UPDATE 行锁同时挡住并发清理。
const DedupGateSQL = `INSERT INTO notify_dedup (biz_id, dedup_key, event_id, expire_time) VALUES ($1, $2, $3, now() + $4)
	ON CONFLICT (biz_id, dedup_key) DO UPDATE SET
	  event_id = CASE WHEN notify_dedup.expire_time < now() THEN EXCLUDED.event_id ELSE notify_dedup.event_id END,
	  expire_time = CASE WHEN notify_dedup.expire_time < now() THEN EXCLUDED.expire_time ELSE notify_dedup.expire_time END
	RETURNING event_id::text`
