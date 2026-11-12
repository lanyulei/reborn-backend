// Package notifycontract 是 core 控制面与 notify 管道之间的数据契约（同库不同进程，
// 不跨模块 import 业务代码，只共享"落库长什么样"）：jsonb 列结构、状态机取值、
// 事件类型/模式的正则与匹配语义、两端都会执行的 SQL。任何一方改契约必须改这里，
// 由编译器把漂移点全部暴露。
package notifycontract

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
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
// 模板输出格式（notify_template.format）；渠道按能力降级见 EffectiveFormat。
const (
	TemplateFormatText     = "text"
	TemplateFormatMarkdown = "markdown"
	TemplateFormatHTML     = "html"
	TemplateFormatCard     = "card"
)

// EffectiveFormat 渠道生效格式：email 只认 html（其余纯文本）；群机器人 card→card、
// markdown/html→markdown、text→text；webhook 原样透传；空/未知按 text。
func EffectiveFormat(channelType, format string) string {
	switch channelType {
	case ChannelTypeEmail:
		if format == TemplateFormatHTML {
			return TemplateFormatHTML
		}
		return TemplateFormatText
	case ChannelTypeDingtalk, ChannelTypeWecom, ChannelTypeFeishu:
		switch format {
		case TemplateFormatCard:
			return TemplateFormatCard
		case TemplateFormatMarkdown, TemplateFormatHTML:
			return TemplateFormatMarkdown
		}
		return TemplateFormatText
	}
	switch format {
	case TemplateFormatMarkdown, TemplateFormatHTML, TemplateFormatCard:
		return format
	}
	return TemplateFormatText
}

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
		// CardURL 卡片消息的跳转链接（format=card 时钉钉 actionCard / 企微 template_card /
		// 飞书 interactive 的按钮目标；空 = 钉钉/企微降级 markdown、飞书卡片无按钮）。
		CardURL string `json:"card_url,omitempty"`
		// CardButton 卡片按钮文字（缺省"查看详情"）。
		CardButton string `json:"card_button,omitempty"`
	}
	// RateLimit notify_channel.rate_limit；QPS=0 不限。
	RateLimit struct {
		QPS   int64 `json:"qps"`
		Burst int64 `json:"burst"`
		// PerMinute 每分钟配额（QPS=0 时生效）：群机器人平台限制以"每分钟 N 条"计
		// （钉钉/企微 20/min），整数 qps 表达不了 < 1/s 的速率。
		PerMinute int64 `json:"per_minute,omitempty"`
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

// TagsMatch 路由 tag_match 判定（管道选路与控制面试算同一实现）：tags 为空无条件；
// 否则 payload 须为对象且每个键的字面值相等（数字/布尔按 JSON 文本比较）。键先按
// 顶层字面键查找，未命中再按点分路径下钻嵌套对象（"labels.env" → payload.labels.env）。
func TagsMatch(tags map[string]string, payload map[string]any) bool {
	if len(tags) == 0 {
		return true
	}
	if payload == nil {
		return false
	}
	for k, want := range tags {
		v, ok := lookupPath(payload, k)
		if !ok {
			return false
		}
		if ScalarString(v) != want {
			return false
		}
	}
	return true
}

// lookupPath 顶层字面键优先；否则按 "." 逐级进入嵌套 map。
func lookupPath(payload map[string]any, key string) (any, bool) {
	if v, ok := payload[key]; ok {
		return v, true
	}
	if !strings.Contains(key, ".") {
		return nil, false
	}
	var cur any = payload
	for _, part := range strings.Split(key, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// ScalarString JSON 标量的文本形态（tag_match 比较口径：3 与 "3"、true 与 "true" 等价）。
func ScalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", x)
	}
}

// WindowContains 静默时间窗判定：按 tz 本地时间（非法 tz 按 UTC）；start > end 跨午夜；
// 时刻解析失败视为不命中（宁放行不误静默）。
func WindowContains(w SilenceWindow, now time.Time) bool {
	loc := time.UTC
	if w.Tz != "" {
		if l, err := time.LoadLocation(w.Tz); err == nil {
			loc = l
		}
	}
	start, ok1 := parseClock(w.Start)
	end, ok2 := parseClock(w.End)
	if !ok1 || !ok2 {
		return false
	}
	t := now.In(loc)
	cur := t.Hour()*60 + t.Minute()
	if start <= end {
		return cur >= start && cur < end
	}
	return cur >= start || cur < end // 跨午夜
}

func parseClock(s string) (int, bool) {
	var h, m int
	if _, err := fmt.Sscanf(s, "%d:%d", &h, &m); err != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// Rate 令牌补充速率（tokens/s）：QPS 优先，否则 PerMinute/60；两者皆 0 = 不限。
func (r RateLimit) Rate() float64 {
	if r.QPS > 0 {
		return float64(r.QPS)
	}
	if r.PerMinute > 0 {
		return float64(r.PerMinute) / 60
	}
	return 0
}

// Capacity 令牌桶容量：Burst > 0 取 Burst，否则 max(1, 速率)。
func (r RateLimit) Capacity() float64 {
	if r.Burst > 0 {
		return float64(r.Burst)
	}
	if rate := r.Rate(); rate > 1 {
		return rate
	}
	return 1
}

// DefaultRateLimit 渠道类型缺省限流（渠道未配置时生效）：群机器人按平台公开限制——
// 钉钉/企微 20 条/分钟（容量 1：严格匀速，平台按分钟窗口计数，突发必被 130101/45009
// 拒绝）、飞书 100 条/分钟（容量 5）；邮件/webhook 不限。
func DefaultRateLimit(channelType string) RateLimit {
	switch channelType {
	case ChannelTypeDingtalk, ChannelTypeWecom:
		return RateLimit{PerMinute: 20, Burst: 1}
	case ChannelTypeFeishu:
		return RateLimit{PerMinute: 100, Burst: 5}
	}
	return RateLimit{}
}

// EffectiveRateLimit 渠道生效限流：显式配置优先，否则类型缺省。
func EffectiveRateLimit(channelType string, configured RateLimit) RateLimit {
	if configured.Rate() > 0 {
		return configured
	}
	return DefaultRateLimit(channelType)
}

// TemplateFuncs 模板白名单函数（core 写入校验与管道渲染同一集合注册，两端一致）：
// 全部为纯函数、无 IO、无反射逃逸——沙箱约束不变。
//   - fmtTime t "2006-01-02 15:04:05"：按布局格式化时间（布局可省略）
//   - severity n：1-4 → info/warning/error/critical
//   - json v：紧凑 JSON 文本（载荷/样本打印）
//   - trunc s n：按 rune 截断并追加 …
//   - upper / lower
func TemplateFuncs() map[string]any {
	return map[string]any{
		"fmtTime":  fmtTime,
		"severity": SeverityLabel,
		"json":     jsonText,
		"trunc":    truncRunes,
		"upper":    strings.ToUpper,
		"lower":    strings.ToLower,
	}
}

// SeverityLabel 严重级文本（1-4 → info/warning/error/critical，越界原样数字）。
func SeverityLabel(n int32) string {
	switch n {
	case 1:
		return "info"
	case 2:
		return "warning"
	case 3:
		return "error"
	case 4:
		return "critical"
	}
	return fmt.Sprintf("%d", n)
}

func fmtTime(t time.Time, layout ...string) string {
	l := "2006-01-02 15:04:05"
	if len(layout) > 0 && layout[0] != "" {
		l = layout[0]
	}
	return t.Format(l)
}

func jsonText(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(raw)
}

func truncRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// PrivateIP 报告地址是否为回环/私网/链路本地/ULA/CGNAT/未指定/组播（webhook 出站
// SSRF 防护：控制面校验字面量，管道在解析后对真实连接地址再校验，免疫 DNS 重绑定）。
func PrivateIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1]&0xc0 == 64 // 100.64.0.0/10 CGNAT
	}
	return false
}

// UnsafeWebhookHost 控制面侧的字面量校验：localhost / 字面 IP 为私网。主机名不解析
// （解析交给管道连接时校验）。
func UnsafeWebhookHost(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "" || h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return true
	}
	if ip := net.ParseIP(strings.Trim(h, "[]")); ip != nil {
		return PrivateIP(ip)
	}
	return false
}
