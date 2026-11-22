// Package monitorcontract 是 core 管理面（monitor 域）与评估引擎（backend/monitor）之间的数据契约：
// 两者同库不同进程、互不 import 业务代码，共享的只有 monitor_* 表"落库长什么样"——jsonb 列结构
// （json 标签即落库格式）、状态与枚举字面量、通知事件类型、选主锁键，以及两端必须算出同一结果的
// 纯函数（标签匹配、事件指纹、聚合键、生效时间窗判定、标签校验）。任何一方改契约必须改这里，
// 由编译器把漂移点全部暴露。本包不 import 任何驱动与传输层，biz 层可用。
//
// 表结构见 core/migrations/0069_monitor_init.up.sql，设计见 backend/docs/monitor-tech-design.md。
package monitorcontract

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/notifycontract"
)

// 告警级别（monitor_rule.queries[].severity / monitor_alert.severity，与 notify 事件级别同一口径）。
const (
	SeverityInfo     int32 = 1 // 提示
	SeverityWarning  int32 = 2 // 警告
	SeverityCritical int32 = 3 // 严重
	SeverityFatal    int32 = 4 // 致命
)

// ValidSeverity 报告级别是否在 1-4 内。
func ValidSeverity(n int32) bool { return n >= SeverityInfo && n <= SeverityFatal }

// 告警事件状态（monitor_alert.status）。
const (
	AlertFiring   int32 = 1 // 告警中
	AlertResolved int32 = 2 // 已恢复
	AlertClosed   int32 = 3 // 已关闭（规则删除 / 停用、数据源移出范围；不发恢复通知）
)

// 判定状态（monitor_eval_state.state）。
const (
	StatePending    int32 = 1 // 待触发：异常未满持续时长
	StateFiring     int32 = 2 // 告警中
	StateRecovering int32 = 3 // 留观中：已不异常，等待留观期满
)

// 评估结果（monitor_eval_task.last_status / monitor_eval_record.status）。
const (
	EvalNever    int32 = 0 // 未评估（仅任务表）
	EvalOK       int32 = 1 // 正常
	EvalFiring   int32 = 2 // 告警中
	EvalNoData   int32 = 3 // 无数据
	EvalError    int32 = 4 // 查询错误
	EvalInactive int32 = 5 // 不在生效时间
)

// 屏蔽方式（monitor_mute.mode）。
const (
	MuteModeNotify int32 = 1 // 只屏蔽通知：事件照常产生并标记 muted
	MuteModeEvent  int32 = 2 // 屏蔽事件：不产生事件
)

// 屏蔽时间类型（monitor_mute.time_type）。
const (
	MuteTimeOnce     int32 = 1 // 一次性：start_time ~ end_time
	MuteTimePeriodic int32 = 2 // 周期：periodic（Effective 结构）
)

// 发件箱通知类型（monitor_notify_outbox.kind / monitor_notify_log.kind）。
const (
	NotifyKindFiring   int32 = 1 // 触发
	NotifyKindRepeat   int32 = 2 // 重复
	NotifyKindResolved int32 = 3 // 恢复
)

// 发件箱状态（monitor_notify_outbox.status）。
const (
	OutboxPending int32 = 1 // 待派发
	OutboxDone    int32 = 2 // 已派发
	OutboxDropped int32 = 3 // 已丢弃
)

// OutboxMaxAttempts 发件箱派发失败次数上限，超过即丢弃。
const OutboxMaxAttempts = 10

// 通知记录来源（monitor_notify_log.source）。
const (
	NotifySourceRule         int32 = 1 // 规则的通知对象
	NotifySourceSubscription int32 = 2 // 订阅规则
)

// 规则的数据源范围（monitor_rule.datasource_scope）。
const (
	DatasourceScopeSpecified int32 = 1 // 指定数据源（datasource_ids）
	DatasourceScopeAll       int32 = 2 // 全部启用的数据源
)

// DatasourceTypePrometheus 数据源类型（monitor_datasource.type），当前唯一取值。
const DatasourceTypePrometheus = "prometheus"

// 数据源认证方式（monitor_datasource.auth_type；与 kit/promclient 的 Auth* 数值一致）。
const (
	AuthNone   int32 = 1
	AuthBasic  int32 = 2
	AuthBearer int32 = 3
)

// 数据源健康状态（monitor_datasource.health_status）。
const (
	HealthUnknown int32 = 0
	HealthOK      int32 = 1
	HealthFailed  int32 = 2
)

// 角色数据范围级别（monitor_role_scope.level）；0 表示无权限，不落库。
const (
	ScopeNone   int32 = 0
	ScopeRead   int32 = 1 // 只读
	ScopeManage int32 = 2 // 可管理
)

// 规则版本操作（monitor_rule_version.action）。
const (
	RuleActionCreate   = "create"
	RuleActionUpdate   = "update"
	RuleActionEnable   = "enable"
	RuleActionDisable  = "disable"
	RuleActionMove     = "move"
	RuleActionRollback = "rollback"
	RuleActionImport   = "import"
	RuleActionDelete   = "delete"
	RuleActionBatch    = "batch"
)

// 通知事件：业务命名空间与事件类型（notify_event.biz_id / event_type）。
const (
	// NotifyBizID 与 oncall / workflow / ticket 一致，路由才能选到现有渠道。
	NotifyBizID = notifycontract.DefaultBizID

	EventAlertFiring          = "monitor.alert.firing"
	EventAlertResolved        = "monitor.alert.resolved"
	EventSubscriptionFiring   = "monitor.subscription.firing"
	EventSubscriptionResolved = "monitor.subscription.resolved"
)

// 通知载荷里的事件状态（NotifyPayload.Status）。
const (
	PayloadStatusFiring   = "firing"
	PayloadStatusResolved = "resolved"
)

// 选主锁键（PG advisory lock，kit/dbx.TryAdvisoryLock）。
const (
	LockPlanner     = "monitor:planner"     // 任务物化 + 数据源探活
	LockMaintenance = "monitor:maintenance" // 分区维护 + 过期数据清理
)

// 配置版本名（monitor_cfg_version.name）：core 写对应配置表时同事务 +1，引擎据此刷新只读缓存。
const (
	CfgDatasource = "monitor_datasource"
	CfgMute       = "monitor_mute"
	CfgRuleGroup  = "monitor_rule_group"
)

// RootGroupID 迁移内置的根分组「默认分组」。
const RootGroupID = "018f3e1a-0cc0-7000-8000-000000000001"

// 数量与长度上限（与迁移的 CHECK、proto 的校验规则同一口径）。
const (
	MaxRuleQueries     = 4   // 每条规则的查询条数
	MaxRuleLabels      = 20  // 每条规则的附加标签数
	MaxRuleAnnotations = 10  // 每条规则的附加信息数
	MaxMatchers        = 20  // 每条屏蔽 / 订阅规则的匹配条件行数
	MaxMatcherValues   = 50  // in / not in 的取值个数
	MaxMatcherValueLen = 256 // 匹配值与正则的长度（字节）
	MaxLabelNameLen    = 128 // 标签名长度（字节）
	MaxLabelValueLen   = 256 // 标签值长度（字符）
	MaxWindows         = 10  // 每个生效时间配置的时间窗数
	MaxEvalPoints      = 50  // 判定记录明细保留的异常点数
	MaxGroupKeyLabels  = 10  // 聚合键标签数
)

// jsonb 列结构：json 标签即落库格式，两端同时发布才能增删字段。
type (
	// Query monitor_rule.queries 的一项：一条 PromQL 对应一个级别，查询有结果即为异常。
	Query struct {
		Expr     string `json:"expr"`
		Severity int32  `json:"severity"`
	}

	// TimeWindow 一个按周重复的时间窗。Days 取 1-7（周一至周日），指时间窗开始的那一天；
	// Start / End 为 "HH:MM"，Start > End 表示跨午夜，Start == End 表示全天。
	TimeWindow struct {
		Days  []int32 `json:"days"`
		Start string  `json:"start"`
		End   string  `json:"end"`
	}

	// Effective monitor_rule.effective 与 monitor_mute.periodic：Windows 为空 = 全时段；
	// Timezone 为 IANA 时区名，空按 UTC。零值可用（全时段）。
	// Mode 仅规则的生效时间使用（屏蔽规则的周期时间恒按时间窗）：空或 EffectiveModeWindows 按 Windows；
	// EffectiveModeWorkTime / EffectiveModeOffTime 按工作日历（monitor_calendar），此时 Windows 须为空，
	// 判定用 ContainsWithCalendar。
	Effective struct {
		Timezone string       `json:"timezone"`
		Windows  []TimeWindow `json:"windows"`
		Mode     string       `json:"mode,omitempty"`
	}

	// NotifyTargets monitor_rule.notify：用户、值班表（取当前值班人）、部门（取负责人）、
	// 事件关联 CI 的负责人（CIOwner；CI 的 owner 是可用用户的 ID 时才有收件人）。
	NotifyTargets struct {
		UserIDs     []string `json:"user_ids"`
		ScheduleIDs []string `json:"schedule_ids"`
		DeptIDs     []string `json:"dept_ids"`
		CIOwner     bool     `json:"ci_owner,omitempty"`
	}

	// Matcher monitor_mute.matchers / monitor_subscription.matchers 的一行。Op 为 == / != / =~ / !~
	// 时用 Value，为 in / not in 时用 Values。
	Matcher struct {
		Label  string   `json:"label"`
		Op     string   `json:"op"`
		Value  string   `json:"value,omitempty"`
		Values []string `json:"values,omitempty"`
	}

	// Receivers monitor_subscription.receivers：本人、指定用户、值班表当前值班人、通知群标识
	// （notify 的群收件人写法，如 "$group:ops"）。
	Receivers struct {
		Self        bool     `json:"self"`
		UserIDs     []string `json:"user_ids"`
		ScheduleIDs []string `json:"schedule_ids"`
		Groups      []string `json:"groups"`
	}

	// EvalQueryDetail 判定记录里一条查询的执行情况。
	EvalQueryDetail struct {
		Index     int32 `json:"index"`
		Severity  int32 `json:"severity"`
		Series    int32 `json:"series"`
		Truncated bool  `json:"truncated,omitempty"`
		// Partial 上游声明该查询的结果不完整（promclient.Result.Partial）。
		Partial bool   `json:"partial,omitempty"`
		Error   string `json:"error,omitempty"`
	}

	// EvalPoint 判定记录里的一个异常点及其停在状态机的哪一步（Step 取 Step* 常量）。
	// Value 为 FormatValue 的文本（NaN / ±Inf 无法用 JSON 数字表示）。
	EvalPoint struct {
		Fingerprint string            `json:"fingerprint"`
		Labels      map[string]string `json:"labels"`
		Severity    int32             `json:"severity"`
		Value       string            `json:"value"`
		Step        string            `json:"step"`
		AlertID     string            `json:"alert_id,omitempty"`
		MuteID      string            `json:"mute_id,omitempty"`
		// InhibitAlertID 抑制它的源告警（Step 为 StepInhibitedRule / StepInhibitedTopology 时）。
		InhibitAlertID string `json:"inhibit_alert_id,omitempty"`
		// CIID 关联到的 CMDB 资源。
		CIID string `json:"ci_id,omitempty"`
	}

	// EvalDetail monitor_eval_record.detail。Points 至多 MaxEvalPoints 条，超出时 Truncated 为 true。
	EvalDetail struct {
		Queries   []EvalQueryDetail `json:"queries"`
		Points    []EvalPoint       `json:"points"`
		Truncated bool              `json:"truncated,omitempty"`
	}

	// NotifyPayload 告警通知事件的内容体（notify_event.payload），迁移内置的四个模板按这些键取值。
	// Value / RecoveryValue / FirstFireTime / Duration 是已格式化的文本：载荷经 JSON 往返后数字会变成
	// float64，模板直接打印不可读，故由写入方格式化。
	NotifyPayload struct {
		AlertID          string            `json:"alert_id"`
		AlertURL         string            `json:"alert_url"`
		RuleID           string            `json:"rule_id"`
		RuleName         string            `json:"rule_name"`
		GroupName        string            `json:"group_name"`
		DatasourceName   string            `json:"datasource_name"`
		Severity         int32             `json:"severity"`
		Status           string            `json:"status"`
		Labels           map[string]string `json:"labels"`
		Annotations      map[string]string `json:"annotations"`
		Value            string            `json:"value"`
		RecoveryValue    string            `json:"recovery_value,omitempty"`
		Expr             string            `json:"expr"`
		FirstFireTime    string            `json:"first_fire_time"`
		Duration         string            `json:"duration"`
		NotifySeq        int32             `json:"notify_seq"`
		SubscriptionName string            `json:"subscription_name,omitempty"`
		// 以下各项为第二、三期增加，均可省略（旧模板不读取它们）。
		// EscalationLevel / EscalationSeq / EscalationName 升级通知（EventAlertEscalated）：第几级、该级的
		// 第几次通知（1 = 升级到该级时的首次通知）、升级策略名称。
		EscalationLevel int32  `json:"escalation_level,omitempty"`
		EscalationSeq   int32  `json:"escalation_seq,omitempty"`
		EscalationName  string `json:"escalation_name,omitempty"`
		// CIID / CIName 事件关联的 CMDB 资源。
		CIID   string `json:"ci_id,omitempty"`
		CIName string `json:"ci_name,omitempty"`
		// Test 模拟触发发出的测试通知（标题带 [TEST]）。
		Test bool `json:"test,omitempty"`
	}
)

// 判定步骤（EvalPoint.Step）：本次评估后该序列停在哪一步。
const (
	StepPending     = "pending"      // 异常，未满持续时长
	StepFired       = "fired"        // 本次触发，已建事件
	StepFiring      = "firing"       // 持续告警中
	StepMutedEvent  = "muted_event"  // 满足触发条件，被「屏蔽事件」命中，未建事件
	StepMutedNotify = "muted_notify" // 已建事件，被「只屏蔽通知」命中，未通知
	StepInhibited   = "inhibited"    // 被同序列更高级别抑制
	StepRecovering  = "recovering"   // 已不异常，留观中
	StepResolved    = "resolved"     // 本次判为恢复
	// 以下为第二、三期增加（事件流水线的各阶段，顺序见 kit/monitorpipeline）。
	StepDropped           = "dropped"            // 被规则的条件丢弃命中，未建事件
	StepOutOfScope        = "out_of_scope"       // 不在规则限定的资源范围内，未建事件
	StepInhibitedRule     = "inhibited_rule"     // 已建事件，被抑制规则命中，未通知
	StepInhibitedTopology = "inhibited_topology" // 已建事件，上游资源有活跃告警，未通知
	StepMutedMaintenance  = "muted_maintenance"  // 已建事件，关联资源处于维护态，未通知
)

// FormatValue 把样本值格式化为最短且可还原的十进制文本（NaN / +Inf / -Inf 原样）。
func FormatValue(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// 标签匹配操作符（Matcher.Op）。
const (
	OpEqual    = "=="
	OpNotEqual = "!="
	OpRegex    = "=~"
	OpNotRegex = "!~"
	OpIn       = "in"
	OpNotIn    = "not in"
)

// ErrInvalid 契约校验失败的哨兵错误；具体原因经 %w 包装在返回值里。
var ErrInvalid = errors.New("monitorcontract: invalid")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalid}, args...)...)
}

// LabelNamePattern 标签名正则字面量（Prometheus 标签名；proto 中重复的字面量须与此一致）。
const LabelNamePattern = `^[a-zA-Z_][a-zA-Z0-9_]*$`

var labelNameRE = regexp.MustCompile(LabelNamePattern)

// ValidLabelName 报告是否为合法的 Prometheus 标签名（且不超过 MaxLabelNameLen）。
func ValidLabelName(name string) bool {
	return len(name) <= MaxLabelNameLen && labelNameRE.MatchString(name)
}

// ValidLabelValue 报告标签值是否合法：有效 UTF-8、不超过 MaxLabelValueLen 个字符。
func ValidLabelValue(value string) bool {
	return utf8.ValidString(value) && utf8.RuneCountInString(value) <= MaxLabelValueLen
}

// ValidateLabels 校验规则的附加标签：至多 MaxRuleLabels 个，名称合法且不以 "__" 开头
// （保留给时序库内部标签），值非空且合法。
func ValidateLabels(labels map[string]string) error {
	if len(labels) > MaxRuleLabels {
		return invalidf("at most %d labels", MaxRuleLabels)
	}
	for name, value := range labels {
		if !ValidLabelName(name) || (len(name) >= 2 && name[:2] == "__") {
			return invalidf("label name %q", name)
		}
		if value == "" || !ValidLabelValue(value) {
			return invalidf("label %q value", name)
		}
	}
	return nil
}

// Validate 校验单个匹配条件：标签名合法；== / != / =~ / !~ 只用 Value（正则须可编译）；
// in / not in 只用 Values（1 至 MaxMatcherValues 个）；值长度不超过 MaxMatcherValueLen。
func (m Matcher) Validate() error {
	_, err := compileMatcher(m)
	return err
}

// ValidateMatchers 校验一组匹配条件（至多 MaxMatchers 行，逐行 Validate）。
func ValidateMatchers(matchers []Matcher) error {
	_, err := CompileMatchers(matchers)
	return err
}

// Matchers 一组已校验、正则已编译的匹配条件，供热路径重复匹配；并发只读安全。
// 零值与 nil 均匹配任意标签集。
type Matchers struct {
	items []compiledMatcher
}

type compiledMatcher struct {
	label  string
	op     string
	value  string
	values []string
	re     *regexp.Regexp
}

// CompileMatchers 校验并编译一组匹配条件；出错时返回包装了 ErrInvalid 的错误。
func CompileMatchers(matchers []Matcher) (*Matchers, error) {
	if len(matchers) > MaxMatchers {
		return nil, invalidf("at most %d matchers", MaxMatchers)
	}
	out := &Matchers{items: make([]compiledMatcher, 0, len(matchers))}
	for i, m := range matchers {
		c, err := compileMatcher(m)
		if err != nil {
			return nil, fmt.Errorf("matcher %d: %w", i, err)
		}
		out.items = append(out.items, c)
	}
	return out, nil
}

func compileMatcher(m Matcher) (compiledMatcher, error) {
	c := compiledMatcher{label: m.Label, op: m.Op}
	if !ValidLabelName(m.Label) {
		return c, invalidf("label name %q", m.Label)
	}
	switch m.Op {
	case OpEqual, OpNotEqual, OpRegex, OpNotRegex:
		if len(m.Values) != 0 {
			return c, invalidf("op %q takes value, not values", m.Op)
		}
		if len(m.Value) > MaxMatcherValueLen || !utf8.ValidString(m.Value) {
			return c, invalidf("value of %q too long or not UTF-8", m.Label)
		}
		c.value = m.Value
		if m.Op == OpRegex || m.Op == OpNotRegex {
			// 先单独编译用户正则：`a)|(?:b` 这类不配对的写法包装后恰好合法，会破坏锚定。
			if _, err := regexp.Compile(m.Value); err != nil {
				return c, invalidf("regex of %q: %v", m.Label, err)
			}
			// 与 Prometheus 一致：正则整体锚定，匹配的是完整标签值；(?s) 使 . 匹配换行。
			re, err := regexp.Compile("^(?s:" + m.Value + ")$")
			if err != nil {
				return c, invalidf("regex of %q: %v", m.Label, err)
			}
			c.re = re
		}
	case OpIn, OpNotIn:
		if m.Value != "" {
			return c, invalidf("op %q takes values, not value", m.Op)
		}
		if len(m.Values) == 0 || len(m.Values) > MaxMatcherValues {
			return c, invalidf("op %q takes 1-%d values", m.Op, MaxMatcherValues)
		}
		for _, v := range m.Values {
			if len(v) > MaxMatcherValueLen || !utf8.ValidString(v) {
				return c, invalidf("value of %q too long or not UTF-8", m.Label)
			}
		}
		c.values = slices.Clone(m.Values)
	default:
		return c, invalidf("op %q", m.Op)
	}
	return c, nil
}

// Match 报告标签集是否满足全部条件（行间为与）。不存在的标签按空串参与比较（Prometheus 语义）：
// env != "prod"、env !~ "prod"、env not in [...] 对没有 env 标签的事件成立。
func (ms *Matchers) Match(labels map[string]string) bool {
	if ms == nil {
		return true
	}
	for i := range ms.items {
		if !ms.items[i].match(labels[ms.items[i].label]) {
			return false
		}
	}
	return true
}

// Len 条件行数。
func (ms *Matchers) Len() int {
	if ms == nil {
		return 0
	}
	return len(ms.items)
}

func (c *compiledMatcher) match(v string) bool {
	switch c.op {
	case OpEqual:
		return v == c.value
	case OpNotEqual:
		return v != c.value
	case OpRegex:
		return c.re.MatchString(v)
	case OpNotRegex:
		return !c.re.MatchString(v)
	case OpIn:
		return slices.Contains(c.values, v)
	case OpNotIn:
		return !slices.Contains(c.values, v)
	}
	return false
}

// MatchAll 一次性匹配：等价于 CompileMatchers 后 Match；条件非法时返回 false（宁可不命中，
// 不误屏蔽 / 误订阅）。每次调用都会编译正则，热路径请持有 CompileMatchers 的结果。
func MatchAll(matchers []Matcher, labels map[string]string) bool {
	ms, err := CompileMatchers(matchers)
	return err == nil && ms.Match(labels)
}

// fieldSep 哈希输入的字段分隔符：0xff 不会出现在合法 UTF-8 文本里，拼接无歧义。
const fieldSep = "\xff"

// Fingerprint 事件指纹：sha256(规则 ID | 数据源 ID | 按名称排序的标签 [| 级别]) 的前 32 位 hex。
// labels 为事件标签（序列标签去掉 __name__ 后与规则附加标签合并）。规则开启级别抑制时 severity
// 传 0（同一序列的各级别是同一个事件）；未开启时传该查询的级别（各级别是各自独立的事件）。
func Fingerprint(ruleID, datasourceID string, labels map[string]string, severity int32) string {
	buf := make([]byte, 0, 256)
	buf = append(buf, ruleID...)
	buf = append(buf, fieldSep...)
	buf = append(buf, datasourceID...)
	buf = appendSortedLabels(buf, labels, sortedKeys(labels))
	if severity != 0 {
		buf = append(buf, fieldSep+"\xfe"...)
		buf = strconv.AppendInt(buf, int64(severity), 10)
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:16])
}

// GroupKey 交给通知管道的聚合键（monitor_alert.group_key，≤128 字符）。groupKeyLabels 为空时按规则
// 聚合（"monitor:rule:<规则 ID>"）；否则再拼上这些标签取值的摘要（缺失的标签按空串，标签名去重排序）。
func GroupKey(ruleID string, labels map[string]string, groupKeyLabels []string) string {
	const prefix = "monitor:rule:"
	if len(groupKeyLabels) == 0 {
		return prefix + ruleID
	}
	names := slices.Clone(groupKeyLabels)
	slices.Sort(names)
	names = slices.Compact(names)
	sum := sha256.Sum256(appendSortedLabels(make([]byte, 0, 128), labels, names))
	return prefix + ruleID + ":" + hex.EncodeToString(sum[:8])
}

func sortedKeys(labels map[string]string) []string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func appendSortedLabels(buf []byte, labels map[string]string, names []string) []byte {
	for _, name := range names {
		buf = append(buf, fieldSep...)
		buf = append(buf, name...)
		buf = append(buf, fieldSep...)
		buf = append(buf, labels[name]...)
	}
	return buf
}

// locations 时区缓存：time.LoadLocation 每次都读时区库，判定在评估热路径上。只缓存加载成功的
// IANA 名称（有限集合），进程内只读优化，不承载任何正确性。
var locations sync.Map // map[string]*time.Location

// LoadLocation 带进程内缓存的 time.LoadLocation（空 / "UTC" 直接返回 time.UTC）：通知与模拟等每条都要
// 按规则时区格式化时间的路径用它，免得每次都读时区库。
func LoadLocation(name string) (*time.Location, error) { return loadLocation(name) }

func loadLocation(name string) (*time.Location, error) {
	if name == "" || name == "UTC" {
		return time.UTC, nil
	}
	if loc, ok := locations.Load(name); ok {
		return loc.(*time.Location), nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	locations.Store(name, loc)
	return loc, nil
}

// parseClock 解析 "HH:MM"（严格两位数字）为当天的分钟数。
func parseClock(s string) (int, bool) {
	if len(s) != 5 || s[2] != ':' {
		return 0, false
	}
	for _, i := range [...]int{0, 1, 3, 4} {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	h, m := int(s[0]-'0')*10+int(s[1]-'0'), int(s[3]-'0')*10+int(s[4]-'0')
	if h > 23 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// Validate 校验生效时间配置：时区可加载（Local 不接受：结果不得随部署机器变化）；Mode 合法且按工作
// 日历时不带时间窗；至多 MaxWindows 个时间窗；Days 非空、取 1-7 且不重复；Start / End 为 "HH:MM"。
func (e Effective) Validate() error {
	if e.Timezone == "Local" {
		return invalidf("timezone %q", e.Timezone)
	}
	if _, err := loadLocation(e.Timezone); err != nil {
		return invalidf("timezone %q", e.Timezone)
	}
	switch e.Mode {
	case "", EffectiveModeWindows:
	case EffectiveModeWorkTime, EffectiveModeOffTime:
		if len(e.Windows) != 0 {
			return invalidf("mode %q does not take windows", e.Mode)
		}
	default:
		return invalidf("mode %q", e.Mode)
	}
	if len(e.Windows) > MaxWindows {
		return invalidf("at most %d windows", MaxWindows)
	}
	for i, w := range e.Windows {
		if len(w.Days) == 0 || len(w.Days) > 7 {
			return invalidf("window %d: days", i)
		}
		var seen [8]bool
		for _, d := range w.Days {
			if d < 1 || d > 7 || seen[d] {
				return invalidf("window %d: day %d", i, d)
			}
			seen[d] = true
		}
		if _, ok := parseClock(w.Start); !ok {
			return invalidf("window %d: start %q", i, w.Start)
		}
		if _, ok := parseClock(w.End); !ok {
			return invalidf("window %d: end %q", i, w.End)
		}
	}
	return nil
}

// Contains 报告 now 是否落在时间窗内（不看 Mode；按工作日历的配置用 ContainsWithCalendar）。
// Windows 为空恒为 true；按 Timezone 的本地时间判定
// （无法加载的时区按 UTC）；时间窗含 Start 不含 End，Start > End 跨午夜（次日凌晨的部分归属
// 开始那一天的 Days），Start == End 为全天；无法解析的时间窗视为不命中。
func (e Effective) Contains(now time.Time) bool {
	if len(e.Windows) == 0 {
		return true
	}
	loc, err := loadLocation(e.Timezone)
	if err != nil {
		loc = time.UTC
	}
	t := now.In(loc)
	cur := t.Hour()*60 + t.Minute()
	today := isoWeekday(t.Weekday())
	yesterday := today - 1
	if yesterday == 0 {
		yesterday = 7
	}
	for _, w := range e.Windows {
		start, ok1 := parseClock(w.Start)
		end, ok2 := parseClock(w.End)
		if !ok1 || !ok2 {
			continue
		}
		switch {
		case start == end:
			if slices.Contains(w.Days, today) {
				return true
			}
		case start < end:
			if cur >= start && cur < end && slices.Contains(w.Days, today) {
				return true
			}
		default: // 跨午夜：当天 Start 之后，或前一天开始的窗口延续到今天 End 之前。
			if (cur >= start && slices.Contains(w.Days, today)) || (cur < end && slices.Contains(w.Days, yesterday)) {
				return true
			}
		}
	}
	return false
}

// isoWeekday 把 time.Weekday（周日 = 0）换成 1-7（周一至周日）。
func isoWeekday(d time.Weekday) int32 {
	if d == time.Sunday {
		return 7
	}
	return int32(d) // #nosec G115 -- time.Weekday 取值 0-6
}
