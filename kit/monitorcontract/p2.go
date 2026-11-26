package monitorcontract

import (
	"regexp"
	"slices"
	"time"
	"unicode/utf8"
)

// 第二、三期（认领 / 升级 / 抑制 / CMDB 联动 / 事件动作 / 自监控 / 记录规则）的落库契约。
// 表结构见 core/migrations/0076_monitor_p2.up.sql，设计见 backend/docs/monitor-tech-design-p2p3.md；
// 流水线的行为（改写、丢弃、关联、抑制、升级的计算）在 kit/monitorpipeline，本文件只定「长什么样」。

// 通知类型、来源与事件类型的第二期增量。
const (
	// NotifyKindEscalation 升级通知（monitor_notify_outbox.kind / monitor_notify_log.kind）：
	// 升级到某一级时的首次通知与该级的重复提醒。
	NotifyKindEscalation int32 = 4
	// NotifySourceEscalation 通知记录的来源：升级策略某一级的通知对象（monitor_notify_log.source）。
	NotifySourceEscalation int32 = 3

	EventAlertEscalated       = "monitor.alert.escalated"
	EventEngineDatasourceDown = "monitor.engine.datasource_down"
	EventEngineDatasourceUp   = "monitor.engine.datasource_recovered"
	EventEngineRuleFailing    = "monitor.engine.rule_failing"
	EventEngineRuleRecovered  = "monitor.engine.rule_recovered"
)

// EscalationSeqBase 升级通知在发件箱里的序号编码基数。发件箱沿用第一期的唯一键 (alert_id, kind, seq)
// （第一期引擎的 ON CONFLICT 以它为仲裁索引，滚动发布期间不能换）：kind = NotifyKindEscalation 的行
// seq = level × EscalationSeqBase + 级内序号，于是该唯一键等价于 (alert_id, kind, level, 级内序号)。
// 发件箱另有 level 列（冗余、有 CHECK 与 seq 对账）；通知记录（monitor_notify_log）的 seq 存级内序号。
const EscalationSeqBase int32 = 100000

// maxEscalationLevelSeq 一级之内的通知序号上限（编码不得进位到下一级）。
const maxEscalationLevelSeq = EscalationSeqBase - 1

// EscalationOutboxSeq 把（第几级，级内第几次通知）编码为发件箱的 seq；参数越界返回 0（不是合法的 seq）。
func EscalationOutboxSeq(level, n int32) int32 {
	if level < 1 || level > MaxEscalationLevels || n < 1 || n > maxEscalationLevelSeq {
		return 0
	}
	return level*EscalationSeqBase + n
}

// SplitEscalationSeq 是 EscalationOutboxSeq 的逆运算；seq 不是升级通知的编码时 ok 为 false。
func SplitEscalationSeq(seq int32) (level, n int32, ok bool) {
	level, n = seq/EscalationSeqBase, seq%EscalationSeqBase
	if level < 1 || level > MaxEscalationLevels || n < 1 {
		return 0, 0, false
	}
	return level, n, true
}

// EngineEventTypes 平台自监控的全部通知事件类型（monitor_engine_event.type）。返回新切片。
func EngineEventTypes() []string {
	return []string{EventEngineDatasourceDown, EventEngineDatasourceUp, EventEngineRuleFailing, EventEngineRuleRecovered}
}

// ValidEngineEventType 报告是否为平台自监控的事件类型。
func ValidEngineEventType(t string) bool { return slices.Contains(EngineEventTypes(), t) }

// 平台自监控事件的对象类型（EngineEventPayload.ObjectType）。
const (
	EngineObjectDatasource = "datasource"
	EngineObjectRule       = "rule"
)

// EngineEventPayload 平台自监控通知事件的内容体（notify_event.payload），同时是
// monitor_engine_event.detail 的结构：引擎写入对象与故障信息，core 派发时补上 URL。
// Since / Duration 是已格式化的文本（理由同 NotifyPayload）。
type EngineEventPayload struct {
	// ObjectType EngineObjectDatasource / EngineObjectRule。
	ObjectType string `json:"object_type"`
	ObjectID   string `json:"object_id"`
	ObjectName string `json:"object_name"`
	// DatasourceName 规则类事件：评估失败所在的数据源。
	DatasourceName string `json:"datasource_name,omitempty"`
	// Error 最近一次失败原因（不含凭据）；恢复事件为空。
	Error string `json:"error,omitempty"`
	// Since 故障开始时间；Duration 已持续 / 共持续时长；Failures 连续失败次数。
	Since    string `json:"since,omitempty"`
	Duration string `json:"duration,omitempty"`
	Failures int32  `json:"failures,omitempty"`
	// URL 引擎状态页或对象详情页地址，由 core 派发时填写。
	URL string `json:"url,omitempty"`
}

// 选主锁键与配置版本名的第二期增量。
const (
	// LockCISync core 的 monitor-ci-sync 循环：物化 monitor_ci_index 与 monitor_calendar。
	LockCISync = "monitor:ci-sync"
	// LockEngineWatch 引擎选主实例上的自监控检测（数据源持续不可用、规则持续评估失败）。
	LockEngineWatch = "monitor:engine-watch"

	CfgEscalation = "monitor_escalation"
	CfgInhibit    = "monitor_inhibit"
	CfgCIIndex    = "monitor_ci_index"
	CfgCalendar   = "monitor_calendar"
)

// 生效时间的方式（Effective.Mode）。
const (
	EffectiveModeWindows  = "windows"   // 按时间窗（缺省，空串等价）
	EffectiveModeWorkTime = "work_time" // 仅工作时间（工作日历）
	EffectiveModeOffTime  = "off_time"  // 仅非工作时间
)

// 事件时间线的类型（monitor_alert_event.type）。写入方：引擎写 fire / resolve / close / mute / unmute /
// inhibit / uninhibit / escalate；core 写其余各项。通知本身的时间线仍来自 monitor_notify_log。
const (
	AlertEventFire         = "fire"          // 触发
	AlertEventResolve      = "resolve"       // 恢复
	AlertEventClose        = "close"         // 关闭（手动关闭，或规则被删除 / 停用）
	AlertEventMute         = "mute"          // 被屏蔽（屏蔽规则或 CI 维护态）
	AlertEventUnmute       = "unmute"        // 屏蔽解除
	AlertEventInhibit      = "inhibit"       // 被抑制
	AlertEventUninhibit    = "uninhibit"     // 抑制解除
	AlertEventEscalate     = "escalate"      // 升级到某一级
	AlertEventNotify       = "notify"        // 与通知有关、但不对应通知记录的说明
	AlertEventAck          = "ack"           // 认领
	AlertEventUnack        = "unack"         // 取消认领
	AlertEventTransfer     = "transfer"      // 转交
	AlertEventComment      = "comment"       // 备注
	AlertEventTicket       = "ticket"        // 转工单 / 自动建单
	AlertEventTicketNote   = "ticket_note"   // 恢复时在关联工单上追加了记录
	AlertEventWorkflow     = "workflow"      // 触发了流程
	AlertEventAISummary    = "ai_summary"    // 生成了 AI 摘要
	AlertEventActionFailed = "action_failed" // 事件动作（建单 / 流程 / 摘要 / 回写）最终失败
	AlertEventIncident     = "incident"      // 被关联到故障 / 从故障移出（故障管理域写入，Detail.Text 为说明）
)

// AlertEventTypes 事件时间线的全部类型。返回新切片。
func AlertEventTypes() []string {
	return []string{
		AlertEventFire, AlertEventResolve, AlertEventClose, AlertEventMute, AlertEventUnmute, AlertEventInhibit,
		AlertEventUninhibit, AlertEventEscalate, AlertEventNotify, AlertEventAck, AlertEventUnack, AlertEventTransfer,
		AlertEventComment, AlertEventTicket, AlertEventTicketNote, AlertEventWorkflow, AlertEventAISummary, AlertEventActionFailed,
		AlertEventIncident,
	}
}

// ValidAlertEventType 报告是否为合法的时间线类型。
func ValidAlertEventType(t string) bool { return slices.Contains(AlertEventTypes(), t) }

// 关闭原因（AlertEventDetail.Cause，随 AlertEventClose）。
const (
	CloseCauseManual = "manual" // 手动关闭
	CloseCauseRule   = "rule"   // 规则被删除 / 停用，或数据源不再适用
)

// AlertEventDetail monitor_alert_event.detail。各类型用到的键：
//   - comment：Text；close：Cause、Text（关闭原因）；transfer：FromUser、ToUser、Text
//   - mute / unmute：MuteID（屏蔽规则；CI 维护态时为空且 CIID 非空）
//   - inhibit / uninhibit：InhibitKind、InhibitID（抑制规则）、InhibitAlertID（源告警）
//   - escalate：Level；fire / resolve：Severity
//   - ticket / ticket_note：TicketID、TicketNumber、Auto（自动建单为 true）
//   - workflow：WorkflowInstanceID；notify：Text
//   - action_failed：事件动作最终失败时为 ActionKind、Error；引擎放弃了一条连续失败的手动关闭指令时只有 Text
//     （没有操作人，说明里带原因）
//
// 用不到的键不落库（omitempty）。
type AlertEventDetail struct {
	Text               string `json:"text,omitempty"`
	Cause              string `json:"cause,omitempty"`
	FromUser           string `json:"from_user,omitempty"`
	ToUser             string `json:"to_user,omitempty"`
	MuteID             string `json:"mute_id,omitempty"`
	CIID               string `json:"ci_id,omitempty"`
	InhibitKind        int32  `json:"inhibit_kind,omitempty"`
	InhibitID          string `json:"inhibit_id,omitempty"`
	InhibitAlertID     string `json:"inhibit_alert_id,omitempty"`
	Level              int32  `json:"level,omitempty"`
	Severity           int32  `json:"severity,omitempty"`
	TicketID           string `json:"ticket_id,omitempty"`
	TicketNumber       string `json:"ticket_number,omitempty"`
	Auto               bool   `json:"auto,omitempty"`
	WorkflowInstanceID string `json:"workflow_instance_id,omitempty"`
	ActionKind         int32  `json:"action_kind,omitempty"`
	Error              string `json:"error,omitempty"`
}

// 抑制方式（monitor_alert.inhibit_kind）。
const (
	InhibitNone     int32 = 0 // 未被抑制
	InhibitRule     int32 = 1 // 被抑制规则（monitor_inhibit）命中
	InhibitTopology int32 = 2 // 上游资源有活跃告警
)

// 事件指令（monitor_alert_cmd：core 写入、引擎消费）。
const (
	AlertCmdClose int32 = 1 // 手动关闭

	CmdPending  int32 = 1 // 待处理
	CmdDone     int32 = 2 // 已执行
	CmdRejected int32 = 3 // 未执行：事件已不在告警中；或引擎连续失败已满上限而放弃（result 与时间线的 action_failed 事件写明原因）
)

// 事件动作（monitor_alert_action：引擎按规则配置写入、core 的 monitor-action 循环执行）。
const (
	ActionTicket     int32 = 1 // 自动建单
	ActionWorkflow   int32 = 2 // 触发流程
	ActionAISummary  int32 = 3 // AI 摘要
	ActionTicketNote int32 = 4 // 告警恢复时在关联工单上追加记录

	ActionPending int32 = 1 // 待执行
	ActionDone    int32 = 2 // 已完成
	ActionFailed  int32 = 3 // 重试耗尽，已放弃（记入时间线）
	ActionSkipped int32 = 4 // 无需执行（如未配置 AI 密钥、事件没有关联工单）

	// ActionMaxAttempts 事件动作的尝试次数上限；ActionAIMaxAttempts 为 AI 摘要的上限。
	ActionMaxAttempts   = 5
	ActionAIMaxAttempts = 2
)

// ValidActionKind 报告是否为合法的事件动作类型。
func ValidActionKind(k int32) bool { return k >= ActionTicket && k <= ActionTicketNote }

// WorkflowBusinessType 告警触发的流程所挂的业务对象类型（业务 ID = 告警事件 ID）。
const WorkflowBusinessType = "monitor_alert"

// 规则上的事件动作与 CMDB 配置（monitor_rule 的 jsonb 列）。
type (
	// TicketAction 触发时自动建单。Enabled 时 TemplateID（工单模板）与 AgentUserID（代理提单人，
	// 一个可用用户：权限按他判定）必填。
	TicketAction struct {
		Enabled     bool   `json:"enabled,omitempty"`
		TemplateID  string `json:"template_id,omitempty"`
		AgentUserID string `json:"agent_user_id,omitempty"`
	}

	// WorkflowAction 触发时发起流程。Enabled 时 DefinitionKey（流程标识）与 AgentUserID（发起人）必填。
	WorkflowAction struct {
		Enabled       bool   `json:"enabled,omitempty"`
		DefinitionKey string `json:"definition_key,omitempty"`
		AgentUserID   string `json:"agent_user_id,omitempty"`
	}

	// RuleActions monitor_rule.actions。零值 = 没有任何动作。
	RuleActions struct {
		Ticket    TicketAction   `json:"ticket"`
		Workflow  WorkflowAction `json:"workflow"`
		AISummary bool           `json:"ai_summary,omitempty"`
	}

	// ActionPayload monitor_alert_action.payload：引擎入队时从规则配置拍下的快照，执行不受规则后续修改影响。
	ActionPayload struct {
		TemplateID    string `json:"template_id,omitempty"`
		AgentUserID   string `json:"agent_user_id,omitempty"`
		DefinitionKey string `json:"definition_key,omitempty"`
	}

	// CIMatch monitor_rule.ci_match：事件如何关联 CMDB 资源。零值 = 全局缺省（DefaultCIMatches）。
	// Label 非空时只按这一项匹配：取事件的 Label 标签（值形如 host:port 时去掉端口），By 为
	// CIMatchByName 时比对资源名称，为 CIMatchByAttr 时比对资源的 Attr 属性（缺省 DefaultCIMatchAttr）。
	// ModelCodes 非空时只在这些模型里找；EnrichTags 里列出的资源标签会作为 ci_tag_<标签> 写入事件。
	CIMatch struct {
		Disabled   bool     `json:"disabled,omitempty"`
		Label      string   `json:"label,omitempty"`
		By         string   `json:"by,omitempty"`
		Attr       string   `json:"attr,omitempty"`
		ModelCodes []string `json:"model_codes,omitempty"`
		EnrichTags []string `json:"enrich_tags,omitempty"`
	}

	// ResourceScope monitor_rule.resource_scope：规则仅对指定资源生效。Enabled 时事件必须关联到资源，
	// 且资源的模型在 ModelCodes 内、环境在 Envs 内、带有 Tags 中的任意一个（各项为空 = 不限），否则丢弃。
	ResourceScope struct {
		Enabled    bool     `json:"enabled,omitempty"`
		ModelCodes []string `json:"model_codes,omitempty"`
		Envs       []string `json:"envs,omitempty"`
		Tags       []string `json:"tags,omitempty"`
	}

	// RelabelStep monitor_rule.relabel 的一步（语义同 Prometheus relabel_config 的同名动作）：
	//   - replace：SourceLabels 的取值以 Separator（缺省 ";"）拼接，整体匹配 Regex（缺省 "(.*)"）时把
	//     Replacement（缺省 "$1"，可引用捕获组）写入 TargetLabel；结果为空则删除该标签；不匹配则不动
	//   - labelmap：标签名整体匹配 Regex 的，按 Replacement 生成新标签名并复制取值
	//   - labeldrop / labelkeep：删除 / 只保留标签名整体匹配 Regex 的标签
	RelabelStep struct {
		Action       string   `json:"action"`
		SourceLabels []string `json:"source_labels,omitempty"`
		Separator    string   `json:"separator,omitempty"`
		Regex        string   `json:"regex,omitempty"`
		TargetLabel  string   `json:"target_label,omitempty"`
		Replacement  string   `json:"replacement,omitempty"`
	}

	// InhibitSelector monitor_inhibit.source / target：一侧的匹配条件，各项之间是「并且」，空 = 不限。
	InhibitSelector struct {
		RuleIDs    []string  `json:"rule_ids,omitempty"`
		Severities []int32   `json:"severities,omitempty"`
		Matchers   []Matcher `json:"matchers,omitempty"`
	}

	// AlertViewFilter monitor_alert_view.filters：保存的活跃告警聚合视图的过滤条件。
	AlertViewFilter struct {
		GroupID      string   `json:"group_id,omitempty"`
		DatasourceID string   `json:"datasource_id,omitempty"`
		Severities   []int32  `json:"severities,omitempty"`
		Muted        *bool    `json:"muted,omitempty"`
		Acked        *bool    `json:"acked,omitempty"`
		Inhibited    *bool    `json:"inhibited,omitempty"`
		Labels       []string `json:"labels,omitempty"`
	}
)

// CMDB 关联的取值与缺省。
const (
	CIMatchByName = "name"
	CIMatchByAttr = "attr"
	// DefaultCIMatchAttr By = attr 而未指定属性时比对的属性。
	DefaultCIMatchAttr = "ip"
	// CIStatusMaintenance 资源处于维护态（cmdb_ci.status）：关联到它的告警只屏蔽通知。
	CIStatusMaintenance = "maintenance"
	// CILabelPrefix 富化标签的前缀：ci_env / ci_owner / ci_model / ci_tag_<标签>。富化标签不参与指纹。
	CILabelPrefix = "ci_"
)

// DefaultCIMatches 规则未配置关联方式（CIMatch 零值）时依次尝试的匹配：ident 标签 → 资源名称，
// 其次 instance 标签 → ip 属性。返回新切片。
func DefaultCIMatches() []CIMatch {
	return []CIMatch{
		{Label: "ident", By: CIMatchByName},
		{Label: "instance", By: CIMatchByAttr, Attr: DefaultCIMatchAttr},
	}
}

// 改写动作（RelabelStep.Action）。
const (
	RelabelReplace   = "replace"
	RelabelLabelMap  = "labelmap"
	RelabelLabelDrop = "labeldrop"
	RelabelLabelKeep = "labelkeep"
)

// 第二、三期的数量与长度上限（与迁移的 CHECK、proto 的校验规则同一口径）。
const (
	MaxRelabelSteps        = 10   // 每条规则的改写步骤数
	MaxRelabelSourceLabels = 5    // replace 的源标签数
	MaxRelabelRegexLen     = 256  // 改写正则的长度（字节）
	MaxRelabelTextLen      = 256  // Replacement / Separator 的长度（字节）
	MaxDropExprBytes       = 2048 // 条件丢弃表达式的长度
	MaxCIMatchModels       = 20   // 关联 / 资源范围限定的模型数
	MaxCIEnrichTags        = 10   // 富化写入的资源标签数
	MaxScopeValues         = 20   // 资源范围的环境 / 标签个数
	MaxCodeLen             = 64   // 模型编码、环境、资源标签的长度（字节）
	MaxInhibitEqual        = 10   // 抑制规则的相等标签数
	MaxInhibitRuleIDs      = 200  // 抑制规则一侧限定的规则数
	MaxEscalationLevels    = 10   // 升级策略的级数
	MaxEscalationTargets   = 20   // 升级策略每级每类通知对象的个数
	MaxEscalationAfterM    = 10080
	MaxEscalationNotify    = 100 // 升级策略每级的通知次数上限的上限
	MaxEscalationDeptUp    = 5   // 部门负责人上溯的层数
	MaxCommentLen          = 2000
	MaxCloseReasonLen      = 255
	MaxAISummaryBytes      = 4096
)

// CIAttrPattern 可用于关联的资源属性名（即 CMDB 的字段编码）；proto 中重复的字面量须与此一致。
const CIAttrPattern = `^[a-z][a-z0-9_]{0,63}$`

var attrNameRE = regexp.MustCompile(CIAttrPattern)

// validCode 报告是否为合法的短编码（模型编码、环境、资源标签）：非空、不超过 MaxCodeLen、合法 UTF-8、
// 不含控制字符。
func validCode(s string) bool {
	if s == "" || len(s) > MaxCodeLen || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func validCodes(list []string, limit int) bool {
	if len(list) > limit {
		return false
	}
	for i, s := range list {
		if !validCode(s) || slices.Contains(list[:i], s) {
			return false
		}
	}
	return true
}

// Validate 校验关联配置：Label 为空时 By / Attr 也须为空（全局缺省）；否则 Label 为合法标签名，
// By 取 name / attr，Attr 只在 By = attr 时可填。ModelCodes / EnrichTags 不超限、不重复。
func (m CIMatch) Validate() error {
	if !validCodes(m.ModelCodes, MaxCIMatchModels) {
		return invalidf("ci_match model_codes")
	}
	if !validCodes(m.EnrichTags, MaxCIEnrichTags) {
		return invalidf("ci_match enrich_tags")
	}
	if m.Label == "" {
		if m.By != "" || m.Attr != "" {
			return invalidf("ci_match by / attr require label")
		}
		return nil
	}
	if !ValidLabelName(m.Label) {
		return invalidf("ci_match label %q", m.Label)
	}
	switch m.By {
	case CIMatchByName:
		if m.Attr != "" {
			return invalidf("ci_match attr is only used when by is %q", CIMatchByAttr)
		}
	case CIMatchByAttr:
		if m.Attr != "" && !attrNameRE.MatchString(m.Attr) {
			return invalidf("ci_match attr %q", m.Attr)
		}
	default:
		return invalidf("ci_match by %q", m.By)
	}
	return nil
}

// Validate 校验资源范围：各项不超限、不重复；Enabled 为假时各项须为空。
func (s ResourceScope) Validate() error {
	if !validCodes(s.ModelCodes, MaxCIMatchModels) || !validCodes(s.Envs, MaxScopeValues) || !validCodes(s.Tags, MaxScopeValues) {
		return invalidf("resource_scope values")
	}
	if !s.Enabled && len(s.ModelCodes)+len(s.Envs)+len(s.Tags) > 0 {
		return invalidf("resource_scope conditions require enabled")
	}
	return nil
}

// Empty 报告一侧的匹配条件是否为空（不限）。
func (s InhibitSelector) Empty() bool {
	return len(s.RuleIDs) == 0 && len(s.Severities) == 0 && len(s.Matchers) == 0
}

func (s InhibitSelector) validate(side string) error {
	if len(s.RuleIDs) > MaxInhibitRuleIDs {
		return invalidf("%s: at most %d rules", side, MaxInhibitRuleIDs)
	}
	for i, sev := range s.Severities {
		if !ValidSeverity(sev) || slices.Contains(s.Severities[:i], sev) {
			return invalidf("%s: severity %d", side, sev)
		}
	}
	if err := ValidateMatchers(s.Matchers); err != nil {
		return invalidf("%s: %v", side, err)
	}
	return nil
}

// ValidateInhibit 校验一条抑制规则：源条件不得为空（否则任何告警都是源）；目标条件与相等标签不得
// 同时为空（否则源会抑制作用范围内的一切）；相等标签为合法标签名、不重复、至多 MaxInhibitEqual 个。
func ValidateInhibit(source, target InhibitSelector, equal []string) error {
	if err := source.validate("source"); err != nil {
		return err
	}
	if err := target.validate("target"); err != nil {
		return err
	}
	if source.Empty() {
		return invalidf("source must have at least one condition")
	}
	if target.Empty() && len(equal) == 0 {
		return invalidf("target conditions and equal labels must not both be empty")
	}
	if len(equal) > MaxInhibitEqual {
		return invalidf("at most %d equal labels", MaxInhibitEqual)
	}
	for i, name := range equal {
		if !ValidLabelName(name) || slices.Contains(equal[:i], name) {
			return invalidf("equal label %q", name)
		}
	}
	return nil
}

// 升级策略（monitor_escalation）。
const (
	EscalationModeStack    int32 = 1 // 叠加：升级后前面各级与规则通知对象的重复提醒照常继续
	EscalationModeTakeover int32 = 2 // 接替：升级后只提醒当前这一级

	EscalationTriggerUnacked    int32 = 1 // 告警持续 after_m 分钟仍未认领
	EscalationTriggerUnresolved int32 = 2 // 告警持续 after_m 分钟仍未恢复（不受认领影响）
)

type (
	// EscalationSchedule 升级通知对象里的一张值班表；BackupOnly 为真时只通知备班。
	EscalationSchedule struct {
		ID         string `json:"id"`
		BackupOnly bool   `json:"backup_only,omitempty"`
	}

	// EscalationDept 升级通知对象里的一个部门：通知它往上第 Up 级部门的负责人（0 = 本部门）。
	EscalationDept struct {
		ID string `json:"id"`
		Up int32  `json:"up,omitempty"`
	}

	// EscalationTargets 升级策略一级的通知对象：指定用户、值班表当前值班人、值班分组的管理员、部门负责人。
	EscalationTargets struct {
		UserIDs        []string             `json:"user_ids,omitempty"`
		Schedules      []EscalationSchedule `json:"schedules,omitempty"`
		OncallGroupIDs []string             `json:"oncall_group_ids,omitempty"`
		Depts          []EscalationDept     `json:"depts,omitempty"`
	}

	// EscalationLevel monitor_escalation.levels 的一项（数组下标 + 1 = 第几级）。
	// AfterM 自首次触发起算的分钟数；RepeatIntervalM 该级的重复提醒间隔（0 = 只在升级时通知一次）；
	// MaxNotify 该级的通知次数上限（含升级时的那一次，0 = 不限）。
	EscalationLevel struct {
		Trigger         int32             `json:"trigger"`
		AfterM          int32             `json:"after_m"`
		Targets         EscalationTargets `json:"targets"`
		RepeatIntervalM int32             `json:"repeat_interval_m"`
		MaxNotify       int32             `json:"max_notify"`
	}
)

// Empty 报告一级的通知对象是否为空。
func (t EscalationTargets) Empty() bool {
	return len(t.UserIDs) == 0 && len(t.Schedules) == 0 && len(t.OncallGroupIDs) == 0 && len(t.Depts) == 0
}

// ValidEscalationMode 报告是否为合法的升级方式。
func ValidEscalationMode(m int32) bool {
	return m == EscalationModeStack || m == EscalationModeTakeover
}

// ValidateEscalationLevels 校验升级策略的各级：1 至 MaxEscalationLevels 级；触发条件合法；AfterM 为
// 1 至 MaxEscalationAfterM 且逐级严格递增；RepeatIntervalM 为 0 或 5-10080；MaxNotify 为 0 至
// MaxEscalationNotify；每级至少一个通知对象，各类对象不超过 MaxEscalationTargets 个且不重复，
// 部门上溯层数为 0 至 MaxEscalationDeptUp。引用对象的 ID 格式与存在性由调用方校验。
func ValidateEscalationLevels(levels []EscalationLevel) error {
	if len(levels) == 0 || len(levels) > MaxEscalationLevels {
		return invalidf("escalation takes 1-%d levels", MaxEscalationLevels)
	}
	var prev int32
	for i, l := range levels {
		if l.Trigger != EscalationTriggerUnacked && l.Trigger != EscalationTriggerUnresolved {
			return invalidf("level %d: trigger %d", i+1, l.Trigger)
		}
		if l.AfterM < 1 || l.AfterM > MaxEscalationAfterM || l.AfterM <= prev {
			return invalidf("level %d: after_m must be 1-%d and greater than the previous level", i+1, MaxEscalationAfterM)
		}
		prev = l.AfterM
		if l.RepeatIntervalM != 0 && (l.RepeatIntervalM < 5 || l.RepeatIntervalM > 10080) {
			return invalidf("level %d: repeat_interval_m must be 0 or 5-10080", i+1)
		}
		if l.MaxNotify < 0 || l.MaxNotify > MaxEscalationNotify {
			return invalidf("level %d: max_notify must be 0-%d", i+1, MaxEscalationNotify)
		}
		if err := l.Targets.validate(); err != nil {
			return invalidf("level %d: %v", i+1, err)
		}
	}
	return nil
}

func (t EscalationTargets) validate() error {
	if t.Empty() {
		return invalidf("at least one notify target is required")
	}
	if len(t.UserIDs) > MaxEscalationTargets || len(t.Schedules) > MaxEscalationTargets ||
		len(t.OncallGroupIDs) > MaxEscalationTargets || len(t.Depts) > MaxEscalationTargets {
		return invalidf("at most %d targets of each kind", MaxEscalationTargets)
	}
	for i, id := range t.UserIDs {
		if id == "" || slices.Contains(t.UserIDs[:i], id) {
			return invalidf("user %q is empty or duplicated", id)
		}
	}
	for i, id := range t.OncallGroupIDs {
		if id == "" || slices.Contains(t.OncallGroupIDs[:i], id) {
			return invalidf("oncall group %q is empty or duplicated", id)
		}
	}
	for i, s := range t.Schedules {
		if s.ID == "" || slices.ContainsFunc(t.Schedules[:i], func(o EscalationSchedule) bool { return o.ID == s.ID }) {
			return invalidf("schedule %q is empty or duplicated", s.ID)
		}
	}
	for i, d := range t.Depts {
		if d.ID == "" || d.Up < 0 || d.Up > MaxEscalationDeptUp ||
			slices.ContainsFunc(t.Depts[:i], func(o EscalationDept) bool { return o == d }) {
			return invalidf("dept %q is empty, duplicated or has an invalid level", d.ID)
		}
	}
	return nil
}

// 记录规则（monitor_record_rule / monitor_record_task）。
const (
	RecordNever      int32 = 0 // 未执行
	RecordOK         int32 = 1 // 已写回
	RecordNoData     int32 = 2 // 查询无结果，未写入
	RecordQueryError int32 = 3 // 查询失败
	RecordWriteError int32 = 4 // 写回失败

	// MetricNamePattern 记录规则的指标名（Prometheus 指标名）；proto 中重复的字面量须与此一致。
	MetricNamePattern = `^[a-zA-Z_:][a-zA-Z0-9_:]*$`
)

var metricNameRE = regexp.MustCompile(MetricNamePattern)

// ValidMetricName 报告是否为合法的指标名（且不超过 MaxLabelNameLen）。
func ValidMetricName(name string) bool {
	return len(name) <= MaxLabelNameLen && metricNameRE.MatchString(name)
}

// 工作日历（monitor_calendar.config）。结构与流程域的工作日历配置一致：core 原样物化，引擎只读。
type (
	// CalendarSegment 一段每日工作时段（"HH:MM"，含 Start 不含 End；End 可写 "24:00"）。
	CalendarSegment struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}

	// CalendarSnapshot 工作日历快照。Weekdays 取 0-6（周日 = 0）；Holidays / Workdays 为按 Timezone
	// 解释的日期 "yyyy-mm-dd"（节假日：是工作日也不上班；调休上班日：是周末也上班，优先于节假日）。
	// 零值表示尚未物化（Compile 报错，判定时按「没有日历」处理）。
	CalendarSnapshot struct {
		Timezone string            `json:"timezone"`
		Weekdays []int32           `json:"weekdays"`
		Segments []CalendarSegment `json:"segments"`
		Holidays []string          `json:"holidays"`
		Workdays []string          `json:"workdays"`
	}
)

// Calendar 编译后的工作日历：只读，可并发使用。nil 表示没有日历。
type Calendar struct {
	loc      *time.Location
	weekdays [7]bool
	segments [][2]int // 自零点起的分钟数 [start, end)
	holidays map[string]struct{}
	workdays map[string]struct{}
}

// 工作日历的上限（与流程域的校验同一口径）。
const (
	maxCalendarSegments = 6
	maxCalendarDays     = 400
)

// parseSegmentClock 解析 "HH:MM" 为分钟数；额外接受 "24:00"（当日结束）。
func parseSegmentClock(s string) (int, bool) {
	if s == "24:00" {
		return 24 * 60, true
	}
	return parseClock(s)
}

// Compile 校验并编译快照：时区可加载且非空；至少一个工作日（0-6，不重复）；1-6 个按先后排列、
// 互不重叠的工作时段；日期为 "yyyy-mm-dd"。失败返回包装了 ErrInvalid 的错误。
func (s CalendarSnapshot) Compile() (*Calendar, error) {
	if s.Timezone == "" || s.Timezone == "Local" {
		return nil, invalidf("calendar timezone %q", s.Timezone)
	}
	loc, err := loadLocation(s.Timezone)
	if err != nil {
		return nil, invalidf("calendar timezone %q", s.Timezone)
	}
	c := &Calendar{loc: loc, holidays: make(map[string]struct{}, len(s.Holidays)), workdays: make(map[string]struct{}, len(s.Workdays))}
	if len(s.Weekdays) == 0 || len(s.Weekdays) > 7 {
		return nil, invalidf("calendar weekdays")
	}
	for _, d := range s.Weekdays {
		if d < 0 || d > 6 || c.weekdays[d] {
			return nil, invalidf("calendar weekday %d", d)
		}
		c.weekdays[d] = true
	}
	if len(s.Segments) == 0 || len(s.Segments) > maxCalendarSegments {
		return nil, invalidf("calendar takes 1-%d segments", maxCalendarSegments)
	}
	prev := 0
	for _, seg := range s.Segments {
		start, ok1 := parseSegmentClock(seg.Start)
		end, ok2 := parseSegmentClock(seg.End)
		if !ok1 || !ok2 || start >= end || start < prev {
			return nil, invalidf("calendar segment %s-%s", seg.Start, seg.End)
		}
		c.segments = append(c.segments, [2]int{start, end})
		prev = end
	}
	if len(s.Holidays) > maxCalendarDays || len(s.Workdays) > maxCalendarDays {
		return nil, invalidf("calendar takes at most %d holidays and %d workdays", maxCalendarDays, maxCalendarDays)
	}
	for _, d := range s.Holidays {
		if _, err := time.Parse(time.DateOnly, d); err != nil {
			return nil, invalidf("calendar date %q", d)
		}
		c.holidays[d] = struct{}{}
	}
	for _, d := range s.Workdays {
		if _, err := time.Parse(time.DateOnly, d); err != nil {
			return nil, invalidf("calendar date %q", d)
		}
		c.workdays[d] = struct{}{}
	}
	return c, nil
}

// Working 报告 t 是否在工作时间内：当天上班（调休上班日 > 节假日 > 每周工作日，按日历时区的日期）
// 且钟点落在某个工作时段内。nil 日历恒为 false。
func (c *Calendar) Working(t time.Time) bool {
	if c == nil {
		return false
	}
	local := t.In(c.loc)
	var buf [10]byte
	day := local.AppendFormat(buf[:0], time.DateOnly)
	// map[string(字节切片)] 的查找不分配：判定在评估热路径上。
	if _, ok := c.workdays[string(day)]; !ok {
		if _, off := c.holidays[string(day)]; off || !c.weekdays[local.Weekday()] {
			return false
		}
	}
	cur := local.Hour()*60 + local.Minute()
	for _, seg := range c.segments {
		if cur >= seg[0] && cur < seg[1] {
			return true
		}
	}
	return false
}

// UsesCalendar 报告生效时间是否按工作日历判定。
func (e Effective) UsesCalendar() bool {
	return e.Mode == EffectiveModeWorkTime || e.Mode == EffectiveModeOffTime
}

// ContainsWithCalendar 报告 now 是否落在生效时间内，引擎与 core（模拟触发）共用：
//   - Mode 为空 / windows：等同 Contains（cal 不参与）
//   - work_time：cal.Working(now)；off_time：!cal.Working(now)
//   - 按工作日历而 cal 为 nil（日历尚未物化或配置非法）：恒为 true——宁可多评估，不因缺日历漏掉告警
//   - 无法识别的 Mode：按 windows 处理
func (e Effective) ContainsWithCalendar(now time.Time, cal *Calendar) bool {
	if !e.UsesCalendar() {
		return e.Contains(now)
	}
	if cal == nil {
		return true
	}
	return cal.Working(now) == (e.Mode == EffectiveModeWorkTime)
}
