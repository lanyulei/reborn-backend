package biz

import (
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
)

// Task 已领取的评估任务（规则 × 数据源）；Token 是属主令牌，所有后续写以其围栏。
type Task struct {
	RuleID       string
	DatasourceID string
	Token        string
	// Lag 领取时刻 − next_eval_time（评估延迟指标）。
	Lag time.Duration
	// LastStatus / LastEval 领取时任务行记录的上一轮结果与时刻（零值 = 从未评估）：
	// 状态机据此判断待触发 / 留观计时是否跨越了未完成评估的轮次。
	LastStatus int32
	LastEval   time.Time
	// Postponed 上一轮完成的评估之后，这个任务被领取过但因排不上数据源并发槽而顺延（那一轮没有观察）。
	// 领取时由任务行带回；归还任务时原样写回任务行，直到有一轮评估完成才清除。评估器在顺延本轮之前置真。
	Postponed bool
	// ErrorSince / ErrorCount 领取时任务行记录的连续查询错误的起始时刻与次数（零值 = 上一轮不是查询错误）。
	// 记录规则的任务没有这两项。
	ErrorSince time.Time
	ErrorCount int32
}

// Rule 告警规则的引擎视图（monitor_rule 一行 + 所属分组的物化路径）。
type Rule struct {
	ID        string
	Name      string
	GroupID   string
	GroupPath string
	// Active 启用且未删除。
	Active         bool
	Labels         map[string]string
	Queries        []monitorcontract.Query
	LevelInhibit   bool
	Interval       time.Duration
	For            time.Duration
	Recover        time.Duration
	NodataKeep     time.Duration
	Effective      monitorcontract.Effective
	Annotations    map[string]string
	RecoveryExpr   string
	NotifyRecovery bool
	// RepeatInterval 重复通知间隔；0 = 不重复。
	RepeatInterval time.Duration
	// MaxNotify 触发 + 重复通知的总次数上限；0 = 不限。
	MaxNotify      int32
	GroupKeyLabels []string
	// Pipeline 事件流水线配置（标签改写 / 条件丢弃 / 资源关联 / 资源范围），每轮经
	// monitorpipeline.Compile 编译；零值 = 不改写、不丢弃、按全局缺省关联、不限范围。
	Pipeline monitorpipeline.Spec
	// TopologyInhibit 上游资源有活跃告警时抑制本规则的事件。
	TopologyInhibit bool
	// EscalationID 引用的升级策略；空 = 没有。策略已停用 / 删除时按没有处理。
	EscalationID string
	// Actions 触发 / 恢复时交给 core 执行的事件动作（自动建单 / 触发流程 / AI 摘要）。
	Actions monitorcontract.RuleActions
}

// keep 留观时长：序列不再出现后保持告警多久才判恢复（recover_s 与 nodata_keep_s 取大——
// Prometheus 类规则「条件不成立」与「序列消失」无法区分）。
func (r *Rule) keep() time.Duration { return max(r.Recover, r.NodataKeep) }

// Datasource 数据源的引擎视图（monitor_datasource 一行）。Secret 是密文，只在构造查询客户端时解密。
type Datasource struct {
	ID            string
	Name          string
	URL           string
	AuthType      int32
	Username      string
	Secret        []byte
	Headers       map[string]string
	Timeout       time.Duration
	TLSSkipVerify bool
	Enabled       bool
	// RemoteWriteURL 记录规则的写入地址（完整地址）；空 = 不可写。
	RemoteWriteURL string
	// Version / UpdateTime 更新标记：任一变化即重建查询客户端。
	Version    int32
	UpdateTime time.Time
}

// Mute 屏蔽规则的引擎视图（monitor_mute 一行 + 作用分组的物化路径）。
type Mute struct {
	ID string
	// GroupID 空 = 全部分组；非空而 GroupPath 为空 = 分组已删除，该规则不再命中任何事件。
	GroupID       string
	GroupPath     string
	RuleIDs       []string
	DatasourceIDs []string
	Severities    []int32
	Matchers      []monitorcontract.Matcher
	Mode          int32
	TimeType      int32
	// Start / End 零值 = 未设置（周期规则的可选起止）。
	Start    time.Time
	End      time.Time
	Periodic monitorcontract.Effective
}

// CfgVersions monitor_cfg_version 的各个版本号。
type CfgVersions struct {
	Datasource int64
	Mute       int64
	RuleGroup  int64
	Escalation int64
	Inhibit    int64
	CIIndex    int64
	Calendar   int64
}

// CIRow monitor_ci_index 的一行。Valid 为假 = 资源已删除 / 退役（增量加载时据此从索引里去掉）。
type CIRow struct {
	CI       monitorpipeline.CI
	Valid    bool
	SyncTime time.Time
}

// Escalation 一条启用的升级策略（monitor_escalation 一行）。
type Escalation struct {
	ID     string
	Policy monitorpipeline.EscalationPolicy
}

// ActiveAlert 抑制快照里的一个活跃告警：Ref 是它在抑制判定里的样子，Topology 表示它的规则开启了
// 拓扑抑制（它自己可能被上游资源的告警抑制）。
type ActiveAlert struct {
	Ref      monitorpipeline.AlertRef
	Topology bool
}

// SeriesState 判定状态（monitor_eval_state 一行）及其活跃告警的通知进度（monitor_alert 的只读视图）。
type SeriesState struct {
	Fingerprint string
	State       int32
	Severity    int32
	QueryIndex  int32
	Value       float64
	// Labels 事件标签的原始 JSON；只在序列本轮未出现时才需要解码（出现时用样本自带的标签）。
	Labels       []byte
	ActiveSince  time.Time
	LastSeen     time.Time
	RecoverSince time.Time
	// AlertID 活跃告警 ID；空 = 尚未触发，或告警已不再活跃。以下各项仅在非空时有意义。
	AlertID        string
	Muted          bool
	MuteID         string
	NotifyCount    int32
	LastNotifyTime time.Time
	// AlertSeverity / AlertValue / LastFireTime 告警行当前的级别、取值与最近一次写入时刻：
	// 内容没变且未到心跳间隔时本轮不写告警行。
	AlertSeverity int32
	AlertValue    float64
	LastFireTime  time.Time
	// PendingNotify 发件箱里还有该告警待派发的触发 / 重复通知（已入箱、core 尚未回写 NotifyCount）。
	PendingNotify bool
	// FirstNotifyDropped 首次通知行（触发，seq 1）被 core 丢弃的时刻；零值 = 该行不存在或不处于已丢弃状态。
	FirstNotifyDropped time.Time
	// FirstFire 告警的首次触发时间（升级各级的 after_m 自它起算）。
	FirstFire time.Time
	// AckTime 认领时间（core 写）；零值 = 未认领。认领后停止规则的重复通知与「未认领」类升级。
	AckTime time.Time
	// Inhibited / Inhibit 告警行当前的抑制标记与抑制来源。
	Inhibited bool
	Inhibit   monitorpipeline.InhibitHit
	// CIID 告警行当前关联的资源；空 = 没有。
	CIID string
	// EscalationLevel 告警行记录的已到达的最高升级级数。
	EscalationLevel int32
	// TicketID 关联的工单（core 写）；空 = 没有。
	TicketID string
	// Escalation 各级升级通知的进度（下标 i = 第 i+1 级）；只在规则引用了升级策略时由数据层填充。
	Escalation []monitorpipeline.EscalationLevelState
}

// notified 已发出或已入箱待发的触发 / 重复通知数：恢复通知的「此前通知过」与序号都按它算，
// core 派发滞后时也不会漏掉恢复通知。
func (s *SeriesState) notified() int32 {
	if s.PendingNotify {
		return s.NotifyCount + 1
	}
	return s.NotifyCount
}

// Sample 本轮的一个异常点（已去掉 __name__ 并合并规则附加标签）。
type Sample struct {
	Fingerprint string
	Labels      map[string]string
	Value       float64
	Severity    int32
	QueryIndex  int32
	// CI 关联到的资源（只读，指向资源索引）；nil = 没有关联。
	CI *monitorpipeline.CI
}

// ciID 关联资源的 ID；没有关联为空。
func (s *Sample) ciID() string {
	if s.CI == nil {
		return ""
	}
	return s.CI.ID
}

// upstream 关联资源的上游资源（只读）。
func (s *Sample) upstream() []string {
	if s.CI == nil {
		return nil
	}
	return s.CI.UpstreamIDs
}

// SampleSet 合并后的本轮样本。
type SampleSet struct {
	// Kept 参与判定的异常点（同指纹只留一个；开启级别抑制时为最高级别）。
	Kept []Sample
	// Inhibited 被同序列更高级别抑制的点（只保留判定记录用得到的前若干个）。
	Inhibited []Sample
	// Series 各查询返回的序列总数。
	Series int32
	// Limited 标签超过上限（个数 / 名称长度 / 值长度）而被删减或截断的序列数；这些序列照常参与判定。
	Limited int32
	// Dropped 被条件丢弃命中的点、OutOfScope 不在资源范围内的点（各只保留判定记录用得到的前若干个）；
	// 两者都不参与判定——对已有的状态而言等同于本轮没有出现。DroppedCount / OutOfScopeCount 是总数。
	Dropped         []Sample
	OutOfScope      []Sample
	DroppedCount    int32
	OutOfScopeCount int32
	// DropErrors 条件丢弃表达式渲染失败（因而没有丢弃）的点数。
	DropErrors int32
	index      map[string]int32
}

// has 报告指纹本轮是否出现。
func (s *SampleSet) has(fingerprint string) bool {
	_, ok := s.index[fingerprint]
	return ok
}

// StateInsert 新增判定状态行（active_since = last_seen = 本轮时刻）。
type StateInsert struct {
	Fingerprint string
	State       int32
	Severity    int32
	QueryIndex  int32
	Value       float64
	Labels      map[string]string
	AlertID     string
}

// StateUpdate 更新判定状态行。State 为留观中时表示留观从本轮起算（recover_since = 本轮时刻，
// last_seen 不变）；否则 last_seen = 本轮时刻、recover_since 清空。
type StateUpdate struct {
	Fingerprint string
	State       int32
	Severity    int32
	QueryIndex  int32
	Value       float64
	AlertID     string
	// Restart 持续时长从本轮重新起算（active_since = 本轮时刻）。
	Restart bool
}

// AlertInsert 新建告警事件（first_fire_time = last_fire_time = 本轮时刻）。
type AlertInsert struct {
	ID          string
	Fingerprint string
	Severity    int32
	QueryIndex  int32
	Value       float64
	Labels      map[string]string
	Annotations map[string]string
	GroupKey    string
	// Muted 为真而 MuteID 为空 = 因关联资源处于维护态而屏蔽。
	Muted  bool
	MuteID string
	// Inhibited / Inhibit 被抑制及其来源（抑制规则 + 源告警，或拓扑抑制的上游告警）。
	Inhibited bool
	Inhibit   monitorpipeline.InhibitHit
	CIID      string
}

// AlertUpdate 仍在告警：刷新 value / last_fire_time / severity / expr / 屏蔽与抑制标记 / 关联资源 /
// 升级级数。只在内容有变化或到了心跳间隔时产生（alertHeartbeat），不是每轮都写。
type AlertUpdate struct {
	ID              string
	Severity        int32
	QueryIndex      int32
	Value           float64
	Muted           bool
	MuteID          string
	Inhibited       bool
	Inhibit         monitorpipeline.InhibitHit
	CIID            string
	EscalationLevel int32
}

// AlertResolve 判恢复：status = 已恢复，resolve_time = 本轮时刻。
type AlertResolve struct {
	ID            string
	HasRecovery   bool
	RecoveryValue float64
	Muted         bool
	MuteID        string
}

// OutboxInsert 一条通知发件箱行。(alert_id, kind, seq) 唯一：重复写入是空操作；
// Rearm 为真时（屏蔽 / 抑制解除后的新通知、重复通知、首次通知或升级通知被丢弃后的重试）会把同键的
// 已丢弃行重新置为待派发。升级通知（kind 4）的 Seq 是 monitorcontract.EscalationOutboxSeq 的编码值，
// Level 是它的级数；其余通知 Level 为 0。
type OutboxInsert struct {
	ID      string
	AlertID string
	Kind    int32
	Seq     int32
	Level   int32
	Rearm   bool
}

// AlertEventInsert 一条事件时间线行（monitor_alert_event）。Type 取 monitorcontract.AlertEvent*。
// 明细只带引擎写的几类行用得到的项（落库时转成 monitorcontract.AlertEventDetail）：结构体尽量小——
// 整批恢复时每个事件一行。
type AlertEventInsert struct {
	ID      string
	AlertID string
	Type    string
	// Severity 触发 / 恢复时的级别；Level 升级到第几级。
	Severity int32
	Level    int32
	// MuteID 屏蔽规则（屏蔽 / 解除屏蔽）；为空而 CIID 非空 = 因资源维护态。
	MuteID string
	CIID   string
	// Inhibit 抑制来源（被抑制 / 解除抑制）。
	Inhibit monitorpipeline.InhibitHit
}

// Detail 落库的明细（monitor_alert_event.detail）。
func (e *AlertEventInsert) Detail() monitorcontract.AlertEventDetail {
	return monitorcontract.AlertEventDetail{
		Severity: e.Severity, Level: e.Level, MuteID: e.MuteID, CIID: e.CIID,
		InhibitKind: e.Inhibit.Kind, InhibitID: e.Inhibit.InhibitID, InhibitAlertID: e.Inhibit.AlertID,
	}
}

// ActionInsert 一条事件动作（monitor_alert_action）。(alert_id, kind) 唯一：重复写入是空操作。
type ActionInsert struct {
	ID      string
	AlertID string
	Kind    int32
	Payload monitorcontract.ActionPayload
}

// EngineEventInsert 一条自监控发件箱行（monitor_engine_event）。DedupKey 全表唯一：重复写入是空操作。
// Requires 非空时只在该去重键的事件存在时才写入（恢复事件以「对应的故障事件发过」为前提）。
type EngineEventInsert struct {
	ID       string
	Type     string
	ObjectID string
	DedupKey string
	Requires string
	Detail   monitorcontract.EngineEventPayload
}

// Transition 状态机一步的全部落库变更。切片归调用方所有。
type Transition struct {
	StateInserts  []StateInsert
	StateUpdates  []StateUpdate
	StateDeletes  []string
	AlertInserts  []AlertInsert
	AlertUpdates  []AlertUpdate
	AlertResolves []AlertResolve
	Outbox        []OutboxInsert
	// Events 时间线行；Actions 事件动作。与其余变更同一事务落库。
	Events  []AlertEventInsert
	Actions []ActionInsert
	// FiringCount 本轮之后持有活跃告警的序列数（告警中 + 留观中）。
	FiringCount int32
}

// Round 一轮评估的落库内容：判定记录 + 任务写回 +（可选的）状态变更。
type Round struct {
	// Now 本轮的数据库时刻：判定记录的 eval_time，以及全部状态 / 告警时间列。
	Now      time.Time
	Interval time.Duration
	Duration time.Duration
	Status   int32
	// Error 查询错误的说明；结果不完整（截断 / 部分结果）时 Status 仍是评估结果，这里写不完整的原因。
	Error string
	// Exprs 规则各查询的 PromQL（告警事件的 expr 按 QueryIndex 取）。
	Exprs     []string
	Series    int32
	Anomalies int32
	Recovered int32
	Detail    monitorcontract.EvalDetail
	// Alert 告警事件的冗余名称列。
	Alert AlertMeta
	// Transition nil = 不改任何状态（查询错误 / 不在生效时间），任务的 firing_count 保持原值。
	Transition *Transition
	// EngineEvent 非 nil = 该任务从「持续评估失败」中恢复：随任务写回同事务写入的 rule_recovered 事件。
	EngineEvent *EngineEventInsert
}

// AlertMeta 告警事件上冗余的规则 / 数据源信息。
type AlertMeta struct {
	RuleName       string
	GroupID        string
	DatasourceName string
}

// MaterializeResult 一轮任务物化的结果。
type MaterializeResult struct {
	Added        int
	Removed      int
	ClosedAlerts int
	// Hastened 因规则在上次评估之后被修改而提前到立即评估的任务数。
	Hastened int
}

// HealthResult 一个数据源的探活结果。Error 为空 = 正常。
type HealthResult struct {
	DatasourceID string
	Error        string
}

// HealthRecovery 一个数据源本次探活从异常转为正常：Since 是那次故障的起始时刻，Now 是数据库此刻。
type HealthRecovery struct {
	DatasourceID string
	Name         string
	Since        time.Time
	Now          time.Time
}

// DatasourceOutage 探活连续失败已超过阈值的数据源。
type DatasourceOutage struct {
	DatasourceID string
	Name         string
	Error        string
	Since        time.Time
	Now          time.Time
}

// RuleFailure 连续查询错误已超过阈值的评估任务（其数据源同期可用）。
type RuleFailure struct {
	RuleID         string
	RuleName       string
	DatasourceID   string
	DatasourceName string
	Error          string
	Since          time.Time
	Now            time.Time
	Failures       int32
}

// AlertCommand 一条待处理的事件指令（monitor_alert_cmd）。
type AlertCommand struct {
	ID      string
	AlertID string
	Kind    int32
}

// RecordRule 记录规则的引擎视图（monitor_record_rule 一行）。
type RecordRule struct {
	ID           string
	Name         string
	DatasourceID string
	Expr         string
	Interval     time.Duration
	Labels       map[string]string
	// Active 启用且未删除。
	Active bool
}

// RecordResult 一次记录规则执行的结果（写回 monitor_record_task）。
type RecordResult struct {
	// Status 取 monitorcontract.Record*。
	Status   int32
	Error    string
	Duration time.Duration
	// Series 本次写回的序列数。
	Series int32
	// Interval 距下一次执行的时长（已含抖动）。
	Interval time.Duration
}

// InstanceBeat 一次实例心跳（monitor_engine_instance 一行）。
type InstanceBeat struct {
	Instance string
	Roles    string
	Version  string
	Leader   bool
	// TaskCount / EvalLag 上一个心跳周期内完成的评估数与最大评估延迟；EvalCount / FailCount 启动以来的累计。
	TaskCount int32
	EvalLag   time.Duration
	EvalCount int64
	FailCount int64
}

// maxErrorBytes monitor_eval_task.last_error / monitor_eval_record.error / health_error 的字节上限。
const maxErrorBytes = 512

// truncate 把 s 整理成可落库的文本：去掉 NUL、修正非法 UTF-8，并截断到至多 n 字节且不切断字符。
func truncate(s string, n int) string {
	s = strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "?")
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}
