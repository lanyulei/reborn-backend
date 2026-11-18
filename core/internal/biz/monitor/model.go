package biz

import (
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 上限（与迁移的 CHECK、proto 校验同一口径；monitorcontract 未覆盖的部分）。
const (
	// MaxGroupDepth 规则分组的最大层级。
	MaxGroupDepth = 5
	// MaxGroups 规则分组总数上限。
	MaxGroups = 500
	// MaxRuleDatasources 规则指定的数据源个数上限。
	MaxRuleDatasources = 20
	// MaxBatchRules 一次批量操作的规则数上限。
	MaxBatchRules = 200
	// MaxImportRules 一次导入（预览）的规则数上限。
	MaxImportRules = 500
	// MaxExportRules 一次导出的规则数上限。
	MaxExportRules = 2000
	// MaxAnnotationLen 附加信息模板的最大字符数。
	MaxAnnotationLen = 2048
)

// NamedRef 被引用对象的 ID 与显示名；Missing 表示对象已不存在或不可用。
type NamedRef struct {
	ID      string
	Name    string
	Missing bool
}

// GroupFilter 数据层列表查询的分组过滤：All 为不限（超级管理员且未指定分组），否则只返回
// group_id 在 IDs 内的行（IDs 为空 = 什么都不返回）。由 Scope 生成，data 层只照做。
type GroupFilter struct {
	All bool
	IDs []string
}

// Empty 报告过滤结果是否必然为空（调用方可据此跳过查询）。
func (f GroupFilter) Empty() bool { return !f.All && len(f.IDs) == 0 }

// ---------------------------------------------------------------------------
// 规则分组与角色数据范围
// ---------------------------------------------------------------------------

// RuleGroup 规则分组（树节点）。Path 为物化路径 /<祖先 id>/…/<自身 id>/。
type RuleGroup struct {
	ID          string
	ParentID    string
	Path        string
	Name        string
	Description string
	Sort        int32
	RuleCount   int32
	ChildCount  int32
	CreateTime  time.Time
	UpdateTime  time.Time
}

// RoleScope 角色对分组的授权。
type RoleScope struct {
	RoleID     string
	GroupID    string
	Level      int32
	UpdateUser string
	UpdateTime time.Time
}

// ---------------------------------------------------------------------------
// 数据源
// ---------------------------------------------------------------------------

// Connection 数据源的连接配置。Secret 为明文凭据，只在测试连接 / 查询的调用链上短暂存在，
// 不落日志、不出现在任何返回值里。
type Connection struct {
	URL           string
	AuthType      int32
	Username      string
	Secret        string
	Headers       map[string]string
	TimeoutMS     int32
	TLSSkipVerify bool
}

// Datasource 数据源。凭据密文不在模型里（只写不读），HasSecret 表示是否已设置。
type Datasource struct {
	ID             string
	Name           string
	Type           string
	URL            string
	AuthType       int32
	Username       string
	HasSecret      bool
	Headers        map[string]string
	TimeoutMS      int32
	TLSSkipVerify  bool
	RemoteWriteURL string
	Description    string
	IsDefault      bool
	Enabled        bool
	HealthStatus   int32
	HealthError    string
	HealthTime     *time.Time
	RuleCount      int32
	Version        int32
	CreateTime     time.Time
	UpdateTime     time.Time
	UpdateUser     string
	// UpdateUserName 展示名，由用例填充。
	UpdateUserName string
}

// connection 取连接配置（不含凭据）。
func (d *Datasource) connection() Connection {
	return Connection{
		URL: d.URL, AuthType: d.AuthType, Username: d.Username, Headers: d.Headers,
		TimeoutMS: d.TimeoutMS, TLSSkipVerify: d.TLSSkipVerify,
	}
}

// DatasourceInput 新建 / 编辑数据源的入参。Secret 为 nil 表示不修改已保存的凭据。
type DatasourceInput struct {
	Name           string
	Conn           Connection
	Secret         *string
	RemoteWriteURL string
	Description    string
	IsDefault      bool
	Enabled        bool
}

// DatasourceFilter 数据源列表过滤条件。
type DatasourceFilter struct {
	Keyword      string
	Enabled      *bool
	HealthStatus *int32
}

// DatasourceBrief 数据源的名称与启用状态（引用校验与展示用）。
type DatasourceBrief struct {
	Name    string
	Enabled bool
}

// SecretChange 写数据源时对凭据密文的处理：Set=false 保留原值；Set=true 写入 Sealed（nil = 清除）。
type SecretChange struct {
	Set    bool
	Sealed []byte
}

// QueryPoint 一个采样点。
type QueryPoint struct {
	Time  time.Time
	Value float64
}

// QuerySeries 一条序列。
type QuerySeries struct {
	Labels map[string]string
	Points []QueryPoint
}

// QueryResult 查询结果。
type QueryResult struct {
	Type      string
	Series    []QuerySeries
	Truncated bool
	Warnings  []string
	Duration  time.Duration
}

// TestResult 测试连接的结果：失败不作为接口错误返回，Error 为数据源返回的原文（已去凭据）。
type TestResult struct {
	OK      bool
	Latency time.Duration
	Error   string
}

// ---------------------------------------------------------------------------
// 告警规则
// ---------------------------------------------------------------------------

// RuleSpec 规则的可编辑内容。json 标签即 monitor_rule_version.snapshot 的落库格式
// （仅 core 读写；增删字段须保持旧快照可读）。
type RuleSpec struct {
	GroupID         string            `json:"group_id"`
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	Enabled         bool              `json:"enabled"`
	Labels          map[string]string `json:"labels"`
	DatasourceScope int32             `json:"datasource_scope"`
	DatasourceIDs   []string          `json:"datasource_ids"`
	Queries         []mc.Query        `json:"queries"`
	LevelInhibit    bool              `json:"level_inhibit"`
	EvalIntervalS   int32             `json:"eval_interval_s"`
	ForS            int32             `json:"for_s"`
	RecoverS        int32             `json:"recover_s"`
	NodataKeepS     int32             `json:"nodata_keep_s"`
	Effective       mc.Effective      `json:"effective"`
	Annotations     map[string]string `json:"annotations"`
	RecoveryExpr    string            `json:"recovery_expr"`
	Notify          mc.NotifyTargets  `json:"notify"`
	NotifyRecovery  bool              `json:"notify_recovery"`
	RepeatIntervalM int32             `json:"repeat_interval_m"`
	MaxNotify       int32             `json:"max_notify"`
	GroupKeyLabels  []string          `json:"group_key_labels"`
	// 第二、三期的字段：第一期的快照里没有这些键，解码后是零值（= 未配置）。
	CIMatch         mc.CIMatch       `json:"ci_match"`
	ResourceScope   mc.ResourceScope `json:"resource_scope"`
	Relabel         []mc.RelabelStep `json:"relabel"`
	DropExpr        string           `json:"drop_expr"`
	TopologyInhibit bool             `json:"topology_inhibit"`
	EscalationID    string           `json:"escalation_id"`
	Actions         mc.RuleActions   `json:"actions"`
}

// MaxSeverity 各查询中的最高级别。
func (s *RuleSpec) MaxSeverity() int32 {
	var max int32
	for _, q := range s.Queries {
		if q.Severity > max {
			max = q.Severity
		}
	}
	return max
}

// Rule 告警规则。
type Rule struct {
	ID         string
	Spec       RuleSpec
	Version    int32
	SpecTime   time.Time
	CreateTime time.Time
	UpdateTime time.Time
	UpdateUser string
}

// RuleChange 一次规则变更的留痕：Action 取 monitorcontract.RuleAction*；BumpSpecTime 表示本次
// 变更影响判定，data 层同事务把 spec_time 置为当前时间。
type RuleChange struct {
	Action       string
	Summary      string
	Operator     string
	BumpSpecTime bool
}

// RuleFilter 规则列表过滤条件。
type RuleFilter struct {
	Groups       GroupFilter
	Keyword      string
	Severity     *int32
	DatasourceID string
	Enabled      *bool
	Firing       *bool
	LastStatus   *int32
	Labels       map[string]string
	// EscalationID 非空时只返回引用了该升级策略的规则。
	EscalationID string
}

// EvalTask 规则在一个数据源上的评估状态（monitor_eval_task，引擎写）。
type EvalTask struct {
	RuleID         string
	DatasourceID   string
	DatasourceName string
	LastStatus     int32
	LastEvalTime   *time.Time
	LastError      string
	LastDurationMS int32
	FiringCount    int32
	NextEvalTime   time.Time
}

// RuleView 规则及其展示信息。
type RuleView struct {
	*Rule
	GroupName       string
	Datasources     []NamedRef
	NotifyUsers     []NamedRef
	NotifySchedules []NamedRef
	NotifyDepts     []NamedRef
	UpdateUserName  string
	CanManage       bool
	// Status / FiringCount / LastEvalTime / LastError 为各数据源评估状态的汇总。
	Status       int32
	FiringCount  int32
	LastEvalTime *time.Time
	LastError    string
	// Tasks 各数据源上的评估状态；仅详情填充。
	Tasks []*EvalTask
}

// RuleVersion 规则的一个历史版本。Spec 仅详情填充。
type RuleVersion struct {
	RuleID       string
	Version      int32
	Action       string
	Summary      string
	OperatorID   string
	OperatorName string
	CreateTime   time.Time
	Spec         *RuleSpec
	GroupName    string
}

// EvalRecordFilter 判定记录过滤条件。
type EvalRecordFilter struct {
	RuleID       string
	DatasourceID string
	Status       *int32
	Start        *time.Time
	End          *time.Time
}

// EvalRecord 一次评估的判定记录（monitor_eval_record，引擎写）。
type EvalRecord struct {
	ID             string
	RuleID         string
	DatasourceID   string
	DatasourceName string
	EvalTime       time.Time
	DurationMS     int32
	Status         int32
	Error          string
	Series         int32
	Anomalies      int32
	Recovered      int32
	Detail         mc.EvalDetail
}

// ---------------------------------------------------------------------------
// 告警事件
// ---------------------------------------------------------------------------

// Alert 告警事件（monitor_alert，引擎写）。
type Alert struct {
	ID             string
	Fingerprint    string
	RuleID         string
	RuleName       string
	GroupID        string
	DatasourceID   string
	DatasourceName string
	Severity       int32
	Status         int32
	Labels         map[string]string
	Annotations    map[string]string
	Expr           string
	Value          *float64
	RecoveryValue  *float64
	FirstFireTime  time.Time
	LastFireTime   time.Time
	ResolveTime    *time.Time
	Muted          bool
	MuteID         string
	NotifyCount    int32
	LastNotifyTime *time.Time
	GroupKey       string
	// 第二、三期的列。AckUser / AckTime / TicketID / WorkflowInstanceID / AISummary 由 core 写；
	// 其余由评估引擎写。
	AckUser            string
	AckTime            *time.Time
	ClosedBy           string
	CloseReason        string
	Inhibited          bool
	InhibitKind        int32
	InhibitID          string
	InhibitAlertID     string
	CIID               string
	EscalationLevel    int32
	TicketID           string
	WorkflowInstanceID string
	AISummary          string
}

// 处理状态（Alert.handle_state，按 4 > 3 > 2 > 1 取第一个成立的）。
const (
	HandleUnacked   int32 = 1
	HandleAcked     int32 = 2
	HandleMuted     int32 = 3
	HandleInhibited int32 = 4
)

// HandleState 事件的处理状态：被抑制 > 已屏蔽 > 已认领 > 未认领。
func (a *Alert) HandleState() int32 {
	switch {
	case a.Inhibited:
		return HandleInhibited
	case a.Muted:
		return HandleMuted
	case a.AckUser != "":
		return HandleAcked
	}
	return HandleUnacked
}

// AlertView 告警事件及其展示信息。
type AlertView struct {
	*Alert
	GroupName  string
	RuleExists bool
	MuteName   string
	DurationS  int64
	// CanAck / CanManage 当前用户对事件所在分组的范围（只读以上 / 可管理）；收件人视图恒为 false。
	CanAck    bool
	CanManage bool
	// Recipient 收件人视图：只因是通知接收方才看得到这条事件（指向其他对象的引用已去掉）。
	Recipient bool
	// 以下由 AlertDecorator 填充（第二、三期的展示名）。
	AckUserName      string
	AckDurationS     int64
	ClosedByName     string
	Closing          bool
	InhibitName      string
	InhibitAlertName string
	CIName           string
	TicketNumber     string
}

// AlertFilter 告警事件过滤条件。FireStart / FireEnd 为首次触发时间的闭区间；HistorySince 非空时
// 表示调用方未给时间范围：活跃事件不受限，历史事件只回看到该时刻（避免对不断增长的表做无界统计）。
type AlertFilter struct {
	Groups        GroupFilter
	Statuses      []int32
	RuleIDs       []string
	DatasourceIDs []string
	Severities    []int32
	Muted         *bool
	Keyword       string
	Labels        map[string]string
	FireStart     *time.Time
	FireEnd       *time.Time
	HistorySince  *time.Time
	// RecipientID 非空时只返回通知过该用户的事件（monitor_notify_log.recipients）。
	RecipientID string
	// OrAckUser 与 RecipientID 一起用：通知过该用户的，或由该用户认领的（「我负责的」）。
	OrAckUser   bool
	Fingerprint string
	// 第二、三期的过滤条件。HandleState 取 Handle* 常量，0 = 不限。
	CIID        string
	Acked       *bool
	Inhibited   *bool
	AckUserID   string
	HandleState int32
}

// 聚合维度。
const (
	AggregateByRule     = "rule"
	AggregateBySeverity = "severity"
	AggregateByGroup    = "group"
	AggregateByLabel    = "label"
)

// AggregateBy 聚合维度；Kind 为 AggregateByLabel 时 Label 为标签名。
type AggregateBy struct {
	Kind  string
	Label string
}

// AlertAggregate 聚合视图的一行。
type AlertAggregate struct {
	Key            string
	Name           string
	Count          int32
	MaxSeverity    int32
	SeverityCounts [4]int32
	LastFireTime   time.Time
}

// NotifyLog 一次通知记录（monitor_notify_log，core 写）。
type NotifyLog struct {
	ID               string
	AlertID          string
	Kind             int32
	Seq              int32
	Source           int32
	SubscriptionID   string
	SubscriptionName string
	Recipients       []string
	RecipientRefs    []RecipientRef
	NotifyEventIDs   []string
	Note             string
	CreateTime       time.Time
	// Level 升级通知的级数 1-10；其余通知为 0。升级通知的 Seq 是级内序号。
	Level int32
}

// 通知接收方类型。
const (
	RecipientUser  = "user"
	RecipientGroup = "group"
)

// RecipientRef 一次通知的一个接收方。
type RecipientRef struct {
	Type string
	ID   string
	Name string
}

// AlertDetail 告警事件详情。
type AlertDetail struct {
	Alert       *AlertView
	Logs        []*NotifyLog
	CanViewRule bool
}

// ---------------------------------------------------------------------------
// 屏蔽规则
// ---------------------------------------------------------------------------

// MuteSpec 屏蔽规则的可编辑内容。GroupID 为空 = 作用于全部分组。
type MuteSpec struct {
	GroupID       string
	Name          string
	Reason        string
	RuleIDs       []string
	DatasourceIDs []string
	Severities    []int32
	Matchers      []mc.Matcher
	Mode          int32
	TimeType      int32
	StartTime     *time.Time
	EndTime       *time.Time
	Periodic      *mc.Effective
	Enabled       bool
}

// Mute 屏蔽规则。
type Mute struct {
	ID         string
	Spec       MuteSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	CreateUser string
	UpdateUser string
}

// 屏蔽规则的当前状态。
const (
	MuteStatePending  = "pending"
	MuteStateActive   = "active"
	MuteStateExpired  = "expired"
	MuteStateDisabled = "disabled"
)

// MuteView 屏蔽规则及其展示信息。
type MuteView struct {
	*Mute
	GroupName      string
	Rules          []NamedRef
	Datasources    []NamedRef
	State          string
	MutedCount     int32
	CanManage      bool
	CreateUserName string
	UpdateUserName string
}

// MuteFilter 屏蔽规则列表过滤条件。IncludeGlobal 表示同时返回作用于全部分组的屏蔽规则。
// State 只接受 MuteStateDisabled / MuteStateExpired（可由 SQL 精确判定）；Live 表示只要启用
// 且未过期的（pending / active 由用例按时间窗再细分）。
type MuteFilter struct {
	Groups        GroupFilter
	IncludeGlobal bool
	Keyword       string
	Enabled       *bool
	Mode          *int32
	State         string
	Live          bool
}

// ---------------------------------------------------------------------------
// 订阅规则
// ---------------------------------------------------------------------------

// SubscriptionSpec 订阅规则的可编辑内容。
type SubscriptionSpec struct {
	Name          string
	GroupIDs      []string
	RuleIDs       []string
	DatasourceIDs []string
	Severities    []int32
	OnFiring      bool
	OnResolved    bool
	Matchers      []mc.Matcher
	Receivers     mc.Receivers
	FollowRepeat  bool
	Enabled       bool
}

// Subscription 订阅规则。
type Subscription struct {
	ID         string
	Spec       SubscriptionSpec
	OwnerID    string
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
}

// SubscriptionHit 订阅的命中统计。
type SubscriptionHit struct {
	LastTime time.Time
	Count    int32
}

// SubscriptionView 订阅规则及其展示信息。
type SubscriptionView struct {
	*Subscription
	OwnerName         string
	Groups            []NamedRef
	Rules             []NamedRef
	Datasources       []NamedRef
	ReceiverUsers     []NamedRef
	ReceiverSchedules []NamedRef
	OutOfScopeGroups  []string
	LastHitTime       *time.Time
	HitCount7d        int32
	CanManage         bool
}

// SubscriptionFilter 订阅规则列表过滤条件。OwnerID 非空 = 只看该所有者的；否则按 Groups
// （订阅分组与之有交集）过滤，ViewerID 自己的订阅恒可见。CoverGroupIDs 非空时只返回订阅了其中
// 任一分组的（用于「订阅了某分组或其上级」过滤）。
type SubscriptionFilter struct {
	OwnerID       string
	ViewerID      string
	Groups        GroupFilter
	Keyword       string
	Enabled       *bool
	CoverGroupIDs []string
}

// SubscriptionCandidate 派发时预筛订阅的条件：分组（事件所在分组及其全部上级）、规则、数据源、
// 级别与事件类型；标签匹配在用例里用已编译的匹配器完成。
type SubscriptionCandidate struct {
	GroupIDs     []string
	RuleID       string
	DatasourceID string
	Severity     int32
	Resolved     bool
	RepeatOnly   bool
}

// ---------------------------------------------------------------------------
// 通知派发
// ---------------------------------------------------------------------------

// OutboxItem 通知发件箱的一行（monitor_notify_outbox，引擎写入、core 派发）。
type OutboxItem struct {
	ID      string
	AlertID string
	Kind    int32
	// Seq 通知序号。升级通知（Kind = NotifyKindEscalation）是编码值 level × 100000 + 级内序号，
	// 用之前先经 monitorcontract.SplitEscalationSeq 拆开。
	Seq      int32
	Attempts int32
	// Level 升级通知的级数（发件箱的冗余列）；其余通知为 0。
	Level int32
}
