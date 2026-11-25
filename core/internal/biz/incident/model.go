package biz

import (
	"time"
)

// 等级 1-4（数字越小越严重）。
const (
	SeverityS1 int32 = 1
	SeverityS2 int32 = 2
	SeverityS3 int32 = 3
	SeverityS4 int32 = 4
)

// SeverityLabel 等级的中文展示名。
func SeverityLabel(s int32) string {
	switch s {
	case SeverityS1:
		return "S1 致命"
	case SeverityS2:
		return "S2 严重"
	case SeverityS3:
		return "S3 一般"
	case SeverityS4:
		return "S4 轻微"
	}
	return "未知"
}

// SeverityCode 等级的事件类型段（s1-s4）。
func SeverityCode(s int32) string {
	if s < SeverityS1 || s > SeverityS4 {
		return "s4"
	}
	return [...]string{"s1", "s2", "s3", "s4"}[s-1]
}

// ValidSeverity 报告是否为合法等级。
func ValidSeverity(s int32) bool { return s >= SeverityS1 && s <= SeverityS4 }

// NotifySeverity 等级 → 通知事件级别（notify 的 1-4，4 最严重）。
func NotifySeverity(s int32) int32 { return 5 - s }

// 状态。
const (
	StatusOpen       int32 = 1
	StatusResponding int32 = 2
	StatusMitigated  int32 = 3
	StatusResolved   int32 = 4
	StatusClosed     int32 = 5
	StatusMerged     int32 = 6
)

// StatusLabel 状态的中文展示名。
func StatusLabel(s int32) string {
	switch s {
	case StatusOpen:
		return "待响应"
	case StatusResponding:
		return "处理中"
	case StatusMitigated:
		return "已缓解"
	case StatusResolved:
		return "已恢复"
	case StatusClosed:
		return "已关闭"
	case StatusMerged:
		return "已合并"
	}
	return "未知"
}

// IsActive 活跃状态（待响应 / 处理中 / 已缓解）。
func IsActive(status int32) bool { return status >= StatusOpen && status <= StatusMitigated }

// 复盘状态。
const (
	ReviewNone     int32 = 0
	ReviewPending  int32 = 1
	ReviewDrafting int32 = 2
	ReviewDone     int32 = 3
)

// 来源。
const (
	SourceManual int32 = 1
	SourceRule   int32 = 2
	SourceOpen   int32 = 3
	SourceAgent  int32 = 4
)

// 操作者类型。
const (
	ActorUser   int32 = 1
	ActorSystem int32 = 2
	ActorAgent  int32 = 3
)

// 角色。
const (
	RoleCommander int32 = 1
	RoleResponder int32 = 2
	RoleWatcher   int32 = 3
)

// 告警关联方式。
const (
	LinkRule   int32 = 1
	LinkManual int32 = 2
	LinkAgent  int32 = 3
	LinkMerge  int32 = 4
)

// 变更关系。
const (
	RelationUnknown   int32 = 0
	RelationSuspected int32 = 1
	RelationConfirmed int32 = 2
	RelationUnrelated int32 = 3
)

// 候选故障状态。
const (
	CandidateOpen      int32 = 1
	CandidatePromoted  int32 = 2
	CandidateDismissed int32 = 3
	CandidateExpired   int32 = 4
)

// 动作建议状态。
const (
	ProposalPending   int32 = 1
	ProposalApproved  int32 = 2
	ProposalRejected  int32 = 3
	ProposalRunning   int32 = 4
	ProposalSucceeded int32 = 5
	ProposalFailed    int32 = 6
	ProposalExpired   int32 = 7
	ProposalCancelled int32 = 8
)

// 预案步骤审批策略。
const (
	ApprovalDirect    int32 = 1
	ApprovalCommander int32 = 2
	ApprovalExisting  int32 = 3
)

// 风险级别。
const (
	RiskLow    int32 = 1
	RiskMedium int32 = 2
	RiskHigh   int32 = 3
)

// RiskLabel 风险级别的中文展示名。
func RiskLabel(r int32) string {
	switch r {
	case RiskLow:
		return "低"
	case RiskHigh:
		return "高"
	}
	return "中"
}

// 作业靶标来源（预案步骤）。
const (
	TargetAffected int32 = 1
	TargetUpstream int32 = 2
	TargetTemplate int32 = 3
)

// 预案触发方式（触发规则）。
const (
	PlaybookRecommend int32 = 1
	PlaybookPropose   int32 = 2
	PlaybookAuto      int32 = 3
)

// 自动恢复方式（触发规则）。
const (
	AutoResolveHint     int32 = 1
	AutoResolveMitigate int32 = 2
	AutoResolveResolve  int32 = 3
)

// 公告类型。
const (
	UpdateOpened   int32 = 1
	UpdateProgress int32 = 2
	UpdateResolved int32 = 3
)

// UpdateKindLabel 公告类型的中文展示名。
func UpdateKindLabel(k int32) string {
	switch k {
	case UpdateOpened:
		return "开启"
	case UpdateResolved:
		return "恢复"
	}
	return "进展"
}

// 改进项。
const (
	ItemFix     int32 = 1
	ItemPrevent int32 = 2
	ItemDetect  int32 = 3
	ItemProcess int32 = 4

	ItemOpen int32 = 1
	ItemDone int32 = 2
)

// 动作发件箱类型。
const (
	ActionAISummary    int32 = 1
	ActionProposalExec int32 = 2
	ActionVerify       int32 = 3
	ActionTicketNote   int32 = 4
)

// 发件箱 / 动作状态。
const (
	OutboxPending int32 = 1
	OutboxDone    int32 = 2
	OutboxDropped int32 = 3
)

// 时间线类型。
const (
	EventOpened           = "opened"
	EventSeverity         = "severity"
	EventStatus           = "status"
	EventCommander        = "commander"
	EventMemberAdd        = "member_add"
	EventMemberRemove     = "member_remove"
	EventComment          = "comment"
	EventAlertAttach      = "alert_attach"
	EventAlertDetach      = "alert_detach"
	EventAlertResolved    = "alert_resolved"
	EventChange           = "change"
	EventUpdate           = "update"
	EventProposal         = "proposal"
	EventProposalDecision = "proposal_decision"
	EventProposalResult   = "proposal_result"
	EventProposalVerify   = "proposal_verify"
	EventTicket           = "ticket"
	EventAISummary        = "ai_summary"
	EventFinding          = "agent_finding"
	EventMerged           = "merged"
	EventMergedFrom       = "merged_from"
	EventReopen           = "reopen"
	EventEscalate         = "escalate"
	EventStale            = "stale"
	EventReview           = "review"
	EventActionItem       = "action_item"
	EventAgentLease       = "agent_lease"
	EventAutoApprove      = "auto_approve"
	EventNotify           = "notify"
	EventStorm            = "storm"
	EventEdit             = "edit"
)

// BusinessType 故障作为其他域业务对象时的类型标识（流程实例 business_type、附件 biz_type）。
const BusinessType = "incident"

// Incident 故障聚合根（incident 表一行）。
type Incident struct {
	ID              string
	Number          string
	Title           string
	Severity        int32
	Status          int32
	ReviewStatus    int32
	Source          int32
	SourceRef       string
	ExternalKey     string
	BusinessID      string
	RuleID          string
	CorrelationKey  string
	Summary         string
	AISummary       string
	RootCauseCode   string
	RootCause       string
	ResolveNote     string
	CommanderID     string
	AffectedCIIDs   []string
	Groups          []string
	IsDrill         bool
	OccurTime       time.Time
	OpenTime        time.Time
	RespondTime     *time.Time
	MitigateTime    *time.Time
	FirstResolve    *time.Time
	ResolveTime     *time.Time
	CloseTime       *time.Time
	ReopenCount     int32
	MergedInto      string
	EscalationLevel int32
	EscalateTime    *time.Time
	LastActivity    time.Time
	StaleNotifyTime *time.Time
	UpdateCount     int32
	AutoApprove     bool
	AgentOwner      string
	AgentLeaseUntil *time.Time
	AgentFindings   int32
	ReviewDue       *time.Time
	ReviewRemind    int32
	Version         int32
	CreateTime      time.Time
	UpdateTime      time.Time
	CreateUser      string
	UpdateUser      string

	// 派生只读字段（列表 / 详情查询时由仓储填充，不写回）。
	AlertCount       int32
	PendingProposals int32
}

// Member 故障成员（处理人 / 关注人）。
type Member struct {
	IncidentID string
	UserID     string
	Role       int32
	CreateTime time.Time
}

// Finding Agent 发现。
type Finding struct {
	Title      string   `json:"title"`
	Conclusion string   `json:"conclusion,omitempty"`
	Confidence int32    `json:"confidence,omitempty"`
	Evidence   []string `json:"evidence,omitempty"`
	NextStep   string   `json:"next_step,omitempty"`
	// RunID 产出该发现的运维 Agent 运行 ID（Agent 写入时携带；前端据此跳到运行详情）。
	RunID string `json:"run_id,omitempty"`
}

// EventDetail 时间线细节（incident_event.detail）；各类型按需填充，未用到的键不落库。
type EventDetail struct {
	Text           string     `json:"text,omitempty"`
	From           int32      `json:"from,omitempty"`
	To             int32      `json:"to,omitempty"`
	UserID         string     `json:"user_id,omitempty"`
	AlertIDs       []string   `json:"alert_ids,omitempty"`
	AlertCount     int32      `json:"alert_count,omitempty"`
	IncidentID     string     `json:"incident_id,omitempty"`
	IncidentNumber string     `json:"incident_number,omitempty"`
	ProposalID     string     `json:"proposal_id,omitempty"`
	ProposalTitle  string     `json:"proposal_title,omitempty"`
	TicketID       string     `json:"ticket_id,omitempty"`
	TicketNumber   string     `json:"ticket_number,omitempty"`
	UpdateID       string     `json:"update_id,omitempty"`
	UpdateKind     int32      `json:"update_kind,omitempty"`
	Level          int32      `json:"level,omitempty"`
	Version        int32      `json:"version,omitempty"`
	Severity       int32      `json:"severity,omitempty"`
	Change         *ChangeRef `json:"change,omitempty"`
	Finding        *Finding   `json:"finding,omitempty"`
	FileIDs        []string   `json:"file_ids,omitempty"`
	ExecKind       string     `json:"exec_kind,omitempty"`
	ExecRef        string     `json:"exec_ref,omitempty"`
	Error          string     `json:"error,omitempty"`
}

// Event 时间线记录（append-only）。
type Event struct {
	ID         string
	IncidentID string
	Type       string
	ActorType  int32
	ActorID    string
	Detail     EventDetail
	Pinned     bool
	CreateTime time.Time
}

// AlertLink 故障 ↔ 告警。
type AlertLink struct {
	IncidentID    string
	AlertID       string
	Method        int32
	Released      bool
	RuleName      string
	Severity      int32
	CIID          string
	FirstFireTime *time.Time
	AttachTime    time.Time
	AttachUser    string
}

// ChangeDomainManual 处理人手填的变更（没有外部对象，ref_id 可空）。
const ChangeDomainManual = "manual"

// ChangeRef 变更引用（弱引用 + 摘要）。OperatorID / CIID / URL 为实时读取的展示字段，不落库。
type ChangeRef struct {
	ID         string     `json:"id,omitempty"`
	IncidentID string     `json:"-"`
	Domain     string     `json:"domain"`
	Kind       string     `json:"kind"`
	RefID      string     `json:"ref_id"`
	Summary    string     `json:"summary,omitempty"`
	RefTime    *time.Time `json:"ref_time,omitempty"`
	Relation   int32      `json:"relation,omitempty"`
	CreateUser string     `json:"-"`
	CreateTime time.Time  `json:"-"`
	UpdateTime time.Time  `json:"-"`
	OperatorID string     `json:"-"`
	CIID       string     `json:"-"`
	URL        string     `json:"-"`
}

// Candidate 候选故障。
type Candidate struct {
	ID             string
	RuleID         string
	CorrelationKey string
	AlertIDs       []string
	CIIDs          []string
	Status         int32
	IncidentID     string
	FirstTime      time.Time
	UpdateTime     time.Time
}

// Matcher 告警标签匹配条件（语法与监控域一致；编译由监控域的匹配器承担，见 deps.go MatcherCompiler）。
type Matcher struct {
	Label  string   `json:"label"`
	Op     string   `json:"op"`
	Value  string   `json:"value,omitempty"`
	Values []string `json:"values,omitempty"`
}

// RuleMatch 触发规则的匹配条件。
type RuleMatch struct {
	MinSeverity int32     `json:"min_severity,omitempty"`
	GroupIDs    []string  `json:"group_ids,omitempty"`
	RuleIDs     []string  `json:"rule_ids,omitempty"`
	Matchers    []Matcher `json:"matchers,omitempty"`
	ModelCodes  []string  `json:"model_codes,omitempty"`
}

// RuleCorrelation 归并方式与开启门槛。
type RuleCorrelation struct {
	Keys      []string `json:"keys,omitempty"`
	WindowM   int32    `json:"window_m,omitempty"`
	MinAlerts int32    `json:"min_alerts,omitempty"`
	MinCIs    int32    `json:"min_cis,omitempty"`
}

// RuleIncident 开出故障的属性。
type RuleIncident struct {
	Severity      int32  `json:"severity,omitempty"`
	TitleTemplate string `json:"title_template,omitempty"`
	BusinessID    string `json:"business_id,omitempty"`
	BusinessLabel string `json:"business_label,omitempty"`
	IsDrill       bool   `json:"is_drill,omitempty"`
}

// RuleResponse 响应配置。
type RuleResponse struct {
	CommanderScheduleID    string   `json:"commander_schedule_id,omitempty"`
	CommanderUserID        string   `json:"commander_user_id,omitempty"`
	ResponderBackup        bool     `json:"responder_backup,omitempty"`
	ResponderOncallGroupID string   `json:"responder_oncall_group_id,omitempty"`
	ResponderCIOwner       bool     `json:"responder_ci_owner,omitempty"`
	ResponderUserIDs       []string `json:"responder_user_ids,omitempty"`
	Groups                 []string `json:"groups,omitempty"`
}

// RuleAction 自动处置与自动恢复。
type RuleAction struct {
	PlaybookID   string `json:"playbook_id,omitempty"`
	PlaybookMode int32  `json:"playbook_mode,omitempty"`
	AutoResolve  int32  `json:"auto_resolve,omitempty"`
	AISummary    bool   `json:"ai_summary,omitempty"`
	// Agent 开故障与等级上调时入队一次运维 Agent 诊断任务。
	Agent bool `json:"agent,omitempty"`
}

// RuleSpec 触发规则内容（incident_rule.spec）。
type RuleSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Sort        int32           `json:"sort,omitempty"`
	Enabled     bool            `json:"enabled"`
	Match       RuleMatch       `json:"match"`
	Correlation RuleCorrelation `json:"correlation"`
	Incident    RuleIncident    `json:"incident"`
	Response    RuleResponse    `json:"response"`
	Action      RuleAction      `json:"action"`
}

// Rule 触发规则。
type Rule struct {
	ID         string
	Spec       RuleSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	CreateUser string
	UpdateUser string
}

// Business 业务字典的一条。
type Business struct {
	ID          string
	Code        string
	Name        string
	Description string
	OwnerID     string
	ScheduleID  string
	Groups      []string
	CIID        string
	LabelValues []string
	Sort        int32
	Enabled     bool
	Version     int32
	CreateTime  time.Time
	UpdateTime  time.Time
	CreateUser  string
	UpdateUser  string
}

// PlaybookStep 预案的一个可执行步骤。
type PlaybookStep struct {
	Name       string            `json:"name"`
	Kind       string            `json:"kind"`
	Ref        string            `json:"ref"`
	Params     map[string]string `json:"params,omitempty"`
	Approval   int32             `json:"approval"`
	Risk       int32             `json:"risk"`
	TargetMode int32             `json:"target_mode,omitempty"`
}

// PlaybookMatch 预案适用条件。
type PlaybookMatch struct {
	Matchers    []Matcher `json:"matchers,omitempty"`
	ModelCodes  []string  `json:"model_codes,omitempty"`
	BusinessIDs []string  `json:"business_ids,omitempty"`
}

// PlaybookSpec 预案内容。
type PlaybookSpec struct {
	Name        string
	Description string
	Doc         string
	Steps       []PlaybookStep
	Match       PlaybookMatch
	AgentUserID string
	Enabled     bool
}

// Playbook 预案。
type Playbook struct {
	ID         string
	Spec       PlaybookSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	CreateUser string
	UpdateUser string
}

// ProposalAction 动作建议的动作内容（incident_proposal.action）。
type ProposalAction struct {
	Kind    string            `json:"kind"`
	Ref     string            `json:"ref"`
	RefName string            `json:"ref_name,omitempty"`
	Params  map[string]string `json:"params,omitempty"`
	CIIDs   []string          `json:"ci_ids,omitempty"`
}

// VerifyResult 验证窗口结果。
type VerifyResult struct {
	AlertsBefore int32     `json:"alerts_before"`
	AlertsAfter  int32     `json:"alerts_after"`
	Resolved     int32     `json:"resolved"`
	NewAlerts    int32     `json:"new_alerts"`
	Worse        bool      `json:"worse"`
	CheckedTime  time.Time `json:"checked_time"`
}

// Proposal 动作建议。
type Proposal struct {
	ID         string
	IncidentID string
	Title      string
	Reason     string
	Evidence   []string
	Action     ProposalAction
	PlaybookID string
	StepIndex  int32
	Risk       int32
	IdemKey    string
	Status     int32
	Auto       bool
	// AutoEligible Agent 提出时申请自动通过（Agent 侧条件已满足）；是否真的自动仍由本域判定（Auto）。
	AutoEligible bool
	ProposerType int32
	ProposerID   string
	DecidedBy    string
	DecidedTime  *time.Time
	DecisionNote string
	ExpireTime   time.Time
	ExecKind     string
	ExecRef      string
	ExecUser     string
	Result       string
	VerifyUntil  *time.Time
	VerifyResult *VerifyResult
	CreateTime   time.Time
	UpdateTime   time.Time
}

// Update 公告。
type Update struct {
	ID         string
	IncidentID string
	Kind       int32
	Content    string
	AuthorID   string
	CreateTime time.Time
}

// ShareLink 分享链接。
type ShareLink struct {
	ID             string
	IncidentID     string
	Name           string
	TokenHash      []byte
	TokenSealed    []byte
	ExpireTime     *time.Time
	AccessCount    int32
	LastAccessTime *time.Time
	CreateUser     string
	CreateTime     time.Time
}

// PostmortemDoc 复盘文档章节（incident_postmortem.doc）。
type PostmortemDoc struct {
	Summary       string `json:"summary,omitempty"`
	Impact        string `json:"impact,omitempty"`
	Timeline      string `json:"timeline,omitempty"`
	RootCauseCode string `json:"root_cause_code,omitempty"`
	RootCause     string `json:"root_cause,omitempty"`
	Trigger       string `json:"trigger,omitempty"`
	Detection     string `json:"detection,omitempty"`
	Handling      string `json:"handling,omitempty"`
	WentWell      string `json:"went_well,omitempty"`
	WentWrong     string `json:"went_wrong,omitempty"`
}

// Postmortem 复盘（当前稿）。
type Postmortem struct {
	IncidentID       string
	Doc              PostmortemDoc
	CurrentVersion   int32
	ReviewInstanceID string
	UpdateUser       string
	UpdateTime       time.Time
}

// PostmortemVersion 不可变版本。
type PostmortemVersion struct {
	ID         string
	IncidentID string
	Version    int32
	Doc        PostmortemDoc
	Note       string
	CreateUser string
	CreateTime time.Time
}

// ActionItem 改进项。
type ActionItem struct {
	ID             string
	IncidentID     string
	IncidentNumber string
	Title          string
	Kind           int32
	OwnerID        string
	DueDate        *time.Time
	Status         int32
	TicketID       string
	TicketNumber   string
	CreateUser     string
	CreateTime     time.Time
	UpdateTime     time.Time
}

// Source 开放接口接入源。
type Source struct {
	ID              string
	Name            string
	Description     string
	TokenHash       []byte
	TokenPrefix     string
	DefaultSeverity int32
	BusinessID      string
	Groups          []string
	AllowedIPs      []string
	Enabled         bool
	LastUsedTime    *time.Time
	Version         int32
	CreateTime      time.Time
	UpdateTime      time.Time
}

// Webhook 订阅。
type Webhook struct {
	ID         string
	Name       string
	URL        string
	Secret     []byte
	Events     []string
	Enabled    bool
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	// Pending / Failed 出站箱里的待投递 / 已丢弃数（查询时填充）。
	Pending int32
	Failed  int32
}

// OutboxItem Webhook 出站箱一行。
type OutboxItem struct {
	ID         string
	WebhookID  string
	IncidentID string
	EventType  string
	Payload    []byte
	Attempts   int32
}

// Action 动作发件箱一行。
type Action struct {
	ID         string
	IncidentID string
	Kind       int32
	RefID      string
	Payload    []byte
	Attempts   int32
}

// SeveritySetting 一个等级的响应要求。
type SeveritySetting struct {
	RespondM         int32 `json:"respond_m"`
	StaleM           int32 `json:"stale_m"`
	FirstUpdateM     int32 `json:"first_update_m"`
	ReviewDays       int32 `json:"review_days"`
	AllowAutoResolve bool  `json:"allow_auto_resolve"`
	AllowAutoApprove bool  `json:"allow_auto_approve"`
}

// RootCause 根因分类。
type RootCause struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// Settings 故障管理设置（incident_setting.spec）。
type Settings struct {
	Severities          map[int32]SeveritySetting `json:"severities"`
	RootCauses          []RootCause               `json:"root_causes"`
	FallbackScheduleID  string                    `json:"fallback_schedule_id,omitempty"`
	StormThreshold      int32                     `json:"storm_threshold"`
	CandidateTTLMinutes int32                     `json:"candidate_ttl_m"`
	ReopenWindowHours   int32                     `json:"reopen_window_h"`
	AutoCloseDays       int32                     `json:"auto_close_days"`
	PersonalEvents      []string                  `json:"personal_events"`
	ProposalTTLMinutes  int32                     `json:"proposal_ttl_m"`
	VerifyWindowMinutes int32                     `json:"verify_window_m"`
	ChangeLookbackHours int32                     `json:"change_lookback_h"`
	AgentMaxFindings    int32                     `json:"agent_max_findings"`
	// Agent 建议自动通过的故障侧条件：冷却（同一资源同一预案步骤）、全局 / 同业务并发上限。
	AgentAutoCooldownMinutes       int32 `json:"agent_auto_cooldown_m"`
	AgentAutoMaxRunning            int32 `json:"agent_auto_max_running"`
	AgentAutoMaxRunningPerBusiness int32 `json:"agent_auto_max_running_per_business"`
	Version                        int32 `json:"-"`
}

// IncidentFilter 故障列表过滤条件；零值不过滤。
type IncidentFilter struct {
	Keyword       string
	Statuses      []int32
	Severities    []int32
	BusinessID    string
	CommanderID   string
	MemberUserID  string
	IsDrill       *bool
	ReviewStatus  *int32
	ReviewOverdue bool
	OpenStart     *time.Time
	OpenEnd       *time.Time
	CIID          string
	IDs           []string
	// RuleID / CorrelationKey 关联器用：按触发规则与归并键精确定位。
	RuleID         string
	CorrelationKey string
	// ExcludeDrill 统计用：排除演练与已合并。
	ExcludeDrill bool
}

// InspectQuery 巡检用的筛选（巡检器按等级与时间阈值取待处理的故障）。
type InspectQuery struct {
	Statuses         []int32
	Severities       []int32
	OpenBefore       *time.Time
	EscalateTimeNull bool
	// EscalationLevelLT 只取 escalation_level 小于该值的故障（升级巡检：已升满级的不占批次名额）。
	EscalationLevelLT *int32
	// EscalateTimeBefore 只取 escalate_time 为空或早于该时刻的故障（上一次升级已过一个响应时限）。
	EscalateTimeBefore *time.Time
	LastActivityBefore *time.Time
	StaleNotifyBefore  *time.Time
	ReviewStatuses     []int32
	ReviewDueBefore    *time.Time
	ReviewRemindLT     int32
	ResolveBefore      *time.Time
	AgentLeaseBefore   *time.Time
	ExcludeDrill       bool
}

// TimelineFilter 时间线过滤。
type TimelineFilter struct {
	Types      []string
	PinnedOnly bool
}

// ActionItemFilter 改进项过滤。
type ActionItemFilter struct {
	IncidentID string
	Status     *int32
	OwnerID    string
	Overdue    bool
	Now        time.Time
}

// StatsRange 统计范围（按开启时间）。
type StatsRange struct {
	Start      time.Time
	End        time.Time
	BusinessID string
}
