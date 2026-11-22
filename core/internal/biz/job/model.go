package biz

import (
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// SecretChange 一次更新对某个密文列的处理：Set 为假保留原值；为真则写入 Sealed（空 = 清除）。
type SecretChange struct {
	Set    bool
	Sealed []byte
}

// Credential 作业凭据（不含密文：密文只写不读）。
type Credential struct {
	ID              string
	Name            string
	Type            int32
	Username        string
	BecomeMethod    string
	BecomeUser      string
	HasBecomeSecret bool
	Fingerprint     string
	Description     string
	TemplateCount   int32
	Version         int32
	CreateTime      time.Time
	UpdateTime      time.Time
	UpdateUser      string
	UpdateUserName  string
}

// CredentialInput 新建 / 编辑凭据的入参。Secret / BecomeSecret 为 nil 表示保留原值；
// BecomeSecret 指向空串表示清除。
type CredentialInput struct {
	Name         string
	Type         int32
	Username     string
	Secret       *string
	BecomeMethod string
	BecomeUser   string
	BecomeSecret *string
	Description  string
}

// CredentialFilter 凭据列表过滤；零值不过滤。
type CredentialFilter struct {
	Keyword string
	Type    *int32
}

// Project 作业项目（不含密文）。
type Project struct {
	ID             string
	Name           string
	RepoURL        string
	Ref            string
	AuthType       int32
	Username       string
	HasSecret      bool
	Description    string
	TemplateCount  int32
	Version        int32
	CreateTime     time.Time
	UpdateTime     time.Time
	UpdateUser     string
	UpdateUserName string
}

// ProjectInput 新建 / 编辑项目的入参。Secret 为 nil 表示保留原值。
type ProjectInput struct {
	Name        string
	RepoURL     string
	Ref         string
	AuthType    int32
	Username    string
	Secret      *string
	Description string
}

// Template 作业模板。
type Template struct {
	ID           string
	Name         string
	Description  string
	ProjectID    string
	Playbook     string
	CredentialID string
	Target       jc.Target
	Params       []jc.Param
	Options      jc.Options
	RunnerZone   string
	Exclusive    bool
	Enabled      bool
	// RequireApproval 每次执行都须审批（预检模式除外）。
	RequireApproval bool
	// SliceSize 作业切片的每片主机数；0 = 不切片。
	SliceSize int32
	// ProjectName / ProjectRef / CredentialName 随实体返回的展示名；对象已删除时为空。
	ProjectName    string
	ProjectRef     string
	CredentialName string
	// ModelName 目标资源类型名称（不在当前用户数据范围内时为空）；由用例填充。
	ModelName      string
	Version        int32
	CreateTime     time.Time
	UpdateTime     time.Time
	UpdateUser     string
	UpdateUserName string
}

// TemplateFilter 模板列表过滤；零值不过滤。
type TemplateFilter struct {
	Keyword   string
	ProjectID string
	Enabled   *bool
}

// 目标主机不能执行的原因（Host.Problem）。
const (
	ProblemAddressEmpty     = "address_empty"
	ProblemAddressInvalid   = "address_invalid"
	ProblemAddressDuplicate = "address_duplicate"
)

// Host 解析并校验后的一台目标主机。
type Host struct {
	CIID string
	Name string
	Env  string
	// Address 规范化后的连接地址；Problem 非空时为原始取值。
	Address string
	// Problem 不能执行的原因（Problem*），空表示可以执行。
	Problem string
}

// HostSet 目标主机解析结果。
type HostSet struct {
	Hosts     []Host
	Truncated bool
	// OutOfScope 模板指定、但被排除的主机数（见 ResourceSet.Skipped）。
	OutOfScope int
}

// Execution 一次作业执行。
type Execution struct {
	ID string
	// Kind 作业类型（jobcontract.Kind*）。
	Kind int32
	// TemplateID 作业模板；快速执行为空。
	TemplateID      string
	TemplateName    string
	TemplateVersion int32
	Trigger         int32
	// TriggerRef 触发来源的对象 ID（定时作业 / 触发器）；TriggerName 其名称（用例填充）。
	TriggerRef  string
	TriggerName string
	// GroupID / SliceNo / SliceTotal 作业切片：同一次提交拆出的执行共用 GroupID。
	GroupID    string
	SliceNo    int32
	SliceTotal int32
	// ApprovalReason 需要审批的原因；ApprovalInstanceID 审批流程实例。
	ApprovalReason     string
	ApprovalInstanceID string
	RiskHits           []jc.RiskHit
	Creator            string
	RequestID          string
	Status             int32
	Spec               jc.Spec
	RunnerZone         string
	Exclusive          bool
	TimeoutS           int32
	HostTotal          int32
	HostOK             int32
	HostFailed         int32
	HostUnreachable    int32
	CommitID           string
	Error              string
	CancelRequested    bool
	CancelUser         string
	RunnerID           string
	LogSeq             int64
	LogTruncated       bool
	CreateTime         time.Time
	StartTime          *time.Time
	FinishTime         *time.Time
	// 展示名，由用例填充。
	CreatorName    string
	CancelUserName string
	RunnerName     string
}

// ExecutionFilter 执行记录过滤；零值不过滤。
type ExecutionFilter struct {
	TemplateID string
	Statuses   []int32
	Creator    string
	Keyword    string
	StartTime  *time.Time
	EndTime    *time.Time
	CIID       string
	// CIIDs 靶标含这些主机中任一台（故障管理的变更关联用）。
	CIIDs      []string
	Kinds      []int32
	Triggers   []int32
	GroupID    string
	TriggerRef string
}

// ExecutionHost 一次执行中的一台主机及其结果。
type ExecutionHost struct {
	Seq         int32
	CIID        string
	Name        string
	Env         string
	Address     string
	Port        int32
	Status      int32
	OK          int32
	Changed     int32
	Failed      int32
	Unreachable int32
	Skipped     int32
}

// ExecutionHostFilter 执行主机过滤；零值不过滤。
type ExecutionHostFilter struct {
	Statuses []int32
	Keyword  string
}

// SubmitInput 提交执行的入参。
type SubmitInput struct {
	TemplateID string
	// Params 参数取值（变量名 → 值），JSON 解码后的原始值。
	Params map[string]any
	// CIIDs 只在这些主机上执行；空 = 模板的全部目标主机。
	CIIDs []string
	// Check 是否预检模式；nil = 取模板的设置。
	Check *bool
	// RequestID 幂等键，可空。
	RequestID string
	// TemplateVersion 非 0 时要求模板仍是这个版本（定时作业 / 触发器保存时确认过的版本），否则拒绝。
	TemplateVersion int32
	// Trigger / TriggerRef 触发来源与其对象 ID；零值 = 手动。接口层不接受调用方指定。
	Trigger    int32
	TriggerRef string
}

// LogPage 一次增量读取到的日志。
type LogPage struct {
	Content   string
	LastSeq   int64
	More      bool
	Status    int32
	Truncated bool
}

// Runner 执行节点。
type Runner struct {
	ID           string
	Name         string
	Zone         string
	Enabled      bool
	Description  string
	Online       bool
	LastSeen     *time.Time
	AgentVersion string
	Hostname     string
	Capacity     int32
	Running      int32
	Version      int32
	CreateTime   time.Time
	UpdateTime   time.Time
}

// RunnerInput 新建 / 编辑执行节点的入参。
type RunnerInput struct {
	Name        string
	Zone        string
	Enabled     bool
	Description string
}

// AdhocInput 快速执行的入参。
type AdhocInput struct {
	// Kind 作业类型（命令 / 脚本 / 文件）。
	Kind  int32
	Adhoc jc.Adhoc
	// FileID 要分发的文件（文件服务里提交人本人上传的临时文件）。
	FileID       string
	Target       jc.Target
	CredentialID string
	Options      jc.Options
	RunnerZone   string
	RequestID    string
}

// Preflight 提交前的检查结果：目标主机、命中的高危规则、是否需要审批。
type Preflight struct {
	HostSet
	RiskHits []jc.RiskHit
	// Blocked 命中了「拦截」规则。
	Blocked bool
	// ApprovalReasons 需要审批的原因；空 = 无需审批。
	ApprovalReasons []string
	// ApprovalReady 审批流程当前可发起。
	ApprovalReady bool
	// MaxHosts 本次提交的主机数上限。
	MaxHosts int
}

// Script 脚本库里的脚本。
type Script struct {
	ID          string
	Name        string
	Interpreter string
	// Content 脚本内容；列表查询不带。
	Content        string
	Size           int32
	Description    string
	Version        int32
	CreateTime     time.Time
	UpdateTime     time.Time
	UpdateUser     string
	UpdateUserName string
}

// ScriptFilter 脚本列表过滤；零值不过滤。
type ScriptFilter struct {
	Keyword     string
	Interpreter string
}

// RiskRule 高危命令规则。
type RiskRule struct {
	ID             string
	Name           string
	Pattern        string
	Action         int32
	Enabled        bool
	Description    string
	Version        int32
	UpdateTime     time.Time
	UpdateUser     string
	UpdateUserName string
}

// Policy 作业中心全局策略。
type Policy struct {
	ProtectedEnvs       []string
	ApprovalWorkflowKey string
	AdhocApproval       bool
	// MaxHosts 单条执行的主机数上限；0 = 系统上限。
	MaxHosts       int32
	Version        int32
	UpdateTime     time.Time
	UpdateUser     string
	UpdateUserName string
	// ApprovalWorkflowName / ApprovalWorkflowReady 审批流程的名称与当前是否可发起（用例填充）。
	ApprovalWorkflowName  string
	ApprovalWorkflowReady bool
}

// HostLimit 单条执行的主机数上限。
func (p *Policy) HostLimit() int {
	if p.MaxHosts > 0 && p.MaxHosts < jc.MaxHosts {
		return int(p.MaxHosts)
	}
	return jc.MaxHosts
}

// Schedule 定时作业。
type Schedule struct {
	ID         string
	Name       string
	TemplateID string
	// TemplateVersion 负责人保存时确认过的模板版本。
	TemplateVersion int32
	TemplateName    string
	Cron            string
	Timezone        string
	Params          map[string]any
	Misfire         int32
	Enabled         bool
	Owner           string
	OwnerName       string
	NextTime        *time.Time
	LastTime        *time.Time
	LastExecutionID string
	LastError       string
	Description     string
	Version         int32
	CreateTime      time.Time
	UpdateTime      time.Time
}

// ScheduleFilter 定时作业列表过滤；零值不过滤。
type ScheduleFilter struct {
	Keyword    string
	TemplateID string
	Enabled    *bool
}

// ScheduleRun 一次触发的落账。
type ScheduleRun struct {
	// Fired 本次触发的计划时间（CAS 条件：只有 next_time 仍是它才落账）。
	Fired time.Time
	// Next 下一次触发时间。
	Next        time.Time
	ExecutionID string
	Error       string
}

// Trigger 触发器。
type Trigger struct {
	ID         string
	Name       string
	Type       int32
	TemplateID string
	// TemplateVersion 负责人保存时确认过的模板版本。
	TemplateVersion int32
	TemplateName    string
	Params          map[string]any
	PassParams      bool
	Signal          string
	Alert           jc.AlertMatch
	CooldownS       int32
	Enabled         bool
	Owner           string
	OwnerName       string
	LastFireTime    *time.Time
	Description     string
	Version         int32
	CreateTime      time.Time
	UpdateTime      time.Time
	// RuleNames 告警触发器所选规则的名称（与 Alert.RuleIDs 一一对应；用例填充）。
	RuleNames []string
}

// TriggerFilter 触发器列表过滤；零值不过滤。
type TriggerFilter struct {
	Keyword    string
	Type       *int32
	TemplateID string
}

// TriggerFire 一次触发记录。
type TriggerFire struct {
	ID                 string
	TriggerID          string
	TriggerName        string
	Ref                string
	Result             int32
	Reason             string
	ExecutionID        string
	Summary            string
	WorkflowInstanceID string
	Signal             string
	SignalState        int32
	CreateTime         time.Time
}

// FireFilter 触发记录过滤；零值不过滤。
type FireFilter struct {
	TriggerID string
	Result    *int32
}

// FireRequest 开放接口触发的请求内容。
type FireRequest struct {
	// Ref 幂等键；空 = 每次调用都是新的一次触发。
	Ref string
	// Params 调用方给出的参数取值（触发器允许传参时生效）。
	Params map[string]any
	// Variables 流程服务节点带来的流程变量：与模板参数同名的项在允许传参时作为参数取值。
	Variables map[string]any
	// InstanceID 发起调用的流程实例（用于作业结束后回发信号）；可空。
	InstanceID string
	Summary    string
}

// FireAuth 开放接口触发的认证材料：Bearer 令牌，或流程 Webhook 的时间戳 + 签名（对原始请求体）。
type FireAuth struct {
	Bearer    string
	Timestamp string
	Signature string
	Body      []byte
}

// PendingSignal 一条待发送的流程信号：其作业（含全部切片）已结束。
type PendingSignal struct {
	FireID      string
	InstanceID  string
	Signal      string
	ExecutionID string
	// Status 作业的终态（切片时取最差的一个）；Success 是否全部成功。
	Status   int32
	Success  bool
	Attempts int32
	// FiredAt 触发记录的登记时间：信号按它起算的存活期（signalMaxAge）放弃，而不按尝试次数。
	FiredAt time.Time
}

// AlertEvent 一条告警事件（监控域提供）。
type AlertEvent struct {
	ID            string
	RuleID        string
	RuleName      string
	Severity      int32
	Labels        map[string]string
	FirstFireTime time.Time
}

// StatCount 一项分组计数。
type StatCount struct {
	Key    string
	Name   string
	Count  int64
	Failed int64
}

// StatTrendPoint 某一天的执行情况。
type StatTrendPoint struct {
	Day     string
	Total   int64
	Success int64
	Failed  int64
}

// Stats 执行统计。
type Stats struct {
	Total, Finished, Success int64
	AvgDurationSeconds       int64
	HostRuns                 int64
	ByStatus, ByKind         []StatCount
	ByTrigger, TopTemplates  []StatCount
	Trend                    []StatTrendPoint
	Timezone                 string
	Active                   int64
	RunnersOnline            int32
	RunnersTotal             int32
}

// FinishedExecution 一条已结束、尚未通知的执行。
type FinishedExecution struct {
	ID              string
	Name            string
	Kind            int32
	Trigger         int32
	Creator         string
	Status          int32
	Error           string
	HostTotal       int32
	HostOK          int32
	HostFailed      int32
	HostUnreachable int32
	StartTime       *time.Time
	FinishTime      time.Time
}
