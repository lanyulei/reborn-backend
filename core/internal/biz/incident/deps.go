package biz

import (
	"context"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 本文件收拢故障域对其他域的全部依赖：只读 / 单向，一律是在此定义的窄接口，由组合根
// （internal/biz/incident_adapters.go）用 system / oncall / notify / monitor / cmdb / ticket / workflow / job /
// assistant / file 各域已有的公开用例方法适配。其他域不依赖 incident。

// Tx / TxManager / EventWriter 沿用平台既有的事务与通知事件契约。
type (
	Tx          = txrun.Tx
	TxManager   = txrun.Manager
	EventWriter = biznotify.NotificationWriter
	NotifyEvent = biznotify.Event
)

// Directory 组织目录的只读查询（system 域 DirectoryUsecase）。
type Directory interface {
	// ActiveUserIDs 过滤出存在且可用的用户，保持入参顺序并去重。
	ActiveUserIDs(ctx context.Context, ids []string) ([]string, error)
	// UserNames 批量取用户显示名；不存在的不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
	// PrimaryDept 用户的主部门 ID（没有返回空串）。
	PrimaryDept(ctx context.Context, userID string) (string, error)
}

// DeptLeaders 部门负责人查询（system 域 DirectoryUsecase）。
type DeptLeaders interface {
	// DeptLeaderIDs 从 deptID 向上第 level 级部门（1 = 自身）取可用的负责人；该级为空则继续向上。
	DeptLeaderIDs(ctx context.Context, deptID string, level int, exclude string) ([]string, error)
}

// Admins 超级管理员判定（system 域 AuthzUsecase）。
type Admins interface {
	// IsAdmin 超管返回 true；非可用用户返回 false。
	IsAdmin(ctx context.Context, userID string) (bool, error)
}

// Accounts 账号类型判定（system 域 UserUsecase）：只有机器账号（运维 Agent）持租约写入的建议才能申请自动通过。
type Accounts interface {
	// IsMachine 账号是否为机器账号；不存在返回错误。
	IsMachine(ctx context.Context, userID string) (bool, error)
}

// AgentTaskRef 交给运维 Agent 域入队的一条故障任务引用：只带排序与过滤要用的快照字段，
// Profile 选择、适用范围、配额与状态检查全部在 Agent 域领取时执行。
type AgentTaskRef struct {
	IncidentID string
	Number     string
	Title      string
	Severity   int32
	BusinessID string
	RuleID     string
	// Reason 入队原因：opened 故障开启 / escalated 等级上调 / postmortem 复盘完成。
	Reason string
	// Kind 任务类型：diagnose（缺省）/ postmortem。
	Kind string
	// Version 复盘版本（Kind = postmortem 时有效；每个版本只入队一次）。
	Version int32
}

// AgentDispatcher 运维 Agent 域的入队窄接口（可为空实现：未装配 Agent 域时 Deps.Agent 为 nil，故障行为不变）。
// Enqueue 在故障写入的同一事务内只插一行最小任务，只可能返回数据库错误。
type AgentDispatcher interface {
	Enqueue(ctx context.Context, tx Tx, ref AgentTaskRef) error
}

// Permissions 接口权限判定（system 域 AuthzUsecase 按操作码判定）。只用于接口授权中间件覆盖不到的两处：
// 附件访问与免接口授权的「按告警查故障」。
type Permissions interface {
	// CanReadIncidents 是否持有「查询故障详情」接口权限。
	CanReadIncidents(ctx context.Context, userID string) (bool, error)
}

// Duty 值班表当前值班人（oncall 域）。
type Duty interface {
	// OnDuty 主值班在前、备值班在后；值班表不存在、已停用或无人值班返回空。
	OnDuty(ctx context.Context, scheduleID string, at time.Time) ([]string, error)
	// OnDutyByRole 当前主班与备班。
	OnDutyByRole(ctx context.Context, scheduleID string) (primary, backup []string, err error)
}

// OncallGroups 值班分组管理员（oncall 域）。
type OncallGroups interface {
	// AdminIDs 分组的管理员（只含可用用户）；分组不存在返回空。
	AdminIDs(ctx context.Context, groupID string) ([]string, error)
}

// NamedLookup 批量取对象名称（值班表 / 通知渠道 / CMDB 资源等），不存在的不在结果中。
type NamedLookup interface {
	Names(ctx context.Context, ids []string) (map[string]string, error)
}

// Named 可选项（最小披露）。
type Named struct {
	ID   string
	Name string
}

// Schedules 值班表候选与名称（oncall 域）。
type Schedules interface {
	// Schedules 启用的值班表（至多 500）。
	Schedules(ctx context.Context) ([]Named, error)
	// ScheduleNames 批量取名称（启用的）。
	ScheduleNames(ctx context.Context, ids []string) (map[string]string, error)
}

// AlertRef 告警事件（来自监控域）的摘要。Status 为监控域口径：1=告警中 2=已恢复 3=已关闭。
type AlertRef struct {
	ID            string
	RuleID        string
	RuleName      string
	GroupID       string
	Severity      int32
	Status        int32
	Labels        map[string]string
	CIID          string
	FirstFireTime time.Time
	ResolveTime   *time.Time
	Muted         bool
	Inhibited     bool
	Value         string
}

// AlertSignal 告警时间线上的一条触发 / 恢复 / 解除屏蔽 / 解除抑制 / 关闭事件及其告警快照。
type AlertSignal struct {
	Seq   int64
	Kind  string
	Time  time.Time
	Alert AlertRef
}

// Alerts 监控域的告警事件（只读 + 一条「关联故障」说明的回写）。
type Alerts interface {
	// Feed seq 之后的告警事件（按 seq 升序），至多 limit 条。
	Feed(ctx context.Context, afterSeq int64, limit int) ([]*AlertSignal, error)
	// FeedRange seq 在 (afterSeq, untilSeq] 内的事件（与 Feed 同一组类型，按 seq 升序），至多 limit 条；
	// 关联器回看窗口用，已处理的序号由关联器自行去重。
	FeedRange(ctx context.Context, afterSeq, untilSeq int64, limit int) ([]*AlertSignal, error)
	// Get 不存在返回 ErrAlertNotFound。
	Get(ctx context.Context, id string) (*AlertRef, error)
	// ListByIDs 批量取（不存在的不在结果中）。
	ListByIDs(ctx context.Context, ids []string) ([]*AlertRef, error)
	// CountFiring ids 里当前仍在告警中（告警中且未被屏蔽 / 抑制，与关联器归并口径一致）的条数。
	CountFiring(ctx context.Context, ids []string) (int, error)
	// RecentFiring 首次触发时间不早于 since 的告警（含已恢复的），至多 limit 条；试算用。
	RecentFiring(ctx context.Context, since time.Time, limit int) ([]*AlertRef, error)
	// CanRead userID 能否在监控域看到这条告警（数据范围）。
	CanRead(ctx context.Context, userID string, alert *AlertRef) (bool, error)
	// Note 在告警时间线追加一条「关联故障」说明。
	Note(ctx context.Context, alertID, operator, text string) error
}

// MatcherSet 已编译的标签匹配条件。
type MatcherSet interface {
	Match(labels map[string]string) bool
}

// MatcherCompiler 标签匹配条件的校验与编译（监控域 kit/monitorcontract）。
type MatcherCompiler interface {
	Compile(matchers []Matcher) (MatcherSet, error)
}

// CICard CMDB 资源卡片（摘要级）。
type CICard struct {
	ID        string
	Name      string
	ModelCode string
	ModelName string
	Env       string
	Status    string
	OwnerID   string
	OwnerName string
	Attrs     map[string]any
}

// CINode 影响面里的一个下游资源。
type CINode struct {
	ID        string
	Name      string
	ModelCode string
	ModelName string
	Depth     int32
	Via       string
}

// ChangeItem 其他域的一条变更记录。
type ChangeItem struct {
	Domain     string
	Kind       string
	RefID      string
	Summary    string
	Time       time.Time
	OperatorID string
	CIID       string
	URL        string
}

// CMDB 资源目录（cmdb 域）。
type CMDB interface {
	// Cards 批量取资源卡片（不存在的不在结果中）。
	Cards(ctx context.Context, ids []string) ([]*CICard, error)
	// Downstream 资源的下游（按部署配置的拓扑关系，上游影响下游）至多 depth 层、limit 个。
	Downstream(ctx context.Context, ciID string, depth, limit int) ([]*CINode, bool, error)
	// Upstream 资源的上游 ID（深度 ≤ 3）。
	Upstream(ctx context.Context, ciID string) ([]string, error)
	// FindByNames 按名称精确匹配资源。
	FindByNames(ctx context.Context, names []string) ([]*CICard, error)
	// Viewer userID 在资源管理里的可见性判定（按模型 / 环境）。
	Viewer(ctx context.Context, userID string) (func(modelCode, env string) bool, error)
	// History 资源在时间窗内的变更历史。
	History(ctx context.Context, ciIDs []string, from, to time.Time, limit int) ([]*ChangeItem, error)
}

// ChangeQuery 变更关联查询。
type ChangeQuery struct {
	CIIDs      []string
	IncidentID string
	From, To   time.Time
	Limit      int
}

// ChangeSource 一个域的变更来源（k8s / job / workflow）；note 为该域对结果的说明（如只能粗筛）。
type ChangeSource interface {
	Domain() string
	Changes(ctx context.Context, q ChangeQuery) (items []*ChangeItem, note string, err error)
}

// TicketCreate 提单入参。
type TicketCreate struct {
	TemplateID  string
	Title       string
	Priority    int32
	Description string
	Creator     string
	CreatorDept string
	RequestID   string
	ResourceIDs []string
}

// TicketInfo 工单摘要。
type TicketInfo struct {
	ID      string
	Number  string
	Title   string
	Status  int32
	Creator string
	// Finished 工单已结束。
	Finished bool
}

// Tickets 工单（ticket 域）。
type Tickets interface {
	// Templates 启用的工单模板。
	Templates(ctx context.Context) ([]Named, error)
	// Template 模板对 userID 是否可用。
	Template(ctx context.Context, templateID, userID string) (*Named, error)
	// Create 以 Creator 的名义提单。
	Create(ctx context.Context, p TicketCreate) (*TicketInfo, error)
	// View 以 userID 身份读取工单；无权或不存在返回 (nil, nil)。
	View(ctx context.Context, ticketID, userID string) (*TicketInfo, error)
	// Comment 以 userID 的名义追加评论。
	Comment(ctx context.Context, ticketID, userID, content string) error
}

// WorkflowStart 发起流程的入参。
type WorkflowStart struct {
	DefinitionKey string
	BusinessID    string
	Title         string
	Initiator     string
	Variables     map[string]any
	RequestID     string
}

// Workflows 流程（workflow 域）。
type Workflows interface {
	// Definitions 启用且已发布的流程定义。
	Definitions(ctx context.Context) ([]Named, error)
	// Startable 流程标识当前能否发起；能则返回名称。
	Startable(ctx context.Context, key string) (name string, ok bool, err error)
	// Start 发起流程，返回实例 ID。
	Start(ctx context.Context, p WorkflowStart) (instanceID string, err error)
	// InstanceStatus 实例状态（workflow 域口径：1 进行中 2 完成 3 拒绝 4 撤回 5 终止）。
	InstanceStatus(ctx context.Context, instanceID string) (int32, error)
}

// JobTemplate 作业模板摘要。
type JobTemplate struct {
	ID          string
	Name        string
	Description string
	ParamNames  []string
	Enabled     bool
}

// JobSubmit 提交作业入参。
type JobSubmit struct {
	TemplateID string
	Params     map[string]any
	CIIDs      []string
	RequestID  string
}

// JobExecution 作业执行摘要。
type JobExecution struct {
	ID      string
	Status  int32
	Summary string
	// Finished 已结束；Succeeded 成功结束。
	Finished  bool
	Succeeded bool
}

// Jobs 作业（job 域）。
type Jobs interface {
	// Templates 启用的作业模板（以 userID 身份可见的）。
	Templates(ctx context.Context, userID string) ([]*JobTemplate, error)
	// Template 单个模板；不存在或不可用返回 (nil, nil)。
	Template(ctx context.Context, userID, id string) (*JobTemplate, error)
	// Submit 以 userID 的身份提交执行。
	Submit(ctx context.Context, userID string, in JobSubmit) (*JobExecution, error)
	// Execution 执行当前状态。
	Execution(ctx context.Context, id string) (*JobExecution, error)
}

// Summarizer 智能助手的单次生成（assistant 域）。
type Summarizer interface {
	Available() bool
	Summarize(ctx context.Context, system, prompt string) (string, error)
}

// Files 附件绑定（file 域）。
type Files interface {
	// BindTx 把 uploader 上传的临时附件绑定到故障。
	BindTx(ctx context.Context, tx Tx, uploader, incidentID string, fileIDs []string) error
}

// Workdays 工作日历（workflow 域）：按工作日推算到期时间。
type Workdays interface {
	AddWorkdays(ctx context.Context, from time.Time, days int) (time.Time, error)
}

// TokenCipher 令牌 / 密钥的加解密（data 层以 kit/secretbox 实现）；主密钥未配置时 Seal 返回 ErrSecretUnavailable。
type TokenCipher interface {
	Seal(plaintext string) ([]byte, error)
	Open(sealed []byte) (string, error)
}

// WebhookDelivery 一次 Webhook 投递。
type WebhookDelivery struct {
	URL     string
	Secret  string
	EventID string
	Body    []byte
}

// WebhookSender Webhook 出站（data 层：仅 https、不跟随跳转、解析后地址做内网校验）。
// retryable 为 false 表示确定性失败（地址被拒、4xx）。
type WebhookSender interface {
	Send(ctx context.Context, d WebhookDelivery) (retryable bool, err error)
}

// SiteURL 前端站点地址（通知与 Webhook 里的链接前缀；为空则是站内相对路径）。
type SiteURL string

// IncidentURL 故障详情页的路径。
func (s SiteURL) IncidentURL(id string) string { return string(s) + "/incident/" + id }

// PublicURL 公开公告页的路径。
func (s SiteURL) PublicURL(token string) string { return string(s) + "/public/incident/" + token }
