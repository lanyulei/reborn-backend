package biz

import (
	"context"
	"time"
)

// RunnerOnlineWindow 执行节点的在线判定窗口：最近一次心跳在此之内视为在线（节点每 10 秒心跳一次）。
const RunnerOnlineWindow = 45 * time.Second

// CredentialReader 凭据读取。恒不含密文。
type CredentialReader interface {
	List(ctx context.Context, filter CredentialFilter, page, pageSize int64) ([]*Credential, int64, error)
	// Get 不存在返回 ErrCredentialNotFound。
	Get(ctx context.Context, id string) (*Credential, error)
	// ListOptions 全部凭据的最小信息，按名称排序。
	ListOptions(ctx context.Context) ([]*Credential, error)
}

// CredentialWriter 凭据写入。
type CredentialWriter interface {
	// Create 名称重复返回 ErrCredentialNameExists。
	Create(ctx context.Context, c *Credential, secret, becomeSecret []byte, actor string) error
	// Update 乐观锁更新；版本不一致返回 ErrVersionConflict。
	Update(ctx context.Context, c *Credential, secret, becomeSecret SecretChange, expectVersion int32, actor string) error
	// Delete 软删并清除密文；仍被模板使用返回 InUse(ErrCredentialInUse, …)。
	Delete(ctx context.Context, id, actor string) error
}

// ProjectReader 项目读取。恒不含密文。
type ProjectReader interface {
	List(ctx context.Context, keyword string, page, pageSize int64) ([]*Project, int64, error)
	// Get 不存在返回 ErrProjectNotFound。
	Get(ctx context.Context, id string) (*Project, error)
	ListOptions(ctx context.Context) ([]*Project, error)
}

// ProjectWriter 项目写入。
type ProjectWriter interface {
	// Create 名称重复返回 ErrProjectNameExists。
	Create(ctx context.Context, p *Project, secret []byte, actor string) error
	Update(ctx context.Context, p *Project, secret SecretChange, expectVersion int32, actor string) error
	// Delete 软删并清除密文；仍被模板使用返回 InUse(ErrProjectInUse, …)。
	Delete(ctx context.Context, id, actor string) error
}

// TemplateReader 作业模板读取。
type TemplateReader interface {
	List(ctx context.Context, filter TemplateFilter, page, pageSize int64) ([]*Template, int64, error)
	// Get 不存在返回 ErrTemplateNotFound。
	Get(ctx context.Context, id string) (*Template, error)
}

// TemplateWriter 作业模板写入。
type TemplateWriter interface {
	// Create 在写事务内确认项目与凭据仍存在（并与它们的删除互斥）；不存在返回 ErrTemplateInvalid 的说明。
	Create(ctx context.Context, t *Template, actor string) error
	Update(ctx context.Context, t *Template, expectVersion int32, actor string) error
	Delete(ctx context.Context, id, actor string) error
}

// NewExecution 待建的一条执行（切片时一次提交有多条）。
type NewExecution struct {
	Execution *Execution
	Hosts     []Host
	// Payload 分发文件的内容（仅 KindFile）。
	Payload []byte
}

// ExecutionWriter 执行记录写入。core 只建单与请求取消；状态由执行引擎推进。
type ExecutionWriter interface {
	// CreateTx 在调用方事务内建单并写入主机快照（与审批流程的发起同事务）。items 是同一次提交的各片，
	// 幂等键只在第一片上。幂等键命中已有执行时不写入，返回该执行的 ID 与 created = false；
	// 模板禁止并发且已有未结束的执行返回 ErrExecutionConflict。
	CreateTx(ctx context.Context, tx Tx, items []NewExecution) (id string, created bool, err error)
	// SetApprovalTx 记下审批流程实例（按执行 ID 或分组 ID）。
	SetApprovalTx(ctx context.Context, tx Tx, businessID, instanceID string) error
	// ResolveApprovalTx 审批结束：approved 时待审批 → 排队中，否则 → 审批未通过（记原因）。
	// 只动仍是待审批的行，返回受影响的行数。
	ResolveApprovalTx(ctx context.Context, tx Tx, businessID string, approved bool, reason string) (int64, error)
	// Cancel 排队中的直接置为已取消；执行中与结果未知（节点失联）的登记取消请求、状态不变；
	// 其余终态返回 ErrExecutionFinished。
	Cancel(ctx context.Context, id, actor string) error
}

// ExecutionReader 执行记录读取。
type ExecutionReader interface {
	List(ctx context.Context, filter ExecutionFilter, page, pageSize int64) ([]*Execution, int64, error)
	// Get 不存在返回 ErrExecutionNotFound。
	Get(ctx context.Context, id string) (*Execution, error)
	ListHosts(ctx context.Context, id string, filter ExecutionHostFilter, page, pageSize int64) ([]*ExecutionHost, int64, error)
}

// ExecutionLogReader 执行日志读取。
type ExecutionLogReader interface {
	// ReadLog 读取序号大于 afterSeq 的日志块，按序号升序，累计内容达到 maxBytes 即止（至少一块）。
	// 返回内容与读到的最后一块的序号（没有读到任何块时为 0）。
	ReadLog(ctx context.Context, id string, afterSeq int64, maxBytes int) (content string, lastSeq int64, err error)
}

// RunnerReader 执行节点读取。
type RunnerReader interface {
	List(ctx context.Context, keyword string, page, pageSize int64) ([]*Runner, int64, error)
	// Get 不存在返回 ErrRunnerNotFound。
	Get(ctx context.Context, id string) (*Runner, error)
	// Zones 未删除节点的非空区域名，去重排序。
	Zones(ctx context.Context) ([]string, error)
}

// RunnerWriter 执行节点写入。
type RunnerWriter interface {
	// Create 名称重复返回 ErrRunnerNameExists。
	Create(ctx context.Context, r *Runner, tokenHash []byte, actor string) error
	Update(ctx context.Context, r *Runner, expectVersion int32, actor string) error
	Delete(ctx context.Context, id, actor string) error
}

// RunnerTokens 执行节点的接入令牌与名称查询。
type RunnerTokens interface {
	// SetToken 更换接入令牌摘要（旧令牌随即失效）；节点不存在返回 ErrRunnerNotFound。
	SetToken(ctx context.Context, id string, tokenHash []byte, actor string) error
	// Names 批量取节点名称（含已删除的：历史执行记录仍要显示）。
	Names(ctx context.Context, ids []string) (map[string]string, error)
}

// ScriptRepo 脚本库仓储。
type ScriptRepo interface {
	// List 不带脚本内容。
	List(ctx context.Context, filter ScriptFilter, page, pageSize int64) ([]*Script, int64, error)
	// Get 带脚本内容；不存在返回 ErrScriptNotFound。
	Get(ctx context.Context, id string) (*Script, error)
	Write(ctx context.Context, op WriteOp, s *Script, expectVersion int32, actor string) error
}

// WriteOp 配置对象的写操作。
type WriteOp int

// 写操作。
const (
	OpCreate WriteOp = iota + 1
	OpUpdate
	OpDelete
)

// RiskRuleRepo 高危命令规则仓储。
type RiskRuleRepo interface {
	// List 全部未删除的规则，拦截在前、再按名称。
	List(ctx context.Context) ([]*RiskRule, error)
	// Get 不存在返回 ErrRiskRuleNotFound。
	Get(ctx context.Context, id string) (*RiskRule, error)
	Write(ctx context.Context, op WriteOp, r *RiskRule, expectVersion int32, actor string) error
}

// PolicyRepo 全局策略仓储（单行）。
type PolicyRepo interface {
	Get(ctx context.Context) (*Policy, error)
	// Update 乐观锁更新；版本不一致返回 ErrVersionConflict。
	Update(ctx context.Context, p *Policy, actor string) error
}

// ScheduleRepo 定时作业仓储。
type ScheduleRepo interface {
	List(ctx context.Context, filter ScheduleFilter, page, pageSize int64) ([]*Schedule, int64, error)
	// Get 不存在返回 ErrScheduleNotFound。
	Get(ctx context.Context, id string) (*Schedule, error)
	// Write 新建 / 编辑 / 删除；编辑时负责人改为 actor，next_time 由调用方算好放在 s.NextTime。
	Write(ctx context.Context, op WriteOp, s *Schedule, expectVersion int32, actor string) error
}

// ScheduleQueue 定时作业的触发队列。
type ScheduleQueue interface {
	// Due 到期（next_time <= 数据库 now()）的启用中的定时作业，至多 limit 条，并返回数据库当前时间。
	Due(ctx context.Context, limit int) (items []*Schedule, now time.Time, err error)
	// Fire 在一个事务里认领并触发一条到期的定时作业：以 FOR UPDATE SKIP LOCKED 重新读取该条（要求 next_time
	// 仍等于 planned、仍启用且未删除）——别的实例正持有或已经推进的直接返回 false、不调用 fn；认领到的
	// 在持锁期间调用 fn 取得本次触发的结果并落账（next_time / last_time / last_execution_id / last_error），
	// 提交后返回 true。fn 出错则整体回滚（这一次触发留待下一轮重做）。
	Fire(ctx context.Context, id string, planned time.Time, fn func(ctx context.Context, s *Schedule) (ScheduleRun, error)) (bool, error)
}

// TriggerRepo 触发器仓储。
type TriggerRepo interface {
	List(ctx context.Context, filter TriggerFilter, page, pageSize int64) ([]*Trigger, int64, error)
	// Get 不存在返回 ErrTriggerNotFound。
	Get(ctx context.Context, id string) (*Trigger, error)
	// Write 新建 / 编辑 / 删除；新建接口触发器时带令牌摘要与密文。
	Write(ctx context.Context, op WriteOp, t *Trigger, expectVersion int32, actor string, token *TriggerToken) error
}

// TriggerToken 接口触发器调用令牌的落库形态。
type TriggerToken struct {
	Hash []byte
	// Sealed 令牌密文（校验 HMAC 签名用）；主密钥未配置时为空。
	Sealed []byte
}

// TriggerSecrets 触发器的令牌与告警触发器清单。
type TriggerSecrets interface {
	// SetToken 更换调用令牌；触发器不存在或不是接口类型返回 ErrTriggerNotFound。
	SetToken(ctx context.Context, id string, token TriggerToken, actor string) error
	// Token 取令牌的落库形态（开放接口认证用）；不存在返回 ErrTriggerNotFound。
	Token(ctx context.Context, id string) (TriggerToken, error)
	// EnabledAlertTriggers 全部启用的告警触发器。
	EnabledAlertTriggers(ctx context.Context) ([]*Trigger, error)
}

// FireRepo 触发记录仓储。
type FireRepo interface {
	List(ctx context.Context, filter FireFilter, page, pageSize int64) ([]*TriggerFire, int64, error)
	// Begin 登记一次触发（去重门闩）：同一触发器同一 ref 已有记录时返回该记录与 fresh = false。
	// fresh 时同时校验冷却：距上次已提交的触发不足 cooldown 则直接记为已跳过并返回。
	// 记为「已提交」的记录此时不落「信号待发送」（f.SignalState 本身不改）：执行还没提交，由 Complete 写入。
	Begin(ctx context.Context, f *TriggerFire, cooldownS int32) (existing *TriggerFire, fresh bool, err error)
	// Complete 写回触发结果（已提交执行时同时刷新触发器的最近触发时间）。
	Complete(ctx context.Context, f *TriggerFire) error
}

// SignalQueue 待发送的流程信号。
type SignalQueue interface {
	// Due 作业已结束、信号到了该发时间的触发记录，至多 limit 条。
	Due(ctx context.Context, limit int) ([]*PendingSignal, error)
	// Mark 写回发送结果：state 为 jobcontract.Signal*；仍待发送时 retryIn 后再试。
	Mark(ctx context.Context, fireID string, state int32, retryIn time.Duration) error
}

// AlertCursor 告警事件的读取位置：(首次触发时间, 事件 ID)；ID 为空表示该时刻之后的全部。
type AlertCursor struct {
	FirstFireTime time.Time
	ID            string
}

// AlertFeed 告警事件来源（组合根适配 monitor 域；按 (首次触发时间, ID) 升序键集翻页）。
type AlertFeed interface {
	// FiringAfter (first_fire_time, id) 严格大于 after、仍在告警中且未被屏蔽的告警事件，按该序升序，至多 limit 条。
	FiringAfter(ctx context.Context, after AlertCursor, limit int) ([]*AlertEvent, error)
}

// NotifyQueue 已结束、尚未通知的执行。
type NotifyQueue interface {
	// ClaimTx 在事务内领取一条（行锁，SKIP LOCKED）并标记为已通知；没有返回 nil。
	ClaimTx(ctx context.Context, tx Tx) (*FinishedExecution, error)
}

// StatsReader 执行统计。
type StatsReader interface {
	Stats(ctx context.Context, since time.Time, timezone string) (*Stats, error)
}

// TriggerNames 定时作业 / 触发器的名称（执行记录展示触发来源）。
type TriggerNames interface {
	// SourceNames 按触发来源分别取名称：scheduleIDs → 定时作业，triggerIDs → 触发器（含已删除的）。
	SourceNames(ctx context.Context, scheduleIDs, triggerIDs []string) (map[string]string, error)
}
