// Package biz 承载作业执行引擎的业务核心：执行节点鉴权与心跳、任务派发（领取 → 组装 → 下发）、
// 执行中的上报（续租、日志、取消）与结束落账、租约清扫与日志清理，以及这些用例依赖的仓储接口。
// 本包禁止 import 传输层与任何存储驱动（depguard 构建期强制）。
//
// 与 core 的契约（表结构见 core/migrations/0070_job_init）：
//   - 引擎只读写 job_* 表，不 import core；模板表对引擎不可见——执行内容已固化在 job_execution.spec；
//   - 配置表（凭据、项目、执行节点）由 core 写、引擎只读，执行节点的心跳列例外，由引擎写；
//   - 执行记录由 core 建单与请求取消，引擎推进状态、写主机结果与日志；引擎不建单、不删单；
//   - 凭据密文只有引擎解密（kit/secretbox.Open），明文只存在于下发给节点的任务里，不落库、不进日志；
//   - jsonb 结构、状态字面量、协议结构、校验口径与终态判定一律取自 kit/jobcontract；
//   - 所有落库时间与到期判定使用数据库 now()，不使用应用时钟。
//
// 结构：上报与结束的判定是纯函数（decide：锁定的执行行 → 要落库的变更），data 层在单事务内
// 「锁行 → decide → 落库」，判定逻辑因此可以脱离数据库测试。
//
// 多实例：领取靠 FOR UPDATE SKIP LOCKED 的单语句条件更新；上报 / 结束以行锁 + lease_token 围栏；
// 租约清扫与日志清理语句自身幂等。引擎进程内没有任何跨请求状态。
package biz

import (
	"context"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"

	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewRunnerUsecase, NewHousekeepUsecase)

// Pinger 数据库连通性探测（健康检查）。
type Pinger interface {
	Ping() error
}

// 协议层错误（service 层据此映射状态码）。
var (
	// ErrUnauthorized 接入令牌无效：格式不对、节点不存在或已删除、密钥不匹配（三者对外不区分）。
	ErrUnauthorized = errors.New("invalid runner token")
	// ErrRunnerDisabled 节点已停用：不再派发新任务。
	ErrRunnerDisabled = errors.New("runner is disabled")
	// ErrLeaseLost 租约已失效：领取令牌不匹配、执行不属于该节点或执行已结束。
	ErrLeaseLost = errors.New("execution lease lost")
	// ErrInvalidRequest 请求不合法（超出协议限额或字段非法）；以 %w 包装具体原因。
	ErrInvalidRequest = errors.New("invalid request")
	// ErrNotFound 仓储未找到目标行。
	ErrNotFound = errors.New("not found")
)

// Runner 执行节点（job_runner 的引擎可见部分）。
type Runner struct {
	ID   string
	Zone string
	// Enabled 是否启用；停用后不再派发，在途任务照常上报。
	Enabled bool
	// TokenHash 接入令牌密钥的 SHA-256。
	TokenHash []byte
}

// Heartbeat 节点心跳上报的内容（已按列宽裁剪）。
type Heartbeat struct {
	AgentVersion string
	Hostname     string
	Capacity     int32
	Running      int32
}

// Claimed 一条刚被领取的执行（状态已置为执行中）。
type Claimed struct {
	ID string
	// Spec job_execution.spec 的原始 JSON（jobcontract.Spec）。
	Spec []byte
}

// Host 执行主机快照（job_execution_host）。
type Host struct {
	Address string
	Port    int32
	CIID    string
	Name    string
	Env     string
}

// Credential 登录凭据（job_credential）；Secret / BecomeSecret 为密文。
type Credential struct {
	Type         int32
	Username     string
	Secret       []byte
	BecomeMethod string
	BecomeUser   string
	// BecomeSecret 提权密码密文；nil = 免密提权。
	BecomeSecret []byte
}

// Project 项目的 Git 认证信息（job_project）；Secret 为密文，nil = 未配置。
type Project struct {
	AuthType int32
	Username string
	Secret   []byte
}

// ExecState 被行锁锁定的执行行里，上报 / 结束判定要用到的列。
type ExecState struct {
	Status          int32
	RunnerID        string
	LeaseToken      string
	LogSeq          int64
	LogBytes        int64
	LogTruncated    bool
	CommitID        string
	CancelRequested bool
}

// ReportMutation 一次上报要落库的变更。
type ReportMutation struct {
	// Revive 执行此前因租约到期被判为结果未知，节点又回来了：恢复为执行中并清掉结束时间与原因。
	// 例外：模板禁止并发、且同一模板已有新的未结束执行占着互斥位时无法恢复——执行保持结果未知，
	// 日志与续租照常，结束时直接落为真实终态。
	Revive bool
	// Chunks 要追加的日志块（序号连续，内容已清洗）。
	Chunks []jobcontract.LogChunk
	// LogSeq / LogBytes / LogTruncated 写回执行行的日志进度。
	LogSeq       int64
	LogBytes     int64
	LogTruncated bool
	// CommitID 写回的提交号（原值或本次上报的值）。
	CommitID string
	// Lease 续租时长：lease_until = now() + Lease。
	Lease time.Duration
}

// HostUpdate 一台主机的执行结果（按 (execution_id, address) 更新）。
type HostUpdate struct {
	Address     string
	Status      int32
	OK          int32
	Changed     int32
	Failed      int32
	Unreachable int32
	Skipped     int32
}

// FinishMutation 结束落账的变更；主机统计列（host_ok / host_failed / host_unreachable）由仓储在
// 更新主机行之后从主机行汇总，保证与主机明细一致。
type FinishMutation struct {
	Status int32
	Error  string
	// CommitID 非空时覆盖执行行的提交号。
	CommitID string
	Hosts    []HostUpdate
}

// RunnerRepo 执行节点的读取与心跳回写。
type RunnerRepo interface {
	// Find 按 ID 读未删除的节点；不存在返回 ErrNotFound。
	Find(ctx context.Context, id string) (*Runner, error)
	// Heartbeat 回写心跳列（last_seen 取数据库 now()），返回节点是否启用；节点不存在返回 ErrNotFound。
	Heartbeat(ctx context.Context, id string, hb Heartbeat) (enabled bool, err error)
}

// QueueRepo 执行队列：领取与领取后的撤销。
type QueueRepo interface {
	// ClaimNext 原子领取该节点可执行的最早一条排队中的执行（未请求取消、区域为空或与节点一致）：
	// 状态置为执行中，写入节点、领取令牌、租约与开始时间。没有可领取的返回 (nil, nil)。
	ClaimNext(ctx context.Context, runnerID, zone, leaseToken string, lease time.Duration) (*Claimed, error)
	// Fail 把刚领取、尚未下发的执行置为失败（令牌围栏）。
	Fail(ctx context.Context, executionID, leaseToken, reason string) error
	// Release 把刚领取、尚未下发的执行放回队列（令牌围栏）：组装任务时遇到瞬时故障、应答没能送达节点时用。
	// 领取之后已被请求取消的执行不回队列（回去了也不会再被领取），直接落为已取消。
	Release(ctx context.Context, executionID, leaseToken string) error
}

// MaterialRepo 组装任务所需的只读数据。
type MaterialRepo interface {
	// Hosts 执行的主机快照，按序号排列。
	Hosts(ctx context.Context, executionID string) ([]Host, error)
	// Credential 未删除的凭据；不存在返回 ErrNotFound。
	Credential(ctx context.Context, id string) (*Credential, error)
	// Project 未删除的项目；不存在返回 ErrNotFound。
	Project(ctx context.Context, id string) (*Project, error)
}

// PayloadRepo 分发文件（KindFile）的内容快照（job_execution_payload）。
type PayloadRepo interface {
	// Payload 执行的文件内容；没有对应的行返回 ErrNotFound。
	Payload(ctx context.Context, executionID string) ([]byte, error)
}

// ProgressRepo 执行中的上报与结束落账。两个方法都在单事务内「锁定执行行 → 调 decide → 落库」；
// decide 必须是纯计算（不得做任何 I/O），返回错误则整个事务回滚并原样透传。执行不存在返回 ErrNotFound。
type ProgressRepo interface {
	// Report 落库一次上报（续租、日志、提交号）；revived 报告本次是否真的把结果未知的执行恢复成了执行中
	// （模板禁止并发且互斥位已被新的执行占用时不恢复，见 ReportMutation.Revive）。
	Report(ctx context.Context, executionID string, decide func(cur ExecState) (*ReportMutation, error)) (revived bool, err error)
	// Finish 落库结束；decide 返回 nil 变更表示空操作（重复的结束请求）。
	Finish(ctx context.Context, executionID string, decide func(cur ExecState) (*FinishMutation, error)) error
}

// HousekeepRepo 后台清理（语句自身幂等，多实例并发安全）。
type HousekeepRepo interface {
	// ExpireLeases 把租约已到期的执行中的执行置为结果未知（至多 limit 条），返回条数。
	ExpireLeases(ctx context.Context, reason string, limit int) (int, error)
	// PurgeLogs 删除已结束的执行里早于 now() − retention 的日志块（至多 limit 块），返回块数。
	PurgeLogs(ctx context.Context, retention time.Duration, limit int) (int, error)
}

// SecretOpener 解密凭据（data.SecretBox 实现）。错误信息不含任何凭据。
type SecretOpener interface {
	Open(ciphertext []byte) (string, error)
}

// Observer 引擎指标钩子（server.Metrics 实现）。
type Observer interface {
	// ObserveCall 一次执行节点协议调用：op 取 Op* 常量，result 取 Result* 常量。
	ObserveCall(ctx context.Context, op, result string)
	// AddExecutions 执行的状态事件计数：event 取 Event* 常量。
	AddExecutions(ctx context.Context, event string, n int)
}

// 协议调用名（Observer.ObserveCall 的 op）。
const (
	OpHeartbeat = "heartbeat"
	OpClaim     = "claim"
	OpReport    = "report"
	OpFinish    = "finish"
)

// 协议调用结果（Observer.ObserveCall 的 result）。
const (
	ResultOK           = "ok"
	ResultEmpty        = "empty"        // 领取：没有可执行的任务
	ResultUnauthorized = "unauthorized" // 令牌无效
	ResultDisabled     = "disabled"     // 节点已停用
	ResultLeaseLost    = "lease_lost"   // 租约已失效
	ResultInvalid      = "invalid"      // 请求不合法
	ResultError        = "error"        // 引擎自身故障（数据库等）
)

// 执行事件（Observer.AddExecutions 的 event）。
const (
	EventClaimed        = "claimed"         // 已派发给节点
	EventDispatchFailed = "dispatch_failed" // 领取后无法派发（凭据 / 项目已删除、解密失败、内容不合法），置为失败
	EventRevived        = "revived"         // 结果未知的执行因节点恢复上报而回到执行中
	EventLeaseExpired   = "lease_expired"   // 租约到期，置为结果未知
	EventLogTruncated   = "log_truncated"   // 日志超出上限，开始丢弃
)

// finishEvents 终态对应的事件名（下标 = job_execution.status）。
var finishEvents = [...]string{
	jobcontract.StatusSuccess:  "finished_success",
	jobcontract.StatusFailed:   "finished_failed",
	jobcontract.StatusPartial:  "finished_partial",
	jobcontract.StatusCanceled: "finished_canceled",
	jobcontract.StatusTimeout:  "finished_timeout",
}

// resultOf 把用例错误归类为指标标签。
func resultOf(err error) string {
	switch {
	case err == nil:
		return ResultOK
	case errors.Is(err, ErrUnauthorized):
		return ResultUnauthorized
	case errors.Is(err, ErrRunnerDisabled):
		return ResultDisabled
	case errors.Is(err, ErrLeaseLost):
		return ResultLeaseLost
	case errors.Is(err, ErrInvalidRequest):
		return ResultInvalid
	}
	return ResultError
}
