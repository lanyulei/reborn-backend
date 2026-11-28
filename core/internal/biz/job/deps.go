package biz

import (
	"context"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 本文件收拢 job 域对其他域的全部依赖：只读、单向，一律是在此定义的窄接口，由组合根
// （internal/biz）绑定到 system / cmdb 域的用例。其他域不依赖 job。

// Directory 用户显示名（system 域 DirectoryUsecase 实现）。
type Directory interface {
	// UserNames 批量取用户显示名；不存在的用户不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
}

// Resource 目标主机解析出的一个 CMDB 资源。
type Resource struct {
	ID   string
	Name string
	Env  string
	// Address 地址字段的原始取值（未校验）；Target.AddressField 为空时即资源名称。
	Address string
}

// ResourceSet 一次目标主机解析的结果。
type ResourceSet struct {
	Items []Resource
	// Truncated 匹配的资源超过了上限（超出部分未返回）。
	Truncated bool
	// Skipped 「指定资源」方式下被排除的资源数：已不存在、已退役、不属于该类型，或不在用户的数据范围内。
	Skipped int
}

// HostSource 按用户的 CMDB 数据范围解析目标主机（组合根适配 cmdb 域的 ScopeUsecase / ModelUsecase / CIUsecase）。
// job 域读取 CMDB 的唯一入口：数据范围在这里统一判定。
type HostSource interface {
	// Check 校验目标主机配置本身：资源类型存在且在用户的数据范围内（否则 ErrTargetForbidden），
	// 地址字段存在、未废弃、不是敏感属性（否则 TargetInvalid）。
	Check(ctx context.Context, userID string, target jc.Target) error
	// Resolve 先做 Check，再解析出资源，至多 limit 个。已退役的资源不参与执行。
	Resolve(ctx context.Context, userID string, target jc.Target, limit int) (ResourceSet, error)
	// ModelNames 批量取资源类型名称；不存在或不在用户数据范围内的不在结果中。
	ModelNames(ctx context.Context, userID string, codes []string) (map[string]string, error)
}

// SecretSealer 凭据加密（data 层以 kit/secretbox 实现）。只有 Seal：core 不解密作业凭据。
type SecretSealer interface {
	// Seal 加密明文；主密钥未配置返回 ErrSecretUnavailable。
	Seal(plaintext string) ([]byte, error)
}

// ---------------------------------------------------------------------------
// 二期 / 三期的跨域依赖：流程引擎（审批、信号）、通知、文件、监控（规则名称）。同样只读 / 单向。
// ---------------------------------------------------------------------------

// ApprovalStart 发起审批流程的内容。
type ApprovalStart struct {
	// DefinitionKey 审批流程的标识（全局策略）。
	DefinitionKey string
	// BusinessID 业务对象 ID：执行 ID（切片时为分组 ID）。
	BusinessID string
	Title      string
	Initiator  string
	Variables  map[string]any
}

// ApprovalOutcome 发起审批流程的结果。流程可能在发起时即结束（全部节点被跳过 / 自动通过）。
type ApprovalOutcome struct {
	InstanceID string
	// Finished 发起即结束；Approved 结束时是否通过；Reason 发起即结束且未通过时记在执行上的原因（空 = 缺省说明）。
	Finished bool
	Approved bool
	Reason   string
}

// MsgApprovalUnreviewed 流程以「通过」结束、却没有任何人审批过时记在执行上的原因。
const MsgApprovalUnreviewed = "审批环节因处理人为空被自动跳过，没有人实际审批，作业未执行"

// ApprovalEvidence 一条执行所挂审批流程树上的审批证据（见 workflow 域的 ApprovalEvidence）。
type ApprovalEvidence struct {
	// Skipped 因处理人为空被引擎自动跳过的审批环节数。
	Skipped int
	// Approved 由人（except 以外）点「同意」的次数。
	Approved int
}

// ApprovalEvidenceReader 审批证据查询端口（由组合根适配流程引擎的实例仓储）。须在流程引擎的事务内读：
// 实例结束的那条命令里新产生的历史此时尚未提交，另开连接读不到。
type ApprovalEvidenceReader interface {
	// ApprovalEvidenceTx 统计 businessID 这条执行的审批证据；except 是不计入「由人同意」的用户（通常是提交人）。
	ApprovalEvidenceTx(ctx context.Context, tx Tx, businessID, except string) (ApprovalEvidence, error)
}

// ApprovalReviewed 报告审批流程「走完」能否落为批准：流程里确有审批环节被自动跳过、又没有任何人
// （提交人以外）点过同意 → false。流程本来就没有审批环节（或这次走的分支上没有）时没有「被跳过的
// 审批」，维持原行为。与容器平台的变更审批同一口径。
func ApprovalReviewed(ev ApprovalEvidence) bool {
	return ev.Approved > 0 || ev.Skipped == 0
}

// Approvals 审批流程（组合根适配 workflow 域的 RuntimeUsecase / DefinitionUsecase）。
type Approvals interface {
	// Startable 报告流程标识是否存在、当前能否发起（启用且已发布），以及流程名称。
	Startable(ctx context.Context, key string) (name string, exists, startable bool, err error)
	// StartTx 在调用方事务内发起审批流程：执行建单与流程发起同事务提交。流程不可发起返回
	// ErrApprovalUnavailable。
	StartTx(ctx context.Context, tx Tx, p ApprovalStart) (ApprovalOutcome, error)
	// Cancel 发起人撤回审批流程（流程结束时经监听器把执行置为审批未通过）。
	Cancel(ctx context.Context, instanceID, userID string) error
	// Involved 报告用户是否参与了这个审批流程（处理人 / 被代理人；发起人即执行的提交人）。
	Involved(ctx context.Context, instanceID, userID string) (bool, error)
}

// Signals 向流程实例发送信号（组合根适配 workflow 域的 RuntimeUsecase）。
type Signals interface {
	// Signal 发送信号。delivered = 已有节点收到；gone = 实例已结束或不存在（不必再试）；
	// 两者皆假且 err 为 nil = 还没有节点在等这个信号（稍后再试）。
	Signal(ctx context.Context, instanceID, name string, vars map[string]any) (delivered, gone bool, err error)
}

// Notice 一条通知。
type Notice struct {
	// Severity 级别 1-4。
	Severity int32
	Title    string
	Payload  map[string]any
	// DedupKey 去重键：重复写入是空操作。
	DedupKey   string
	Recipients []string
}

// Notifier 写通知事件（组合根适配 notify 域的 NotificationWriter）；事件类型恒为
// jobcontract.EventExecutionFinished。
type Notifier interface {
	NotifyTx(ctx context.Context, tx Tx, n Notice) error
}

// UploadedFile 文件服务里的一个文件。
type UploadedFile struct {
	Name string
	Data []byte
}

// Files 读取用户上传的文件（组合根适配 file 域的 Usecase）：只有上传者本人的临时文件可读。
type Files interface {
	// Read 读取文件内容；不存在或无权读取返回 ErrAdhocInvalid 的说明。
	Read(ctx context.Context, userID, fileID string) (*UploadedFile, error)
}

// RunPermission 「执行作业」的接口权限（组合根适配 system 域的 AuthzUsecase）。
// 定时作业与触发器以负责人的身份提交执行，不经过接口授权中间件——必须在这里显式判定，
// 否则只有「新建定时作业」权限的人就能借定时作业执行任意模板。
type RunPermission interface {
	// CanRun 报告用户当前是否持有按作业模板提交执行的接口权限（用户被停用 / 不存在为 false）。
	CanRun(ctx context.Context, userID string) (bool, error)
	// CanViewAll 报告用户是否持有「执行记录」列表的接口权限（能看全部执行）。
	CanViewAll(ctx context.Context, userID string) (bool, error)
}

// AlertRules 告警规则名称（组合根适配 monitor 域的规则仓储）。
type AlertRules interface {
	// RuleNames 批量取规则名称；不存在的不在结果中。
	RuleNames(ctx context.Context, ids []string) (map[string]string, error)
}

// Tx 事务句柄（internal/pkg/txrun.Tx 的别名）；TxManager 开启事务（internal/data.TxManager）。
type (
	Tx        = txrun.Tx
	TxManager = txrun.Manager
)
