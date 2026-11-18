package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 仓储接口（依赖倒置，data 层实现）。按职责拆分，每个接口不超过 3 个方法。
// 约定：写方法各自是一个完整事务——数据源 / 屏蔽规则 / 规则分组的写入同事务 bump
// monitor_cfg_version，规则的每次写入同事务追加版本快照；带 expectVersion 的更新为乐观锁，
// 版本不一致返回 ErrVersionConflict。

// GroupReader 规则分组读仓储。
type GroupReader interface {
	// ListAll 全部未删除分组（至多 MaxGroups），上级在前，同级按 sort、名称。不含计数：每次范围解析、
	// 每次通知派发都要读这棵树，计数只有分组列表页需要（RuleCounts）。
	ListAll(ctx context.Context) ([]*RuleGroup, error)
	// RuleCounts 各分组直属的未删除规则数（没有规则的分组不在结果中）。
	RuleCounts(ctx context.Context) (map[string]int32, error)
}

// GroupWriter 规则分组写仓储。
type GroupWriter interface {
	// Create 在写事务内校验上级存在、层级与总数上限并生成 Path；重名返回 ErrGroupNameExists。
	Create(ctx context.Context, g *RuleGroup, actor string) error
	// Update 改名称 / 说明 / 排序；不存在返回 ErrGroupNotFound。
	Update(ctx context.Context, g *RuleGroup, actor string) error
	// Delete 软删并清除其角色授权；仍有下级或规则返回 ErrGroupInUse，仍被屏蔽规则 / 订阅规则引用返回
	// GroupReferenced。
	Delete(ctx context.Context, id, actor string) error
}

// RoleScopeRepo 角色数据范围仓储。
type RoleScopeRepo interface {
	// ListByRoles 这些角色的全部授权。
	ListByRoles(ctx context.Context, roleIDs []string) ([]*RoleScope, error)
	// ListByGroups 直接授权在这些分组上的全部角色。
	ListByGroups(ctx context.Context, groupIDs []string) ([]*RoleScope, error)
	// Replace 整组替换某分组的角色授权；分组不存在返回 ErrGroupNotFound。
	Replace(ctx context.Context, groupID string, items []*RoleScope, actor string) error
}

// DatasourceReader 数据源读仓储。
type DatasourceReader interface {
	List(ctx context.Context, filter DatasourceFilter, page, pageSize int64) ([]*Datasource, int64, error)
	// Get 不存在返回 ErrDatasourceNotFound。
	Get(ctx context.Context, id string) (*Datasource, error)
	// ListOptions 建规则用的最小列表（至多 500 条，缺省数据源在前）。
	ListOptions(ctx context.Context, enabledOnly bool) ([]*Datasource, error)
}

// DatasourceLookup 数据源的引用查询。
type DatasourceLookup interface {
	// Briefs 批量取名称与启用状态；不存在的不在结果中。
	Briefs(ctx context.Context, ids []string) (map[string]DatasourceBrief, error)
	// GetSecret 取凭据密文（未设置返回 nil）；不存在返回 ErrDatasourceNotFound。
	GetSecret(ctx context.Context, id string) ([]byte, error)
}

// DatasourceWriter 数据源写仓储。is_default 为 true 时同事务取消其他数据源的缺省标记。
type DatasourceWriter interface {
	// Create 重名返回 ErrDatasourceNameExists。
	Create(ctx context.Context, d *Datasource, sealed []byte, actor string) error
	Update(ctx context.Context, d *Datasource, secret SecretChange, expectVersion int32, actor string) error
	// Delete 仍被未删除的规则明确指定时返回 ErrDatasourceInUse（DatasourceInUse 带规则名）。
	Delete(ctx context.Context, id, actor string) error
}

// SecretCipher 数据源凭据的加解密（data 层以 kit/secretbox 实现，主密钥与通知渠道相同）；
// 主密钥未配置返回 ErrSecretUnavailable——绝不静默落明文。
type SecretCipher interface {
	Seal(plaintext string) ([]byte, error)
	Open(sealed []byte) (string, error)
}

// QueryClient 向数据源发起查询（data 层以 kit/promclient 实现）。失败返回 QueryFailed 构造的
// 领域错误，说明已去除凭据。
type QueryClient interface {
	// Validate 只校验连接配置（地址、认证、请求头），不发起网络请求；不合法返回 ErrDatasourceInvalid，
	// 地址字面量指向不允许访问的地址返回 ErrDatasourceAddressBlocked。
	Validate(conn Connection) error
	// Instant 即时查询；ts 为零值时由数据源取当前时间。maxSeries 为返回的序列数上限。
	Instant(ctx context.Context, conn Connection, expr string, ts time.Time, maxSeries int) (*QueryResult, error)
	// Range 区间查询。
	Range(ctx context.Context, conn Connection, expr string, start, end time.Time, step time.Duration, maxSeries int) (*QueryResult, error)
}

// RuleReader 规则读仓储。
type RuleReader interface {
	List(ctx context.Context, filter RuleFilter, page, pageSize int64) ([]*Rule, int64, error)
	// Get 不存在（含已删除）返回 ErrRuleNotFound。
	Get(ctx context.Context, id string) (*Rule, error)
	// ListByIDs 未删除的规则；不存在的不在结果中。
	ListByIDs(ctx context.Context, ids []string) ([]*Rule, error)
}

// RuleLookup 规则的引用与批量查询。
type RuleLookup interface {
	// Names 批量取未删除规则的名称。
	Names(ctx context.Context, ids []string) (map[string]string, error)
	// FindByNames 分组内这些名称对应的未删除规则（名称 → 规则 ID）。
	FindByNames(ctx context.Context, groupID string, names []string) (map[string]string, error)
	// ListByGroups 这些分组内的未删除规则，按分组、名称排序，至多 limit 条。
	ListByGroups(ctx context.Context, groups GroupFilter, limit int) ([]*Rule, error)
}

// RuleWriter 规则写仓储。每个方法一个事务：写规则行 + 追加 monitor_rule_version 快照；并在事务内
// 校验所属分组、指定的数据源仍然存在（与分组 / 数据源的删除互斥）。成功后回填 Version /
// UpdateTime / SpecTime。
type RuleWriter interface {
	// Create 重名返回 ErrRuleNameExists。
	Create(ctx context.Context, r *Rule, change RuleChange) error
	Update(ctx context.Context, r *Rule, expectVersion int32, change RuleChange) error
	// Delete 软删（r.Version 为读取时的版本，用作乐观锁）。
	Delete(ctx context.Context, r *Rule, change RuleChange) error
}

// RuleVersionReader 规则版本读仓储。
type RuleVersionReader interface {
	// List 按版本号倒序，不含快照。
	List(ctx context.Context, ruleID string, page, pageSize int64) ([]*RuleVersion, int64, error)
	// Get 含快照；不存在返回 ErrRuleVersionNotFound。
	Get(ctx context.Context, ruleID string, version int32) (*RuleVersion, error)
}

// EvalReader 评估状态与判定记录读仓储（运行表，引擎写）。
type EvalReader interface {
	// ListTasks 这些规则在各数据源上的评估状态（规则 ID → 任务）。
	ListTasks(ctx context.Context, ruleIDs []string) (map[string][]*EvalTask, error)
	// ListRecords 按评估时间倒序。
	ListRecords(ctx context.Context, filter EvalRecordFilter, page, pageSize int64) ([]*EvalRecord, int64, error)
}

// AlertReader 告警事件读仓储（运行表，引擎写）。
type AlertReader interface {
	// List 按最近触发时间倒序。
	List(ctx context.Context, filter AlertFilter, page, pageSize int64) ([]*Alert, int64, error)
	// Get 不存在返回 ErrAlertNotFound。
	Get(ctx context.Context, id string) (*Alert, error)
	// Scan 同 List 的过滤口径，按最近触发时间倒序取至多 limit 条（预览：标签匹配在用例里完成）。
	Scan(ctx context.Context, filter AlertFilter, limit int) ([]*Alert, error)
}

// AlertStats 告警事件统计。
type AlertStats interface {
	// Aggregate 按维度聚合，按事件数倒序取至多 limit 行；同时返回符合过滤条件的事件总数。
	Aggregate(ctx context.Context, by AggregateBy, filter AlertFilter, limit int) ([]*AlertAggregate, int64, error)
	// MutedCounts 各屏蔽规则当前屏蔽通知的活跃事件数。
	MutedCounts(ctx context.Context, muteIDs []string) (map[string]int32, error)
}

// NotifyLogReader 通知记录读仓储。
type NotifyLogReader interface {
	// ListByAlert 事件的通知时间线，按时间升序，至多 limit 条。
	ListByAlert(ctx context.Context, alertID string, limit int) ([]*NotifyLog, error)
	// IsRecipient 用户是否出现在该事件任一通知记录的接收方里。
	IsRecipient(ctx context.Context, alertID, userID string) (bool, error)
	// SubscriptionHits 各订阅最近一次命中时间与 since 之后的命中次数。
	SubscriptionHits(ctx context.Context, subscriptionIDs []string, since time.Time) (map[string]SubscriptionHit, error)
}

// MuteReader 屏蔽规则读仓储。
type MuteReader interface {
	List(ctx context.Context, filter MuteFilter, page, pageSize int64) ([]*Mute, int64, error)
	// Get 不存在返回 ErrMuteNotFound。
	Get(ctx context.Context, id string) (*Mute, error)
	// Names 批量取名称（含已删除的：历史事件仍要展示命中的屏蔽规则）。
	Names(ctx context.Context, ids []string) (map[string]string, error)
}

// MuteWriter 屏蔽规则写仓储。
type MuteWriter interface {
	Create(ctx context.Context, m *Mute, actor string) error
	Update(ctx context.Context, m *Mute, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// SubscriptionReader 订阅规则读仓储。
type SubscriptionReader interface {
	List(ctx context.Context, filter SubscriptionFilter, page, pageSize int64) ([]*Subscription, int64, error)
	// Get 不存在返回 ErrSubscriptionNotFound。
	Get(ctx context.Context, id string) (*Subscription, error)
	// ListCandidates 派发时预筛：启用且分组 / 规则 / 数据源 / 级别 / 事件类型命中的订阅，按 ID 排序，
	// 至多 limit 条；total 为命中的总数（大于 limit 即被截断）。
	ListCandidates(ctx context.Context, c SubscriptionCandidate, limit int) (items []*Subscription, total int, err error)
}

// SubscriptionWriter 订阅规则写仓储。
type SubscriptionWriter interface {
	// Create / Update 在写事务内确认订阅的分组都还存在（与分组删除互斥），否则返回 ErrGroupNotFound。
	Create(ctx context.Context, s *Subscription) error
	Update(ctx context.Context, s *Subscription, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// OutboxRepo 通知发件箱仓储。core 只允许改发件箱的 status / attempts / last_error / done_time。
type OutboxRepo interface {
	// ClaimTx 在事务内行锁（FOR UPDATE SKIP LOCKED）领取一条可派发的待派发行；exclude 为本轮已
	// 处理失败的行。没有可领取的返回 nil。失败过的行要等退避期过后才会被再次领取；同一事件更早入箱的
	// 行未结束时，后入箱的不会被领取。被引擎重新置为待派发的行（失败次数已清零）立即可领取。
	ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*OutboxItem, error)
	// FinishTx 在事务内把行置为已派发 / 已丢弃（reason 记入 last_error）。
	FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error
	// Fail 独立语句记一次失败（attempts+1、last_error、本次失败时间）；失败次数达到上限时置为已丢弃
	// 并返回 true。
	Fail(ctx context.Context, id, reason string) (dropped bool, err error)
}

// DispatchWriter 派发事务内的写入。
type DispatchWriter interface {
	// InsertLogTx 写一条通知记录。
	InsertLogTx(ctx context.Context, tx txrun.Tx, log *NotifyLog) error
	// TouchAlertTx 回写事件的通知计数与最近通知时间（core 仅有这两列的写权限）：把 notify_count 抬到
	// 本次的通知序号。只为确实派发了的触发 / 重复通知调用。
	TouchAlertTx(ctx context.Context, tx txrun.Tx, alertID string, seq int32) error
}
