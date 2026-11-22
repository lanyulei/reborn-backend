package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// Tx 事务句柄（incident 域在其事务内调用 TaskRepo.EnqueueTx 时传入自己的句柄：两域共用 txrun 契约）。
type Tx = txrun.Tx

// ProfileRepo Profile 仓储。
type ProfileRepo interface {
	List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*Profile, int64, error)
	// ListEnabled 全部启用、未熔断与否不限的 Profile，按 kind、priority、create_time 升序。
	ListEnabled(ctx context.Context, kind string) ([]*Profile, error)
	// Get 不存在返回 ErrProfileNotFound。
	Get(ctx context.Context, id string) (*Profile, error)
	ListByIDs(ctx context.Context, ids []string) ([]*Profile, error)
	// Create 名称冲突返回 ErrProfileNameExists。
	Create(ctx context.Context, p *Profile) error
	// Update 乐观锁：version 不符返回 ErrVersionConflict；名称冲突返回 ErrProfileNameExists。
	Update(ctx context.Context, p *Profile, expectVersion int32) error
	// SetBreaker 打开 / 恢复 Profile 级熔断（不动 version）。
	SetBreaker(ctx context.Context, id string, open bool, reason string) error
	// Delete 软删；version 不符返回 ErrVersionConflict。
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
	AppendHistory(ctx context.Context, h *ProfileHistory) error
	ListHistory(ctx context.Context, profileID string, page, pageSize int64) ([]*ProfileHistory, int64, error)
}

// TaskRepo 任务队列仓储。
type TaskRepo interface {
	// EnqueueTx 在调用方事务内插入最小任务行；命中「诊断任务按故障唯一」时改为更新已有任务的等级与优先级，不报错。
	EnqueueTx(ctx context.Context, tx Tx, t *Task) error
	// Create 独立插入（人工召唤 / 干跑）；命中按故障唯一返回 ErrTaskExists。
	Create(ctx context.Context, t *Task) error
	// Get 不存在返回 ErrTaskNotFound。
	Get(ctx context.Context, id string) (*Task, error)
	List(ctx context.Context, filter TaskFilter, page, pageSize int64) ([]*Task, int64, error)
	// ActiveByIncident 该故障当前排队 / 进行中的指定类型非干跑任务；没有返回 nil。
	ActiveByIncident(ctx context.Context, incidentID, kind string) (*Task, error)
	// Claim 领取到期的排队任务（按等级与入队时间，SKIP LOCKED）：置 running、写领取令牌与租约、attempts+1。
	Claim(ctx context.Context, limit int, lease time.Duration) ([]*Task, error)
	// Renew 续租（持有者）。
	Renew(ctx context.Context, id, token string, lease time.Duration) (bool, error)
	// Finish 写终态（持有者围栏：status=running 且 lock_token 匹配；token 为空不校验持有者）。
	Finish(ctx context.Context, id, token, status, result string) (bool, error)
	// Requeue 持有者把任务放回队列（退避重试）。
	Requeue(ctx context.Context, id, token string, backoff time.Duration, result string) (bool, error)
	// BindRun 持有者绑定运行与 Profile。
	BindRun(ctx context.Context, id, token, runID, profileID string) error
	// Cancel 把排队 / 进行中的任务置 cancelled（不校验持有者；运行中的由运行器在下次写入时发现）。
	Cancel(ctx context.Context, id, result string) (bool, error)
	// ExpireQueued 把排队超时的任务置 expired，返回条数。
	ExpireQueued(ctx context.Context, before time.Time, limit int) (int, error)
	// ReleaseStale 把租约过期的 running 任务放回队列（实例崩溃兜底），返回条数。
	ReleaseStale(ctx context.Context, now time.Time, limit int) (int, error)
	// CountActiveByProfile 该 Profile 排队 / 进行中的任务数。
	CountActiveByProfile(ctx context.Context, profileID string) (int, error)
	// CountByStatus 统计范围内各状态任务数。
	CountByStatus(ctx context.Context, r StatsRange) (map[string]int32, error)
	// Purge 删除终态早于 before 的任务（至多 limit 条），返回条数。
	Purge(ctx context.Context, before time.Time, limit int) (int, error)
}

// RunRepo 运行仓储。
type RunRepo interface {
	Create(ctx context.Context, r *Run) error
	// Get 不存在返回 ErrRunNotFound。
	Get(ctx context.Context, id string) (*Run, error)
	// SaveRunning 仅在运行仍为 running 时写回全部可变列；返回是否写到（false = 已被外部收口）。
	SaveRunning(ctx context.Context, r *Run) (bool, error)
	// Close 把仍为 running 的运行收口为给定状态；返回是否写到。
	Close(ctx context.Context, id, status, reason string, end time.Time) (bool, error)
	ListByIncident(ctx context.Context, incidentID string, limit int) ([]*Run, error)
	// SetFeedback 写人工评价（只保留最后一次）。
	SetFeedback(ctx context.Context, id string, feedback int32, userID, note string, at time.Time) error
	// Stats 统计范围内的运行聚合与按 Profile 分布。
	Stats(ctx context.Context, r StatsRange) (*Overview, []ProfileStat, error)
	// RecentAutoOutcome 该 Profile 近 window 内自动执行的建议验证结果：总数与失败数（Profile 级熔断用）。
	RecentAutoOutcome(ctx context.Context, profileID string, window time.Duration) (total, failed int, err error)
	// PurgeBefore 删除结束时间早于 before 的运行，返回被删运行对应的会话 ID（调用方据此清理助手会话）。
	PurgeBefore(ctx context.Context, before time.Time, limit int) ([]string, error)
}

// BudgetRepo 预算计数仓储。
type BudgetRepo interface {
	// Add 累加某 Profile 某周期的用量（UPSERT）。
	Add(ctx context.Context, profileID, period string, tasks int32, prompt, completion int64) error
	// Get 某 Profile 某周期的用量（没有记录为零值）。
	Get(ctx context.Context, profileID, period string) (*Budget, error)
	// Total 某周期全部 Profile 的用量合计。
	Total(ctx context.Context, period string) (*Budget, error)
}
