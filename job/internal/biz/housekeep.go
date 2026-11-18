package biz

import (
	"context"
	"log/slog"
	"time"
)

const (
	// LeaseExpiredReason 租约到期时写入 job_execution.error 的原因。
	LeaseExpiredReason = "执行节点失联：租约到期未续，作业可能仍在运行"
	// DefaultLogRetention 缺省日志保留期（90 天）。
	DefaultLogRetention = 90 * 24 * time.Hour
	// sweepBatch 单轮清扫的执行数上限（清满即连续执行）。
	sweepBatch = 200
	// purgeBatch 单轮删除的日志块数上限（清满即连续执行）。
	purgeBatch = 2000
)

// HousekeepOptions 后台清理参数。
type HousekeepOptions struct {
	// LogRetention 日志保留期；0 = 永久保留。
	LogRetention time.Duration
	// SweepGrace 进程启动后多久之内不做租约清扫；0 = 不设。装配时取租约时长：引擎停机超过一个租约后
	// 重新启动时，库里所有执行中的租约都已过期，而节点（退避重试，至多 15 秒一次）还没来得及上报——
	// 立刻清扫会把它们全部误判为失联。等一个租约，仍在跑的节点足以续上。
	SweepGrace time.Duration
	// Clock 取当前时刻（只用于上面的启动宽限）；nil = time.Now。
	Clock func() time.Time
}

// HousekeepUsecase 后台清理：租约清扫与日志保留期清理。
type HousekeepUsecase struct {
	repo         HousekeepRepo
	obs          Observer
	log          *slog.Logger
	logRetention time.Duration
	clock        func() time.Time
	sweepFrom    time.Time // 此刻之前不清扫
}

// NewHousekeepUsecase new a HousekeepUsecase（保留期取缺省值，WithOptions 覆盖）。
func NewHousekeepUsecase(repo HousekeepRepo, obs Observer, logger *slog.Logger) *HousekeepUsecase {
	return &HousekeepUsecase{repo: repo, obs: obs, log: logger, logRetention: DefaultLogRetention, clock: time.Now}
}

// WithOptions 注入参数（装配阶段调用一次，非并发安全）。
func (uc *HousekeepUsecase) WithOptions(o HousekeepOptions) {
	uc.logRetention = max(o.LogRetention, 0)
	uc.clock = time.Now
	if o.Clock != nil {
		uc.clock = o.Clock
	}
	uc.sweepFrom = time.Time{}
	if o.SweepGrace > 0 {
		uc.sweepFrom = uc.clock().Add(o.SweepGrace)
	}
}

// SweepLeases 把租约到期未续的执行置为结果未知，返回条数。绝不重新排队或自动重跑：节点可能只是
// 与引擎失联，作业仍在目标主机上运行；节点恢复上报后由 Report 把它恢复为执行中。
func (uc *HousekeepUsecase) SweepLeases(ctx context.Context) (int, error) {
	if uc.clock().Before(uc.sweepFrom) {
		return 0, nil // 启动宽限期内（见 HousekeepOptions.SweepGrace）
	}
	n, err := uc.repo.ExpireLeases(ctx, LeaseExpiredReason, sweepBatch)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		uc.obs.AddExecutions(ctx, EventLeaseExpired, n)
		uc.log.WarnContext(ctx, "execution leases expired, marked unknown", "count", n)
	}
	return n, nil
}

// PurgeLogs 删除已结束的执行里超过保留期的日志块，返回块数；保留期为 0 时不删除。
func (uc *HousekeepUsecase) PurgeLogs(ctx context.Context) (int, error) {
	if uc.logRetention <= 0 {
		return 0, nil
	}
	n, err := uc.repo.PurgeLogs(ctx, uc.logRetention, purgeBatch)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		uc.log.InfoContext(ctx, "expired execution logs purged", "chunks", n)
	}
	return n, nil
}
