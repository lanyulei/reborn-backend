package biz

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// MaintenanceRepo 运行表的维护（全部语句幂等，时间取数据库时钟）。
type MaintenanceRepo interface {
	// PurgeInstances 删除心跳超过 deadAfter 的实例登记，并清空它们残留的集群归属。
	PurgeInstances(ctx context.Context, deadAfter time.Duration) (int, error)
	// EnsureAuditPartitions 预建当月与未来 ahead 个月的审计分区。
	EnsureAuditPartitions(ctx context.Context, ahead int) error
	// PurgeExpired 删除保留期之外的审计分区与终端会话记录、过期的 Agent 注册令牌；retention ≤ 0 = 不清理历史。
	PurgeExpired(ctx context.Context, retention time.Duration) (int, error)
}

// 维护参数。
const (
	// InstanceDeadAfter 实例多久没有心跳视为下线并清理登记（缺省值；须不小于归属租约，见 WithInstanceDeadAfter）。
	// 归属的存活判定用租约本身（见 data 层的 Claim / Attach），这里只管清理。
	InstanceDeadAfter = 2 * DefaultLease
	// DefaultAuditRetention 审计与终端会话记录的缺省保留期。
	DefaultAuditRetention = 180 * 24 * time.Hour
	auditPartitionsAhead  = 2
)

// Maintenance 选主执行的维护任务。
type Maintenance struct {
	repo      MaintenanceRepo
	log       *slog.Logger
	retention time.Duration
	deadAfter time.Duration
}

// NewMaintenance new a Maintenance.
func NewMaintenance(repo MaintenanceRepo, log *slog.Logger) *Maintenance {
	return &Maintenance{repo: repo, log: log, retention: DefaultAuditRetention, deadAfter: InstanceDeadAfter}
}

// WithInstanceDeadAfter 注入实例下线的判定时长（启动期调用；应为归属租约的两倍）。
func (m *Maintenance) WithInstanceDeadAfter(d time.Duration) {
	if d > 0 {
		m.deadAfter = d
	}
}

// WithRetention 注入保留期（0 = 永久保留）。
func (m *Maintenance) WithRetention(d time.Duration) { m.retention = d }

// PurgeInstances 清理下线实例（循环：每 15 秒，选主）。
func (m *Maintenance) PurgeInstances(ctx context.Context) (int, error) {
	_, err := m.repo.PurgeInstances(ctx, m.deadAfter)
	return 0, err
}

// Run 分区维护与过期数据清理（循环：每小时，选主，启动即跑一轮）。各步互不阻断。
func (m *Maintenance) Run(ctx context.Context) (int, error) {
	err := m.repo.EnsureAuditPartitions(ctx, auditPartitionsAhead)
	_, perr := m.repo.PurgeExpired(ctx, m.retention)
	return 0, errors.Join(err, perr)
}
