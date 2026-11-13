package biz

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// policy_ttl 配置边界：既是惰性刷新间隔，也是多实例策略变更的收敛上界。
const (
	minPolicyTTL = time.Second
	maxPolicyTTL = 10 * time.Minute
	// maxRefreshBackoff 刷新失败后的重试退避上限：存储抖动期间按
	// min(ttl, 5s) 间隔重试，而非每个请求都重跑全量策略查询。
	maxRefreshBackoff = 5 * time.Second
	// policyRefreshTimeout 单次全量策略加载的时限：刷新脱离请求取消信号
	// （见 reloadLocked），须自带上界，防慢查询无限期占住刷新锁。
	policyRefreshTimeout = 10 * time.Second
)

// policySnapshot 是构建后冻结的策略快照：角色 code → 操作 code 集合。判定
// 为两次 map 查找（O(1)），与策略总量无关；精确相等、无通配符。用户与角色
// 的关系不进快照：由判定链路每请求从自有表实时读取，绑定变更即刻生效且
// 多实例天然一致；快照只承载角色→操作策略。
type policySnapshot map[string]map[string]struct{}

// Authorizer 是接口授权的内存策略求值器。
//
// 并发与多实例一致性设计：
//   - 快照是构建后冻结的 policySnapshot——绝不原地改写，并发只读安全；整体
//     经 atomic.Pointer 交换，读端无锁。
//   - Enforce 前惰性检查快照年龄，超过 policy_ttl 时由**一个**请求同步重建
//     交换（single-flight）：其余并发请求不排队，直接用旧快照判定。重建
//     脱离请求 ctx 的取消并自带 policyRefreshTimeout 上界，慢策略查询最多
//     拖住一个请求、且有上限。无后台 goroutine、无 ticker、空闲零查询。
//     策略变更方在本实例调用 Reload 立即生效，其余实例在 TTL 内收敛（撤销
//     传播窗口与短 TTL 访问令牌的既有风险姿态同量级；需要 ms 级收敛时再
//     评估 PG LISTEN/NOTIFY）。
//   - 重建失败（存储抖动）继续服务旧快照（stale-serve）并记日志，不放大故障；
//     启动阶段 DB 不可达则构造失败，禁止空策略静默上线。
type Authorizer struct {
	src PermissionPolicySource
	ttl time.Duration
	log *slog.Logger

	refreshMu sync.Mutex // 串行化重建；惰性刷新用 TryLock，抢不到即服务旧快照
	snapshot  atomic.Pointer[policySnapshot]
	loadedAt  atomic.Int64 // 快照加载时间，unix nanoseconds
	retryAt   atomic.Int64 // 刷新失败后的下次重试时间，unix nanoseconds；0=无退避
}

// NewAuthorizer 构造授权器并全量加载策略（初始化阶段快速失败）；
// policy_ttl 越界（<1s 或 >10m）视为配置错误。
func NewAuthorizer(src PermissionPolicySource, c *conf.Security, logger *slog.Logger) (*Authorizer, error) {
	ttl := c.GetAuthz().GetPolicyTtl().AsDuration()
	if ttl < minPolicyTTL || ttl > maxPolicyTTL {
		return nil, fmt.Errorf("authz policy_ttl %s out of range [%s, %s]", ttl, minPolicyTTL, maxPolicyTTL)
	}
	a := &Authorizer{src: src, ttl: ttl, log: logger}
	// 启动装配非请求链路，允许独立控制 ctx；加载失败直接终止启动。
	if err := a.Reload(context.Background()); err != nil {
		return nil, fmt.Errorf("authorizer initial load: %w", err)
	}
	return a, nil
}

// Enforce 判定角色是否被授权操作；调用前先确保快照未过期（惰性刷新）。
// (false, nil) 表示明确无权限；error 仅在快照缺失时非 nil，属基础设施
// 故障，调用方应 500 fail closed。快照刷新失败不返回错误：继续用旧快照
// 服务（stale-serve），由后续调用重试。
func (a *Authorizer) Enforce(ctx context.Context, roleCode, operation string) (bool, error) {
	a.ensureFresh(ctx)
	snap := a.snapshot.Load()
	if snap == nil {
		return false, fmt.Errorf("authz snapshot not loaded")
	}
	_, ok := (*snap)[roleCode][operation]
	return ok, nil
}

// Reload 强制全量重建快照并原子交换。策略变更（SetRolePermissions/
// DeleteRole）提交后由本实例调用，ctx 建议传 context.WithoutCancel(请求 ctx)：
// 策略已提交，快照刷新不得随请求取消而丢弃。
func (a *Authorizer) Reload(ctx context.Context) error {
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()
	return a.reloadLocked(ctx)
}

// ReloadAfterCommit 供策略变更提交后调用：刷新失败只记日志不回报——变更
// 已落库，调用方的业务结果不应因快照刷新抖动变成 500（客户端重试会重复
// 写入或得到 404）；本实例下一次请求按退避重试，其余实例在 policy_ttl 内
// 收敛。
func (a *Authorizer) ReloadAfterCommit(ctx context.Context) {
	if err := a.Reload(ctx); err != nil {
		a.log.ErrorContext(ctx, "authz policy reload after commit failed, serving stale snapshot until next refresh", "error", err)
	}
}

// ensureFresh 惰性刷新：快照未过期直接返回；过期则抢刷新锁同步重建——
// 抢不到说明已有请求（或 Reload）正在重建，本次直接服务旧快照、不排队
// （single-flight；慢策略查询不再让全部非超管请求堵在锁上）。刷新失败
// 进入退避：退避期内全部调用直接服务旧快照、不拿锁不查库，避免存储抖动
// 期间每个请求各自重跑全量策略查询（重试风暴）。
func (a *Authorizer) ensureFresh(ctx context.Context) {
	if !a.stale() {
		return
	}
	if !a.refreshMu.TryLock() {
		return
	}
	defer a.refreshMu.Unlock()
	if !a.stale() {
		return
	}
	if err := a.reloadLocked(ctx); err != nil {
		a.log.ErrorContext(ctx, "authz policy refresh failed, serving stale snapshot", "error", err)
	}
}

// stale 报告快照是否已过期且不在失败退避窗口内（即应当尝试刷新）。
func (a *Authorizer) stale() bool {
	if time.Since(time.Unix(0, a.loadedAt.Load())) <= a.ttl {
		return false
	}
	retryAt := a.retryAt.Load()
	return retryAt == 0 || time.Now().UnixNano() >= retryAt
}

// reloadLocked 重建并交换快照；调用方必须已持有 refreshMu。加载脱离入参
// ctx 的取消（保留 trace 等取值）并限时 policyRefreshTimeout：触发刷新的
// 请求被客户端取消不应作废一次所有请求共享的刷新，慢查询也不得无限期
// 占锁。失败设置退避窗口 min(ttl, maxRefreshBackoff)，成功清零。
func (a *Authorizer) reloadLocked(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policyRefreshTimeout)
	defer cancel()
	policies, err := a.src.ListPolicies(ctx)
	if err != nil {
		a.retryAt.Store(time.Now().Add(min(a.ttl, maxRefreshBackoff)).UnixNano())
		return fmt.Errorf("list permission policies: %w", err)
	}
	snap := buildSnapshot(policies)
	a.snapshot.Store(&snap)
	a.loadedAt.Store(time.Now().UnixNano())
	a.retryAt.Store(0)
	return nil
}

// buildSnapshot 按策略集合构建冻结快照；返回值构建后不得再变更。
func buildSnapshot(policies []PermissionPolicy) policySnapshot {
	snap := make(policySnapshot, 16)
	for _, p := range policies {
		ops, ok := snap[p.RoleCode]
		if !ok {
			ops = make(map[string]struct{}, 32)
			snap[p.RoleCode] = ops
		}
		ops[p.Operation] = struct{}{}
	}
	return snap
}

// OperationsForRoles 返回多个角色授权操作 code 的并集（字典序去重），供
// EffectiveOperations 枚举 /me 出参；快照口径与 Enforce 一致。
func (a *Authorizer) OperationsForRoles(ctx context.Context, roleCodes []string) ([]string, error) {
	a.ensureFresh(ctx)
	snap := a.snapshot.Load()
	if snap == nil {
		return nil, fmt.Errorf("authz snapshot not loaded")
	}
	seen := make(map[string]struct{}, 64)
	for _, code := range roleCodes {
		for op := range (*snap)[code] {
			seen[op] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen)), nil
}
