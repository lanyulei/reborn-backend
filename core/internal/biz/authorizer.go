package biz

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"

	"github.com/lanyulei/monitor/backend/core/internal/conf"
)

// authzModelText 是 Casbin 模型（ACL，无 g 规则）：
// subject = 角色 code（创建后不可变），object = 操作 code
// （OperationCode 派生的 32 位 md5 hex，与授权存储、authz 中间件同口径）。
// 用户与角色的关系不进 Casbin：由判定链路每请求从自有表实时读取，绑定变更
// 即刻生效且多实例天然一致；本模型只承载 p 策略（角色→操作）。
// matcher 为精确相等、无通配符；未来需要模式匹配时仅改此常量，调用点不变。
const authzModelText = `
[request_definition]
r = sub, obj

[policy_definition]
p = sub, obj

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj
`

// policy_ttl 配置边界：既是惰性刷新间隔，也是多实例策略变更的收敛上界。
const (
	minPolicyTTL = time.Second
	maxPolicyTTL = 10 * time.Minute
)

// Authorizer 是接口授权的内存策略求值器（Casbin）。
//
// 并发与多实例一致性设计：
//   - 快照是构建后冻结的 *casbin.Enforcer——绝不原地改写，因此选非 Synced
//     版（省一层内部锁），并发 Enforce 只读安全；整体经 atomic.Pointer 交换，
//     读端无锁。
//   - Enforce 前惰性检查快照年龄，超过 policy_ttl 同步重建交换：无后台
//     goroutine、无 ticker、空闲零查询。策略变更方在本实例调用 Reload 立即
//     生效，其余实例在 TTL 内收敛（撤销传播窗口与短 TTL 访问令牌的既有
//     风险姿态同量级；需要 ms 级收敛时再评估 PG LISTEN/NOTIFY）。
//   - 重建失败（存储抖动）继续服务旧快照（stale-serve）并记日志，不放大故障；
//     启动阶段 DB 不可达则构造失败，禁止空策略静默上线。
type Authorizer struct {
	src PermissionPolicySource
	ttl time.Duration
	log *slog.Logger

	refreshMu sync.Mutex // 串行化重建，防 TTL 到期时的并发刷新击穿
	enforcer  atomic.Pointer[casbin.Enforcer]
	loadedAt  atomic.Int64 // 快照加载时间，unix nanoseconds
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
// (false, nil) 表示明确无权限；error 仅在快照缺失或求值器异常时非 nil，
// 属基础设施故障，调用方应 500 fail closed。快照刷新失败不返回错误：
// 继续用旧快照服务（stale-serve），由下一次调用重试。
func (a *Authorizer) Enforce(ctx context.Context, roleCode, operation string) (bool, error) {
	a.ensureFresh(ctx)
	e := a.enforcer.Load()
	if e == nil {
		return false, fmt.Errorf("authz snapshot not loaded")
	}
	return e.Enforce(roleCode, operation)
}

// Reload 强制全量重建快照并原子交换。策略变更（SetRolePermissions/
// DeleteRole）提交后由本实例调用，ctx 建议传 context.WithoutCancel(请求 ctx)：
// 策略已提交，快照刷新不得随请求取消而丢弃。
func (a *Authorizer) Reload(ctx context.Context) error {
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()
	return a.reloadLocked(ctx)
}

// ensureFresh 惰性刷新：快照未过期直接返回；过期则经 mutex 串行重建
// （double-check 防击穿，等待者拿锁后多半已新鲜）。
func (a *Authorizer) ensureFresh(ctx context.Context) {
	if time.Since(time.Unix(0, a.loadedAt.Load())) <= a.ttl {
		return
	}
	a.refreshMu.Lock()
	defer a.refreshMu.Unlock()
	if time.Since(time.Unix(0, a.loadedAt.Load())) <= a.ttl {
		return
	}
	if err := a.reloadLocked(ctx); err != nil {
		a.log.Error("authz policy refresh failed, serving stale snapshot", "error", err)
	}
}

// reloadLocked 重建并交换快照；调用方必须已持有 refreshMu。
func (a *Authorizer) reloadLocked(ctx context.Context) error {
	policies, err := a.src.ListPolicies(ctx)
	if err != nil {
		return fmt.Errorf("list permission policies: %w", err)
	}
	e, err := buildEnforcer(policies)
	if err != nil {
		return err
	}
	a.enforcer.Store(e)
	a.loadedAt.Store(time.Now().UnixNano())
	return nil
}

// buildEnforcer 按模型与策略集合构建冻结的求值器；返回值构建后不得再变更。
func buildEnforcer(policies []PermissionPolicy) (*casbin.Enforcer, error) {
	m, err := model.NewModelFromString(authzModelText)
	if err != nil {
		return nil, fmt.Errorf("parse authz model: %w", err)
	}
	e, err := casbin.NewEnforcer(m)
	if err != nil {
		return nil, fmt.Errorf("create enforcer: %w", err)
	}
	if len(policies) == 0 {
		return e, nil
	}
	rules := make([][]string, len(policies))
	for i, p := range policies {
		rules[i] = []string{p.RoleCode, p.Operation}
	}
	if _, err := e.AddPolicies(rules); err != nil {
		return nil, fmt.Errorf("load %d policies: %w", len(rules), err)
	}
	return e, nil
}

// OperationsForRoles 返回多个角色授权操作 code 的并集（字典序去重），供
// EffectiveOperations 枚举 /me 出参；快照口径与 Enforce 一致。
func (a *Authorizer) OperationsForRoles(ctx context.Context, roleCodes []string) ([]string, error) {
	a.ensureFresh(ctx)
	e := a.enforcer.Load()
	if e == nil {
		return nil, fmt.Errorf("authz snapshot not loaded")
	}
	seen := make(map[string]struct{}, 64)
	ops := make([]string, 0, 64)
	for _, code := range roleCodes {
		rules, err := e.GetFilteredPolicy(0, code)
		if err != nil {
			return nil, fmt.Errorf("filter policy for role %s: %w", code, err)
		}
		for _, rule := range rules {
			if len(rule) < 2 {
				continue
			}
			if _, dup := seen[rule[1]]; dup {
				continue
			}
			seen[rule[1]] = struct{}{}
			ops = append(ops, rule[1])
		}
	}
	slices.Sort(ops)
	return ops, nil
}
