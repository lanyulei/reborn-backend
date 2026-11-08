package biz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

// policyStoreStub 策略源桩：固定策略集（克隆返回，快照语义对齐 data 层）；
// fail 模拟仓储故障（构造失败与 stale-serve 验证）。
type policyStoreStub struct {
	policies []PermissionPolicy
	fail     bool
}

// ListPolicies implements biz.PermissionPolicySource。
func (s *policyStoreStub) ListPolicies(context.Context) ([]PermissionPolicy, error) {
	if s.fail {
		return nil, errors.New("boom")
	}
	return slices.Clone(s.policies), nil
}

// newTestAuthorizer 构造真实 Authorizer（策略来自桩，TTL 可控）；
// 日志丢弃，断言关注行为不关注日志。
func newTestAuthorizer(t *testing.T, src PermissionPolicySource, ttl time.Duration) *Authorizer {
	t.Helper()
	a, err := NewAuthorizer(src, &conf.Security{
		Authz: &conf.Security_Authz{PolicyTtl: durationpb.New(ttl)},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewAuthorizer() error = %v", err)
	}
	return a
}

// TestAuthorizer_Enforce 策略命中判定：命中放行、未命中明确拒绝、
// 未知角色无策略自然拒绝（deny-by-default）。
func TestAuthorizer_Enforce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "user_viewer", Operation: "/system.v1.UserService/ListUsers"},
	}}
	a := newTestAuthorizer(t, src, time.Minute)

	ok, err := a.Enforce(ctx, "user_viewer", "/system.v1.UserService/ListUsers")
	if err != nil || !ok {
		t.Fatalf("Enforce(granted) = %v, %v; want true, nil", ok, err)
	}
	if ok, err := a.Enforce(ctx, "user_viewer", "/system.v1.UserService/DeleteUser"); err != nil || ok {
		t.Fatalf("Enforce(ungranted op) = %v, %v; want false, nil", ok, err)
	}
	if ok, err := a.Enforce(ctx, "nobody", "/system.v1.UserService/ListUsers"); err != nil || ok {
		t.Fatalf("Enforce(unknown role) = %v, %v; want false, nil", ok, err)
	}
}

// TestAuthorizer_LazyRefresh 快照超过 TTL 后首个请求同步重建：策略源变更
// 被拾取（无后台刷新，空闲零查询）。快照年龄通过 loadedAt 回拨模拟，
// 避免 >1s 的真实等待（TTL 下界 minPolicyTTL）。
func TestAuthorizer_LazyRefresh(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "ops", Operation: "/system.v1.UserService/ListUsers"},
	}}
	a := newTestAuthorizer(t, src, time.Minute)

	if ok, _ := a.Enforce(ctx, "ops", "/system.v1.UserService/ListUsers"); !ok {
		t.Fatal("Enforce(initial) = false, want true")
	}
	// 策略源变更：TTL 内快照未过期，仍按旧快照判定。
	src.policies = []PermissionPolicy{
		{RoleCode: "ops", Operation: "/system.v1.RoleService/ListRoles"},
	}
	if ok, _ := a.Enforce(ctx, "ops", "/system.v1.RoleService/ListRoles"); ok {
		t.Fatal("Enforce(fresh snapshot) = true, want false (stale until TTL)")
	}
	// 回拨快照年龄越过 TTL：下一调用惰性重建，新策略生效。
	a.loadedAt.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	if ok, err := a.Enforce(ctx, "ops", "/system.v1.RoleService/ListRoles"); err != nil || !ok {
		t.Fatalf("Enforce(after lazy refresh) = %v, %v; want true, nil", ok, err)
	}
	if ok, _ := a.Enforce(ctx, "ops", "/system.v1.UserService/ListUsers"); ok {
		t.Fatal("Enforce(old policy) = true, want false (replaced)")
	}
}

// TestAuthorizer_Reload 显式刷新立即生效（授权保存与删角色后的调用路径）。
func TestAuthorizer_Reload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "ops", Operation: "/system.v1.UserService/ListUsers"},
	}}
	a := newTestAuthorizer(t, src, time.Minute)

	src.policies = nil
	if err := a.Reload(ctx); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if ok, _ := a.Enforce(ctx, "ops", "/system.v1.UserService/ListUsers"); ok {
		t.Fatal("Enforce(after reload) = true, want false (cleared)")
	}
}

// TestAuthorizer_StaleServe 重建失败继续服务旧快照（不放大存储抖动）；
// 显式 Reload 失败则如实返回错误（调用方决定是否阻断写路径）。
func TestAuthorizer_StaleServe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "ops", Operation: "/system.v1.UserService/ListUsers"},
	}}
	a := newTestAuthorizer(t, src, time.Minute)

	src.fail = true
	a.loadedAt.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	if ok, err := a.Enforce(ctx, "ops", "/system.v1.UserService/ListUsers"); err != nil || !ok {
		t.Fatalf("Enforce(stale-serve) = %v, %v; want true, nil (old snapshot)", ok, err)
	}
	if err := a.Reload(ctx); err == nil {
		t.Fatal("Reload(source failure) error = nil, want non-nil")
	}
}

// TestAuthorizer_NewValidation 构造校验：TTL 越界（<1s 或 >10m）与策略源
// 初始加载失败均构造失败（禁止空策略静默上线）。
func TestAuthorizer_NewValidation(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	src := &policyStoreStub{}

	for _, ttl := range []time.Duration{10 * time.Millisecond, time.Hour} {
		_, err := NewAuthorizer(src, &conf.Security{
			Authz: &conf.Security_Authz{PolicyTtl: durationpb.New(ttl)},
		}, logger)
		if err == nil {
			t.Fatalf("NewAuthorizer(ttl=%s) error = nil, want non-nil", ttl)
		}
	}

	src.fail = true
	if _, err := NewAuthorizer(src, &conf.Security{
		Authz: &conf.Security_Authz{PolicyTtl: durationpb.New(time.Minute)},
	}, logger); err == nil {
		t.Fatal("NewAuthorizer(source failure) error = nil, want non-nil")
	}
}

// TestAuthorizer_OperationsForRoles 角色集合的操作并集：跨角色去重、
// 字典序、未知角色自然空集；返回全新 slice 不泄漏快照内部引用。
func TestAuthorizer_OperationsForRoles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "user_viewer", Operation: "/system.v1.UserService/ListUsers"},
		{RoleCode: "ops", Operation: "/system.v1.UserService/ListUsers"}, // 跨角色重复，应去重
		{RoleCode: "ops", Operation: "/system.v1.RoleService/ListRoles"},
	}}
	a := newTestAuthorizer(t, src, time.Minute)

	got, err := a.OperationsForRoles(ctx, []string{"ops", "user_viewer"})
	if err != nil {
		t.Fatalf("OperationsForRoles() error = %v", err)
	}
	want := []string{"/system.v1.RoleService/ListRoles", "/system.v1.UserService/ListUsers"}
	if !slices.Equal(got, want) {
		t.Fatalf("OperationsForRoles() = %v, want %v", got, want)
	}

	// 污染返回值不影响后续读取（不共享内部引用）。
	got[0] = "polluted"
	again, err := a.OperationsForRoles(ctx, []string{"ops"})
	if err != nil {
		t.Fatalf("OperationsForRoles(re-read) error = %v", err)
	}
	if !slices.Equal(again, want) {
		t.Fatalf("OperationsForRoles(re-read) = %v, want %v", again, want)
	}

	// 未知角色与空角色集均为空集。
	if got, err := a.OperationsForRoles(ctx, []string{"ghost"}); err != nil || len(got) != 0 {
		t.Fatalf("OperationsForRoles(unknown role) = %v, %v; want empty, nil", got, err)
	}
	if got, err := a.OperationsForRoles(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("OperationsForRoles(no roles) = %v, %v; want empty, nil", got, err)
	}
}

// BenchmarkAuthorizer_Enforce 快照只读判定路径的基准（无锁读）。
func BenchmarkAuthorizer_Enforce(b *testing.B) {
	src := &policyStoreStub{policies: []PermissionPolicy{
		{RoleCode: "rolea", Operation: "/system.v1.UserService/ListUsers"},
	}}
	a, err := NewAuthorizer(src, &conf.Security{
		Authz: &conf.Security_Authz{PolicyTtl: durationpb.New(time.Minute)},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		b.Fatalf("NewAuthorizer() error = %v", err)
	}
	ctx := context.Background()
	for b.Loop() {
		_, _ = a.Enforce(ctx, "rolea", "/system.v1.UserService/ListUsers")
	}
}
