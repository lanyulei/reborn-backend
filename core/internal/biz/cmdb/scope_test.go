package biz

import (
	"context"
	"testing"
)

type scopeFakes struct {
	admin  bool
	roles  []string
	scopes map[string]*RoleScope
	// dir 角色目录：ID → 标识。
	dir map[string]string
}

func (f *scopeFakes) EnabledRoleIDs(context.Context, string) (bool, []string, error) {
	return f.admin, f.roles, nil
}
func (f *scopeFakes) Allowed(context.Context, string, string) (bool, error) { return true, nil }

func (f *scopeFakes) ListByRoles(_ context.Context, roleIDs []string) ([]*RoleScope, error) {
	var out []*RoleScope
	for _, id := range roleIDs {
		if rs, ok := f.scopes[id]; ok {
			out = append(out, rs)
		}
	}
	return out, nil
}
func (f *scopeFakes) ListAll(context.Context) ([]*RoleScope, error) { return nil, nil }
func (f *scopeFakes) Upsert(context.Context, *RoleScope) error      { return nil }

func (f *scopeFakes) ListRoleRefs(context.Context) (ids, names, codes []string, err error) {
	for id, code := range f.dir {
		ids, names, codes = append(ids, id), append(names, code), append(codes, code)
	}
	return ids, names, codes, nil
}

// 范围解析 fail-closed：没有范围行的角色不贡献任何可见范围；只有显式配置为「类型为空」的范围行才是不限；
// 超管恒不限；k8s_self_ 专属角色即便配了范围行也不参与。
func TestScopeUsecase_ResolveFailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &scopeFakes{roles: []string{"r1", "r2"}, scopes: map[string]*RoleScope{}, dir: map[string]string{"r1": "ops", "r2": "dev", "self": "k8s_self_abc"}}
	uc := NewScopeUsecase(f, f, f, nil)

	// 两个启用角色都没有范围行：什么都看不到（旧行为是不限）。
	scope, err := uc.Resolve(ctx, "u")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Unrestricted || scope.EnvUnrestricted || len(scope.Models) != 0 || scope.Allows("host", "prod") {
		t.Fatalf("unconfigured roles must see nothing, got %+v", scope)
	}

	// 只有 r1 配置了范围：r2 没有行，不再把整体放开。
	f.scopes["r1"] = &RoleScope{RoleID: "r1", ModelCodes: []string{"host"}, Envs: []string{"prod"}}
	scope, _ = uc.Resolve(ctx, "u")
	if scope.Unrestricted || scope.EnvUnrestricted || !scope.Allows("host", "prod") || scope.Allows("host", "dev") || scope.Allows("db", "prod") {
		t.Fatalf("one configured role must yield exactly its scope, got %+v", scope)
	}

	// r2 的范围行环境为空 → 环境不限；类型取并集。
	f.scopes["r2"] = &RoleScope{RoleID: "r2", ModelCodes: []string{"db"}}
	scope, _ = uc.Resolve(ctx, "u")
	if scope.Unrestricted || !scope.EnvUnrestricted || !scope.Allows("db", "dev") || !scope.Allows("host", "dev") || scope.Allows("vm", "prod") {
		t.Fatalf("scopes must union across configured roles, got %+v", scope)
	}

	// 显式的「类型为空」范围行才是不限。
	f.scopes["r2"] = &RoleScope{RoleID: "r2", ModelCodes: []string{}}
	if scope, _ = uc.Resolve(ctx, "u"); !scope.Unrestricted {
		t.Fatalf("an explicit empty model list must mean unrestricted, got %+v", scope)
	}

	// k8s_self_ 专属角色上的范围行被忽略——即便它配成了不限。
	f.roles = []string{"r1", "self"}
	f.scopes["self"] = &RoleScope{RoleID: "self", ModelCodes: []string{}}
	scope, _ = uc.Resolve(ctx, "u")
	if scope.Unrestricted || !scope.Allows("host", "prod") || scope.Allows("db", "prod") {
		t.Fatalf("k8s_self_ roles must not take part in CMDB scope, got %+v", scope)
	}
	// 只有专属角色：什么都看不到。
	f.roles = []string{"self"}
	if scope, _ = uc.Resolve(ctx, "u"); scope.Unrestricted || scope.Allows("host", "prod") {
		t.Fatalf("a user with only a k8s_self_ role must see nothing, got %+v", scope)
	}

	// 超管恒不限；无启用角色为空。
	f.admin = true
	if scope, _ = uc.Resolve(ctx, "u"); !scope.Unrestricted {
		t.Fatal("admin must be unrestricted")
	}
	f.admin, f.roles = false, nil
	if scope, _ = uc.Resolve(ctx, "u"); scope.Unrestricted || scope.Allows("host", "prod") {
		t.Fatalf("no enabled roles must see nothing, got %+v", scope)
	}
}
