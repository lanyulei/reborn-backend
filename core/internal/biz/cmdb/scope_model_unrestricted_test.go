package biz

import (
	"context"
	"testing"
)

// 类型与环境是两个独立维度：「类型不限 + 环境受限」只放开类型，环境限制必须保留。此前这种范围行
// 被直接判成完全不限，配成「全部类型 + 仅生产」的角色能看到并修改所有环境的资源。
func TestScopeUsecase_ModelUnrestrictedKeepsEnvRestriction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &scopeFakes{roles: []string{"r1", "r2"}, scopes: map[string]*RoleScope{
		"r1": {RoleID: "r1", ModelCodes: []string{}, Envs: []string{"prod"}},
	}}
	uc := NewScopeUsecase(f, f, f, nil)

	scope, err := uc.Resolve(ctx, "u")
	if err != nil {
		t.Fatal(err)
	}
	if scope.Unrestricted || !scope.ModelUnrestricted || scope.EnvUnrestricted {
		t.Fatalf("all-models + prod-only must not be unrestricted, got %+v", scope)
	}
	if !scope.Allows("anything", "prod") || scope.Allows("anything", "dev") || !scope.AllowsModel("whatever") || scope.AllowsEnv("dev") {
		t.Fatalf("models must be open while envs stay restricted, got %+v", scope)
	}

	// 多角色并集：A（host, prod）+ B（类型不限, dev）= 全部类型 + {prod, dev}。
	f.scopes["r2"] = &RoleScope{RoleID: "r2", ModelCodes: []string{}, Envs: []string{"dev"}}
	if scope, _ = uc.Resolve(ctx, "u"); scope.Unrestricted || !scope.ModelUnrestricted || scope.EnvUnrestricted ||
		!scope.Allows("vm", "prod") || !scope.Allows("vm", "dev") || scope.Allows("vm", "test") {
		t.Fatalf("scope must union into all models + {prod, dev}, got %+v", scope)
	}

	// 类型不限且环境不限（r1 改为环境为空）才是完全不限。
	f.scopes["r1"] = &RoleScope{RoleID: "r1", ModelCodes: []string{}}
	delete(f.scopes, "r2")
	if scope, _ = uc.Resolve(ctx, "u"); !scope.Unrestricted {
		t.Fatalf("all-models + all-envs must be unrestricted, got %+v", scope)
	}
}
