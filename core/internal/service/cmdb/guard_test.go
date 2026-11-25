package service

import (
	"context"
	"errors"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	jwtx "github.com/go-kratos/kratos/contrib/middleware/jwt/v3"
	"github.com/golang-jwt/jwt/v5"
)

// countingRoles 记录范围解析次数的角色来源桩（每次 Resolve 恰好调用一次 EnabledRoleIDs）。
type countingRoles struct {
	biz.RoleSource
	roleIDs  []string
	err      error
	resolved int
}

func (r *countingRoles) EnabledRoleIDs(context.Context, string) (bool, []string, error) {
	r.resolved++
	return false, r.roleIDs, r.err
}

type fixedScopes struct {
	biz.ScopeRepo
	rows []*biz.RoleScope
}

func (s fixedScopes) ListByRoles(context.Context, []string) ([]*biz.RoleScope, error) {
	return s.rows, nil
}

type noRoles struct{}

func (noRoles) ListRoleRefs(context.Context) ([]string, []string, []string, error) {
	return nil, nil, nil, nil
}

func userCtx() context.Context {
	return jwtx.NewContext(context.Background(), &bizsystem.AccessClaims{RegisteredClaims: jwt.RegisteredClaims{Subject: "u1"}})
}

// requireCIScope 与 requireCI 判定一致（先读资源、再判范围），并且只解析一次范围、把它交还调用方复用。
func TestScopeGuard_RequireCIScope(t *testing.T) {
	t.Parallel()
	errResolve := errors.New("role lookup failed")
	get := func(ci *biz.CI, err error) func(context.Context, string) (*biz.CI, error) {
		return func(context.Context, string) (*biz.CI, error) { return ci, err }
	}
	hostProd, hostDev := &biz.CI{ID: "c1", ModelCode: "host", Env: "prod"}, &biz.CI{ID: "c2", ModelCode: "host", Env: "dev"}
	cases := []struct {
		name         string
		get          func(context.Context, string) (*biz.CI, error)
		resolveErr   error
		wantErr      error
		wantResolved int
	}{
		{"in scope", get(hostProd, nil), nil, nil, 1},
		{"out of scope", get(hostDev, nil), nil, biz.ErrScopeForbidden, 1},
		{"missing resource is reported before the scope is resolved", get(nil, biz.ErrCINotFound), errResolve, biz.ErrCINotFound, 0},
		{"resolve failure", get(hostProd, nil), errResolve, errResolve, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roles := &countingRoles{roleIDs: []string{"r1"}, err: tc.resolveErr}
			scopes := fixedScopes{rows: []*biz.RoleScope{{RoleID: "r1", ModelCodes: []string{"host"}, Envs: []string{"prod"}}}}
			g := newScopeGuard(biz.NewScopeUsecase(scopes, roles, noRoles{}, nil))
			ci, scope, err := g.requireCIScope(userCtx(), tc.get, "id")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if roles.resolved != tc.wantResolved {
				t.Fatalf("scope resolved %d times, want %d", roles.resolved, tc.wantResolved)
			}
			// requireCI 是同一判定的薄封装。
			ci2, err2 := g.requireCI(userCtx(), tc.get, "id")
			if !errors.Is(err2, tc.wantErr) || ci2 != ci {
				t.Fatalf("requireCI = (%v, %v), requireCIScope = (%v, %v)", ci2, err2, ci, err)
			}
			if tc.wantErr != nil {
				if ci != nil {
					t.Fatalf("ci must be nil on error, got %+v", ci)
				}
				return
			}
			// 返回的范围就是这次解析的结果，调用方据此判定其它资源。
			if scope.Unrestricted || !scope.Allows("host", "prod") || scope.Allows("host", "dev") || scope.Allows("app", "prod") {
				t.Fatalf("returned scope = %+v", scope)
			}
		})
	}
}
