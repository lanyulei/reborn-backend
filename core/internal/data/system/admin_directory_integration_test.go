//go:build integration

package data

import (
	"context"
	"slices"
	"testing"

	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
)

// 「系统管理员」审批人规则取的是全部可用的超管：启用、未删除、且 is_admin 为真；停用的、已删除的、
// 普通用户都不算。结果按 ID 排序。
func TestIntegration_ActiveSuperAdmins(t *testing.T) {
	d := newIntegrationData(t)
	ctx := context.Background()
	const (
		adminOn    = "018f3e1a-0002-7000-8000-000000000003"
		adminOn2   = "018f3e1a-0002-7000-8000-000000000001"
		adminOff   = "018f3e1a-0002-7000-8000-000000000002"
		adminGone  = "018f3e1a-0002-7000-8000-000000000004"
		plainUser  = "018f3e1a-0002-7000-8000-000000000005"
		hashMarker = "$argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHRzYWx0c2FsdA$c2FsdHNhbHRzYWx0c2FsdA"
	)
	for _, q := range []string{
		`INSERT INTO "system_user" (id, username, password, nickname, is_admin, status) VALUES
			('` + adminOn + `', 'root_a', '` + hashMarker + `', '超管甲', true, 1),
			('` + adminOn2 + `', 'root_b', '` + hashMarker + `', '超管乙', true, 1),
			('` + adminOff + `', 'root_off', '` + hashMarker + `', '停用的超管', true, 4),
			('` + plainUser + `', 'plain', '` + hashMarker + `', '普通用户', false, 1)`,
		`INSERT INTO "system_user" (id, username, password, nickname, is_admin, status, delete_time) VALUES
			('` + adminGone + `', 'root_gone', '` + hashMarker + `', '已删除的超管', true, 1, now())`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatalf("seed users: %v\n%s", err, q)
		}
	}

	got, err := NewDirectoryRepo(d).ListActiveAdminIDs(ctx)
	if err != nil {
		t.Fatalf("ListActiveAdminIDs() error = %v", err)
	}
	if want := []string{adminOn2, adminOn}; !slices.Equal(got, want) {
		t.Fatalf("active super admins = %v, want %v", got, want)
	}
	// 流程引擎用的端口就是它。
	if got, err = bizsystem.NewAdminDirectory(NewDirectoryRepo(d)).AdminUserIDs(ctx); err != nil || !slices.Equal(got, []string{adminOn2, adminOn}) {
		t.Fatalf("AdminUserIDs() = %v, %v", got, err)
	}
}
