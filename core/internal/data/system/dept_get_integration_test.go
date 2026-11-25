//go:build integration

package data

import (
	"context"
	"testing"
)

// GetDept 与 ListDepts 对每个部门给出同样的成员数与字段：
// 有成员、无成员的部门都覆盖。
func TestIntegration_GetDeptMatchesList(t *testing.T) {
	d := newIntegrationData(t)
	repo := NewDeptRepo(d)
	users := seedUsers(t, NewUserRepo(d), 3)
	ctx := context.Background()
	root := newTestDept(1, "研发中心", "")
	if err := repo.CreateDept(ctx, root, nil); err != nil {
		t.Fatalf("CreateDept(root) error = %v", err)
	}
	child := newTestDept(2, "平台组", root.ID)
	if err := repo.CreateDept(ctx, child, []string{users[0]}); err != nil {
		t.Fatalf("CreateDept(child) error = %v", err)
	}
	empty := newTestDept(3, "空部门", root.ID)
	if err := repo.CreateDept(ctx, empty, nil); err != nil {
		t.Fatalf("CreateDept(empty) error = %v", err)
	}
	for i, u := range users {
		dept := child.ID
		if i == 2 {
			dept = root.ID
		}
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO system_user_dept (id, user_id, dept_id, is_primary) VALUES (gen_random_uuid(), $1::uuid, $2::uuid, true)`, u, dept); err != nil {
			t.Fatalf("seed user dept: %v", err)
		}
	}
	all, err := repo.ListDepts(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("ListDepts() = %d, %v", len(all), err)
	}
	want := map[string]int64{root.ID: 1, child.ID: 2, empty.ID: 0}
	for _, listed := range all {
		got, err := repo.GetDept(ctx, listed.ID)
		if err != nil {
			t.Fatalf("GetDept(%s) error = %v", listed.Name, err)
		}
		if int64(got.MemberCount) != want[listed.ID] || got.MemberCount != listed.MemberCount ||
			got.Name != listed.Name || got.Path != listed.Path || got.ParentID != listed.ParentID || len(got.Leaders) != len(listed.Leaders) {
			t.Fatalf("GetDept(%s) = %+v, listed %+v", listed.Name, got, listed)
		}
	}
}
