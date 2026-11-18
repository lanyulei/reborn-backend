//go:build integration

package data

import (
	"context"
	"fmt"
	"slices"
	"testing"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizsystem "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	dataoncall "github.com/lanyulei/reborn/backend/core/internal/data/oncall"
	datasystem "github.com/lanyulei/reborn/backend/core/internal/data/system"
)

// 组合根里把值班 / 组织架构域适配成监控域窄接口的适配器（internal/biz/biz.go），接到真实的表上验证：
// 通知对象候选项（X6）与值班表名称查询（C14：少量 ID 时逐个读取，不再把全部值班表读出来）。
func TestMonitorAdapters_OnRealTables(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, `TRUNCATE TABLE oncall_change_log, oncall_share_link, oncall_shift, oncall_schedule_member,
		oncall_schedule, oncall_group_member, oncall_group, system_dept_leader, system_user_dept, system_dept,
		system_refresh_token, system_user_role, "system_user"`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// 值班组与值班表：启用 6 张（分属两个组）、停用 1 张、已删除 1 张。
	groupOps, groupDB := newID(t), newID(t)
	exec("INSERT INTO oncall_group (id, name) VALUES ($1, '运维组'), ($2, 'DBA 组')", groupOps, groupDB)
	schedule := func(group, name string, status int, deleted bool) string {
		id := newID(t)
		exec(`INSERT INTO oncall_schedule (id, group_id, name, timezone, rotation_unit, rotation_interval, anchor_time, status, delete_time)
		      VALUES ($1, $2, $3, 'Asia/Shanghai', 1, 1, now(), $4, CASE WHEN $5 THEN now() END)`, id, group, name, status, deleted)
		return id
	}
	enabled := make([]string, 0, 6)
	for i := range 5 {
		enabled = append(enabled, schedule(groupOps, fmt.Sprintf("运维-%d", i), 1, false))
	}
	enabled = append(enabled, schedule(groupDB, "数据库值班", 1, false))
	disabled := schedule(groupOps, "已停用", 2, false)
	deleted := schedule(groupOps, "已删除", 1, true)
	// 部门：技术部 → 运维部；一个已删除的部门。
	deptTech, deptOps, deptGone := newID(t), newID(t), newID(t)
	exec(`INSERT INTO system_dept (id, parent_id, path, name, delete_time) VALUES
	        ($1::uuid, NULL, '/' || $1::text || '/', '技术部', NULL),
	        ($2::uuid, $1::uuid, '/' || $1::text || '/' || $2::text || '/', '运维部', NULL),
	        ($3::uuid, NULL, '/' || $3::text || '/', '已撤销', now())`, deptTech, deptOps, deptGone)
	// 用户：两个可用、一个停用、一个已删除；另加 60 个可用用户验证上限。
	exec(`INSERT INTO "system_user" (id, username, password, nickname, phone, email, status, delete_time) VALUES
	        ($1, 'alice', 'x', 'Alice 王', '13800000001', 'alice@example.com', 1, NULL),
	        ($2, 'bob', 'x', '', '13800000002', NULL, 1, NULL),
	        ($3, 'alina', 'x', '已停用', '13800000003', NULL, 2, NULL),
	        ($4, 'alfred', 'x', '已删除', '13800000004', NULL, 1, now())`, uAlice, uBob, uCarol, newID(t))
	exec(`INSERT INTO "system_user" (id, username, password, phone, status)
	      SELECT gen_random_uuid(), 'crowd' || i, 'x', '139' || lpad(i::text, 8, '0'), 1 FROM generate_series(1, 60) AS i`)

	schedules := bizoncall.NewScheduleUsecase(dataoncall.NewStore(e.d), dataoncall.NewGroupRepo(e.d), dataoncall.NewScheduleRepo(e.d), dataoncall.NewShiftRepo(e.d), nil)
	users, depts := datasystem.NewUserRepo(e.d), datasystem.NewDeptRepo(e.d)
	dir := bizsystem.NewDirectoryUsecase(datasystem.NewDirectoryRepo(e.d), users, depts)
	targets := biz.NewTargetUsecase(topbiz.NewMonitorTargets(schedules, bizsystem.NewDeptUsecase(depts, depts, depts, depts, users), dir))

	all, err := targets.Options(ctx, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// 只有启用且未删除的值班表，带值班组名称。
	var gotSchedules []string
	for _, s := range all.Schedules {
		gotSchedules = append(gotSchedules, s.ID)
		if (s.GroupName != "运维组" && s.GroupName != "DBA 组") || s.Name == "" {
			t.Errorf("schedule option = %+v", s)
		}
	}
	slices.Sort(gotSchedules)
	want := slices.Clone(enabled)
	slices.Sort(want)
	if !slices.Equal(gotSchedules, want) {
		t.Fatalf("schedule options = %v, want the six enabled schedules", gotSchedules)
	}
	// 部门：未删除的，上级在前，带上级 ID。
	if len(all.Depts) != 2 || all.Depts[0] != (biz.TargetDept{ID: deptTech, Name: "技术部"}) || all.Depts[1] != (biz.TargetDept{ID: deptOps, Name: "运维部", ParentID: deptTech}) {
		t.Fatalf("dept options = %+v", all.Depts)
	}
	// 用户：至多 50 个；只有 ID、登录名、昵称。
	if len(all.Users) != 50 {
		t.Fatalf("user options = %d, want the cap of 50", len(all.Users))
	}
	// 关键字匹配登录名 / 昵称，只返回可用且未删除的；不按邮箱、手机号匹配（最小披露）。
	byKeyword := func(keyword string) []biz.TargetUser {
		t.Helper()
		got, err := targets.Options(ctx, keyword, []string{biz.TargetKindUser})
		if err != nil || got.Schedules != nil || got.Depts != nil {
			t.Fatalf("options(%q) = %+v err=%v", keyword, got, err)
		}
		return got.Users
	}
	if got := byKeyword("al"); len(got) != 1 || got[0] != (biz.TargetUser{ID: uAlice, Username: "alice", Nickname: "Alice 王"}) {
		t.Fatalf(`users matching "al" = %+v, want only the active alice`, got)
	}
	if got := byKeyword("王"); len(got) != 1 || got[0].ID != uAlice {
		t.Fatalf("users matching a nickname = %+v", got)
	}
	if got := byKeyword("bob"); len(got) != 1 || got[0] != (biz.TargetUser{ID: uBob, Username: "bob"}) {
		t.Fatalf("user without a nickname = %+v", got)
	}
	for _, private := range []string{"example.com", "13800000001"} {
		if got := byKeyword(private); len(got) != 0 {
			t.Fatalf("users must not be searchable by %q: %+v", private, got)
		}
	}

	// 值班表名称：少量 ID 逐个读取、多个 ID 一次读全部，两条路结果一致——停用、已删除、不存在的都不在其中。
	names := topbiz.NewMonitorSchedules(schedules)
	few, err := names.ScheduleNames(ctx, []string{enabled[5], disabled, deleted, newID(t)})
	if err != nil || len(few) != 1 || few[enabled[5]] != "数据库值班" {
		t.Fatalf("ScheduleNames (per-ID path) = %v err=%v", few, err)
	}
	many, err := names.ScheduleNames(ctx, append(slices.Clone(enabled), disabled, deleted, newID(t)))
	if err != nil || len(many) != 6 || many[enabled[5]] != "数据库值班" || many[enabled[0]] != "运维-0" {
		t.Fatalf("ScheduleNames (list path) = %v err=%v", many, err)
	}
	if got, err := names.ScheduleNames(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("ScheduleNames(nil) = %v err=%v", got, err)
	}
}
