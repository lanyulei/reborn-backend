//go:build integration

package data

import (
	"context"
	"slices"
	"testing"
	"time"

	topbiz "github.com/lanyulei/reborn/backend/core/internal/biz"
	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	bizoncall "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	datacmdb "github.com/lanyulei/reborn/backend/core/internal/data/cmdb"
	dataoncall "github.com/lanyulei/reborn/backend/core/internal/data/oncall"
	dataworkflow "github.com/lanyulei/reborn/backend/core/internal/data/workflow"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// cmdbRoles cmdb 域 RoleSource 的桩：谁是超管、谁有哪些角色、谁持有「查询资源详情」接口权限。
type cmdbRoles struct {
	admins  []string
	roles   map[string][]string
	viewers []string
}

func (c cmdbRoles) EnabledRoleIDs(_ context.Context, userID string) (bool, []string, error) {
	if slices.Contains(c.admins, userID) {
		return true, nil, nil
	}
	return false, c.roles[userID], nil
}

func (c cmdbRoles) Allowed(_ context.Context, userID, _ string) (bool, error) {
	return slices.Contains(c.admins, userID) || slices.Contains(c.viewers, userID), nil
}

// 第二、三期的组合根适配器（internal/biz/monitor_p2.go）接到 cmdb / oncall / workflow 的真实表上验证：
// 资源索引的同步来源、资源卡片与影响面（按调用方的 CMDB 权限）、模型候选项、值班分组与主备值班人、班次、
// 工作日历来源。
func TestMonitorP2Adapters_OnRealTables(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := e.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`TRUNCATE TABLE cmdb_relation, cmdb_ci CASCADE`)
	exec(`TRUNCATE TABLE cmdb_role_scope`)
	exec(`TRUNCATE TABLE oncall_change_log, oncall_share_link, oncall_shift, oncall_schedule_member, oncall_schedule,
		oncall_group_member, oncall_group, system_refresh_token, system_user_role, "system_user" CASCADE`)

	// --- CMDB：物理机 → 两台虚拟机（host_runs_vm，上游 = src）；虚拟机 vm1 上联交换机（uplink，上游 = dst）；
	// 交换机再上联核心交换机。另有一台已退役、一台在回收站。
	t0 := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	ci := func(model, name, env, status string, at time.Time, deleted bool, attrs string) string {
		id := newID(t)
		exec(`INSERT INTO cmdb_ci (id, model_code, name, status, env, owner, tags, attrs, unique_key, create_time, update_time, delete_time)
		      VALUES ($1::uuid, $2, $3, $4, $5, $6, ARRAY['core'], $7::jsonb, $1::text, $8::timestamptz, $8::timestamptz, CASE WHEN $9 THEN $8::timestamptz END)`,
			id, model, name, status, env, uAlice, attrs, at, deleted)
		return id
	}
	host := ci("physical_server", "host-01", "prod", "active", t0, false, `{"ip":"10.0.0.1"}`)
	vm1 := ci("virtual_machine", "vm-01", "prod", "maintenance", t0.Add(time.Second), false, `{"ip":"10.0.1.1"}`)
	vm2 := ci("virtual_machine", "vm-02", "test", "active", t0.Add(2*time.Second), false, `{}`)
	sw := ci("network_device", "sw-01", "prod", "active", t0.Add(3*time.Second), false, `{}`)
	core := ci("network_device", "core-01", "prod", "active", t0.Add(4*time.Second), false, `{}`)
	retired := ci("physical_server", "host-old", "prod", "retired", t0.Add(5*time.Second), false, `{}`)
	trashed := ci("physical_server", "host-trash", "prod", "active", t0.Add(6*time.Second), true, `{}`)
	rel := func(typ, src, dst string) {
		exec("INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) VALUES ($1, $2, $3, $4)", newID(t), typ, src, dst)
	}
	rel("host_runs_vm", host, vm1)
	rel("host_runs_vm", host, vm2)
	rel("uplink", vm1, sw)
	rel("uplink", sw, core)
	rel("uplink", host, sw)

	models, fields, relTypes := datacmdb.NewModelRepo(e.d), datacmdb.NewFieldRepo(e.d), datacmdb.NewRelationTypeRepo(e.d)
	cis, relations := datacmdb.NewCIRepo(e.d), datacmdb.NewRelationRepo(e.d)
	relationUC := bizcmdb.NewRelationUsecase(relTypes, cis, relations, fields)
	ciUC := bizcmdb.NewCIUsecase(models, fields, relTypes, cis, relations, datacmdb.NewHistoryRepo(e.d), relationUC, cis)

	// 同步来源：在册 / 回收站分开，按更新时间自新到旧，带游标。
	src, err := topbiz.NewMonitorCISource(ciUC, relationUC, nil)
	if err != nil {
		t.Fatal(err)
	}
	page, next, err := src.Page(ctx, false, "", 4)
	if err != nil || len(page) != 4 || next == "" || page[0].ID != retired || page[3].ID != vm2 {
		t.Fatalf("Page(live, 4) = %d items, next %q, %v", len(page), next, err)
	}
	if page[1].ID != core || page[1].ModelCode != "network_device" || page[1].Env != "prod" || page[1].Owner != uAlice ||
		!slices.Equal(page[1].Tags, []string{"core"}) || !page[1].UpdateTime.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("source ci = %+v", page[1])
	}
	rest, next2, err := src.Page(ctx, false, next, 4)
	if err != nil || len(rest) != 2 || next2 != "" || rest[0].ID != vm1 || rest[1].ID != host || rest[1].Attrs["ip"] != "10.0.0.1" {
		t.Fatalf("Page(live, next) = %+v, next %q, %v", rest, next2, err)
	}
	bin, _, err := src.Page(ctx, true, "", 100)
	if err != nil || len(bin) != 1 || bin[0].ID != trashed {
		t.Fatalf("Page(deleted) = %+v, %v", bin, err)
	}
	direct, complete, err := src.Upstreams(ctx)
	if err != nil || !complete {
		t.Fatalf("Upstreams() complete = %v, %v", complete, err)
	}
	sortAll := func(m map[string][]string) {
		for _, v := range m {
			slices.Sort(v)
		}
	}
	sortAll(direct)
	wantDirect := map[string][]string{vm1: {host, sw}, vm2: {host}, sw: {core}, host: {sw}}
	sortAll(wantDirect)
	if len(direct) != len(wantDirect) {
		t.Fatalf("Upstreams() = %v, want %v", direct, wantDirect)
	}
	for id, ups := range wantDirect {
		if !slices.Equal(direct[id], ups) {
			t.Fatalf("Upstreams()[%s] = %v, want %v", id, direct[id], ups)
		}
	}

	// 端到端：同步用例经适配器把真实的 CMDB 表物化进索引。
	uc, err := biz.NewCISyncUsecase(src, NewCIIndexRepo(e.d), quietLogger(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Run(ctx); err != nil {
		t.Fatalf("ci sync over real cmdb tables: %v", err)
	}
	briefs, err := NewCIIndexRepo(e.d).Briefs(ctx, []string{host, vm1, vm2, sw, core, retired, trashed})
	if err != nil || len(briefs) != 6 {
		t.Fatalf("index after sync = %d rows, %v; want 6 (the trashed one was never indexed)", len(briefs), err)
	}
	if b := briefs[vm1]; !b.Valid || b.Status != mc.CIStatusMaintenance || !slices.Equal(b.Attrs["ip"], []string{"10.0.1.1"}) || len(b.UpstreamIDs) != 3 {
		t.Fatalf("vm1 in index = %+v, want upstreams host, sw, core", b)
	}
	if briefs[retired].Valid || len(briefs[core].UpstreamIDs) != 0 {
		t.Fatalf("retired = %+v, core = %+v", briefs[retired], briefs[core])
	}
	// 资源进回收站：下一轮增量把它置失效。
	exec("UPDATE cmdb_ci SET delete_time = now(), update_time = now() WHERE id = $1", vm2)
	if _, err := uc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if b, _ := NewCIIndexRepo(e.d).Briefs(ctx, []string{vm2}); b[vm2] == nil || b[vm2].Valid {
		t.Fatalf("vm2 after deletion = %+v", b[vm2])
	}
	exec("UPDATE cmdb_ci SET delete_time = NULL, update_time = now() WHERE id = $1", vm2)

	// 资源卡片与影响面：按调用方的 CMDB 接口权限与数据范围。bob 的角色只能看 prod 的物理机与网络设备；
	// carol 有范围但没有「查询资源详情」接口权限。
	exec("INSERT INTO cmdb_role_scope (role_id, model_codes, envs) VALUES ($1, ARRAY['physical_server','network_device'], ARRAY['prod'])", roleB)
	roles := cmdbRoles{admins: []string{uAdmin}, roles: map[string][]string{uBob: {roleB}, uCarol: {roleB}}, viewers: []string{uBob}}
	scopes := bizcmdb.NewScopeUsecase(datacmdb.NewScopeRepo(e.d), roles, nil, models)
	viewer, err := topbiz.NewMonitorCMDBViewer(scopes, ciUC, relationUC, nil)
	if err != nil {
		t.Fatal(err)
	}
	card, exists, err := viewer.Card(ctx, uAdmin, vm1)
	if err != nil || !exists || card == nil || card.Name != "vm-01" || card.ModelCode != "virtual_machine" || card.ModelName == "" ||
		card.Env != "prod" || card.Status != "maintenance" || card.Owner != uAlice || !slices.Equal(card.Tags, []string{"core"}) {
		t.Fatalf("Card(admin) = %+v, %v, %v", card, exists, err)
	}
	if card, exists, err := viewer.Card(ctx, uBob, host); err != nil || !exists || card == nil {
		t.Fatalf("Card(bob, in scope) = %+v, %v, %v", card, exists, err)
	}
	if card, exists, err := viewer.Card(ctx, uBob, vm1); err != nil || !exists || card != nil {
		t.Fatalf("Card(bob, model out of scope) = %+v, %v, %v; want no card", card, exists, err)
	}
	if card, exists, err := viewer.Card(ctx, uCarol, host); err != nil || !exists || card != nil {
		t.Fatalf("Card(carol, no cmdb permission) = %+v, %v, %v; want no card", card, exists, err)
	}
	if card, exists, err := viewer.Card(ctx, uAdmin, trashed); err != nil || exists || card != nil {
		t.Fatalf("Card(deleted ci) = %+v, %v, %v", card, exists, err)
	}
	// 核心交换机的影响面：sw（1 层）→ host、vm1（2 层）→ vm2（3 层，经 host 运行）。uplink 的上游在 dst 一侧，
	// host_runs_vm 的上游在 src 一侧：两种方向都要走对。
	impact, truncated, err := viewer.Impact(ctx, uAdmin, core, 3, 50)
	if err != nil || truncated || len(impact) != 4 {
		t.Fatalf("Impact(admin, core) = %+v, %v, %v", impact, truncated, err)
	}
	depth := map[string]int32{}
	for _, n := range impact {
		depth[n.ID] = n.Depth
		if !n.CanView || n.ModelName == "" {
			t.Fatalf("admin must be able to view %+v", n)
		}
	}
	if depth[sw] != 1 || depth[host] != 2 || depth[vm1] != 2 || depth[vm2] != 3 {
		t.Fatalf("impact depths = %v", depth)
	}
	// 上游不在影响面里（核心交换机本身、以及 sw 往上的方向）。
	if up, _, _ := viewer.Impact(ctx, uAdmin, vm1, 3, 50); len(up) != 0 {
		t.Fatalf("Impact(vm1) = %+v, want none: a leaf has no downstream", up)
	}
	// bob：范围外的节点只有名称，没有模型。
	impact, _, err = viewer.Impact(ctx, uBob, core, 3, 50)
	if err != nil || len(impact) != 4 {
		t.Fatalf("Impact(bob) = %+v, %v", impact, err)
	}
	for _, n := range impact {
		inScope := n.ID == sw || n.ID == host
		if n.CanView != inScope || (n.ModelCode != "") != inScope || n.Name == "" {
			t.Fatalf("Impact(bob) node = %+v, want can_view = %v", n, inScope)
		}
	}
	// 截断与层数。
	if cut, truncated, _ := viewer.Impact(ctx, uAdmin, core, 3, 2); len(cut) != 2 || !truncated {
		t.Fatalf("Impact(limit 2) = %d, truncated %v", len(cut), truncated)
	}
	if one, _, _ := viewer.Impact(ctx, uAdmin, core, 1, 50); len(one) != 1 || one[0].ID != sw {
		t.Fatalf("Impact(depth 1) = %+v", one)
	}
	if none, _, err := viewer.Impact(ctx, uAdmin, newID(t), 3, 50); err != nil || len(none) != 0 {
		t.Fatalf("Impact(missing ci) = %+v, %v", none, err)
	}

	// 模型候选项：启用的模型，按名称排序。
	modelUC := bizcmdb.NewModelUsecase(models, fields, relTypes, relations, nil, nil, nil, nil, nil)
	got, err := topbiz.NewMonitorCMDBCatalog(modelUC).Models(ctx)
	if err != nil || len(got) < 5 || !slices.ContainsFunc(got, func(m biz.CIModel) bool { return m.Code == "physical_server" && m.Name != "" }) {
		t.Fatalf("Models() = %+v, %v", got, err)
	}
	if !slices.IsSortedFunc(got, func(a, b biz.CIModel) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		}
		return 0
	}) {
		t.Fatalf("Models() must be sorted by name: %+v", got)
	}

	// --- 值班：分组管理员（只含可用用户）、主 / 备值班人、班次。
	exec(`INSERT INTO "system_user" (id, username, password, nickname, phone, status) VALUES
	        ($1, 'alice', 'x', 'Alice', '13800000001', 1), ($2, 'bob', 'x', 'Bob', '13800000002', 1), ($3, 'carol', 'x', 'Carol', '13800000003', 2)`,
		uAlice, uBob, uCarol)
	groupOps, groupDB := newID(t), newID(t)
	exec("INSERT INTO oncall_group (id, name) VALUES ($1, '运维组'), ($2, 'DBA 组')", groupOps, groupDB)
	exec(`INSERT INTO oncall_group_member (id, group_id, user_id, role) VALUES
	        ($1, $4, $5, 1), ($2, $4, $6, 2), ($3, $4, $7, 1)`, newID(t), newID(t), newID(t), groupOps, uAlice, uBob, uCarol)
	sched, off := newID(t), newID(t)
	exec(`INSERT INTO oncall_schedule (id, group_id, name, timezone, rotation_unit, rotation_interval, anchor_time, status) VALUES
	        ($1, $3, '运维值班', 'Asia/Shanghai', 1, 1, now(), 1), ($2, $3, '已停用', 'Asia/Shanghai', 1, 1, now(), 2)`, sched, off, groupOps)
	now := time.Now()
	shift := func(schedule string, role int, user string, from, to time.Time) {
		exec(`INSERT INTO oncall_shift (id, schedule_id, group_id, role, user_id, start_time, end_time) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			newID(t), schedule, groupOps, role, user, from, to)
	}
	shift(sched, 1, uAlice, now.Add(-time.Hour), now.Add(time.Hour))
	shift(sched, 2, uBob, now.Add(-time.Hour), now.Add(time.Hour))
	shift(sched, 1, uBob, now.Add(time.Hour), now.Add(3*time.Hour))
	shift(off, 1, uAlice, now.Add(-time.Hour), now.Add(time.Hour))

	store, groupRepo := dataoncall.NewStore(e.d), dataoncall.NewGroupRepo(e.d)
	groupUC := bizoncall.NewGroupUsecase(store, groupRepo, nil)
	scheduleUC := bizoncall.NewScheduleUsecase(store, groupRepo, dataoncall.NewScheduleRepo(e.d), dataoncall.NewShiftRepo(e.d), nil)
	oncallGroups := topbiz.NewMonitorOncallGroups(groupUC)
	groups, err := oncallGroups.Groups(ctx)
	if err != nil || len(groups) != 2 || groups[0] != (biz.OncallGroup{ID: groupDB, Name: "DBA 组"}) || groups[1] != (biz.OncallGroup{ID: groupOps, Name: "运维组"}) {
		t.Fatalf("Groups() = %+v, %v", groups, err)
	}
	if admins, err := oncallGroups.AdminIDs(ctx, groupOps); err != nil || !slices.Equal(admins, []string{uAlice}) {
		t.Fatalf("AdminIDs() = %v, %v; want only the active admin", admins, err)
	}
	if admins, err := oncallGroups.AdminIDs(ctx, newID(t)); err != nil || len(admins) != 0 {
		t.Fatalf("AdminIDs(missing group) = %v, %v", admins, err)
	}
	roster := topbiz.NewMonitorDutyRoster(scheduleUC)
	if primary, backup, err := roster.OnDutyByRole(ctx, sched); err != nil || !slices.Equal(primary, []string{uAlice}) || !slices.Equal(backup, []string{uBob}) {
		t.Fatalf("OnDutyByRole() = %v / %v, %v", primary, backup, err)
	}
	for _, id := range []string{off, newID(t)} {
		if primary, backup, err := roster.OnDutyByRole(ctx, id); err != nil || len(primary)+len(backup) != 0 {
			t.Fatalf("OnDutyByRole(disabled / missing) = %v / %v, %v", primary, backup, err)
		}
	}
	shifts := topbiz.NewMonitorShifts(scheduleUC)
	slots, exists, err := shifts.Shifts(ctx, sched, now.Add(-2*time.Hour), now.Add(4*time.Hour), 500)
	if err != nil || !exists || len(slots) != 3 {
		t.Fatalf("Shifts() = %+v, %v, %v", slots, exists, err)
	}
	if !slices.IsSortedFunc(slots, func(a, b biz.ShiftSlot) int { return a.Start.Compare(b.Start) }) || slots[2].UserID != uBob || slots[2].Role != 1 || slots[2].UserName == "" {
		t.Fatalf("Shifts() order / content = %+v", slots)
	}
	// 班次多于上限（3 个 > limit 2）：明确报错让调用方缩小时间范围，不再静默截断成前 2 个。
	if capped, _, err := shifts.Shifts(ctx, sched, now.Add(-2*time.Hour), now.Add(4*time.Hour), 2); len(capped) != 0 || !kerrors.Is(err, biz.ErrTimeRangeInvalid) {
		t.Fatalf("Shifts(limit 2) = %+v, %v; want an explicit too-many-shifts error", capped, err)
	}
	if _, exists, err := shifts.Shifts(ctx, newID(t), now, now.Add(time.Hour), 10); err != nil || exists {
		t.Fatalf("Shifts(missing schedule) exists = %v, %v", exists, err)
	}

	// --- 工作日历来源 → 快照循环 → monitor_calendar。
	cfgRepo, err := dataworkflow.NewConfigRepo(e.d, &conf.Notify{})
	if err != nil {
		t.Fatalf("NewConfigRepo() error = %v", err)
	}
	calUC := bizworkflow.NewCalendarUsecase(cfgRepo)
	calSrc := topbiz.NewMonitorCalendarSource(calUC)
	want := bizworkflow.CalendarConfig{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5},
		Segments: []bizworkflow.CalendarSegment{{Start: "09:00", End: "12:00"}, {Start: "13:00", End: "18:00"}}, Holidays: []string{"2026-10-01"},
	}
	if err := calUC.Save(ctx, want); err != nil {
		t.Fatalf("save workflow calendar: %v", err)
	}
	snap, ok, err := calSrc.WorkCalendar(ctx)
	if err != nil || !ok || snap.Timezone != "Asia/Shanghai" || len(snap.Segments) != 2 || snap.Workdays == nil {
		t.Fatalf("WorkCalendar() = %+v, %v, %v", snap, ok, err)
	}
	cal, err := snap.Compile()
	if err != nil {
		t.Fatalf("the adapted snapshot must compile under the contract: %v", err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	if !cal.Working(time.Date(2026, 11, 18, 10, 0, 0, 0, loc)) || cal.Working(time.Date(2026, 11, 18, 12, 30, 0, 0, loc)) || cal.Working(time.Date(2026, 10, 1, 10, 0, 0, 0, loc)) {
		t.Fatal("the adapted calendar must agree with the workflow calendar")
	}
	calSync := biz.NewCalendarSyncUsecase(calSrc, NewCalendarRepo(e.d), quietLogger())
	v0 := e.cfgVersion(mc.CfgCalendar)
	for range 3 {
		if _, err := calSync.Run(ctx); err != nil {
			t.Fatalf("calendar sync: %v", err)
		}
	}
	stored, err := NewCalendarRepo(e.d).Snapshot(ctx)
	if err != nil || stored.Timezone != "Asia/Shanghai" || e.cfgVersion(mc.CfgCalendar) != v0+1 {
		t.Fatalf("stored calendar = %+v, version = %d (want one bump over three runs), %v", stored, e.cfgVersion(mc.CfgCalendar)-v0, err)
	}
}
