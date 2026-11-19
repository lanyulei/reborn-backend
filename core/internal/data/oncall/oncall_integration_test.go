//go:build integration

package data

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	datanotify "github.com/lanyulei/reborn/backend/core/internal/data/notify"
	"github.com/lanyulei/reborn/backend/kit/kitconf"
	"github.com/lanyulei/reborn/backend/kit/notifycontract"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 固定测试用户：admin 为分组创建人，a/b/c 为值班成员，gone 为已停用用户。
const (
	uAdmin = "018f3e1a-0c00-7000-8000-000000000001"
	uA     = "018f3e1a-0c00-7000-8000-00000000000a"
	uB     = "018f3e1a-0c00-7000-8000-00000000000b"
	uC     = "018f3e1a-0c00-7000-8000-00000000000c"
	uGone  = "018f3e1a-0c00-7000-8000-00000000000d"
	uOther = "018f3e1a-0c00-7000-8000-00000000000e"
)

type env struct {
	pool      *pgxpool.Pool
	groups    *biz.GroupUsecase
	schedules *biz.ScheduleUsecase
	shares    *biz.ShareUsecase
	worker    *biz.WorkerUsecase
	shiftRepo biz.ShiftRepo
	schedRepo biz.ScheduleRepo
	admin     biz.Actor
}

// newEnv 构造真实连接池上的全套用例；未配置 TEST_DATABASE_SOURCE 时跳过。
// 护栏同 system 域：库名不含 "test" 直接失败（本测试会 TRUNCATE 业务表）。
func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	if cfg, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	} else if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `TRUNCATE TABLE oncall_change_log, oncall_share_link, oncall_shift, oncall_schedule_member,
		oncall_schedule, oncall_group_member, oncall_group, notify_event, notify_dedup,
		system_permission, system_grant, system_refresh_token, system_user_role,
		system_dept_leader, system_user_dept, "system_user"`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
	for i, u := range []struct {
		id, name string
		status   int
	}{{uAdmin, "admin", 1}, {uA, "alice", 1}, {uB, "bob", 1}, {uC, "carol", 1}, {uGone, "gone", 4}, {uOther, "other", 1}} {
		if _, err := d.Pool.Exec(ctx,
			`INSERT INTO "system_user" (id, username, password, nickname, phone, status) VALUES ($1, $2, 'x', $3, $4, $5)`,
			u.id, u.name, "昵称-"+u.name, fmt.Sprintf("1380000000%d", i), u.status); err != nil {
			t.Fatalf("seed user %s: %v", u.name, err)
		}
	}
	store, groups, schedules, shifts, shares := NewStore(d), NewGroupRepo(d), NewScheduleRepo(d), NewShiftRepo(d), NewShareRepo(d)
	tokenBox, err := NewTokenBox(&conf.Notify{Master: &kitconf.Master{Key: "integration-test-master-key"}})
	if err != nil {
		t.Fatalf("NewTokenBox() error = %v", err)
	}
	notifier := biz.NewNotifier(biznotify.NewNotificationUsecase(datanotify.NewEventRepo(d), nil))
	return &env{
		pool:      d.Pool,
		groups:    biz.NewGroupUsecase(store, groups, groups),
		schedules: biz.NewScheduleUsecase(store, groups, schedules, shifts, notifier),
		shares:    biz.NewShareUsecase(store, groups, schedules, shifts, shares, groups, tokenBox),
		worker:    biz.NewWorkerUsecase(store, groups, schedules, shifts, notifier),
		shiftRepo: shifts,
		schedRepo: schedules,
		admin:     biz.Actor{UserID: uAdmin},
	}
}

func (e *env) group(t *testing.T, members ...string) *biz.Group {
	t.Helper()
	ctx := context.Background()
	g, err := e.groups.Create(ctx, e.admin, "运维组", "")
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	if len(members) > 0 {
		if _, err := e.groups.AddMembers(ctx, e.admin, g.ID, members, biz.MemberRoleMember); err != nil {
			t.Fatalf("add members: %v", err)
		}
	}
	return g
}

func dailyConfig(anchor time.Time, members ...string) biz.ScheduleConfig {
	return biz.ScheduleConfig{
		Name: "生产值班", Timezone: "Asia/Shanghai", RotationUnit: biz.UnitDay, RotationInterval: 1, AnchorTime: anchor,
		MemberIDs: members, BackupEnabled: true, HorizonDays: 30, RemindMinutes: []int32{60, 0},
	}
}

func (e *env) events(t *testing.T, eventType string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), "SELECT count(*) FROM notify_event WHERE event_type = $1", eventType).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

// assertTemplatesRender 守护迁移 0055 写入的缺省模板与事件载荷不脱节：把库里现有的每个
// 值班事件用对应模板按管道同一口径（text/template + 白名单函数 + missingkey=zero）渲染，
// 模板引用了载荷里不存在的字段会渲染出 "<no value>"，在此失败。want 为本次必须覆盖到的事件类型。
func (e *env) assertTemplatesRender(t *testing.T, want ...string) {
	t.Helper()
	ctx := context.Background()
	rows, err := e.pool.Query(ctx,
		`SELECT ev.event_type, ev.title, ev.payload, tp.subject, tp.body
		 FROM notify_event ev JOIN notify_template tp
		   ON tp.biz_id = ev.biz_id AND tp.event_type = ev.event_type AND tp.channel_type = '*' AND tp.delete_time IS NULL
		 WHERE ev.biz_id = $1 AND ev.event_type LIKE 'oncall.%'`, biz.NotifyBiz)
	if err != nil {
		t.Fatalf("load events with templates: %v", err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var eventType, title, subject, body string
		var raw []byte
		if err := rows.Scan(&eventType, &title, &raw, &subject, &body); err != nil {
			t.Fatalf("scan: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode %s payload: %v", eventType, err)
		}
		seen[eventType] = true
		for name, text := range map[string]string{"subject": subject, "body": body} {
			tpl, err := template.New(name).Funcs(notifycontract.TemplateFuncs()).Option("missingkey=zero").Parse(text)
			if err != nil {
				t.Fatalf("%s %s template does not parse: %v", eventType, name, err)
			}
			var out strings.Builder
			if err := tpl.Execute(&out, map[string]any{"Title": title, "Payload": payload}); err != nil {
				t.Fatalf("%s %s template does not render: %v", eventType, name, err)
			}
			if out.Len() == 0 || strings.Contains(out.String(), "<no value>") {
				t.Errorf("%s %s references a field missing from the payload:\n%s", eventType, name, out.String())
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, eventType := range want {
		if !seen[eventType] {
			t.Errorf("no %s event with a seeded template was rendered (missing event or missing template)", eventType)
		}
	}
}

// primaries 未来班次里主值班的 (值班人, 来源) 序列。
func (e *env) roleShifts(t *testing.T, scheduleID string, role int32, from, to time.Time) []*biz.Shift {
	t.Helper()
	all, err := e.shiftRepo.ListRange(context.Background(), from, to)
	if err != nil {
		t.Fatalf("list shifts: %v", err)
	}
	return slices.DeleteFunc(all, func(s *biz.Shift) bool { return s.ScheduleID != scheduleID || s.Role != role })
}

// assertContiguous 班次首尾相接、无重叠无空档。
func assertContiguous(t *testing.T, shifts []*biz.Shift) {
	t.Helper()
	for i := 1; i < len(shifts); i++ {
		if !shifts[i-1].End.Equal(shifts[i].Start) {
			t.Fatalf("shift %d ends %s but next starts %s", i-1, shifts[i-1].End, shifts[i].Start)
		}
	}
}

func TestIntegration_GroupMembership(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB)
	if g.MemberCount != 1 || g.MyRole != biz.MemberRoleAdmin {
		t.Fatalf("creator should be the only admin, got %+v", g)
	}
	if _, err := e.groups.Create(ctx, e.admin, "运维组", ""); !errors.Is(err, biz.ErrGroupNameExists) {
		t.Errorf("duplicate name error = %v", err)
	}
	// 非管理员修改被拒绝；系统超级管理员可以越过。
	member := biz.Actor{UserID: uA}
	if err := e.groups.Update(ctx, member, g.ID, "x", ""); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member update error = %v, want ErrForbidden", err)
	}
	if _, err := e.groups.AddMembers(ctx, biz.Actor{UserID: uOther}, g.ID, []string{uC}, biz.MemberRoleMember); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("outsider add error = %v, want ErrForbidden", err)
	}
	if _, err := e.groups.AddMembers(ctx, biz.Actor{UserID: uOther, IsAdmin: true}, g.ID, []string{uC}, biz.MemberRoleMember); err != nil {
		t.Errorf("superadmin add error = %v", err)
	}
	// 停用用户不能加入；重复添加跳过。
	if _, err := e.groups.AddMembers(ctx, e.admin, g.ID, []string{uGone}, biz.MemberRoleMember); !errors.Is(err, biz.ErrMemberInvalid) {
		t.Errorf("unavailable user error = %v", err)
	}
	if n, err := e.groups.AddMembers(ctx, e.admin, g.ID, []string{uA}, biz.MemberRoleMember); err != nil || n != 0 {
		t.Errorf("re-add = %d, %v", n, err)
	}
	// 最后一名管理员不能降级、不能移出。
	if err := e.groups.UpdateMemberRole(ctx, e.admin, g.ID, uAdmin, biz.MemberRoleMember); !errors.Is(err, biz.ErrLastAdmin) {
		t.Errorf("demote last admin error = %v", err)
	}
	if err := e.groups.RemoveMember(ctx, e.admin, g.ID, uAdmin); !errors.Is(err, biz.ErrLastAdmin) {
		t.Errorf("remove last admin error = %v", err)
	}
	// 在轮转名单内的成员不能移出；分组下有值班表不能删除。
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(time.Now().Add(-time.Hour), uA, uB))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	if err := e.groups.RemoveMember(ctx, e.admin, g.ID, uA); !errors.Is(err, biz.ErrMemberInUse) {
		t.Errorf("remove rotating member error = %v", err)
	}
	if err := e.groups.Delete(ctx, e.admin, g.ID); !errors.Is(err, biz.ErrGroupInUse) {
		t.Errorf("delete group with schedule error = %v", err)
	}
	if err := e.schedules.Delete(ctx, e.admin, s.ID); err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
	if err := e.groups.RemoveMember(ctx, e.admin, g.ID, uA); err != nil {
		t.Errorf("remove member after schedule deleted: %v", err)
	}
	if err := e.groups.Delete(ctx, e.admin, g.ID); err != nil {
		t.Errorf("delete group: %v", err)
	}
	logs, total, err := e.groups.ListChangeLogs(ctx, g.ID, "", 1, 50)
	if err != nil || total < 5 || logs[0].OperatorName == "" {
		t.Errorf("change logs total = %d, err = %v", total, err)
	}
}

func TestIntegration_GenerateAndReschedule(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	// 起点在 36 小时前：当前处于第 1 个周期（主 b 备 c），班次从"现在"开始、不产生过去的班次。
	anchor := now.Add(-36 * time.Hour)
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(anchor, uA, uB, uC))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	if s.GeneratedUntil == nil || s.GeneratedUntil.Before(now.AddDate(0, 0, 30)) {
		t.Fatalf("generated_until = %v", s.GeneratedUntil)
	}
	far := now.AddDate(0, 0, 40)
	primary := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far)
	backup := e.roleShifts(t, s.ID, biz.RoleBackup, now.Add(-time.Hour), far)
	if len(primary) < 30 || len(primary) != len(backup) {
		t.Fatalf("primary = %d, backup = %d", len(primary), len(backup))
	}
	assertContiguous(t, primary)
	if primary[0].Start.Before(now.Add(-5*time.Second)) || primary[0].UserID != uB || backup[0].UserID != uC || primary[1].UserID != uC {
		t.Errorf("first shifts: primary %s@%s, backup %s, next %s", primary[0].UserID, primary[0].Start, backup[0].UserID, primary[1].UserID)
	}
	if primary[0].UserName != "昵称-bob" || !primary[0].UserAvailable {
		t.Errorf("shift user projection = %q %v", primary[0].UserName, primary[0].UserAvailable)
	}
	// 新建值班表不发通知。
	if got := e.events(t, "oncall.schedule.published"); got != 0 {
		t.Errorf("published events = %d, want 0", got)
	}

	// 补排是幂等的：再跑不产生重复班次。
	if _, err := e.worker.Extend(ctx); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if again := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far); len(again) != len(primary) {
		t.Errorf("extend changed shift count %d → %d", len(primary), len(again))
	}

	// 替班后改名单重排：进行中的班次与替班保留，其余按新规则重排，仍然连续无重叠。
	ovStart, ovEnd := primary[3].Start, primary[3].End // 替一整天
	if _, err := e.schedules.CreateOverride(ctx, e.admin, biz.OverrideInput{ScheduleID: s.ID, Role: biz.RolePrimary, UserID: uA, Start: ovStart, End: ovEnd, Remark: "临时"}); err != nil {
		t.Fatalf("override: %v", err)
	}
	cfg := dailyConfig(primary[0].End, uC, uA) // 从当前班次结束时刻起，按新名单 c → a 轮转
	cfg.BackupEnabled = false
	if _, err := e.schedules.Update(ctx, e.admin, s.ID, cfg); err != nil {
		t.Fatalf("update schedule: %v", err)
	}
	after := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far)
	assertContiguous(t, after)
	if after[0].UserID != uB || !after[0].End.Equal(primary[0].End) {
		t.Errorf("in-progress shift changed: %+v", after[0])
	}
	if after[1].UserID != uC || after[2].UserID != uA {
		t.Errorf("new rotation = %s, %s, want c, a", after[1].UserID, after[2].UserID)
	}
	var kept *biz.Shift
	for _, sh := range after {
		if sh.Source == biz.SourceOverride {
			kept = sh
		}
	}
	if kept == nil || kept.UserID != uA || !kept.Start.Equal(ovStart) || !kept.End.Equal(ovEnd) {
		t.Errorf("override not preserved by reschedule: %+v", kept)
	}
	// 关闭备值班：未开始的备值班被清除，进行中的保留到结束。
	if b := e.roleShifts(t, s.ID, biz.RoleBackup, now.Add(-time.Hour), far); len(b) != 1 || !b[0].End.Equal(primary[0].End) {
		t.Errorf("backup shifts after disabling = %d", len(b))
	}

	// 停用：进行中的班次截断到此刻，未来班次清空；再启用按配置重新生成。
	if err := e.schedules.SetStatus(ctx, e.admin, s.ID, biz.StatusDisabled); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if left := e.roleShifts(t, s.ID, biz.RolePrimary, time.Now().Add(time.Minute), far); len(left) != 0 {
		t.Errorf("%d future shifts remain after disabling", len(left))
	}
	if err := e.schedules.SetStatus(ctx, e.admin, s.ID, biz.StatusEnabled); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if back := e.roleShifts(t, s.ID, biz.RolePrimary, time.Now().Add(time.Minute), far); len(back) < 29 {
		t.Errorf("only %d shifts regenerated after enabling", len(back))
	}
}

func TestIntegration_OverrideAndRevoke(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(now.Add(-time.Hour), uA, uB))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	far := now.AddDate(0, 0, 40)
	before := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far)
	target := before[2]                    // 第 3 班（主值班 a）
	start, end := target.Start, target.End // 以天为最小单位：替一整天

	// 权限与参数校验。
	in := biz.OverrideInput{ScheduleID: s.ID, Role: biz.RolePrimary, UserID: uC, Start: start, End: end, Remark: "看病"}
	if _, err := e.schedules.CreateOverride(ctx, biz.Actor{UserID: uA}, in); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member override error = %v", err)
	}
	bad := in
	bad.UserID = uOther
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("non-member substitute error = %v", err)
	}
	bad = in
	bad.Start, bad.End = start.Add(3*time.Hour), end
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("off-handover start error = %v", err)
	}
	bad = in
	bad.End = end.Add(-2 * time.Hour)
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("off-handover end error = %v", err)
	}
	bad = in
	bad.End = end.Add(90 * 24 * time.Hour)
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("beyond horizon error = %v", err)
	}
	bad = in
	bad.UserID = target.UserID
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("no-op override error = %v", err)
	}

	// 主备值班人不能是同一个人：当天的备值班不能再替主值班。
	backups := e.roleShifts(t, s.ID, biz.RoleBackup, start, end)
	if len(backups) != 1 || backups[0].UserID == target.UserID {
		t.Fatalf("backup of the target day = %+v", backups)
	}
	bad = in
	bad.UserID = backups[0].UserID
	if _, err := e.schedules.CreateOverride(ctx, e.admin, bad); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("same person as backup error = %v", err)
	}

	created, err := e.schedules.CreateOverride(ctx, e.admin, in)
	if err != nil || len(created) != 1 {
		t.Fatalf("override = %d, %v", len(created), err)
	}
	after := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far)
	assertContiguous(t, after)
	if len(after) != len(before) {
		t.Fatalf("shift count %d → %d, want unchanged (the whole day is replaced)", len(before), len(after))
	}
	head, ov := after[1], after[2]
	if ov.Source != biz.SourceOverride || ov.UserID != uC || ov.OriginUserID != target.UserID || ov.OriginUserName == "" ||
		ov.Remark != "看病" || !ov.Start.Equal(start) || !ov.End.Equal(end) {
		t.Errorf("override wrong: %+v", ov)
	}
	if got := e.events(t, biz.EventShiftChanged); got != 1 {
		t.Errorf("shift.changed events = %d", got)
	}

	// 撤销：恢复为自动排班，整体仍然连续，且该时间段回到原值班人。
	if err := e.schedules.RevokeOverride(ctx, e.admin, head.ID); !errors.Is(err, biz.ErrOverrideInvalid) {
		t.Errorf("revoke non-override error = %v", err)
	}
	if err := e.schedules.RevokeOverride(ctx, e.admin, ov.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	restored := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), far)
	assertContiguous(t, restored)
	for _, sh := range restored {
		if sh.Source != biz.SourceAuto {
			t.Errorf("override still present: %+v", sh)
		}
		if sh.Start.Before(end) && sh.End.After(start) && sh.UserID != target.UserID {
			t.Errorf("slot not restored to original user: %+v", sh)
		}
	}
	if err := e.schedules.RevokeOverride(ctx, e.admin, ov.ID); !errors.Is(err, biz.ErrShiftNotFound) {
		t.Errorf("double revoke error = %v", err)
	}
	e.assertTemplatesRender(t, biz.EventShiftChanged)
}

// 并发替班与补排同一张值班表：行锁串行化 + 排他约束兜底，结果仍然连续无重叠。
func TestIntegration_ConcurrentWrites(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(now.Add(-time.Hour), uA, uB))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	base := e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), now.AddDate(0, 0, 40))
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 8 {
		wg.Go(func() {
			// 按天轮转：每次替整一天（起止取自已排班次的边界，即每天的交接时刻）
			from := base[1+i].Start
			_, err := e.schedules.CreateOverride(ctx, e.admin, biz.OverrideInput{
				ScheduleID: s.ID, Role: biz.RolePrimary, UserID: uC, Start: from, End: from.Add(24 * time.Hour),
			})
			errs <- err
		})
		wg.Go(func() {
			_, err := e.worker.Extend(ctx)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent write: %v", err)
		}
	}
	assertContiguous(t, e.roleShifts(t, s.ID, biz.RolePrimary, now.Add(-time.Hour), now.AddDate(0, 0, 40)))

	// 排他约束：绕过应用直接写入重叠班次被数据库拒绝。
	_, err = e.pool.Exec(ctx,
		`INSERT INTO oncall_shift (id, schedule_id, group_id, role, user_id, start_time, end_time)
		 VALUES (gen_random_uuid(), $1, $2, 1, $3, $4, $5)`, s.ID, g.ID, uA, now.Add(30*time.Hour), now.Add(31*time.Hour))
	if !isExclusionViolation(err) {
		t.Errorf("overlapping insert error = %v, want exclusion violation", err)
	}
}

func TestIntegration_IssuesAndCurrent(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB)
	now := time.Now().UTC().Truncate(time.Second)
	cfg := dailyConfig(now.Add(-time.Hour), uA) // 单人名单 → 连续值班过长
	// 主备值班人不能是同一个人：单人名单不允许开启备值班。
	if _, err := e.schedules.Create(ctx, e.admin, g.ID, cfg); !errors.Is(err, biz.ErrScheduleInvalid) {
		t.Fatalf("single member with backup error = %v", err)
	}
	cfg.BackupEnabled = false
	s1, err := e.schedules.Create(ctx, e.admin, g.ID, cfg)
	if err != nil {
		t.Fatalf("create s1: %v", err)
	}
	// 历史数据里的主备同人仍要被检出：绕过应用直接写入一段同人的备值班。
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO oncall_shift (id, schedule_id, group_id, role, user_id, start_time, end_time)
		 VALUES (gen_random_uuid(), $1, $2, 2, $3, $4, $5)`, s1.ID, g.ID, uA, now.Add(-time.Minute), now.AddDate(0, 0, 7)); err != nil {
		t.Fatal(err)
	}
	cfg.Name = "夜间值班"
	if _, err := e.schedules.Create(ctx, e.admin, g.ID, cfg); err != nil { // 同一个人 → 跨表撞班
		t.Fatalf("create s2: %v", err)
	}
	// a 离职 → 值班人不可用。
	if _, err := e.pool.Exec(ctx, `UPDATE "system_user" SET status = 4 WHERE id = $1`, uA); err != nil {
		t.Fatal(err)
	}
	_, issues, err := e.schedules.ListShifts(ctx, biz.ShiftQuery{From: now, To: now.AddDate(0, 0, 7), ScheduleID: s1.ID})
	if err != nil {
		t.Fatalf("list shifts: %v", err)
	}
	seen := map[int32]bool{}
	for _, is := range issues {
		seen[is.Type] = true
		if is.ScheduleID != s1.ID {
			t.Errorf("issue of another schedule leaked: %+v", is)
		}
	}
	for _, typ := range []int32{biz.IssueUserUnavailable, biz.IssueSamePerson, biz.IssueClash, biz.IssueLongStreak} {
		if !seen[typ] {
			t.Errorf("missing issue type %d (%s); got %v", typ, biz.IssueLabel(typ), seen)
		}
	}
	// 人为挖一个空档 → 无人值班。
	if _, err := e.pool.Exec(ctx, "DELETE FROM oncall_shift WHERE schedule_id = $1 AND role = 1 AND start_time > $2 AND start_time < $3",
		s1.ID, now.Add(24*time.Hour), now.Add(60*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, issues, _ = e.schedules.ListShifts(ctx, biz.ShiftQuery{From: now, To: now.AddDate(0, 0, 7), ScheduleID: s1.ID})
	if !slices.ContainsFunc(issues, func(is *biz.Issue) bool { return is.Type == biz.IssueGap }) {
		t.Error("missing gap issue")
	}
	// 补排把空档补回来（generated_until 置回过去以触发）。
	if _, err := e.pool.Exec(ctx, "UPDATE oncall_schedule SET generated_until = now() WHERE id = $1", s1.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := e.worker.Extend(ctx); err != nil || n != 1 {
		t.Fatalf("extend = %d, %v", n, err)
	}
	_, issues, _ = e.schedules.ListShifts(ctx, biz.ShiftQuery{From: now, To: now.AddDate(0, 0, 7), ScheduleID: s1.ID})
	if slices.ContainsFunc(issues, func(is *biz.Issue) bool { return is.Type == biz.IssueGap }) {
		t.Error("gap not healed by extend")
	}

	duties, err := e.schedules.Current(ctx, "", s1.ID)
	if err != nil || len(duties) != 1 || duties[0].Primary == nil || duties[0].Backup == nil || duties[0].Primary.UserID != uA {
		t.Fatalf("current = %+v, %v", duties, err)
	}

	// 巡检：按分组通知管理员，同一天只通知一次。
	for range 2 {
		if _, err := e.worker.Inspect(ctx); err != nil {
			t.Fatalf("inspect: %v", err)
		}
	}
	if got := e.events(t, biz.EventIssueDetected); got != 1 {
		t.Errorf("issue events = %d, want 1 (deduplicated per group per day)", got)
	}
	// 「连续值班过长」只在日历里提示，不进通知；其它异常照常通知。
	var listed string
	if err := e.pool.QueryRow(ctx, "SELECT payload->>'issues' FROM notify_event WHERE event_type = $1", biz.EventIssueDetected).Scan(&listed); err != nil {
		t.Fatalf("load issue event: %v", err)
	}
	if strings.Contains(listed, biz.IssueLabel(biz.IssueLongStreak)) || !strings.Contains(listed, biz.IssueLabel(biz.IssueSamePerson)) {
		t.Errorf("notified issues = %s", listed)
	}
	e.assertTemplatesRender(t, biz.EventIssueDetected)

	// 预览：不写库；草稿改用可用成员后不再有"不可用"。
	var before int
	_ = e.pool.QueryRow(ctx, "SELECT count(*) FROM oncall_shift").Scan(&before)
	preview, pIssues, err := e.schedules.Preview(ctx, g.ID, s1.ID, dailyConfig(now, uB, uAdmin), 14)
	if err != nil || len(preview) < 28 {
		t.Fatalf("preview = %d, %v", len(preview), err)
	}
	if slices.ContainsFunc(pIssues, func(is *biz.Issue) bool { return is.Type == biz.IssueUserUnavailable || is.Type == biz.IssueSamePerson }) {
		t.Errorf("preview issues = %+v", pIssues)
	}
	var after int
	_ = e.pool.QueryRow(ctx, "SELECT count(*) FROM oncall_shift").Scan(&after)
	if before != after {
		t.Errorf("preview wrote shifts: %d → %d", before, after)
	}
}

// 值班人查询（流程引擎的值班人规则用）：主在前、备在后；替班取替班人；停用、不存在或
// 那一刻没有排班都返回空而不报错。
func TestIntegration_OnDuty(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	sc, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(now.Add(-time.Hour), uA, uB, uC))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	duty := biz.NewDutyUsecase(e.schedRepo, e.shiftRepo)
	onDuty := func(at time.Time) []string {
		t.Helper()
		users, err := duty.OnDuty(ctx, sc.ID, at)
		if err != nil {
			t.Fatalf("OnDuty() error = %v", err)
		}
		return users
	}
	// 与值班模块自己的「当前值班」口径一致。
	cur, err := e.schedules.Current(ctx, "", sc.ID)
	if err != nil || len(cur) != 1 || cur[0].Primary == nil || cur[0].Backup == nil {
		t.Fatalf("Current() = %+v, err %v", cur, err)
	}
	want := []string{cur[0].Primary.UserID, cur[0].Backup.UserID}
	if got := onDuty(now); !slices.Equal(got, want) || got[0] == got[1] {
		t.Fatalf("OnDuty(now) = %v, want %v", got, want)
	}
	// 明天是下一班：值班人随日期轮转。
	if got := onDuty(now.Add(24 * time.Hour)); len(got) != 2 || got[0] == want[0] {
		t.Fatalf("OnDuty(tomorrow) = %v, want a different primary than %s", got, want[0])
	}
	// 排班范围之外没有班次。
	if got := onDuty(now.AddDate(1, 0, 0)); len(got) != 0 {
		t.Fatalf("OnDuty(next year) = %v, want none", got)
	}
	// 替班：把后天那一整班的主值班换成不在那一班的人（替班以天为单位，须对齐交接时刻）。
	shifts := e.roleShifts(t, sc.ID, biz.RolePrimary, now.Add(-time.Hour), now.AddDate(0, 0, 5))
	target := shifts[2]
	at := target.Start.Add(time.Hour)
	planned := onDuty(at)
	if len(planned) != 2 || planned[0] != target.UserID {
		t.Fatalf("OnDuty(day 3) = %v, want primary %s", planned, target.UserID)
	}
	var sub string
	for _, u := range []string{uA, uB, uC} {
		if !slices.Contains(planned, u) {
			sub = u
		}
	}
	if _, err := e.schedules.CreateOverride(ctx, e.admin, biz.OverrideInput{
		ScheduleID: sc.ID, Role: biz.RolePrimary, UserID: sub, Start: target.Start, End: target.End,
	}); err != nil {
		t.Fatalf("CreateOverride() error = %v", err)
	}
	if got := onDuty(at); !slices.Equal(got, []string{sub, planned[1]}) {
		t.Fatalf("OnDuty(after override) = %v, want [%s %s]", got, sub, planned[1])
	}
	// 交接时刻归新的一班：班次结束的那一刻已不是这一班的人。
	if got := onDuty(target.End); len(got) != 2 || got[0] == sub {
		t.Fatalf("OnDuty(at handover) = %v, want the next shift", got)
	}
	// 停用、不存在的值班表：空，不报错。
	if err := e.schedules.SetStatus(ctx, e.admin, sc.ID, biz.StatusDisabled); err != nil {
		t.Fatal(err)
	}
	if got := onDuty(now); len(got) != 0 {
		t.Fatalf("OnDuty(disabled schedule) = %v, want none", got)
	}
	if got, err := duty.OnDuty(ctx, uOther, now); err != nil || len(got) != 0 {
		t.Fatalf("OnDuty(unknown schedule) = %v, err %v", got, err)
	}
}

func TestIntegration_ShareLinks(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB)
	now := time.Now().UTC()
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(now.Add(-time.Hour), uA, uB))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	if _, _, err := e.shares.Create(ctx, biz.Actor{UserID: uA}, biz.ShareInput{ScopeType: biz.ScopeSchedule, ScopeID: s.ID, Name: "x"}); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member create error = %v", err)
	}
	link, token, err := e.shares.Create(ctx, e.admin, biz.ShareInput{ScopeType: biz.ScopeSchedule, ScopeID: s.ID, Name: "大屏"})
	if err != nil || len(token) != 43 {
		t.Fatalf("create = %v, token len %d", err, len(token))
	}
	// 明文令牌不落库。
	var leaked int
	_ = e.pool.QueryRow(ctx,
		`SELECT count(*) FROM oncall_share_link WHERE position(convert_to($1, 'UTF8') in token_hash) > 0
		    OR position(convert_to($1, 'UTF8') in token_sealed) > 0 OR name = $1`, token).Scan(&leaked)
	if leaked != 0 {
		t.Error("plaintext token stored")
	}
	// 再次查看：管理员拿回同一个令牌；普通成员与组外用户被拒绝。
	if got, err := e.shares.Token(ctx, e.admin, link.ID); err != nil || got != token {
		t.Errorf("Token() = %q, %v, want the created token", got, err)
	}
	if _, err := e.shares.Token(ctx, biz.Actor{UserID: uA}, link.ID); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member Token() error = %v, want ErrForbidden", err)
	}
	if _, err := e.shares.Token(ctx, biz.Actor{UserID: uOther}, link.ID); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("outsider Token() error = %v, want ErrForbidden", err)
	}
	cal, err := e.shares.Public(ctx, token, "1.1.1.1", now, now.AddDate(0, 0, 7))
	if err != nil || cal.Title != g.Name+" / 生产值班" || len(cal.Shifts) == 0 {
		t.Fatalf("public = %+v, %v", cal, err)
	}
	for _, sh := range cal.Shifts {
		if sh.UserPhone != "" {
			t.Fatal("phone leaked without show_phone")
		}
	}
	if _, err := e.shares.Public(ctx, token, "1.1.1.1", now, now.AddDate(0, 0, 90)); !errors.Is(err, biz.ErrTimeRangeInvalid) {
		t.Errorf("range error = %v", err)
	}
	if links, _ := e.shares.List(ctx, e.admin, biz.ScopeSchedule, s.ID); len(links) != 1 || links[0].LastAccessTime == nil || links[0].CreatorName == "" {
		t.Errorf("list = %+v", links)
	}

	// 密文列引入之前创建的地址（列为 NULL）：查看时提示重置，而不是 500。
	if _, err := e.pool.Exec(ctx, "UPDATE oncall_share_link SET token_sealed = NULL WHERE id = $1", link.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.shares.Token(ctx, e.admin, link.ID); !errors.Is(err, biz.ErrShareLinkInvalid) {
		t.Errorf("legacy Token() error = %v, want ErrShareLinkInvalid", err)
	}
	// 重置：旧令牌立即失效；作废：新令牌失效。失败原因对外不可区分。
	next, err := e.shares.Reset(ctx, e.admin, link.ID)
	if err != nil || next == token {
		t.Fatalf("reset = %v", err)
	}
	if _, err := e.shares.Public(ctx, token, "1.1.1.1", now, now.AddDate(0, 0, 7)); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("old token error = %v", err)
	}
	if got, err := e.shares.Token(ctx, e.admin, link.ID); err != nil || got != next {
		t.Errorf("Token() after reset = %q, %v, want the new token", got, err)
	}
	if _, err := e.shares.Public(ctx, next, "1.1.1.1", now, now.AddDate(0, 0, 7)); err != nil {
		t.Errorf("new token error = %v", err)
	}
	if err := e.shares.Delete(ctx, e.admin, link.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := e.shares.Public(ctx, next, "1.1.1.1", now, now.AddDate(0, 0, 7)); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("revoked token error = %v", err)
	}

	// 过期地址 404；带手机号的地址才返回手机号。
	exp := time.Now().Add(2 * time.Second)
	_, short, err := e.shares.Create(ctx, e.admin, biz.ShareInput{ScopeType: biz.ScopeGroup, ScopeID: g.ID, Name: "短期", ShowPhone: true, ExpireTime: &exp})
	if err != nil {
		t.Fatalf("create expiring: %v", err)
	}
	cal, err = e.shares.Public(ctx, short, "2.2.2.2", now, now.AddDate(0, 0, 7))
	if err != nil || cal.Shifts[0].UserPhone == "" {
		t.Fatalf("show_phone calendar = %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	if _, err := e.shares.Public(ctx, short, "2.2.2.2", now, now.AddDate(0, 0, 7)); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("expired token error = %v", err)
	}

	// 个人范围：恒为本人；他人不可管理且不暴露是否存在。
	mine, mineToken, err := e.shares.Create(ctx, biz.Actor{UserID: uA}, biz.ShareInput{ScopeType: biz.ScopeUser, ScopeID: uB, Name: "我的"})
	if err != nil || mine.ScopeID != uA {
		t.Fatalf("personal link scope = %v, %v", mine, err)
	}
	cal, err = e.shares.Public(ctx, mineToken, "3.3.3.3", now, now.AddDate(0, 0, 7))
	if err != nil || slices.ContainsFunc(cal.Shifts, func(sh *biz.Shift) bool { return sh.UserID != uA }) {
		t.Errorf("personal calendar leaked others' shifts: %v", err)
	}
	if got, err := e.shares.Token(ctx, biz.Actor{UserID: uA}, mine.ID); err != nil || got != mineToken {
		t.Errorf("owner Token() = %q, %v", got, err)
	}
	if _, err := e.shares.Token(ctx, biz.Actor{UserID: uB}, mine.ID); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("foreign Token() error = %v, want ErrShareLinkNotFound", err)
	}
	if err := e.shares.Delete(ctx, biz.Actor{UserID: uB}, mine.ID); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("foreign delete error = %v", err)
	}
	// 删除值班表：其公共地址随之作废。
	_, schedToken, _ := e.shares.Create(ctx, e.admin, biz.ShareInput{ScopeType: biz.ScopeSchedule, ScopeID: s.ID, Name: "临时"})
	if err := e.schedules.Delete(ctx, e.admin, s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.shares.Public(ctx, schedToken, "4.4.4.4", now, now.AddDate(0, 0, 7)); !errors.Is(err, biz.ErrShareLinkNotFound) {
		t.Errorf("token of deleted schedule error = %v", err)
	}
}

func TestIntegration_RemindAndDigest(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB)
	now := time.Now().UTC().Truncate(time.Second)
	// 下一次交接在 30 分钟后：下一班（bob）提前 60 分钟的提醒已到点、交接时刻（0）的未到；
	// 当前班次（alice）随值班表创建即刻开始，立即收到一条"你正在值班"的提醒。
	cfg := dailyConfig(now.Add(-24*time.Hour+30*time.Minute), uA, uB)
	cfg.BackupEnabled = false
	s, err := e.schedules.Create(ctx, e.admin, g.ID, cfg)
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	for i := range 3 { // 重复执行不重发
		if _, err := e.worker.Remind(ctx); err != nil {
			t.Fatalf("remind #%d: %v", i, err)
		}
	}
	if got := e.events(t, biz.EventShiftRemind); got != 2 {
		t.Fatalf("remind events = %d, want 2", got)
	}
	var recipients []string
	if err := e.pool.QueryRow(ctx, "SELECT array_agg(r ORDER BY r) FROM notify_event, jsonb_array_elements_text(recipients) r WHERE event_type = $1", biz.EventShiftRemind).Scan(&recipients); err != nil ||
		!slices.Equal(recipients, []string{uA, uB}) {
		t.Errorf("remind recipients = %v, %v (want alice and bob, one each)", recipients, err)
	}
	// 把交接时刻拨到 1 分钟前（先缩短进行中的班次，否则与排他约束冲突）：交接时刻的提醒
	// 到点，且只补发这一条。
	if _, err := e.pool.Exec(ctx, "UPDATE oncall_shift SET start_time = start_time - interval '2 hours', end_time = now() - interval '1 minute' WHERE schedule_id = $1 AND start_time <= now() AND end_time > now()", s.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx,
		`UPDATE oncall_shift SET start_time = now() - interval '1 minute'
		 WHERE schedule_id = $1 AND start_time > now() AND start_time < now() + interval '1 hour'`, s.ID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := e.worker.Remind(ctx); err != nil {
			t.Fatalf("remind at handover: %v", err)
		}
	}
	if got := e.events(t, biz.EventShiftRemind); got != 3 {
		t.Errorf("remind events = %d, want 3", got)
	}

	// 每日播报：到点播报一次，当日不重复。
	local := time.Now().In(time.FixedZone("CST", 8*3600)).Add(-time.Minute).Format("15:04")
	cfg.DigestTime = local
	if _, err := e.schedules.Update(ctx, e.admin, s.ID, cfg); err != nil {
		t.Fatalf("set digest time: %v", err)
	}
	for range 2 {
		if _, err := e.worker.Digest(ctx); err != nil {
			t.Fatalf("digest: %v", err)
		}
	}
	if got := e.events(t, biz.EventDailyDigest); got != 1 {
		t.Errorf("digest events = %d, want 1", got)
	}
	e.assertTemplatesRender(t, biz.EventShiftRemind, biz.EventDailyDigest)
}

// 手动发送值班通知：权限、收件人、发送间隔、与自动通知互不影响、投递结果只能查本表的事件。
func TestIntegration_ManualNotification(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	g := e.group(t, uA, uB, uC)
	now := time.Now().UTC().Truncate(time.Second)
	s, err := e.schedules.Create(ctx, e.admin, g.ID, dailyConfig(now.Add(-time.Hour), uA, uB, uC))
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	// 新建值班表不发通知，库里此刻没有任何值班事件。
	if n := e.events(t, biz.EventShiftRemind) + e.events(t, biz.EventDailyDigest); n != 0 {
		t.Fatalf("events before manual send = %d", n)
	}
	remind := biz.ManualNotifyInput{ScheduleID: s.ID, Kind: biz.NotifyKindRemind, Target: biz.NotifyTargetCurrent}

	// 普通成员不能发；未知类型被拒。
	if _, err := e.schedules.SendNotification(ctx, biz.Actor{UserID: uA}, remind); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member send error = %v", err)
	}
	if _, err := e.schedules.SendNotification(ctx, e.admin, biz.ManualNotifyInput{ScheduleID: s.ID, Kind: 9}); !errors.Is(err, biz.ErrScheduleInvalid) {
		t.Errorf("unknown kind error = %v", err)
	}

	// 当前班次：主、备值班人各一条，收件人就是当前在岗的两个人。
	res, err := e.schedules.SendNotification(ctx, e.admin, remind)
	if err != nil {
		t.Fatalf("send remind: %v", err)
	}
	duties, err := e.schedules.Current(ctx, "", s.ID)
	if err != nil || len(duties) != 1 || duties[0].Primary == nil || duties[0].Backup == nil {
		t.Fatalf("current = %+v, %v", duties, err)
	}
	if len(res.EventIDs) != 2 || len(res.Recipients) != 2 ||
		res.Recipients[0].UserID != duties[0].Primary.UserID || res.Recipients[1].UserID != duties[0].Backup.UserID {
		t.Fatalf("remind result = %d events, recipients %+v", len(res.EventIDs), res.Recipients)
	}
	if got := e.events(t, biz.EventShiftRemind); got != 2 {
		t.Errorf("remind events = %d, want 2", got)
	}
	var recipient string
	var manual, started bool
	if err := e.pool.QueryRow(ctx, `SELECT recipients->>0, (payload->>'manual')::bool, (payload->>'started')::bool
		FROM notify_event WHERE id = $1`, res.EventIDs[0]).Scan(&recipient, &manual, &started); err != nil {
		t.Fatalf("load event: %v", err)
	}
	if recipient != duties[0].Primary.UserID || !manual || !started {
		t.Errorf("event recipient = %s manual = %v started = %v", recipient, manual, started)
	}

	// 一分钟内重复发送同一种通知被限流；另一种通知不受影响。
	if _, err := e.schedules.SendNotification(ctx, e.admin, remind); !errors.Is(err, biz.ErrNotifyThrottled) {
		t.Errorf("repeat send error = %v", err)
	}
	digest, err := e.schedules.SendNotification(ctx, e.admin, biz.ManualNotifyInput{ScheduleID: s.ID, Kind: biz.NotifyKindDigest})
	if err != nil || len(digest.EventIDs) != 1 || len(digest.Recipients) != 0 {
		t.Fatalf("send digest = %+v, %v", digest, err)
	}

	// 手动发送不改动自动提醒的标记：自动提醒（提前 60 分钟 / 交接时刻）照常发出。
	if _, err := e.pool.Exec(ctx, "DELETE FROM notify_event WHERE event_type = $1", biz.EventShiftRemind); err != nil {
		t.Fatal(err)
	}
	if n, err := e.worker.Remind(ctx); err != nil || n == 0 {
		t.Errorf("automatic reminders after manual send = %d, %v", n, err)
	}

	// 下一个班次：发给下一班的主、备值班人（尚未开始）。间隔按「值班表 + 通知类型」计，
	// 先清掉刚才的手动记录。
	if _, err := e.pool.Exec(ctx, "DELETE FROM notify_event WHERE event_type = $1", biz.EventShiftRemind); err != nil {
		t.Fatal(err)
	}
	next, err := e.schedules.SendNotification(ctx, e.admin, biz.ManualNotifyInput{ScheduleID: s.ID, Kind: biz.NotifyKindRemind, Target: biz.NotifyTargetNext})
	if err != nil || len(next.Recipients) != 2 || !next.Recipients[0].Start.After(now) || next.Recipients[0].UserID == duties[0].Primary.UserID {
		t.Fatalf("next shift remind = %+v, %v", next, err)
	}

	// 投递结果：只返回属于这张值班表的值班事件；附带各渠道的投递情况。
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO notify_dispatch (id, event_id, channel_id, recipient, status, attempts, last_error, send_key, create_time)
		 VALUES (gen_random_uuid(), $1, gen_random_uuid(), 'a@example.com', 5, 3, 'mailbox unavailable', 'k1', now())`, next.EventIDs[0]); err != nil {
		t.Fatalf("insert dispatch: %v", err)
	}
	results, err := e.schedules.NotificationResults(ctx, e.admin, s.ID, append(next.EventIDs, digest.EventIDs...))
	if err != nil || len(results) != 3 {
		t.Fatalf("results = %d, %v", len(results), err)
	}
	var withDelivery *biz.NotifyResult
	for _, r := range results {
		if r.EventID == next.EventIDs[0] {
			withDelivery = r
		}
	}
	if withDelivery == nil || len(withDelivery.Deliveries) != 1 || withDelivery.Deliveries[0].Status != 5 ||
		withDelivery.Deliveries[0].Recipient != "a@example.com" || withDelivery.Deliveries[0].Error != "mailbox unavailable" {
		t.Errorf("delivery = %+v", withDelivery)
	}
	if _, err := e.schedules.NotificationResults(ctx, biz.Actor{UserID: uA}, s.ID, next.EventIDs); !errors.Is(err, biz.ErrForbidden) {
		t.Errorf("member results error = %v", err)
	}
	// 另一张值班表查不到这张表的事件。
	cfg := dailyConfig(now.Add(-time.Hour), uA, uB)
	cfg.Name = "夜间值班"
	other, err := e.schedules.Create(ctx, e.admin, g.ID, cfg)
	if err != nil {
		t.Fatalf("create other schedule: %v", err)
	}
	if leaked, err := e.schedules.NotificationResults(ctx, e.admin, other.ID, next.EventIDs); err != nil || len(leaked) != 0 {
		t.Errorf("events of another schedule leaked: %d, %v", len(leaked), err)
	}

	// 停用的值班表不能发；写入了变更记录；缺省模板能渲染手动事件。
	if err := e.schedules.SetStatus(ctx, e.admin, other.ID, biz.StatusDisabled); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := e.schedules.SendNotification(ctx, e.admin, biz.ManualNotifyInput{ScheduleID: other.ID, Kind: biz.NotifyKindDigest}); !errors.Is(err, biz.ErrScheduleInvalid) {
		t.Errorf("disabled schedule send error = %v", err)
	}
	var logs int
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM oncall_change_log WHERE schedule_id = $1 AND action = $2", s.ID, biz.ActionScheduleNotify).Scan(&logs); err != nil || logs != 3 {
		t.Errorf("notify change logs = %d, %v, want 3", logs, err)
	}
	e.assertTemplatesRender(t, biz.EventShiftRemind, biz.EventDailyDigest)
}

// 值班通知配置状态：按管道同一口径试算会命中的规则与其上已启用的渠道数。迁移预置的两条
// 规则初始没有渠道（发不出去）；绑定启用的渠道后计数随之变化，停用的渠道不算。
func TestIntegration_NotificationStatus(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	const (
		routePersonal = "018f3e1a-0ca1-7000-8000-000000000001"
		routeDigest   = "018f3e1a-0ca1-7000-8000-000000000002"
	)
	restore := func() {
		_, _ = e.pool.Exec(ctx, "UPDATE notify_route SET channel_ids = '{}' WHERE id IN ($1, $2)", routePersonal, routeDigest)
		_, _ = e.pool.Exec(ctx, "DELETE FROM notify_channel WHERE name LIKE 'oncall-status-%'")
	}
	restore()
	t.Cleanup(restore)
	status := func() map[int32]*biz.NotifyRouteStatus {
		t.Helper()
		items, err := e.schedules.NotificationStatus(ctx)
		if err != nil || len(items) != 2 {
			t.Fatalf("status = %d items, %v", len(items), err)
		}
		out := map[int32]*biz.NotifyRouteStatus{}
		for _, it := range items {
			out[it.Class] = it
		}
		return out
	}
	got := status()
	// 每日播报命中更优先的专用规则，而不是通配的「值班通知」。
	if p, d := got[biz.NotifyClassPersonal], got[biz.NotifyClassDigest]; p.RouteName != "值班通知" || d.RouteName != "值班每日播报" || p.ChannelCount != 0 || d.ChannelCount != 0 {
		t.Fatalf("initial status: personal %+v, digest %+v", p, d)
	}
	var enabled, disabled string
	if err := e.pool.QueryRow(ctx, `INSERT INTO notify_channel (id, type, name, config) VALUES (gen_random_uuid(), 'email', 'oncall-status-on', '{}') RETURNING id::text`).Scan(&enabled); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	if err := e.pool.QueryRow(ctx, `INSERT INTO notify_channel (id, type, name, config, enabled) VALUES (gen_random_uuid(), 'email', 'oncall-status-off', '{}', false) RETURNING id::text`).Scan(&disabled); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE notify_route SET channel_ids = $2::uuid[] WHERE id = $1", routePersonal, []string{enabled, disabled}); err != nil {
		t.Fatalf("bind channels: %v", err)
	}
	got = status()
	if p, d := got[biz.NotifyClassPersonal], got[biz.NotifyClassDigest]; p.ChannelCount != 1 || d.ChannelCount != 0 {
		t.Fatalf("after binding: personal %+v, digest %+v", p, d)
	}
	// 专用规则停用后，每日播报退到通配规则。
	if _, err := e.pool.Exec(ctx, "UPDATE notify_route SET enabled = false WHERE id = $1", routeDigest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = e.pool.Exec(ctx, "UPDATE notify_route SET enabled = true WHERE id = $1", routeDigest) })
	if d := status()[biz.NotifyClassDigest]; d.RouteName != "值班通知" || d.ChannelCount != 1 {
		t.Fatalf("digest after disabling its route: %+v", d)
	}
}
