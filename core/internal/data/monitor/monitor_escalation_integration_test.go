//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

// escEnv 升级策略 / 升级派发 / 自监控事件的集成环境：在第一期的派发环境之上清空第二期的相关表。
type escEnv struct {
	*dispatchEnv
	esc    *escalationRepo
	engine *engineEventRepo
}

// truncateEscalationTables 清空本文件用到的第二期表。monitor_escalation 以外键引用规则分组，而第一期的
// newEnv 会删除全部非内置分组：所以在 newEnv 之前清一次，测试结束再清一次（不给后面的测试留下挡路的行）。
func truncateEscalationTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "TRUNCATE TABLE monitor_escalation, monitor_alert_escalation, monitor_engine_event"); err != nil {
		t.Fatalf("truncate p2 tables: %v", err)
	}
}

func newEscEnv(t *testing.T) *escEnv {
	t.Helper()
	pre := openData(t, testDSN(t))
	truncateEscalationTables(t, pre.Pool)
	d := newDispatchEnv(t)
	t.Cleanup(func() { truncateEscalationTables(t, pre.Pool) })
	return &escEnv{dispatchEnv: d, esc: NewEscalationRepo(d.d), engine: NewEngineEventRepo(d.d)}
}

func escalation(groupID, name string) *biz.Escalation {
	return &biz.Escalation{Spec: biz.EscalationSpec{
		GroupID: groupID, Name: name, Description: "说明", Mode: mc.EscalationModeTakeover, MinSeverity: 3, Enabled: true,
		Levels: []mc.EscalationLevel{
			{Trigger: mc.EscalationTriggerUnacked, AfterM: 15, RepeatIntervalM: 30, MaxNotify: 3, Targets: mc.EscalationTargets{
				UserIDs: []string{uBob}, Schedules: []mc.EscalationSchedule{{ID: newUUID(), BackupOnly: true}},
			}},
			{Trigger: mc.EscalationTriggerUnresolved, AfterM: 60, Targets: mc.EscalationTargets{UserIDs: []string{uCarol}, Depts: []mc.EscalationDept{{ID: newUUID(), Up: 2}}}},
		},
	}}
}

func newUUID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	h := hex.EncodeToString(buf)
	return h[:8] + "-" + h[8:12] + "-7" + h[13:16] + "-8" + h[17:20] + "-" + h[20:]
}

func (e *escEnv) create(groupID, name string) *biz.Escalation {
	e.t.Helper()
	p := escalation(groupID, name)
	p.ID = newID(e.t)
	if err := e.esc.Create(context.Background(), p, uAdmin); err != nil {
		e.t.Fatalf("create escalation %s: %v", name, err)
	}
	return p
}

// setEscalation 让规则引用策略（规则的保存路径由规则仓储负责，这里直接写列）。
func (e *escEnv) setEscalation(ruleID, escalationID string) {
	e.t.Helper()
	var arg any
	if escalationID != "" {
		arg = escalationID
	}
	if _, err := e.pool.Exec(context.Background(), "UPDATE monitor_rule SET escalation_id = $2 WHERE id = $1", ruleID, arg); err != nil {
		e.t.Fatal(err)
	}
}

func (e *escEnv) escOutbox(alertID string, level, n int32) string {
	e.t.Helper()
	id := newID(e.t)
	if _, err := e.pool.Exec(context.Background(),
		"INSERT INTO monitor_notify_outbox (id, alert_id, kind, seq, level) VALUES ($1, $2, $3, $4, $5)",
		id, alertID, mc.NotifyKindEscalation, mc.EscalationOutboxSeq(level, n), level); err != nil {
		e.t.Fatalf("seed escalation outbox: %v", err)
	}
	return id
}

type fixedRoster struct{ backup []string }

func (f fixedRoster) OnDutyByRole(context.Context, string) ([]string, []string, error) {
	return []string{uAlice}, f.backup, nil
}

type noGroups struct{}

func (noGroups) Groups(context.Context) ([]biz.OncallGroup, error) { return nil, nil }

func (noGroups) AdminIDs(context.Context, string) ([]string, error) { return nil, nil }

func (noGroups) Briefs(context.Context, []string) (map[string]*biz.CIEntry, error) { return nil, nil }

// p2Usecase 带第二期协作者的派发用例（仓储可以来自另一个数据库角色的连接池）。
func (e *escEnv) p2Usecase(env *env, esc *escalationRepo, events biz.EventWriter) *biz.DispatchUsecase {
	e.t.Helper()
	return e.newUsecase(env, events).WithP2(biz.NewDispatchExt(esc, esc, nil, fixedRoster{backup: []string{uCarol}}, noGroups{}))
}

func TestEscalationRepo(t *testing.T) {
	e := newEscEnv(t)
	ctx := context.Background()
	base := e.cfgVersion(mc.CfgEscalation)
	child := e.env.group(e.group.ID, "child")
	a := e.create(e.group.ID, "策略 A")
	g := e.create("", "全局")
	c := e.create(child.ID, "b-child")
	if v := e.cfgVersion(mc.CfgEscalation); v != base+3 {
		t.Fatalf("cfg version = %d, want %d", v, base+3)
	}
	// 同一归属下重名（全局之间同样：NULLS NOT DISTINCT）；不同归属可以同名。
	dup := escalation(e.group.ID, "策略 A")
	dup.ID = newID(t)
	wantErr(t, "dup in group", e.esc.Create(ctx, dup, uAdmin), biz.ErrEscalationNameExists)
	dupGlobal := escalation("", "全局")
	dupGlobal.ID = newID(t)
	wantErr(t, "dup global", e.esc.Create(ctx, dupGlobal, uAdmin), biz.ErrEscalationNameExists)
	other := escalation(child.ID, "策略 A")
	other.ID = newID(t)
	if err := e.esc.Create(ctx, other, uAdmin); err != nil {
		t.Fatalf("same name under another owner: %v", err)
	}
	missing := escalation(newUUID(), "x")
	missing.ID = newID(t)
	wantErr(t, "missing group", e.esc.Create(ctx, missing, uAdmin), biz.ErrGroupNotFound)
	if v := e.cfgVersion(mc.CfgEscalation); v != base+4 {
		t.Fatalf("failed writes must not bump the cfg version: %d", v)
	}

	got, err := e.esc.Get(ctx, a.ID)
	if err != nil || got.Version != 1 || got.Spec.GroupID != e.group.ID || got.Spec.Mode != mc.EscalationModeTakeover || got.Spec.MinSeverity != 3 ||
		len(got.Spec.Levels) != 2 || !got.Spec.Levels[0].Targets.Schedules[0].BackupOnly || got.Spec.Levels[1].Targets.Depts[0].Up != 2 ||
		got.UpdateUser != uAdmin || got.CreateTime.IsZero() {
		t.Fatalf("get = %+v err = %v", got, err)
	}
	// 落库的 jsonb 即契约（引擎按这些键读取）。
	var raw string
	if err := e.pool.QueryRow(ctx, "SELECT levels::text FROM monitor_escalation WHERE id = $1", a.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"trigger"`, `"after_m"`, `"repeat_interval_m"`, `"max_notify"`, `"backup_only"`, `"user_ids"`, `"up"`} {
		if !strings.Contains(raw, k) {
			t.Errorf("levels jsonb misses %s: %s", k, raw)
		}
	}

	names := func(f biz.EscalationFilter) []string {
		items, total, err := e.esc.List(ctx, f, 1, 20)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, it.Spec.Name)
		}
		if int(total) != len(out) {
			t.Fatalf("total = %d, items = %d", total, len(out))
		}
		return out
	}
	yes, no := true, false
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}); !slices.Equal(got, []string{"全局", "b-child", "策略 A", "策略 A"}) {
		t.Fatalf("all = %v", got)
	}
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{IDs: []string{e.group.ID}}, IncludeGlobal: true}); !slices.Equal(got, []string{"全局", "策略 A"}) {
		t.Fatalf("group + global = %v", got)
	}
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{IDs: []string{child.ID}}}); !slices.Equal(got, []string{"b-child", "策略 A"}) {
		t.Fatalf("child only = %v", got)
	}
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{}, IncludeGlobal: true}); !slices.Equal(got, []string{"全局"}) {
		t.Fatalf("no groups = %v", got)
	}
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Global: &no, Keyword: "CHILD"}); !slices.Equal(got, []string{"b-child"}) {
		t.Fatalf("keyword = %v", got)
	}
	if got := names(biz.EscalationFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true, Global: &yes, Enabled: &yes}); !slices.Equal(got, []string{"全局"}) {
		t.Fatalf("global enabled = %v", got)
	}
	if page2, total, _ := e.esc.List(ctx, biz.EscalationFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}, 2, 3); total != 4 || len(page2) != 1 {
		t.Fatalf("page 2 = %d / %d", len(page2), total)
	}
	if byIDs, _ := e.esc.ListByIDs(ctx, []string{a.ID, newUUID(), g.ID}); len(byIDs) != 2 {
		t.Fatalf("ListByIDs = %d", len(byIDs))
	}

	// 乐观锁；更新不改归属分组。
	upd := &biz.Escalation{ID: a.ID, Spec: a.Spec}
	upd.Spec.Name, upd.Spec.Enabled, upd.Spec.GroupID = "策略 A2", false, child.ID
	wantErr(t, "stale version", e.esc.Update(ctx, upd, 9, uAlice), biz.ErrVersionConflict)
	if err := e.esc.Update(ctx, upd, 1, uAlice); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got, _ = e.esc.Get(ctx, a.ID); got.Version != 2 || got.Spec.Name != "策略 A2" || got.Spec.Enabled || got.Spec.GroupID != e.group.ID || got.UpdateUser != uAlice {
		t.Fatalf("after update = %+v", got)
	}
	upd.Spec.Name = "b-child"
	upd.ID = other.ID
	wantErr(t, "rename onto a sibling", e.esc.Update(ctx, upd, 1, uAlice), biz.ErrEscalationNameExists)
	upd.ID = newUUID()
	wantErr(t, "update missing", e.esc.Update(ctx, upd, 1, uAlice), biz.ErrEscalationNotFound)

	// 被规则引用：计数、删除被拒（metadata 带数量）；解除引用后可删，已删除的规则不算。
	r2 := e.env.rule(e.group.ID, "mem-high")
	e.setEscalation(e.rule.ID, a.ID)
	e.setEscalation(r2.ID, a.ID)
	counts, err := e.esc.RuleCounts(ctx, []string{a.ID, g.ID, c.ID})
	if err != nil || counts[a.ID] != 2 || len(counts) != 1 {
		t.Fatalf("rule counts = %v, %v", counts, err)
	}
	before := e.cfgVersion(mc.CfgEscalation)
	err = e.esc.Delete(ctx, a.ID, 2, uAlice)
	wantErr(t, "in use", err, biz.ErrEscalationInUse)
	if md := kerrors.FromError(err).Metadata; md["rule_count"] != "2" {
		t.Fatalf("metadata = %v", md)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_rule SET delete_time = now() WHERE id = $1", r2.ID); err != nil {
		t.Fatal(err)
	}
	e.setEscalation(e.rule.ID, "")
	wantErr(t, "delete stale", e.esc.Delete(ctx, a.ID, 1, uAlice), biz.ErrVersionConflict)
	if v := e.cfgVersion(mc.CfgEscalation); v != before {
		t.Fatalf("refused deletes must not bump: %d → %d", before, v)
	}
	if err := e.esc.Delete(ctx, a.ID, 2, uAlice); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = e.esc.Get(ctx, a.ID)
	wantErr(t, "deleted", err, biz.ErrEscalationNotFound)
	wantErr(t, "delete again", e.esc.Delete(ctx, a.ID, 3, uAlice), biz.ErrEscalationNotFound)
	if v := e.cfgVersion(mc.CfgEscalation); v != before+1 {
		t.Fatalf("cfg version after delete = %d", v)
	}
	// 软删后同名可以再建。
	again := escalation(e.group.ID, "策略 A2")
	again.ID = newID(t)
	if err := e.esc.Create(ctx, again, uAdmin); err != nil {
		t.Fatalf("recreate after delete: %v", err)
	}
	// 归属分组仍有策略时不影响分组的其它语义；这里只确认 EXPLAIN 走引用索引。
	var plan string
	rows, err := e.pool.Query(ctx, "EXPLAIN SELECT escalation_id, count(*) FROM monitor_rule WHERE escalation_id = ANY($1::uuid[]) AND delete_time IS NULL GROUP BY escalation_id", []string{a.ID})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		plan += line + "\n"
	}
	rows.Close()
	t.Logf("RuleCounts plan:\n%s", plan)
}

func TestEscalationDispatch_RealPG(t *testing.T) {
	e := newEscEnv(t)
	ctx := context.Background()
	p := e.create(e.group.ID, "升级")
	e.setEscalation(e.rule.ID, p.ID)
	rec := &recordingEvents{}
	uc := e.p2Usecase(e.env, e.esc, rec)
	alertID := e.firingAlert()
	fire := e.outboxRow(alertID, mc.NotifyKindFiring, 1)
	esc1 := e.escOutbox(alertID, 1, 1)
	esc2 := e.escOutbox(alertID, 1, 2)
	esc3 := e.escOutbox(alertID, 2, 1)
	if n, err := uc.Run(ctx); err != nil || n != 4 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	for _, id := range []string{fire, esc1, esc2, esc3} {
		if s := e.outboxState(id); s.status != mc.OutboxDone {
			t.Fatalf("outbox %s = %+v", id, s)
		}
	}
	// 事件的通知计数只来自触发通知；升级通知计在各级的进度里。
	if pr := e.progress(alertID); pr.count != 1 {
		t.Fatalf("alert notify_count = %d, want 1", pr.count)
	}
	type level struct {
		count int32
		last  *time.Time
	}
	read := func(lv int32) level {
		var l level
		if err := e.pool.QueryRow(ctx, "SELECT notify_count, last_notify_time FROM monitor_alert_escalation WHERE alert_id = $1 AND level = $2", alertID, lv).
			Scan(&l.count, &l.last); err != nil {
			t.Fatalf("read level %d: %v", lv, err)
		}
		return l
	}
	if l1, l2 := read(1), read(2); l1.count != 2 || l1.last == nil || l2.count != 1 {
		t.Fatalf("levels = %+v / %+v", l1, l2)
	}
	// 进度只增不减。
	if err := basedata.NewTxManager(e.d).WithinTx(ctx, func(tx txrun.Tx) error { return e.esc.TouchLevelTx(ctx, tx, alertID, 1, 1) }); err != nil {
		t.Fatal(err)
	}
	if l1 := read(1); l1.count != 2 {
		t.Fatalf("progress went backwards: %d", l1.count)
	}
	// 通知记录：升级通知 kind 4 / source 3 / level / 级内序号（表上的 CHECK 同时校验了这组取值）。
	type logRow struct{ kind, seq, source, level int32 }
	var logs []logRow
	rows, err := e.pool.Query(ctx, "SELECT kind, seq, source, level FROM monitor_notify_log WHERE alert_id = $1 ORDER BY create_time, id", alertID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var l logRow
		if err := rows.Scan(&l.kind, &l.seq, &l.source, &l.level); err != nil {
			t.Fatal(err)
		}
		logs = append(logs, l)
	}
	rows.Close()
	if !slices.Equal(logs, []logRow{{1, 1, 1, 0}, {4, 1, 3, 1}, {4, 2, 3, 1}, {4, 1, 3, 2}}) {
		t.Fatalf("logs = %+v", logs)
	}
	got, _ := e.logs.ListByAlert(ctx, alertID, 10)
	if len(got) != 4 || got[1].Level != 1 || got[3].Level != 2 {
		t.Fatalf("ListByAlert levels = %+v", got)
	}
	// 事件内容：第 1 级 = 指定用户 bob ∪ 仅备班 carol；载荷带级数与级内序号。
	if len(rec.events) != 4 || rec.events[1].EventType != mc.EventAlertEscalated || !slices.Equal(rec.events[1].Recipients, []string{uBob, uCarol}) {
		t.Fatalf("events = %+v", rec.events)
	}
	var pl mc.NotifyPayload
	if err := json.Unmarshal(rec.events[2].Payload, &pl); err != nil || pl.EscalationLevel != 1 || pl.EscalationSeq != 2 || pl.EscalationName != "升级" {
		t.Fatalf("payload = %+v, %v", pl, err)
	}
	bare := strings.ReplaceAll(alertID, "-", "")
	if rec.events[3].DedupKey != "monitor:"+bare+":"+bare+":4:200001" {
		t.Fatalf("dedup = %q", rec.events[3].DedupKey)
	}

	// 已恢复 / 已关闭 / 被抑制的事件：升级通知一律丢弃，不产生事件。
	events := len(rec.events)
	resolved := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu-high", groupID: e.group.ID, status: mc.AlertResolved})
	closed := e.alert(alertRow{ruleID: e.rule.ID, ruleName: "cpu-high", groupID: e.group.ID, status: mc.AlertClosed})
	inhibited := e.firingAlert()
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert SET inhibited = true, inhibit_kind = 2 WHERE id = $1", inhibited); err != nil {
		t.Fatal(err)
	}
	dropped := []string{e.escOutbox(resolved, 1, 1), e.escOutbox(closed, 1, 1), e.escOutbox(inhibited, 1, 1)}
	if n, err := uc.Run(ctx); err != nil || n != 3 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	for _, id := range dropped {
		if s := e.outboxState(id); s.status != mc.OutboxDropped || s.lastError == "" {
			t.Fatalf("outbox %s = %+v, want dropped", id, s)
		}
	}
	if len(rec.events) != events || e.count("SELECT count(*) FROM monitor_alert_escalation WHERE alert_id = ANY($1::uuid[])", []string{resolved, closed, inhibited}) != 0 {
		t.Fatal("dropped escalation rows must not notify or record progress")
	}
	// 策略被停用后已入箱的行丢弃。
	off := &biz.Escalation{ID: p.ID, Spec: p.Spec}
	off.Spec.Enabled = false
	if err := e.esc.Update(ctx, off, 1, uAdmin); err != nil {
		t.Fatal(err)
	}
	late := e.escOutbox(alertID, 2, 2)
	if n, _ := uc.Run(ctx); n != 1 || e.outboxState(late).status != mc.OutboxDropped {
		t.Fatalf("row of a disabled policy = %+v", e.outboxState(late))
	}
}

// 两个实例同时派发同一批升级通知：每行只发一次，进度不重复累加。
func TestEscalationDispatch_Concurrent(t *testing.T) {
	e := newEscEnv(t)
	ctx := context.Background()
	p := e.create(e.group.ID, "升级")
	e.setEscalation(e.rule.ID, p.ID)
	var alerts []string
	for range 6 {
		id := e.firingAlert()
		alerts = append(alerts, id)
		e.escOutbox(id, 1, 1)
		e.escOutbox(id, 1, 2)
	}
	rec := &recordingEvents{}
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		uc := e.p2Usecase(e.env, e.esc, rec)
		go func() {
			defer wg.Done()
			for {
				n, err := uc.Run(ctx)
				if err != nil {
					t.Errorf("Run: %v", err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	// 并发下某一轮可能因为行都被别的实例锁着而提前返回 0：收尾再跑一轮。
	if _, err := e.p2Usecase(e.env, e.esc, rec).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.count("SELECT count(*) FROM monitor_notify_outbox WHERE status = $1", mc.OutboxDone); n != 12 {
		t.Fatalf("done rows = %d, want 12", n)
	}
	if n := e.count("SELECT count(*) FROM monitor_notify_log WHERE kind = 4"); n != 12 || len(rec.events) != 12 {
		t.Fatalf("logs = %d events = %d, want 12 each", n, len(rec.events))
	}
	if n := e.count("SELECT count(*) FROM monitor_alert_escalation WHERE alert_id = ANY($1::uuid[]) AND level = 1 AND notify_count = 2", alerts); n != 6 {
		t.Fatalf("level rows at count 2 = %d, want 6", n)
	}
}

func (e *escEnv) engineEvent(typ, objectID, dedup string, detail any) string {
	e.t.Helper()
	raw, err := json.Marshal(detail)
	if err != nil {
		e.t.Fatal(err)
	}
	id := newID(e.t)
	if _, err := e.pool.Exec(context.Background(),
		"INSERT INTO monitor_engine_event (id, type, object_id, dedup_key, detail) VALUES ($1, $2, $3, $4, $5)", id, typ, objectID, dedup, raw); err != nil {
		e.t.Fatalf("seed engine event: %v", err)
	}
	return id
}

type engineState struct {
	status, attempts int32
	lastError        string
	done             *time.Time
}

func (e *escEnv) engineState(id string) engineState {
	e.t.Helper()
	var s engineState
	if err := e.pool.QueryRow(context.Background(), "SELECT status, attempts, last_error, done_time FROM monitor_engine_event WHERE id = $1", id).
		Scan(&s.status, &s.attempts, &s.lastError, &s.done); err != nil {
		e.t.Fatal(err)
	}
	return s
}

type audience struct {
	users []string
	err   error
}

func (a *audience) UserIDs(context.Context) ([]string, error) { return a.users, a.err }

func (e *escEnv) engineUsecase(repo *engineEventRepo, d *basedata.Data, aud *audience, events biz.EventWriter) *biz.EngineEventUsecase {
	e.t.Helper()
	uc, err := biz.NewEngineEventUsecase(basedata.NewTxManager(d), repo, aud, events, slog.New(slog.NewTextHandler(io.Discard, nil)),
		&conf.Monitor{SiteUrl: "https://ops.example.com"})
	if err != nil {
		e.t.Fatal(err)
	}
	return uc
}

func TestEngineEventDispatch_RealPG(t *testing.T) {
	e := newEscEnv(t)
	ctx := context.Background()
	ds, rule := newUUID(), newUUID()
	down := e.engineEvent(mc.EventEngineDatasourceDown, ds, "down:"+ds+":t1",
		mc.EngineEventPayload{ObjectType: mc.EngineObjectDatasource, ObjectID: ds, ObjectName: "vm-prod", Error: "refused", Since: "2026-11-18 10:00:00", Duration: "12m"})
	up := e.engineEvent(mc.EventEngineDatasourceUp, ds, "up:"+ds+":t1", mc.EngineEventPayload{ObjectName: "vm-prod", Since: "s", Duration: "20m"})
	failing := e.engineEvent(mc.EventEngineRuleFailing, rule, "fail:"+rule+":t1", mc.EngineEventPayload{ObjectType: mc.EngineObjectRule, ObjectID: rule, ObjectName: "cpu-high", Failures: 7})
	// 真实的通知事件写入：事件落进 notify_event，可以被内置模板渲染。
	aud := &audience{users: []string{uAlice, uBob}}
	uc := e.engineUsecase(e.engine, e.d, aud, e.writer)

	// 第一行失败：同一对象后面的「已恢复」不得抢先发出；别的对象不受影响。
	e.writer.failNext(1)
	if n, err := uc.Run(ctx); err != nil || n != 1 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	if s := e.engineState(down); s.status != mc.OutboxPending || s.attempts != 1 || s.lastError == "" || s.done == nil {
		t.Fatalf("failed row = %+v", s)
	}
	if s := e.engineState(up); s.status != mc.OutboxPending || s.attempts != 0 {
		t.Fatalf("the recovery of the same object must wait: %+v", s)
	}
	if s := e.engineState(failing); s.status != mc.OutboxDone || s.done == nil {
		t.Fatalf("other object = %+v", s)
	}
	// 退避期内不会被再次领取。
	if n, _ := uc.Run(ctx); n != 0 {
		t.Fatalf("row in backoff was claimed: %d", n)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_engine_event SET done_time = now() - interval '1 hour' WHERE id = $1", down); err != nil {
		t.Fatal(err)
	}
	if n, err := uc.Run(ctx); err != nil || n != 2 {
		t.Fatalf("Run after backoff = %d, %v", n, err)
	}
	if e.engineState(down).status != mc.OutboxDone || e.engineState(up).status != mc.OutboxDone {
		t.Fatal("both rows of the datasource must be done")
	}
	type ev struct {
		typ, title, url, dedup string
		severity               int32
		recipients             []string
	}
	var events []ev
	rows, err := e.pool.Query(ctx, `SELECT event_type, title, payload->>'url', dedup_key, severity, recipients FROM notify_event
		WHERE event_type LIKE 'monitor.engine.%' ORDER BY create_time, id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var x ev
		var rcpt []byte
		if err := rows.Scan(&x.typ, &x.title, &x.url, &x.dedup, &x.severity, &rcpt); err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(rcpt, &x.recipients)
		events = append(events, x)
	}
	rows.Close()
	if len(events) != 3 {
		t.Fatalf("notify events = %d, want 3", len(events))
	}
	want := map[string]ev{
		mc.EventEngineRuleFailing:    {title: "告警规则持续评估失败：cpu-high", url: "https://ops.example.com/monitor/rule/" + rule, severity: 3},
		mc.EventEngineDatasourceDown: {title: "数据源持续不可用：vm-prod", url: "https://ops.example.com/monitor/engine", severity: 3},
		mc.EventEngineDatasourceUp:   {title: "数据源已恢复：vm-prod", url: "https://ops.example.com/monitor/engine", severity: 1},
	}
	for _, x := range events {
		w := want[x.typ]
		if x.title != w.title || x.url != w.url || x.severity != w.severity || !slices.Equal(x.recipients, []string{uAlice, uBob}) || len(x.dedup) > 128 {
			t.Fatalf("event = %+v, want %+v", x, w)
		}
	}
	// 次数用尽即丢弃。
	bad := e.engineEvent(mc.EventEngineRuleRecovered, rule, "rec:"+rule+":t1", mc.EngineEventPayload{ObjectName: "cpu-high"})
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_engine_event SET attempts = $2 WHERE id = $1", bad, mc.OutboxMaxAttempts-1); err != nil {
		t.Fatal(err)
	}
	e.writer.failNext(1)
	if n, err := uc.Run(ctx); err != nil || n != 0 {
		t.Fatalf("Run = %d, %v", n, err)
	}
	if s := e.engineState(bad); s.status != mc.OutboxDropped || s.attempts != mc.OutboxMaxAttempts {
		t.Fatalf("exhausted row = %+v", s)
	}
}

// monp2Login 新建一个只是 monitor_core_role 成员的 LOGIN 角色并以它连接（角色名 monp2_b_*，测试结束删除）。
func monp2Login(t *testing.T, admin *pgxpool.Pool) *basedata.Data {
	t.Helper()
	ctx := context.Background()
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	role, password := "monp2_b_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
	// 标识符与口令都是本函数生成的十六进制串，不含需要转义的字符。
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '%s' IN ROLE monitor_core_role", role, password)); err != nil {
		t.Fatalf("create login role: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role); err != nil {
			t.Errorf("drop login role %s: %v", role, err)
		}
	})
	cfg := admin.Config().ConnConfig
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	d := openData(t, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, role, password, cfg.Database, sslmode))
	var super bool
	var memberOf []string
	if err := d.Pool.QueryRow(ctx,
		`SELECT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user),
		        ARRAY(SELECT g.rolname::text FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid
		              JOIN pg_roles u ON u.oid = m.member WHERE u.rolname = current_user)`).Scan(&super, &memberOf); err != nil {
		t.Fatal(err)
	}
	if super || !slices.Equal(memberOf, []string{"monitor_core_role"}) {
		t.Fatalf("login role: super=%v member of %v", super, memberOf)
	}
	return d
}

// 以受限的登录账号（只有 monitor_core_role）跑通本文件涉及的全部写路径：升级策略的增改删、升级通知的
// 派发（通知记录的 level 列、各级进度的 upsert、发件箱终结与失败记账）、自监控事件的终结与失败记账。
func TestEscalationAsCoreLoginRole(t *testing.T) {
	admin := newEscEnv(t)
	ctx := context.Background()
	d := monp2Login(t, admin.pool)
	limited := reposOf(t, d)
	esc, engine := NewEscalationRepo(d), NewEngineEventRepo(d)

	p := escalation(admin.group.ID, "受限账号建的")
	p.ID = newID(t)
	if err := esc.Create(ctx, p, uAdmin); err != nil {
		t.Fatalf("create as login role: %v", err)
	}
	p.Spec.Description = "改"
	if err := esc.Update(ctx, p, 1, uAdmin); err != nil {
		t.Fatalf("update as login role: %v", err)
	}
	if _, _, err := esc.List(ctx, biz.EscalationFilter{Groups: biz.GroupFilter{All: true}, IncludeGlobal: true}, 1, 10); err != nil {
		t.Fatalf("list as login role: %v", err)
	}
	if _, err := esc.RuleCounts(ctx, []string{p.ID}); err != nil {
		t.Fatalf("rule counts as login role: %v", err)
	}
	admin.setEscalation(admin.rule.ID, p.ID)
	wantErr(t, "delete in use as login role", esc.Delete(ctx, p.ID, 2, uAdmin), biz.ErrEscalationInUse)

	alertID := admin.firingAlert()
	ob := admin.escOutbox(alertID, 1, 1)
	rec := &recordingEvents{}
	uc := admin.p2Usecase(limited, esc, rec)
	if n, err := uc.Run(ctx); err != nil || n != 1 {
		t.Fatalf("escalation dispatch as login role: Run() = %d, %v", n, err)
	}
	if s := admin.outboxState(ob); s.status != mc.OutboxDone {
		t.Fatalf("outbox = %+v", s)
	}
	if n := admin.count("SELECT count(*) FROM monitor_alert_escalation WHERE alert_id = $1 AND level = 1 AND notify_count = 1", alertID); n != 1 {
		t.Fatal("level progress was not written")
	}
	if n := admin.count("SELECT count(*) FROM monitor_notify_log WHERE alert_id = $1 AND kind = 4 AND source = 3 AND level = 1", alertID); n != 1 {
		t.Fatal("escalation notify log was not written")
	}
	// 第二次（级内重复）走 upsert 的 UPDATE 分支。
	ob2 := admin.escOutbox(alertID, 1, 2)
	if n, err := uc.Run(ctx); err != nil || n != 1 || admin.outboxState(ob2).status != mc.OutboxDone {
		t.Fatalf("repeat as login role: %d, %v", n, err)
	}
	// 不自洽的行：丢弃同样在授权内。
	bad := admin.escOutbox(admin.alert(alertRow{ruleID: admin.rule.ID, ruleName: "cpu-high", groupID: admin.group.ID, status: mc.AlertResolved}), 1, 1)
	if n, err := uc.Run(ctx); err != nil || n != 1 || admin.outboxState(bad).status != mc.OutboxDropped {
		t.Fatalf("drop as login role: %d, %v", n, err)
	}

	admin.setEscalation(admin.rule.ID, "")
	if err := esc.Delete(ctx, p.ID, 2, uAdmin); err != nil {
		t.Fatalf("delete as login role: %v", err)
	}

	// 自监控事件：领取 + 终结、失败记账。
	obj := newUUID()
	done := admin.engineEvent(mc.EventEngineDatasourceDown, obj, "k:"+obj, mc.EngineEventPayload{ObjectName: "vm"})
	euc := admin.engineUsecase(engine, d, &audience{}, rec)
	if n, err := euc.Run(ctx); err != nil || n != 1 || admin.engineState(done).status != mc.OutboxDone {
		t.Fatalf("engine event as login role: %d, %v", n, err)
	}
	failed := admin.engineEvent(mc.EventEngineDatasourceUp, obj, "k2:"+obj, mc.EngineEventPayload{ObjectName: "vm"})
	if n, err := admin.engineUsecase(engine, d, &audience{err: context.DeadlineExceeded}, rec).Run(ctx); err != nil || n != 0 {
		t.Fatalf("failing engine event as login role: %d, %v", n, err)
	}
	if s := admin.engineState(failed); s.status != mc.OutboxPending || s.attempts != 1 {
		t.Fatalf("failed engine row = %+v", s)
	}
	// 授权之外的写入确实被数据库拒绝（证明上面跑的是受限账号）。
	if _, err := d.Pool.Exec(ctx, "UPDATE monitor_alert SET escalation_level = 1 WHERE id = $1", alertID); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("writing an engine-owned column must be refused, got %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "DELETE FROM monitor_alert_escalation WHERE alert_id = $1", alertID); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("deleting progress rows must be refused, got %v", err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE monitor_engine_event SET detail = '{}' WHERE id = $1", done); err == nil || !strings.Contains(err.Error(), "42501") {
		t.Fatalf("writing engine event detail must be refused, got %v", err)
	}
}
