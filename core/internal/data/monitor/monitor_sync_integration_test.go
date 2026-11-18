//go:build integration

package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newSyncRepos 资源索引 / 工作日历 / 事件动作仓储的集成测试环境（清空这几张表）。
func newSyncRepos(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.pool.Exec(ctx, "TRUNCATE TABLE monitor_ci_index, monitor_alert_action, monitor_alert_event"); err != nil {
		t.Fatalf("truncate sync tables: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_calendar SET config = '{}' WHERE id = 1"); err != nil {
		t.Fatalf("reset calendar: %v", err)
	}
	return e
}

func ciEntry(id, name string, at time.Time) *biz.CIEntry {
	return &biz.CIEntry{
		ID: id, ModelCode: "host", Name: name, Env: "prod", Status: "active", Owner: uAlice, Tags: []string{"core", "dmz"},
		Attrs: map[string][]string{"ip": {"10.0.0.1", "10.0.0.2"}}, Valid: true, SourceUpdateTime: &at,
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// syncOnce 跑一轮同步事务。
func syncOnce(t *testing.T, r *ciIndexRepo, fn func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error)) bool {
	t.Helper()
	ran, err := r.Sync(context.Background(), fn)
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	return ran
}

func TestCIIndexRepo(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	r := NewCIIndexRepo(e.d)
	a, b, c := newID(t), newID(t), newID(t)
	t0 := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	v0 := e.cfgVersion(mc.CfgCIIndex)

	if n, last, err := r.Summary(ctx); err != nil || n != 0 || last != nil {
		t.Fatalf("empty Summary() = %d, %v, %v", n, last, err)
	}
	syncOnce(t, r, func(ctx context.Context, w biz.CIIndexWriter, _ biz.CIIndexReconciler) (int, error) {
		if hw, err := w.HighWater(ctx); err != nil || hw != nil {
			t.Fatalf("empty HighWater() = %v, %v", hw, err)
		}
		// 同一批里同一个资源出现两次：取更新时间最新的那一行。
		stale := ciEntry(a, "old-name", t0.Add(-time.Minute))
		n, err := w.Upsert(ctx, []*biz.CIEntry{stale, ciEntry(a, "web-01", t0), ciEntry(b, "db-01", t0.Add(time.Second)), ciEntry(c, "sw-01", t0)})
		if err != nil || n != 3 {
			t.Fatalf("Upsert() = %d, %v; want 3", n, err)
		}
		return n, nil
	})
	if got := e.cfgVersion(mc.CfgCIIndex); got != v0+1 {
		t.Fatalf("cfg version = %d, want %d (one bump per run)", got, v0+1)
	}
	briefs, err := r.Briefs(ctx, []string{a, b, newID(t)})
	if err != nil || len(briefs) != 2 {
		t.Fatalf("Briefs() = %v, %v", briefs, err)
	}
	if got := briefs[a]; got.Name != "web-01" || got.ModelCode != "host" || got.Env != "prod" || got.Owner != uAlice || !got.Valid ||
		!slices.Equal(got.Tags, []string{"core", "dmz"}) || !slices.Equal(got.Attrs["ip"], []string{"10.0.0.1", "10.0.0.2"}) ||
		len(got.UpstreamIDs) != 0 || got.SourceUpdateTime == nil || !got.SourceUpdateTime.Equal(t0) {
		t.Fatalf("brief = %+v", got)
	}
	// attrs 的落库形状：{属性名: [字符串…]}。
	if n := e.count(`SELECT count(*) FROM monitor_ci_index WHERE ci_id = $1 AND attrs = '{"ip":["10.0.0.1","10.0.0.2"]}'::jsonb`, a); n != 1 {
		t.Fatal("attrs must be stored as {name: [strings]}")
	}

	// 幂等：同样的内容再写一遍，一行都不写、不抬版本号、sync_time 不动。
	var before time.Time
	if err := e.pool.QueryRow(ctx, "SELECT max(sync_time) FROM monitor_ci_index").Scan(&before); err != nil {
		t.Fatal(err)
	}
	syncOnce(t, r, func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error) {
		n, err := w.Upsert(ctx, []*biz.CIEntry{ciEntry(a, "web-01", t0), ciEntry(b, "db-01", t0.Add(time.Second))})
		if err != nil || n != 0 {
			t.Fatalf("idempotent Upsert() = %d, %v; want 0", n, err)
		}
		// 更新时间只进不退：更早的时间戳不算变化。
		if n, err := w.Upsert(ctx, []*biz.CIEntry{ciEntry(a, "web-01", t0.Add(-time.Hour))}); err != nil || n != 0 {
			t.Fatalf("Upsert() with an older timestamp = %d, %v; want 0", n, err)
		}
		if n, err := rec.InvalidateExcept(ctx, []string{a, b, c}); err != nil || n != 0 {
			t.Fatalf("InvalidateExcept() keeping all = %d, %v", n, err)
		}
		if n, err := rec.SetUpstreams(ctx, map[string][]string{}); err != nil || n != 0 {
			t.Fatalf("SetUpstreams() with nothing to change = %d, %v", n, err)
		}
		hw, err := w.HighWater(ctx)
		if err != nil || hw == nil || !hw.Equal(t0.Add(time.Second)) {
			t.Fatalf("HighWater() = %v, %v", hw, err)
		}
		return 0, nil
	})
	var after time.Time
	if err := e.pool.QueryRow(ctx, "SELECT max(sync_time) FROM monitor_ci_index").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if got := e.cfgVersion(mc.CfgCIIndex); got != v0+1 || !after.Equal(before) {
		t.Fatalf("idempotent run: cfg version = %d (want %d), sync_time moved = %v", got, v0+1, !after.Equal(before))
	}

	// 变化：改名、置失效、上游；一轮只抬一次版本号。
	syncOnce(t, r, func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error) {
		renamed := ciEntry(a, "web-01-new", t0.Add(time.Minute))
		renamed.Attrs = nil
		n1, err := w.Upsert(ctx, []*biz.CIEntry{renamed})
		if err != nil || n1 != 1 {
			t.Fatalf("Upsert(renamed) = %d, %v", n1, err)
		}
		n2, err := rec.InvalidateExcept(ctx, []string{a, b})
		if err != nil || n2 != 1 {
			t.Fatalf("InvalidateExcept() = %d, %v; want 1", n2, err)
		}
		n3, err := rec.SetUpstreams(ctx, map[string][]string{a: {b, c}, b: {c}})
		if err != nil || n3 != 2 {
			t.Fatalf("SetUpstreams() = %d, %v; want 2", n3, err)
		}
		return n1 + n2 + n3, nil
	})
	if got := e.cfgVersion(mc.CfgCIIndex); got != v0+2 {
		t.Fatalf("cfg version = %d, want %d", got, v0+2)
	}
	briefs, _ = r.Briefs(ctx, []string{a, b, c})
	wantUps := []string{b, c}
	slices.Sort(wantUps)
	gotUps := slices.Clone(briefs[a].UpstreamIDs)
	slices.Sort(gotUps)
	if briefs[a].Name != "web-01-new" || len(briefs[a].Attrs) != 0 || !slices.Equal(gotUps, wantUps) || briefs[c].Valid || briefs[c].Name != "sw-01" {
		t.Fatalf("after changes: a = %+v, c = %+v", briefs[a], briefs[c])
	}
	// Upsert 不动上游。
	syncOnce(t, r, func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error) {
		n, err := w.Upsert(ctx, []*biz.CIEntry{ciEntry(a, "web-01", t0.Add(2*time.Minute))})
		if err != nil || n != 1 {
			t.Fatalf("Upsert() = %d, %v", n, err)
		}
		// 上游整体替换：不在入参里的清空。
		if n, err := rec.SetUpstreams(ctx, map[string][]string{a: {b}}); err != nil || n != 2 {
			t.Fatalf("SetUpstreams(replace) = %d, %v; want 2 (a shrinks, b cleared)", n, err)
		}
		return 1, nil
	})
	briefs, _ = r.Briefs(ctx, []string{a, b})
	if !slices.Equal(briefs[a].UpstreamIDs, []string{b}) || len(briefs[b].UpstreamIDs) != 0 {
		t.Fatalf("upstreams = %v / %v", briefs[a].UpstreamIDs, briefs[b].UpstreamIDs)
	}

	valid, err := r.ListValid(ctx, 10)
	if err != nil || len(valid) != 2 {
		t.Fatalf("ListValid() = %d rows, %v; want 2", len(valid), err)
	}
	if one, _ := r.ListValid(ctx, 1); len(one) != 1 {
		t.Fatalf("ListValid(limit 1) = %d rows", len(one))
	}
	if n, last, err := r.Summary(ctx); err != nil || n != 2 || last == nil {
		t.Fatalf("Summary() = %d, %v, %v", n, last, err)
	}
	if v, err := r.CfgVersion(ctx, mc.CfgCIIndex); err != nil || v != v0+3 {
		t.Fatalf("CfgVersion() = %d, %v; want %d", v, err, v0+3)
	}
	if v, err := r.CfgVersion(ctx, "no_such_table"); err != nil || v != 0 {
		t.Fatalf("CfgVersion(unknown) = %d, %v", v, err)
	}

	// 失败的一轮整体回滚。
	boom := errors.New("cmdb down")
	if _, err := r.Sync(ctx, func(ctx context.Context, w biz.CIIndexWriter, _ biz.CIIndexReconciler) (int, error) {
		if _, err := w.Upsert(ctx, []*biz.CIEntry{ciEntry(newID(t), "ghost", t0)}); err != nil {
			t.Fatal(err)
		}
		return 1, boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Sync() error = %v, want the fn error", err)
	}
	if n := e.count("SELECT count(*) FROM monitor_ci_index WHERE name = 'ghost'"); n != 0 || e.cfgVersion(mc.CfgCIIndex) != v0+3 {
		t.Fatal("a failed run must leave nothing behind")
	}
}

// 两个实例同时同步：只有一个在走；另一个立刻返回、不阻塞。
func TestCIIndexRepo_SyncIsExclusive(t *testing.T) {
	e := newSyncRepos(t)
	r1, r2 := NewCIIndexRepo(e.d), NewCIIndexRepo(e.d)
	inside, release := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ran, err := r1.Sync(context.Background(), func(context.Context, biz.CIIndexWriter, biz.CIIndexReconciler) (int, error) {
			close(inside)
			<-release
			return 0, nil
		})
		if err != nil || !ran {
			t.Errorf("first Sync() = %v, %v", ran, err)
		}
	}()
	<-inside
	called := false
	ran, err := r2.Sync(context.Background(), func(context.Context, biz.CIIndexWriter, biz.CIIndexReconciler) (int, error) {
		called = true
		return 0, nil
	})
	if err != nil || ran || called {
		t.Fatalf("second Sync() while the lock is held = ran %v, called %v, %v; want skipped", ran, called, err)
	}
	close(release)
	wg.Wait()
	// 锁随事务释放。
	if ran, err := r2.Sync(context.Background(), func(context.Context, biz.CIIndexWriter, biz.CIIndexReconciler) (int, error) { return 0, nil }); err != nil || !ran {
		t.Fatalf("Sync() after release = %v, %v", ran, err)
	}
}

// 真实仓储上的两个同步用例并发各跑一轮：结果一致、版本号只抬一次。
func TestCISyncUsecase_ConcurrentOnPostgres(t *testing.T) {
	e := newSyncRepos(t)
	t0 := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	src := &pgSyncSource{}
	for i := range 250 {
		src.items = append(src.items, biz.SourceCI{
			ID: newID(t), ModelCode: "host", Name: fmt.Sprintf("h-%03d", i), Status: "active",
			Attrs: map[string]any{"ip": fmt.Sprintf("10.0.%d.%d", i/250, i%250)}, UpdateTime: t0.Add(-time.Duration(i) * time.Second),
		})
	}
	src.direct = map[string][]string{src.items[0].ID: {src.items[1].ID}}
	v0 := e.cfgVersion(mc.CfgCIIndex)
	var wg sync.WaitGroup
	for range 4 {
		uc, err := biz.NewCISyncUsecase(src, NewCIIndexRepo(e.d), quietLogger(), nil)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				if _, err := uc.Run(context.Background()); err != nil {
					t.Errorf("Run() error = %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if n := e.count("SELECT count(*) FROM monitor_ci_index WHERE valid"); n != 250 {
		t.Fatalf("index rows = %d, want 250", n)
	}
	if n := e.count("SELECT count(*) FROM monitor_ci_index WHERE cardinality(upstream_ids) = 1"); n != 1 {
		t.Fatalf("rows with upstreams = %d, want 1", n)
	}
	if got := e.cfgVersion(mc.CfgCIIndex); got != v0+1 {
		t.Fatalf("cfg version = %d, want %d: concurrent and repeated runs must bump once", got, v0+1)
	}
}

// pgSyncSource 固定内容的同步来源（按更新时间倒序分页）。
type pgSyncSource struct {
	items  []biz.SourceCI
	direct map[string][]string
}

func (s *pgSyncSource) Page(_ context.Context, deleted bool, cursor string, limit int) ([]biz.SourceCI, string, error) {
	if deleted {
		return nil, "", nil
	}
	start := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &start); err != nil {
			return nil, "", err
		}
	}
	end := min(start+limit, len(s.items))
	next := ""
	if end < len(s.items) {
		next = fmt.Sprintf("%d", end)
	}
	return s.items[start:end], next, nil
}

func (s *pgSyncSource) Upstreams(context.Context) (map[string][]string, bool, error) {
	return s.direct, true, nil
}

func TestCalendarRepo(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	r := NewCalendarRepo(e.d)
	v0 := e.cfgVersion(mc.CfgCalendar)
	if snap, err := r.Snapshot(ctx); err != nil || snap.Timezone != "" || len(snap.Weekdays) != 0 {
		t.Fatalf("empty Snapshot() = %+v, %v", snap, err)
	}
	if changed, err := r.Save(ctx, nil); err != nil || changed {
		t.Fatalf("Save(nil) on an empty row = %v, %v; want no write", changed, err)
	}
	snap := &mc.CalendarSnapshot{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5}, Segments: []mc.CalendarSegment{{Start: "09:00", End: "12:00"}, {Start: "13:30", End: "18:00"}},
		Holidays: []string{"2026-10-01"}, Workdays: []string{},
	}
	if changed, err := r.Save(ctx, snap); err != nil || !changed {
		t.Fatalf("Save() = %v, %v", changed, err)
	}
	if changed, err := r.Save(ctx, snap); err != nil || changed {
		t.Fatalf("Save() of the same content = %v, %v; want no write", changed, err)
	}
	got, err := r.Snapshot(ctx)
	if err != nil || got.Timezone != "Asia/Shanghai" || !slices.Equal(got.Weekdays, snap.Weekdays) || !slices.Equal(got.Segments, snap.Segments) ||
		!slices.Equal(got.Holidays, snap.Holidays) {
		t.Fatalf("Snapshot() = %+v, %v", got, err)
	}
	if _, err := got.Compile(); err != nil {
		t.Fatalf("stored snapshot does not compile: %v", err)
	}
	if v := e.cfgVersion(mc.CfgCalendar); v != v0+1 {
		t.Fatalf("cfg version = %d, want %d", v, v0+1)
	}
	if changed, err := r.Save(ctx, nil); err != nil || !changed {
		t.Fatalf("Save(nil) = %v, %v; want the snapshot cleared", changed, err)
	}
	if n := e.count("SELECT count(*) FROM monitor_calendar WHERE id = 1 AND config = '{}'::jsonb"); n != 1 || e.cfgVersion(mc.CfgCalendar) != v0+2 {
		t.Fatal("clearing must store {} and bump the version")
	}
	// 多实例并发写同一份内容：只有一次写入。
	var wg sync.WaitGroup
	var mu sync.Mutex
	writes := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := NewCalendarRepo(e.d).Save(ctx, snap)
			if err != nil {
				t.Errorf("concurrent Save() error = %v", err)
			}
			if changed {
				mu.Lock()
				writes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if writes != 1 || e.cfgVersion(mc.CfgCalendar) != v0+3 {
		t.Fatalf("concurrent saves wrote %d times, version = %d; want 1 write", writes, e.cfgVersion(mc.CfgCalendar))
	}
}

// seedAction 以引擎的身份入队一个事件动作。
func (e *env) seedAction(alertID string, kind int32, payload string) string {
	e.t.Helper()
	id := newID(e.t)
	if _, err := e.pool.Exec(context.Background(),
		"INSERT INTO monitor_alert_action (id, alert_id, kind, payload) VALUES ($1, $2, $3, $4::jsonb)", id, alertID, kind, payload); err != nil {
		e.t.Fatalf("seed alert action: %v", err)
	}
	return id
}

type actionState struct {
	status   int32
	attempts int32
	lastErr  string
	due      bool
	done     bool
}

func (e *env) actionState(id string) actionState {
	e.t.Helper()
	var s actionState
	if err := e.pool.QueryRow(context.Background(),
		"SELECT status, attempts, last_error, next_time <= now(), done_time IS NOT NULL FROM monitor_alert_action WHERE id = $1", id).
		Scan(&s.status, &s.attempts, &s.lastErr, &s.due, &s.done); err != nil {
		e.t.Fatalf("read alert action: %v", err)
	}
	return s
}

func TestActionRepo(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	r := NewActionRepo(e.d)
	txm := basedata.NewTxManager(e.d)
	alertID := newID(t)
	id1 := e.seedAction(alertID, mc.ActionTicket, `{"template_id":"t-1","agent_user_id":"`+uAlice+`"}`)
	id2 := e.seedAction(alertID, mc.ActionAISummary, `{}`)
	// 尚未到期的不领取。
	later := e.seedAction(newID(t), mc.ActionWorkflow, `{"definition_key":"heal"}`)
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_action SET next_time = now() + interval '1 hour' WHERE id = $1", later); err != nil {
		t.Fatal(err)
	}

	got, err := r.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 2 {
		t.Fatalf("Claim() = %d, %v; want 2", len(got), err)
	}
	byID := map[string]*biz.AlertAction{}
	for _, a := range got {
		byID[a.ID] = a
	}
	if a := byID[id1]; a == nil || a.AlertID != alertID || a.Kind != mc.ActionTicket || a.Attempts != 1 || a.Payload.TemplateID != "t-1" || a.Payload.AgentUserID != uAlice {
		t.Fatalf("claimed action = %+v", byID[id1])
	}
	if s := e.actionState(id1); s.status != mc.ActionPending || s.attempts != 1 || s.due {
		t.Fatalf("leased row = %+v, want pending, attempts 1, not due", s)
	}
	// 租约期内别的实例领不到。
	if again, err := r.Claim(ctx, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("second Claim() = %d, %v; want 0", len(again), err)
	}

	// 退避重试：围栏命中才写。
	if err := r.Retry(ctx, byID[id1], time.Hour, "boom"); err != nil {
		t.Fatal(err)
	}
	if s := e.actionState(id1); s.status != mc.ActionPending || s.lastErr != "boom" || s.due {
		t.Fatalf("retried row = %+v", s)
	}
	stale := *byID[id2]
	stale.Attempts = 7
	if err := r.Retry(ctx, &stale, time.Hour, "stale"); err != nil {
		t.Fatal(err)
	}
	if s := e.actionState(id2); s.lastErr != "" {
		t.Fatalf("a stale worker must not touch the row: %+v", s)
	}

	// 终结：围栏未命中返回 false；命中后置终态并记结束时间。
	finish := func(a *biz.AlertAction, status int32, note string) (ok bool) {
		t.Helper()
		if err := txm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			ok, err = r.FinishTx(ctx, tx, a, status, note)
			return err
		}); err != nil {
			t.Fatalf("FinishTx() error = %v", err)
		}
		return ok
	}
	if finish(&stale, mc.ActionDone, "") {
		t.Fatal("FinishTx() with a stale attempts value must miss the fence")
	}
	if !finish(byID[id2], mc.ActionSkipped, "智能助手未配置模型密钥") {
		t.Fatal("FinishTx() must hit the fence")
	}
	if s := e.actionState(id2); s.status != mc.ActionSkipped || !s.done || s.lastErr != "智能助手未配置模型密钥" {
		t.Fatalf("finished row = %+v", s)
	}
	if finish(byID[id2], mc.ActionDone, "") {
		t.Fatal("a finished row must not be finished twice")
	}

	// 租约到期后重新领取：尝试次数 +1，旧的持有者从此被围栏挡住。
	if _, err := e.pool.Exec(ctx, "UPDATE monitor_alert_action SET next_time = now() - interval '1 second' WHERE id = $1", id1); err != nil {
		t.Fatal(err)
	}
	re, err := r.Claim(ctx, 10, time.Minute)
	if err != nil || len(re) != 1 || re[0].ID != id1 || re[0].Attempts != 2 {
		t.Fatalf("re-Claim() = %+v, %v", re, err)
	}
	if finish(byID[id1], mc.ActionDone, "") {
		t.Fatal("the previous holder must be fenced off after a re-claim")
	}
	if !finish(re[0], mc.ActionFailed, "gave up") {
		t.Fatal("the new holder must be able to finish")
	}
}

// 多个实例并发领取：每个动作恰好被领取一次。
func TestActionRepo_ConcurrentClaim(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	const total = 60
	for range total {
		e.seedAction(newID(t), mc.ActionAISummary, `{}`)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := NewActionRepo(e.d)
			for {
				got, err := r.Claim(ctx, 7, time.Minute)
				if err != nil {
					t.Errorf("Claim() error = %v", err)
					return
				}
				if len(got) == 0 {
					return
				}
				if len(got) > 7 {
					t.Errorf("Claim() returned %d rows, limit is 7", len(got))
				}
				mu.Lock()
				for _, a := range got {
					seen[a.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct actions, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("action %s claimed %d times", id, n)
		}
	}
}

// monp2LoginPool 新建一个只属于 monitor_core_role 的登录角色（名字带 monp2_c_ 前缀），以它的身份连接。
func monp2LoginPool(t *testing.T, admin *pgxpool.Pool) *basedata.Data {
	t.Helper()
	ctx := context.Background()
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	role, password := "monp2_c_"+hex.EncodeToString(buf[:4]), hex.EncodeToString(buf[4:])+"Aa1"
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
	var current string
	var super bool
	if err := d.Pool.QueryRow(ctx, "SELECT current_user::text, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)").Scan(&current, &super); err != nil {
		t.Fatal(err)
	}
	if current != role || super {
		t.Fatalf("connected as %s super=%v, want the plain login role", current, super)
	}
	return d
}

// 资源索引、工作日历、事件动作的全部语句以受限账号（只是 monitor_core_role 的成员）执行一遍，并确认
// 列级授权之外的写入被数据库拒绝。
func TestSyncReposAsCoreLoginRole(t *testing.T) {
	e := newSyncRepos(t)
	ctx := context.Background()
	d := monp2LoginPool(t, e.pool)
	t0 := time.Now().Add(-time.Minute).Truncate(time.Microsecond)

	// 资源索引：同步锁、读水位、写入、置失效、上游、配置版本 +1、读取。
	idx := NewCIIndexRepo(d)
	a, b := newID(t), newID(t)
	v0 := e.cfgVersion(mc.CfgCIIndex)
	if ran, err := idx.Sync(ctx, func(ctx context.Context, w biz.CIIndexWriter, rec biz.CIIndexReconciler) (int, error) {
		if _, err := w.HighWater(ctx); err != nil {
			return 0, err
		}
		n, err := w.Upsert(ctx, []*biz.CIEntry{ciEntry(a, "web-01", t0), ciEntry(b, "db-01", t0)})
		if err != nil {
			return 0, err
		}
		m, err := rec.InvalidateExcept(ctx, []string{a})
		if err != nil {
			return 0, err
		}
		k, err := rec.SetUpstreams(ctx, map[string][]string{a: {b}})
		return n + m + k, err
	}); err != nil || !ran {
		t.Fatalf("ci index sync as login role = %v, %v", ran, err)
	}
	if got := e.cfgVersion(mc.CfgCIIndex); got != v0+1 {
		t.Fatalf("cfg version = %d, want %d", got, v0+1)
	}
	if briefs, err := idx.Briefs(ctx, []string{a, b}); err != nil || len(briefs) != 2 || briefs[b].Valid || !slices.Equal(briefs[a].UpstreamIDs, []string{b}) {
		t.Fatalf("Briefs() as login role = %v, %v", briefs, err)
	}
	if _, err := idx.ListValid(ctx, 10); err != nil {
		t.Fatalf("ListValid() as login role: %v", err)
	}
	if _, _, err := idx.Summary(ctx); err != nil {
		t.Fatalf("Summary() as login role: %v", err)
	}
	if _, err := idx.CfgVersion(ctx, mc.CfgCIIndex); err != nil {
		t.Fatalf("CfgVersion() as login role: %v", err)
	}

	// 工作日历。
	cal := NewCalendarRepo(d)
	snap := &mc.CalendarSnapshot{Timezone: "UTC", Weekdays: []int32{1}, Segments: []mc.CalendarSegment{{Start: "09:00", End: "18:00"}}, Holidays: []string{}, Workdays: []string{}}
	if changed, err := cal.Save(ctx, snap); err != nil || !changed {
		t.Fatalf("calendar Save() as login role = %v, %v", changed, err)
	}
	if got, err := cal.Snapshot(ctx); err != nil || got.Timezone != "UTC" {
		t.Fatalf("calendar Snapshot() as login role = %+v, %v", got, err)
	}

	// 事件动作：领取、退避、终结都只动获准的列。
	alertID := newID(t)
	id1 := e.seedAction(alertID, mc.ActionAISummary, `{}`)
	id2 := e.seedAction(alertID, mc.ActionTicket, `{}`)
	actions := NewActionRepo(d)
	got, err := actions.Claim(ctx, 10, time.Minute)
	if err != nil || len(got) != 2 {
		t.Fatalf("Claim() as login role = %d, %v", len(got), err)
	}
	for _, act := range got {
		if act.ID == id1 {
			if err := actions.Retry(ctx, act, time.Minute, "retry"); err != nil {
				t.Fatalf("Retry() as login role: %v", err)
			}
			continue
		}
		var ok bool
		if err := basedata.NewTxManager(d).WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			ok, err = actions.FinishTx(ctx, tx, act, mc.ActionDone, "")
			return err
		}); err != nil || !ok {
			t.Fatalf("FinishTx() as login role = %v, %v", ok, err)
		}
	}
	if s := e.actionState(id1); s.lastErr != "retry" || s.status != mc.ActionPending {
		t.Fatalf("retried row = %+v", s)
	}
	if s := e.actionState(id2); s.status != mc.ActionDone || !s.done {
		t.Fatalf("finished row = %+v", s)
	}

	// 授权之外的写入必须被拒绝：core 不能入队 / 删除动作、不能改动作的内容、不能删资源索引之外的运行表。
	for what, sql := range map[string]string{
		"insert alert action":    "INSERT INTO monitor_alert_action (id, alert_id, kind) VALUES ('" + newID(t) + "', '" + alertID + "', 1)",
		"delete alert action":    "DELETE FROM monitor_alert_action WHERE id = '" + id1 + "'",
		"rewrite action payload": "UPDATE monitor_alert_action SET payload = '{}' WHERE id = '" + id1 + "'",
		"rewrite action kind":    "UPDATE monitor_alert_action SET kind = 2 WHERE id = '" + id1 + "'",
		"write engine instance":  "INSERT INTO monitor_engine_instance (instance) VALUES ('x')",
		"update alert event":     "UPDATE monitor_alert_event SET detail = '{}'",
		"delete alert event":     "DELETE FROM monitor_alert_event",
	} {
		if _, err := d.Pool.Exec(ctx, sql); err == nil {
			t.Errorf("%s must be refused for the core login role", what)
		}
	}
}
