package biz_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// fakeCISource 资源索引同步来源的内存桩：在册与回收站两份资源，按更新时间倒序分页。
type fakeCISource struct {
	mu       sync.Mutex
	live     map[string]biz.SourceCI
	deleted  map[string]biz.SourceCI
	direct   map[string][]string
	partial  bool
	pages    int
	upCalls  int
	pageErr  error
	blockOn  chan struct{}
	entered  chan struct{}
	pageSeen []int
}

func newFakeCMDB() *fakeCISource {
	return &fakeCISource{live: map[string]biz.SourceCI{}, deleted: map[string]biz.SourceCI{}, direct: map[string][]string{}}
}

func (f *fakeCISource) put(ci biz.SourceCI) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.deleted, ci.ID)
	f.live[ci.ID] = ci
}

func (f *fakeCISource) remove(id string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ci := f.live[id]
	ci.UpdateTime = at
	delete(f.live, id)
	f.deleted[id] = ci
}

func (f *fakeCISource) purge(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.live, id)
	delete(f.deleted, id)
}

func (f *fakeCISource) Page(ctx context.Context, deleted bool, cursor string, limit int) ([]biz.SourceCI, string, error) {
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.blockOn != nil {
		select {
		case <-f.blockOn:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages++
	f.pageSeen = append(f.pageSeen, limit)
	if f.pageErr != nil {
		return nil, "", f.pageErr
	}
	src := f.live
	if deleted {
		src = f.deleted
	}
	all := make([]biz.SourceCI, 0, len(src))
	for _, ci := range src {
		all = append(all, ci)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].UpdateTime.Equal(all[j].UpdateTime) {
			return all[i].UpdateTime.After(all[j].UpdateTime)
		}
		return all[i].ID > all[j].ID
	})
	start := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "%d", &start); err != nil {
			return nil, "", err
		}
	}
	end := min(start+limit, len(all))
	next := ""
	if end < len(all) {
		next = fmt.Sprintf("%d", end)
	}
	return all[start:end], next, nil
}

func (f *fakeCISource) Upstreams(context.Context) (map[string][]string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upCalls++
	out := make(map[string][]string, len(f.direct))
	for k, v := range f.direct {
		out[k] = slices.Clone(v)
	}
	return out, !f.partial, nil
}

type syncEnv struct {
	t     *testing.T
	cmdb  *fakeCISource
	store *testfake.MonitorSyncStore
	uc    *biz.CISyncUsecase
	now   time.Time
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newSyncEnv(t *testing.T, c *conf.Monitor) *syncEnv {
	t.Helper()
	e := &syncEnv{t: t, cmdb: newFakeCMDB(), store: testfake.NewMonitorSyncStore(), now: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)}
	uc, err := biz.NewCISyncUsecase(e.cmdb, e.store.CIIndex(), quietLog(), c)
	if err != nil {
		t.Fatalf("NewCISyncUsecase() error = %v", err)
	}
	uc.SetNow(func() time.Time { return e.now })
	e.uc = uc
	return e
}

func (e *syncEnv) run() {
	e.t.Helper()
	if _, err := e.uc.Run(context.Background()); err != nil {
		e.t.Fatalf("Run() error = %v", err)
	}
}

func (e *syncEnv) version() int64 { return e.store.CfgVersionOf(mc.CfgCIIndex) }

func host(id, name string, at time.Time) biz.SourceCI {
	return biz.SourceCI{ID: id, ModelCode: "host", Name: name, Env: "prod", Status: "active", Owner: "u-1", UpdateTime: at}
}

func TestCISync_ReconcileThenIdempotent(t *testing.T) {
	e := newSyncEnv(t, &conf.Monitor{CiMatchAttrs: []string{"ip", "sn"}})
	t0 := e.now.Add(-time.Hour)
	a := host("ci-a", "web-01", t0)
	a.Tags = []string{"core", "dmz", "core"}
	a.Attrs = map[string]any{"ip": []any{"10.0.0.1", "10.0.0.2", "10.0.0.1", 5.0, map[string]any{"x": 1}}, "sn": "  SN1 ", "cpu": 8.0, "password": "******"}
	b := host("ci-b", "db-01", t0.Add(time.Minute))
	b.Status = "retired"
	b.Attrs = map[string]any{"ip": "******", "sn": nil}
	e.cmdb.put(a)
	e.cmdb.put(b)
	e.cmdb.direct = map[string][]string{"ci-a": {"ci-b"}}

	e.run()
	got := e.store.CI("ci-a")
	if got == nil || !got.Valid || got.Name != "web-01" || got.Env != "prod" || got.Owner != "u-1" || !slices.Equal(got.Tags, []string{"core", "dmz"}) {
		t.Fatalf("ci-a = %+v", got)
	}
	if len(got.Attrs) != 2 || !slices.Equal(got.Attrs["ip"], []string{"10.0.0.1", "10.0.0.2", "5"}) || !slices.Equal(got.Attrs["sn"], []string{"SN1"}) {
		t.Fatalf("ci-a attrs = %v, want only the registered attrs as string arrays", got.Attrs)
	}
	if !slices.Equal(got.UpstreamIDs, []string{"ci-b"}) || got.SourceUpdateTime == nil || !got.SourceUpdateTime.Equal(t0) {
		t.Fatalf("ci-a upstream / time = %v / %v", got.UpstreamIDs, got.SourceUpdateTime)
	}
	if rb := e.store.CI("ci-b"); rb == nil || rb.Valid || len(rb.Attrs) != 0 {
		t.Fatalf("retired ci-b = %+v, want kept but invalid, masked attr skipped", rb)
	}
	if e.version() != 1 {
		t.Fatalf("cfg version = %d, want exactly one bump for the whole run", e.version())
	}

	// 第二轮（增量）与强制对账都不该再写任何东西。
	e.run()
	e.now = e.now.Add(biz.CIReconcileInterval)
	e.run()
	if e.version() != 1 {
		t.Fatalf("cfg version after idempotent runs = %d, want 1", e.version())
	}
	if e.cmdb.upCalls != 2 {
		t.Fatalf("topology read %d times, want 2 (only on reconcile)", e.cmdb.upCalls)
	}
}

func TestCISync_IncrementalIsBoundedAndPicksUpChanges(t *testing.T) {
	e := newSyncEnv(t, nil)
	t0 := e.now.Add(-24 * time.Hour)
	for i := range 350 {
		e.cmdb.put(host(fmt.Sprintf("ci-%03d", i), fmt.Sprintf("h-%03d", i), t0.Add(time.Duration(i)*time.Minute)))
	}
	e.run()
	if e.version() != 1 || e.store.CI("ci-349") == nil {
		t.Fatalf("initial reconcile: version = %d", e.version())
	}
	base := e.cmdb.pages

	// 没有变化：增量只读回收站一页 + 在册一页（走到水位即停）。
	e.run()
	if got := e.cmdb.pages - base; got != 2 {
		t.Fatalf("idle incremental read %d pages, want 2", got)
	}
	for _, limit := range e.cmdb.pageSeen {
		if limit > biz.CIPageSize {
			t.Fatalf("page size %d exceeds the cap %d", limit, biz.CIPageSize)
		}
	}

	// 一个资源改名、一个进回收站。
	changed := host("ci-010", "renamed", e.now)
	e.cmdb.put(changed)
	e.cmdb.remove("ci-020", e.now.Add(time.Second))
	base = e.cmdb.pages
	e.run()
	if got := e.cmdb.pages - base; got != 2 {
		t.Fatalf("incremental with 2 changes read %d pages, want 2", got)
	}
	if ci := e.store.CI("ci-010"); ci.Name != "renamed" || !ci.Valid {
		t.Fatalf("ci-010 = %+v", ci)
	}
	if ci := e.store.CI("ci-020"); ci == nil || ci.Valid || ci.Name != "h-020" {
		t.Fatalf("deleted ci-020 = %+v, want the row kept and marked invalid", ci)
	}
	if e.version() != 2 {
		t.Fatalf("cfg version = %d, want 2", e.version())
	}
	// 水位随删除推进：下一轮不再重读这次删除。
	e.run()
	if e.version() != 2 {
		t.Fatalf("cfg version after a quiet run = %d, want 2", e.version())
	}

	// 恢复：回到在册，重新有效。
	e.cmdb.put(host("ci-020", "h-020", e.now.Add(time.Minute)))
	e.run()
	if ci := e.store.CI("ci-020"); !ci.Valid {
		t.Fatalf("restored ci-020 = %+v", ci)
	}
}

func TestCISync_TooManyChangesFallsBackToReconcile(t *testing.T) {
	e := newSyncEnv(t, nil)
	t0 := e.now.Add(-24 * time.Hour)
	e.cmdb.put(host("ci-old", "old", t0))
	e.run()
	// 一次改动超过增量上限的资源数：增量走不到水位，同一轮内改走对账，一个都不漏。
	n := biz.CIPageSize*biz.CIIncrementalPages + 50
	for i := range n {
		e.cmdb.put(host(fmt.Sprintf("ci-%05d", i), "bulk", e.now.Add(time.Duration(i)*time.Millisecond)))
	}
	ups := e.cmdb.upCalls
	e.run()
	if e.store.CI("ci-00000") == nil || e.store.CI(fmt.Sprintf("ci-%05d", n-1)) == nil {
		t.Fatal("bulk change was not fully synced")
	}
	if e.cmdb.upCalls != ups+1 {
		t.Fatalf("expected a reconcile (topology read), got %d reads", e.cmdb.upCalls-ups)
	}
	if e.version() != 2 {
		t.Fatalf("cfg version = %d, want 2 (one bump per run)", e.version())
	}
}

func TestCISync_ReconcileInvalidatesVanishedAndRecomputesUpstreams(t *testing.T) {
	e := newSyncEnv(t, nil)
	t0 := e.now.Add(-time.Hour)
	for _, id := range []string{"ci-a", "ci-b", "ci-c"} {
		e.cmdb.put(host(id, id, t0))
	}
	e.cmdb.direct = map[string][]string{"ci-a": {"ci-b"}, "ci-b": {"ci-c"}}
	e.run()
	if got := e.store.CI("ci-a").UpstreamIDs; !slices.Equal(got, []string{"ci-b", "ci-c"}) {
		t.Fatalf("ci-a upstreams = %v", got)
	}
	// 资源被彻底清除（回收站里也没有）且关系变了：增量看不到，对账时处理。
	e.cmdb.purge("ci-c")
	e.cmdb.direct = map[string][]string{"ci-a": {"ci-b"}}
	e.run()
	if !e.store.CI("ci-c").Valid {
		t.Fatal("an incremental run must not invalidate what it did not see")
	}
	e.now = e.now.Add(biz.CIReconcileInterval)
	e.run()
	if e.store.CI("ci-c").Valid {
		t.Fatal("reconcile must invalidate the vanished resource")
	}
	if got := e.store.CI("ci-a").UpstreamIDs; !slices.Equal(got, []string{"ci-b"}) {
		t.Fatalf("ci-a upstreams after reconcile = %v", got)
	}
	if got := e.store.CI("ci-b").UpstreamIDs; len(got) != 0 {
		t.Fatalf("ci-b upstreams after reconcile = %v, want cleared", got)
	}

	// 只读到一部分关系：不动上游（整体替换会把没读到的清空）。
	e.cmdb.partial, e.cmdb.direct = true, map[string][]string{}
	e.now = e.now.Add(biz.CIReconcileInterval)
	e.run()
	if got := e.store.CI("ci-a").UpstreamIDs; !slices.Equal(got, []string{"ci-b"}) {
		t.Fatalf("ci-a upstreams after a partial topology read = %v, want unchanged", got)
	}
}

func TestCISync_FailureRollsBackTheWholeRun(t *testing.T) {
	e := newSyncEnv(t, nil)
	e.cmdb.put(host("ci-a", "a", e.now.Add(-time.Hour)))
	e.cmdb.pageErr = errors.New("cmdb down")
	if _, err := e.uc.Run(context.Background()); err == nil {
		t.Fatal("Run() must report the source failure")
	}
	if e.store.CI("ci-a") != nil || e.version() != 0 {
		t.Fatal("a failed run must not leave partial writes or bump the version")
	}
	// 失败的那轮不算对过账：下一轮仍是对账。
	e.cmdb.pageErr = nil
	e.run()
	if e.store.CI("ci-a") == nil || e.cmdb.upCalls != 1 {
		t.Fatalf("retry after failure: ci = %v, topology reads = %d", e.store.CI("ci-a"), e.cmdb.upCalls)
	}
}

func TestCISync_ConcurrentRunsDoNotDoubleWork(t *testing.T) {
	e := newSyncEnv(t, nil)
	other, err := biz.NewCISyncUsecase(e.cmdb, e.store.CIIndex(), quietLog(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 30 {
		e.cmdb.put(host(fmt.Sprintf("ci-%02d", i), "h", e.now.Add(-time.Hour)))
	}
	e.cmdb.blockOn, e.cmdb.entered = make(chan struct{}), make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, err := e.uc.Run(context.Background())
		done <- err
	}()
	<-e.cmdb.entered // 第一个实例已持有同步锁并在读 CMDB
	if _, err := other.Run(context.Background()); err != nil {
		t.Fatalf("second instance Run() error = %v", err)
	}
	if e.cmdb.pages != 0 {
		t.Fatal("the instance without the lock must not walk the cmdb")
	}
	close(e.cmdb.blockOn)
	if err := <-done; err != nil {
		t.Fatalf("first instance Run() error = %v", err)
	}
	if e.version() != 1 || e.store.CI("ci-29") == nil {
		t.Fatalf("version = %d", e.version())
	}
	// 没抢到锁的实例之后再跑一遍（它的第一轮是对账）：结果相同，不再抬版本号。
	e.cmdb.blockOn = nil
	if _, err := other.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.version() != 1 {
		t.Fatalf("version after the second instance's own reconcile = %d, want 1", e.version())
	}
}

func TestCloseUpstreams(t *testing.T) {
	direct := map[string][]string{
		"vm": {"host"}, "host": {"switch"}, "switch": {"core"}, "core": {"edge"}, // 链：只取三层
		"x": {"y"}, "y": {"x"}, // 环
		"multi": {"host", "switch", "host"},
	}
	got := biz.CloseUpstreams(direct)
	if !slices.Equal(got["vm"], []string{"core", "host", "switch"}) {
		t.Fatalf("vm = %v, want three levels, sorted", got["vm"])
	}
	if !slices.Equal(got["x"], []string{"y"}) || !slices.Equal(got["y"], []string{"x"}) {
		t.Fatalf("cycle: x = %v, y = %v (must not contain itself)", got["x"], got["y"])
	}
	if !slices.Equal(got["multi"], []string{"core", "edge", "host", "switch"}) {
		t.Fatalf("multi = %v, want deduplicated", got["multi"])
	}
	if _, ok := got["edge"]; ok {
		t.Fatal("a resource without upstreams must not appear")
	}
	wide := map[string][]string{}
	for i := range biz.MaxUpstreams + 20 {
		wide["leaf"] = append(wide["leaf"], fmt.Sprintf("up-%03d", i))
	}
	if n := len(biz.CloseUpstreams(wide)["leaf"]); n != biz.MaxUpstreams {
		t.Fatalf("capped upstreams = %d, want %d", n, biz.MaxUpstreams)
	}
}

func TestMatchAttrs(t *testing.T) {
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'a'
	}
	got := biz.MatchAttrs(map[string]any{
		"ip": []string{"10.0.0.1", "", "10.0.0.1"}, "port": 8080.0, "up": true, "obj": map[string]any{}, "nested": []any{[]any{"x"}},
		"long": string(long), "secret": "******",
	}, []string{"ip", "port", "up", "obj", "nested", "long", "secret", "missing"})
	want := map[string][]string{"ip": {"10.0.0.1"}, "port": {"8080"}, "up": {"true"}}
	if len(got) != len(want) {
		t.Fatalf("MatchAttrs() = %v, want %v", got, want)
	}
	for k, v := range want {
		if !slices.Equal(got[k], v) {
			t.Fatalf("MatchAttrs()[%s] = %v, want %v", k, got[k], v)
		}
	}
}

func TestSyncConfigValidation(t *testing.T) {
	if got, err := biz.CIMatchAttrs(nil); err != nil || !slices.Equal(got, []string{"ip"}) {
		t.Fatalf("default attrs = %v, %v", got, err)
	}
	if got, err := biz.CIMatchAttrs(&conf.Monitor{CiMatchAttrs: []string{"ip", " sn ", "ip"}}); err != nil || !slices.Equal(got, []string{"ip", "sn"}) {
		t.Fatalf("attrs = %v, %v", got, err)
	}
	for _, bad := range []string{"IP", "1ip", "a-b", ""} {
		if _, err := biz.CIMatchAttrs(&conf.Monitor{CiMatchAttrs: []string{bad}}); err == nil {
			t.Fatalf("attr %q must be rejected", bad)
		}
	}
	rels, err := biz.TopologyRelations(nil)
	if err != nil || len(rels) != 2 || rels[0] != (biz.TopologyRelation{TypeCode: "host_runs_vm", UpstreamSrc: true}) ||
		rels[1] != (biz.TopologyRelation{TypeCode: "uplink"}) {
		t.Fatalf("default relations = %+v, %v", rels, err)
	}
	for _, bad := range [][]string{{"uplink"}, {"uplink:up"}, {"Up:src"}, {"a:src", "a:dst"}} {
		if _, err := biz.TopologyRelations(&conf.Monitor{TopologyRelations: bad}); err == nil {
			t.Fatalf("relations %v must be rejected", bad)
		}
	}
}

// fakeCalendar 工作日历来源的桩。
type fakeCalendar struct {
	snap mc.CalendarSnapshot
	ok   bool
	err  error
}

func (f *fakeCalendar) WorkCalendar(context.Context) (mc.CalendarSnapshot, bool, error) {
	return f.snap, f.ok, f.err
}

func TestCalendarSync(t *testing.T) {
	store := testfake.NewMonitorSyncStore()
	src := &fakeCalendar{}
	uc := biz.NewCalendarSyncUsecase(src, store.Calendar(), quietLog())
	ctx := context.Background()
	run := func() {
		t.Helper()
		if _, err := uc.Run(ctx); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	}
	// 流程域没有日历：保持空快照，不抬版本号。
	run()
	if v := store.CfgVersionOf(mc.CfgCalendar); v != 0 {
		t.Fatalf("version = %d, want 0", v)
	}
	src.ok = true
	src.snap = mc.CalendarSnapshot{
		Timezone: "Asia/Shanghai", Weekdays: []int32{1, 2, 3, 4, 5}, Segments: []mc.CalendarSegment{{Start: "09:00", End: "18:00"}},
		Holidays: []string{"2026-10-01"}, Workdays: []string{},
	}
	run()
	run()
	got, _ := store.Calendar().Snapshot(ctx)
	if got.Timezone != "Asia/Shanghai" || len(got.Segments) != 1 || store.CfgVersionOf(mc.CfgCalendar) != 1 {
		t.Fatalf("snapshot = %+v, version = %d (want one bump for one change)", got, store.CfgVersionOf(mc.CfgCalendar))
	}
	// 日历变成契约校验通不过的内容：写回空快照（引擎按没有日历处理）。
	src.snap.Timezone = "Nowhere/None"
	run()
	if got, _ := store.Calendar().Snapshot(ctx); got.Timezone != "" || store.CfgVersionOf(mc.CfgCalendar) != 2 {
		t.Fatalf("snapshot after an invalid calendar = %+v, version = %d", got, store.CfgVersionOf(mc.CfgCalendar))
	}
	src.err = errors.New("workflow down")
	if _, err := uc.Run(ctx); err == nil {
		t.Fatal("Run() must report the source failure")
	}
}
