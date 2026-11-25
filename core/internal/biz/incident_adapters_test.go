package biz

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizincident "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// incidentStubLookup 监控域告警批量读桩：CountFiring 与真实仓储同口径（告警中且未被屏蔽 / 抑制）。
type incidentStubLookup struct {
	byID      map[string]*bizmonitor.Alert
	listCalls [][]string
	listErr   error
}

func (l *incidentStubLookup) ListByIDs(_ context.Context, ids []string) ([]*bizmonitor.Alert, error) {
	l.listCalls = append(l.listCalls, slices.Clone(ids))
	if l.listErr != nil {
		return nil, l.listErr
	}
	out := make([]*bizmonitor.Alert, 0, len(ids))
	for _, id := range ids {
		if a := l.byID[id]; a != nil {
			out = append(out, a)
		}
	}
	return out, nil
}

func (l *incidentStubLookup) CountFiring(_ context.Context, ids []string) (int, error) {
	n := 0
	for _, id := range ids {
		if a := l.byID[id]; a != nil && a.Status == mc.AlertFiring && !a.Muted && !a.Inhibited {
			n++
		}
	}
	return n, nil
}

// 「关联告警是否全部恢复」：仍在告警中的都算未恢复，被屏蔽 / 抑制的也算；已恢复 / 已关闭 / 已不存在的不算。
func TestIncidentAlertsAnyUnresolved(t *testing.T) {
	t.Parallel()
	const resolved, closed = int32(2), int32(3)
	for _, tc := range []struct {
		name      string
		alerts    []*bizmonitor.Alert
		want      bool
		wantLists int
	}{
		{"nothing linked", nil, false, 0},
		{"all resolved or closed", []*bizmonitor.Alert{{ID: "a1", Status: resolved}, {ID: "a2", Status: closed, Muted: true}}, false, 1},
		// 有未屏蔽的告警中事件：一条轻量计数就能下结论，不再读告警。
		{"plain firing", []*bizmonitor.Alert{{ID: "a1", Status: resolved}, {ID: "a2", Status: mc.AlertFiring}}, true, 0},
		{"firing but muted", []*bizmonitor.Alert{{ID: "a1", Status: resolved}, {ID: "a2", Status: mc.AlertFiring, Muted: true}}, true, 1},
		{"firing but inhibited", []*bizmonitor.Alert{{ID: "a1", Status: mc.AlertFiring, Inhibited: true}, {ID: "a2", Status: resolved}}, true, 1},
	} {
		lookup := &incidentStubLookup{byID: map[string]*bizmonitor.Alert{}}
		ids := []string{"gone"}
		for _, a := range tc.alerts {
			lookup.byID[a.ID] = a
			ids = append(ids, a.ID)
		}
		if tc.alerts == nil {
			ids = nil
		}
		got, err := incidentAlerts{lookup: lookup}.AnyUnresolved(context.Background(), ids)
		if err != nil || got != tc.want {
			t.Fatalf("%s: AnyUnresolved() = %v, %v; want %v", tc.name, got, err, tc.want)
		}
		if len(lookup.listCalls) != tc.wantLists {
			t.Fatalf("%s: alert reads = %d, want %d", tc.name, len(lookup.listCalls), tc.wantLists)
		}
	}
}

// 关联告警很多时分批读取，遇到第一条仍在告警中的即停；读取失败原样返回。
func TestIncidentAlertsAnyUnresolvedReadsInChunks(t *testing.T) {
	t.Parallel()
	lookup := &incidentStubLookup{byID: map[string]*bizmonitor.Alert{}}
	ids := make([]string, 0, 3*unresolvedChunk)
	for i := range 3 * unresolvedChunk {
		id := fmt.Sprintf("a%d", i)
		ids = append(ids, id)
		lookup.byID[id] = &bizmonitor.Alert{ID: id, Status: 2}
	}
	m := incidentAlerts{lookup: lookup}
	if got, err := m.AnyUnresolved(context.Background(), ids); err != nil || got {
		t.Fatalf("AnyUnresolved() = %v, %v; want false", got, err)
	}
	if len(lookup.listCalls) != 3 || len(lookup.listCalls[0]) != unresolvedChunk {
		t.Fatalf("alert reads = %d (first of %d), want 3 chunks of %d", len(lookup.listCalls), len(lookup.listCalls[0]), unresolvedChunk)
	}

	lookup.listCalls = nil
	lookup.byID[ids[unresolvedChunk+1]] = &bizmonitor.Alert{ID: ids[unresolvedChunk+1], Status: mc.AlertFiring, Muted: true}
	if got, err := m.AnyUnresolved(context.Background(), ids); err != nil || !got || len(lookup.listCalls) != 2 {
		t.Fatalf("AnyUnresolved() = %v, %v after %d reads; want true, stopping at the second chunk", got, err, len(lookup.listCalls))
	}

	lookup.listErr = errors.New("boom")
	if _, err := m.AnyUnresolved(context.Background(), ids[:1]); !errors.Is(err, lookup.listErr) {
		t.Fatalf("AnyUnresolved() error = %v, want the read error", err)
	}
}

// 监控域范围解析的桩。
type incidentStubRoles struct {
	admins map[string]bool
	roles  map[string][]string
	calls  int
}

func (r *incidentStubRoles) EnabledRoleIDs(_ context.Context, userID string) (bool, []string, error) {
	r.calls++
	return r.admins[userID], r.roles[userID], nil
}

type incidentStubGrants struct {
	bizmonitor.RoleScopeRepo
	grants []*bizmonitor.RoleScope
}

func (g incidentStubGrants) ListByRoles(_ context.Context, roleIDs []string) ([]*bizmonitor.RoleScope, error) {
	var out []*bizmonitor.RoleScope
	for _, s := range g.grants {
		if slices.Contains(roleIDs, s.RoleID) {
			out = append(out, s)
		}
	}
	return out, nil
}

type incidentStubGroups struct {
	bizmonitor.GroupReader
	groups []*bizmonitor.RuleGroup
}

func (g incidentStubGroups) ListAll(context.Context) ([]*bizmonitor.RuleGroup, error) {
	return g.groups, nil
}

// Reader 一次解析范围后在本地逐条判断，口径与监控域一致：超级管理员全部可见，其余按分组授权（含上级继承）。
func TestIncidentAlertsReaderResolvesScopeOnce(t *testing.T) {
	t.Parallel()
	roles := &incidentStubRoles{admins: map[string]bool{"root": true}, roles: map[string][]string{"u1": {"r1"}}}
	scopes := bizmonitor.NewScopeUsecase(roles,
		incidentStubGrants{grants: []*bizmonitor.RoleScope{{RoleID: "r1", GroupID: "g1", Level: mc.ScopeRead}}},
		incidentStubGroups{groups: []*bizmonitor.RuleGroup{
			{ID: "g1", Path: "/g1/", Name: "基础"}, {ID: "g1a", ParentID: "g1", Path: "/g1/g1a/", Name: "数据库"}, {ID: "g2", Path: "/g2/", Name: "业务"},
		}})
	m := incidentAlerts{scopes: scopes}
	groups := []string{"g1", "g1a", "g2", "gone", ""}
	for _, tc := range []struct {
		user string
		want []bool
	}{
		{"u1", []bool{true, true, false, false, false}},
		{"root", []bool{true, true, true, true, true}},
		{"nobody", []bool{false, false, false, false, false}},
	} {
		roles.calls = 0
		can, err := m.Reader(context.Background(), tc.user)
		if err != nil {
			t.Fatalf("Reader(%s) error = %v", tc.user, err)
		}
		got := make([]bool, 0, len(groups))
		for _, g := range groups {
			got = append(got, can(&bizincident.AlertRef{ID: "a", GroupID: g}))
		}
		// 对照：逐条解析范围的判定（改写前 CanRead 的做法）。
		for i, g := range groups {
			scope, err := scopes.Resolve(context.Background(), tc.user)
			if err != nil {
				t.Fatalf("Resolve(%s) error = %v", tc.user, err)
			}
			if legacy := scope.IsAdmin() || scope.CanRead(g); legacy != got[i] {
				t.Fatalf("user %s group %q: Reader = %v, per-alert resolution = %v", tc.user, g, got[i], legacy)
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("user %s: can read = %v, want %v", tc.user, got, tc.want)
		}
		if roles.calls != 1+len(groups) { // Reader 一次 + 对照的逐条解析
			t.Fatalf("user %s: role lookups = %d, want Reader to resolve the scope exactly once", tc.user, roles.calls-len(groups))
		}
	}
}

// incidentStubCIs 资源仓储桩：Get 与数据库一样不区分 UUID 大小写、返回小写规范形式；GetByIDs 一条语句批量取，
// 顺序不保证（这里倒序返回）。
type incidentStubCIs struct {
	bizcmdb.CIRepo
	byID       map[string]*bizcmdb.CI
	getCalls   int
	batchCalls [][]string
	err        error
}

func (r *incidentStubCIs) Get(_ context.Context, id string) (*bizcmdb.CI, error) {
	r.getCalls++
	if r.err != nil {
		return nil, r.err
	}
	if ci := r.byID[strings.ToLower(id)]; ci != nil {
		return ci, nil
	}
	return nil, bizcmdb.ErrCINotFound
}

func (r *incidentStubCIs) GetByIDs(_ context.Context, ids []string) ([]*bizcmdb.CI, error) {
	r.batchCalls = append(r.batchCalls, slices.Clone(ids))
	if r.err != nil {
		return nil, r.err
	}
	var out []*bizcmdb.CI
	seen := map[string]bool{}
	for _, id := range slices.Backward(ids) {
		if ci := r.byID[strings.ToLower(id)]; ci != nil && !seen[ci.ID] {
			seen[ci.ID] = true
			out = append(out, ci)
		}
	}
	return out, nil
}

// legacyCards 改写前的 Cards：逐个 GetByID，作对照。
func legacyCards(ctx context.Context, cis *bizcmdb.CIUsecase, ids []string) ([]*bizincident.CICard, error) {
	out := make([]*bizincident.CICard, 0, min(len(ids), bizincident.MaxCardsPerLookup))
	for _, id := range ids[:min(len(ids), bizincident.MaxCardsPerLookup)] {
		ci, err := cis.GetByID(ctx, id)
		if kerrors.Is(err, bizcmdb.ErrCINotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, cardOf(ci))
	}
	return out, nil
}

// 资源卡片改为一条语句批量读取后，结果与逐个读取完全相同：按入参顺序、不存在的不在结果中、重复的 ID 各出一张、
// 大写的 ID 照样读到、至多 MaxCardsPerLookup 个（多出的不读）。
func TestIncidentCMDBCardsMatchesPerIDLookup(t *testing.T) {
	t.Parallel()
	repo := &incidentStubCIs{byID: map[string]*bizcmdb.CI{}}
	var many []string
	for i := range bizincident.MaxCardsPerLookup + 20 {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012x", i+0xa0)
		ci := &bizcmdb.CI{ID: id, Name: fmt.Sprintf("host-%d", i), ModelCode: "host", ModelName: "主机", Env: "prod", Status: "active", Attrs: map[string]any{"ip": i}}
		if i%2 == 0 {
			ci.Owner, ci.OwnerName = "u1", "张三"
		}
		if i%7 != 3 { // 每 7 个里有一个不存在
			repo.byID[id] = ci
		}
		many = append(many, id)
	}
	cis := bizcmdb.NewCIUsecase(nil, nil, nil, repo, nil, nil, nil, nil)
	m := incidentCMDB{cis: cis}
	for _, tc := range []struct {
		name string
		ids  []string
	}{
		{"empty", nil},
		{"single", many[:1]},
		{"missing only", []string{many[3]}},
		{"reversed with gaps", reversed(many[:30])},
		{"duplicates", []string{many[0], many[1], many[0], many[3], many[1]}},
		{"upper case", []string{strings.ToUpper(many[4]), many[5]}},
		{"over the cap", many},
	} {
		want, err := legacyCards(context.Background(), cis, tc.ids)
		if err != nil {
			t.Fatalf("%s: legacy lookup error = %v", tc.name, err)
		}
		repo.getCalls, repo.batchCalls = 0, nil
		got, err := m.Cards(context.Background(), tc.ids)
		if err != nil {
			t.Fatalf("%s: Cards() error = %v", tc.name, err)
		}
		if got == nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: Cards() returned %d cards, per-ID lookup %d; contents differ", tc.name, len(got), len(want))
		}
		wantBatches := 1
		if len(tc.ids) == 0 {
			wantBatches = 0
		}
		if repo.getCalls != 0 || len(repo.batchCalls) != wantBatches {
			t.Fatalf("%s: %d per-ID reads and %d batch reads, want 0 and %d", tc.name, repo.getCalls, len(repo.batchCalls), wantBatches)
		}
		if wantBatches == 1 && len(repo.batchCalls[0]) > bizincident.MaxCardsPerLookup {
			t.Fatalf("%s: batch of %d ids exceeds the cap", tc.name, len(repo.batchCalls[0]))
		}
	}

	repo.err = errors.New("boom")
	if _, err := m.Cards(context.Background(), many[:2]); !errors.Is(err, repo.err) {
		t.Fatalf("Cards() error = %v, want the repository error", err)
	}
}

func reversed(ids []string) []string {
	out := slices.Clone(ids)
	slices.Reverse(out)
	return out
}
