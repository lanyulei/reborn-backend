package biz

import (
	"context"
	"errors"
	"slices"
	"testing"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

func targetOf(h *History) (map[string]any, bool) {
	target, ok := h.Diff["target"].(map[string]any)
	return target, ok
}

func historyByID(items []*History, id string) *History {
	for _, h := range items {
		if h.ID == id {
			return h
		}
	}
	return nil
}

// 关联历史的 diff 带对端 id / name / modelCode：受限调用者不能借范围内资源的历史看到范围外对端。
// 对端类型不在范围内的直接抹去；环境受限时回查对端环境，取不到（已删除）或不在范围内的同样抹去；
// 不限的调用者与属性变更类历史不受影响；attrs 的敏感键照旧脱敏。
func TestMaskHistoryTargets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		devHost    = "018f3e1a-0400-7000-8000-000000000009"
		goneHost   = "018f3e1a-0400-7000-8000-0000000000aa"
		devApp     = "018f3e1a-0400-7000-8000-00000000000b"
		anyEnvHost = "018f3e1a-0400-7000-8000-00000000000c"
	)
	peer := func(id, model, name, env string) *CI {
		return &CI{ID: id, ModelCode: model, Name: name, Env: env, Source: SourceManual, Version: 1}
	}
	link := func(targetModel, targetID, name string) *History {
		h := linkHistory(ActionLink, "host", targetID, name, targetModel)
		h.ID = "link-" + targetID
		return h
	}
	unlink := linkHistory(ActionUnlink, "host", testIDSwitch, "sw-out", "switch")
	unlink.ID = "unlink-switch"
	update := &History{
		ID: "upd", CIID: testIDHost, ModelCode: "host", Action: ActionUpdate,
		Diff: map[string]any{"before": map[string]any{"attrs": map[string]any{"secret": "s3"}}, "after": map[string]any{"attrs": map[string]any{"secret": "s4"}}},
	}
	// newCase 一台同步托管的 prod 主机，以及各处的对端；返回用例、历史仓储与资源仓储。
	newCase := func() (*CIUsecase, *memHistoryRepo, *memCIRepo) {
		f := newCIFixture()
		f.seedHost()
		for _, p := range []*CI{
			peer(testIDRoot, "host", "peer-prod", "prod"), peer(devHost, "host", "peer-dev", "dev"),
			peer(testIDApp, "app", "app-prod", "prod"), peer(devApp, "app", "app-dev", "dev"), peer(anyEnvHost, "host", "peer-any", "dev"),
		} {
			f.cis.seed(p)
		}
		return f.usecase(), &memHistoryRepo{store: f.cis}, f.cis
	}
	hidden := func(t *testing.T, items []*History, ids ...string) {
		t.Helper()
		for _, id := range ids {
			if h := historyByID(items, id); h == nil {
				t.Errorf("history %s must still be returned", id)
			} else if _, ok := targetOf(h); ok {
				t.Errorf("target of history %s must be hidden: %v", id, h.Diff)
			}
		}
	}
	shown := func(t *testing.T, items []*History, id, wantName string) {
		t.Helper()
		h := historyByID(items, id)
		if h == nil {
			t.Fatalf("history %s must still be returned", id)
		}
		target, ok := targetOf(h)
		if !ok || target["name"] != wantName {
			t.Fatalf("target of history %s must be shown as %q, got %v", id, wantName, h.Diff)
		}
	}

	// 不限：对端原样返回；属性历史的敏感键脱敏。
	uc, history, _ := newCase()
	history.seedHistory(link("switch", testIDSwitch, "sw-out"))
	history.seedHistory(update)
	items, total, err := uc.History(ctx, Scope{Unrestricted: true}, testIDHost, 1, 50)
	if err != nil || total != 2 || len(items) != 2 {
		t.Fatalf("History() = %d items total=%d err=%v", len(items), total, err)
	}
	shown(t, items, "link-"+testIDSwitch, "sw-out")
	for _, side := range []string{"before", "after"} {
		m, _ := historyByID(items, "upd").Diff[side].(map[string]any)
		if attrs, _ := m["attrs"].(map[string]any); attrs["secret"] != MaskedValue {
			t.Fatalf("sensitive attr must be masked: %v", attrs)
		}
	}

	// 类型与环境都受限（host + prod）：类型范围外的 switch 直接抹去，不回查。
	uc, history, cis := newCase()
	history.seedHistory(link("switch", testIDSwitch, "sw-out"))
	items, _, err = uc.History(ctx, scopeHostProd(), testIDHost, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	hidden(t, items, "link-"+testIDSwitch)
	if len(cis.lookedUpID) != 0 {
		t.Fatal("model-out-of-scope targets must not trigger a lookup")
	}
	if h := historyByID(items, "link-"+testIDSwitch); h.Action != ActionLink || h.Diff["relationType"] != "host_runs_app" || h.Diff["direction"] != DirectionOutgoing {
		t.Fatalf("the record itself (action / relation type / direction) must be kept: %v", h.Diff)
	}

	// 环境受限：回查对端环境——prod 主机保留；dev 主机、已删除 / 不存在 / 非法 ID 的对端抹去。
	uc, history, cis = newCase()
	history.seedHistory(link("host", testIDRoot, "peer-prod"))
	history.seedHistory(link("host", devHost, "peer-dev"))
	history.seedHistory(link("host", goneHost, "peer-gone"))
	history.seedHistory(link("host", "not-a-uuid", "peer-bogus"))
	history.seedHistory(unlink)
	items, _, err = uc.History(ctx, scopeHostProd(), testIDHost, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	shown(t, items, "link-"+testIDRoot, "peer-prod")
	hidden(t, items, "link-"+devHost, "link-"+goneHost, "link-not-a-uuid", "unlink-switch")
	// 一次批量回查（3 个类型范围内的合法 ID），不是逐条。
	if len(cis.lookedUpID) != 3 {
		t.Fatalf("peers must be resolved in one batch, lookups = %v", cis.lookedUpID)
	}

	// 类型不限 + 仅 prod：prod 的 app 保留，dev 的 app 抹去。
	uc, history, _ = newCase()
	history.seedHistory(link("app", testIDApp, "app-prod"))
	history.seedHistory(link("app", devApp, "app-dev"))
	items, _, err = uc.History(ctx, Scope{ModelUnrestricted: true, Envs: map[string]struct{}{"prod": {}}}, testIDHost, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	shown(t, items, "link-"+testIDApp, "app-prod")
	hidden(t, items, "link-"+devApp)

	// 仅类型受限（环境不限）：范围内的类型不回查即保留。
	uc, history, cis = newCase()
	history.seedHistory(link("host", anyEnvHost, "peer-any"))
	items, _, err = uc.History(ctx, Scope{Models: map[string]struct{}{"host": {}}, EnvUnrestricted: true}, testIDHost, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	shown(t, items, "link-"+anyEnvHost, "peer-any")
	if len(cis.lookedUpID) != 0 {
		t.Fatal("env-unrestricted scope must not look peers up")
	}
}

// 全局审计（ListHistory 面向用户的入口）同样裁剪；系统链路的 ListHistory 不裁剪。
func TestListHistoryScoped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCIFixture()
	f.seedHost()
	history := &memHistoryRepo{store: f.cis}
	history.seedHistory(linkHistory(ActionLink, "host", testIDSwitch, "sw-out", "switch"))
	uc := f.usecase()

	items, _, err := uc.ListHistory(ctx, HistoryFilter{}, 1, 50)
	if err != nil || len(items) != 1 {
		t.Fatalf("ListHistory() = %d err=%v", len(items), err)
	}
	if target, _ := targetOf(items[0]); target["name"] != "sw-out" {
		t.Fatalf("system path must not trim targets, got %v", items[0].Diff)
	}
	items, _, err = uc.ListHistoryScoped(ctx, scopeHostProd(), HistoryFilter{ModelCodes: []string{"host"}, Envs: []string{"prod"}}, 1, 50)
	if err != nil || len(items) != 1 {
		t.Fatalf("ListHistoryScoped() = %d err=%v", len(items), err)
	}
	if _, ok := targetOf(items[0]); ok {
		t.Fatalf("scoped path must hide out-of-scope targets, got %v", items[0].Diff)
	}
}

// ref 字段：范围外的引用与不存在返回同一文案（否则成败即存在性探测）；不限的调用者沿用原判定。
func TestCheckRefs_Scope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const missing = `field "app" references a missing app resource`
	allModelsProd := Scope{Models: map[string]struct{}{"host": {}, "app": {}}, Envs: map[string]struct{}{"prod": {}}}
	for _, c := range []struct {
		name      string
		scope     Scope
		target    *CI  // 被引用的资源；nil = 不存在
		existsRow bool // ExistsInModel 的返回值：不限路径以它为准；受限路径置真，用来证明受限路径不走它
		wantErr   bool
	}{
		{"unrestricted, exists", Scope{Unrestricted: true}, &CI{ModelCode: "app", Env: "dev"}, true, false},
		{"unrestricted, missing", Scope{Unrestricted: true}, nil, false, true},
		{"unrestricted, type mismatch", Scope{Unrestricted: true}, &CI{ModelCode: "switch", Env: "dev"}, false, true},
		{"restricted, out of scope", scopeHostProd(), &CI{ModelCode: "app", Env: "prod"}, true, true},
		{"restricted, in scope", allModelsProd, &CI{ModelCode: "app", Env: "prod"}, false, false},
		{"restricted, right model wrong env", allModelsProd, &CI{ModelCode: "app", Env: "dev"}, true, true},
		{"restricted, missing", scopeHostProd(), nil, true, true},
		{"restricted, wrong model", scopeHostProd(), &CI{ModelCode: "switch", Env: "prod"}, true, true},
	} {
		f := newCIFixture()
		if c.target != nil {
			c.target.ID, c.target.Source, c.target.Version = testIDApp, SourceManual, 1
			f.cis.seed(c.target)
		}
		f.cis.exists["app|"+testIDApp] = c.existsRow
		err := f.usecase().checkRefs(ctx, c.scope, f.fields.byModel["host"], map[string]any{"app": testIDApp})
		if (err != nil) != c.wantErr {
			t.Errorf("%s: error = %v, want error %v", c.name, err, c.wantErr)
			continue
		}
		if err != nil && !errors.Is(err, ErrCIInvalid) {
			t.Errorf("%s: error = %v, want ErrCIInvalid", c.name, err)
		}
		if err != nil && kerrors.FromError(err).Message != missing {
			t.Errorf("%s: message must be indistinguishable from a missing resource, got %q", c.name, kerrors.FromError(err).Message)
		}
	}

	// 走完整创建 / 更新链路：受限用户把 ref 指向范围外资源 → 400，且不落库；引用在范围内则通过。
	f := newCIFixture()
	f.seedApp("dev")
	uc := f.usecase()
	host := &CI{ModelCode: "host", Name: "h-new", Env: "prod", Attrs: map[string]any{"ip": "10.0.0.9", "app": testIDApp}}
	if _, err := uc.Create(ctx, Actor{UserID: "u"}, scopeHostProd(), host); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("out-of-scope ref must be rejected, got %v", err)
	}
	if len(f.cis.created) != 0 {
		t.Fatal("rejected create must not write")
	}
	host = &CI{ModelCode: "host", Name: "h-new", Env: "prod", Attrs: map[string]any{"ip": "10.0.0.9", "app": testIDApp}}
	hostAndApp := Scope{Models: map[string]struct{}{"host": {}, "app": {}}, Envs: map[string]struct{}{"prod": {}, "dev": {}}}
	if _, err := uc.Create(ctx, Actor{UserID: "u"}, hostAndApp, host); err != nil {
		t.Fatalf("in-scope ref must pass: %v", err)
	}
	// 更新：把 ref 改指范围外的资源同样被拒；清空 ref 不涉及引用校验。
	created := f.cis.items[host.ID]
	if _, err := uc.Update(ctx, Actor{UserID: "u"}, scopeHostProd(), created.ID, created.Version, CIUpdate{Attrs: map[string]any{"app": testIDApp}}); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("updating to an out-of-scope ref must be rejected, got %v", err)
	}
	if _, err := uc.Update(ctx, Actor{UserID: "u"}, scopeHostProd(), created.ID, created.Version, CIUpdate{Attrs: map[string]any{"app": nil, "owner_note": "x"}}); err != nil {
		t.Fatalf("clearing a ref needs no reference check: %v", err)
	}
}

// 详情页关联统计：受限调用者只计范围内的对端（仓储具备按范围计数的能力时下推范围参数），
// 仓储不具备时不下发统计而不是退回全量；不限的调用者与从前一致。
func TestGet_ScopedRelationCounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	summaries := func(list []RelationSummary) map[string]int64 {
		out := map[string]int64{}
		for _, s := range list {
			out[s.RelationType+"/"+s.Direction] = s.Count
		}
		return out
	}
	full := map[RelationKey]int64{
		{TypeCode: "host_runs_app", Direction: DirectionOutgoing}: 1,
		{TypeCode: "host_peer", Direction: DirectionIncoming}:     2,
		{TypeCode: "host_uplink", Direction: DirectionOutgoing}:   3,
	}

	f := newCIFixture()
	// host ↔ host 的关系类型：主机同时是它的两端，统计里两个方向都会列出。
	f.types.types = append(f.types.types, &RelationType{Code: "host_peer", SrcModel: "host", DstModel: "host", ForwardName: "互备", BackwardName: "互备", Cardinality: CardinalityManyToMany})
	f.rels.scoped, f.rels.rels = true, full
	host := f.seedHost()
	uc := f.usecase()
	_, list, err := uc.Get(ctx, Scope{Unrestricted: true}, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := summaries(list); got["host_runs_app/outgoing"] != 1 || got["host_peer/incoming"] != 2 || got["host_uplink/outgoing"] != 3 {
		t.Fatalf("unrestricted counts = %v", got)
	}
	if f.rels.gotCountModels != nil {
		t.Fatal("unrestricted callers must not hit the scoped query")
	}

	_, list, err = uc.Get(ctx, scopeHostProd(), host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.rels.gotCountModels, []string{"host"}) || !slices.Equal(f.rels.gotCountEnvs, []string{"prod"}) {
		t.Fatalf("scope must be pushed down as model/env lists: %v %v", f.rels.gotCountModels, f.rels.gotCountEnvs)
	}
	if got := summaries(list); len(got) != 4 || got["host_runs_app/outgoing"] != 1 {
		t.Fatalf("summaries must still list every applicable (type, direction) with scoped counts: %v", got)
	}

	// 仓储不具备按范围计数的能力：受限调用者拿到 0，而不是全量数字；不限的调用者照旧。
	legacy := &memRelationRepo{rels: full}
	f.rels = legacy
	uc = f.usecase()
	_, list, err = uc.Get(ctx, scopeHostProd(), host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := summaries(list); len(got) != 4 || got["host_runs_app/outgoing"] != 0 || got["host_peer/incoming"] != 0 || got["host_uplink/outgoing"] != 0 {
		t.Fatalf("a repo without the scoped capability must not leak unscoped counts: %v", got)
	}
	_, list, err = uc.Get(ctx, Scope{Unrestricted: true}, host.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := summaries(list); got["host_uplink/outgoing"] != 3 {
		t.Fatalf("unrestricted callers keep the plain count: %v", got)
	}
}

// 拓扑：受限调用者的范围下推到逐层展开（仓储具备能力时），范围外邻居不再占预算；
// 仓储不具备时退回全量展开；不限的调用者与从前一致。
func TestNeighborsScoped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		devHost = "018f3e1a-0400-7000-8000-00000000000d"
		prdHost = "018f3e1a-0400-7000-8000-00000000000e"
	)
	peer := func(id, model, env string) *CI {
		return &CI{ID: id, ModelCode: model, Name: "peer-" + id[len(id)-4:], Env: env, Source: SourceManual, Version: 1}
	}
	f := newCIFixture()
	root := f.seedHost()
	rels := []*memRelation{
		{Peer: peer(testIDApp, "app", "dev")},
		{Peer: peer(testIDSwitch, "switch", "prod")},
		{Peer: peer(devHost, "host", "dev")},
		{Peer: peer(prdHost, "host", "prod")},
	}
	for i, rel := range rels {
		rel.ID, rel.TypeCode, rel.SrcID = "018f3e1a-0400-7000-8000-0000000001"+string(rune('0'+i)), "host_uplink", root.ID
		rel.DstID = rel.Peer.ID
		f.cis.seed(rel.Peer)
	}
	f.rels.relList = rels
	f.rels.scoped = true
	relUC := NewRelationUsecase(f.repoTypes, f.cis, f.rels.scopedRepo(), f.fields)

	nodes, _, _, err := relUC.NeighborsScoped(ctx, scopeHostProd(), root.ID, "", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.rels.gotNodesModels, []string{"host"}) || !slices.Equal(f.rels.gotNodesEnvs, []string{"prod"}) {
		t.Fatalf("scope must be pushed down: %v %v", f.rels.gotNodesModels, f.rels.gotNodesEnvs)
	}
	if len(nodes) != 2 || nodes[0].ID != root.ID || nodes[1].ID != prdHost {
		t.Fatalf("only the in-scope peer may be expanded (the root itself is not filtered): %+v", nodes)
	}

	if _, _, _, err := relUC.NeighborsScoped(ctx, Scope{Unrestricted: true}, root.ID, "", 2, 10); err != nil {
		t.Fatal(err)
	}
	if f.rels.gotNodesModels != nil || f.rels.gotNodesEnvs != nil {
		t.Fatalf("unrestricted callers must not push a scope: %v %v", f.rels.gotNodesModels, f.rels.gotNodesEnvs)
	}
	if _, _, _, err := relUC.Neighbors(ctx, root.ID, "", 2, 10); err != nil {
		t.Fatal(err)
	}
	if f.rels.gotNodesModels != nil || f.rels.gotNodesEnvs != nil {
		t.Fatalf("the system-facing Neighbors must not push a scope: %v %v", f.rels.gotNodesModels, f.rels.gotNodesEnvs)
	}

	// 仓储不具备按范围展开的能力：退回全量展开（范围外节点由调用方事后剔除）。
	f.rels.scoped = false
	relUC = NewRelationUsecase(f.repoTypes, f.cis, f.rels.scopedRepo(), f.fields)
	nodes, _, _, err = relUC.NeighborsScoped(ctx, scopeHostProd(), root.ID, "", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 5 || f.rels.gotNodesModels != nil {
		t.Fatalf("fallback must expand everything and leave filtering to the caller: %d nodes, scope %v", len(nodes), f.rels.gotNodesModels)
	}
	if _, _, _, err := relUC.NeighborsScoped(ctx, scopeHostProd(), "018f3e1a-0400-7000-8000-0000000000ff", "", 2, 10); !errors.Is(err, ErrCINotFound) {
		t.Fatalf("unknown root must be rejected, got %v", err)
	}
}
