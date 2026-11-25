package biz

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// CIUsecase 单测用的最小内存仓储：只实现被测路径用到的方法，未实现的方法沿用嵌入的 nil 接口
// （被调用即 panic，说明测试走到了未预期 / 需要补桩的路径）。数据面语义以 testfake 与集成测试为准。

type memModelRepo struct {
	ModelRepo
	models map[string]*Model
}

func (r *memModelRepo) Get(_ context.Context, code string) (*Model, error) {
	if m, ok := r.models[code]; ok {
		cp := *m
		return &cp, nil
	}
	return nil, ErrModelNotFound
}

type memFieldRepo struct {
	FieldRepo
	byModel map[string][]*Field
}

func (r *memFieldRepo) ListByModel(_ context.Context, model string) ([]*Field, error) {
	return r.byModel[model], nil
}

type memRelTypeRepo struct {
	RelationTypeRepo
	types []*RelationType
}

func (r *memRelTypeRepo) List(_ context.Context, modelCode string) ([]*RelationType, error) {
	if modelCode == "" {
		return r.types, nil
	}
	var out []*RelationType
	for _, t := range r.types {
		if t.SrcModel == ModelAny || t.SrcModel == modelCode || t.DstModel == ModelAny || t.DstModel == modelCode {
			out = append(out, t)
		}
	}
	return out, nil
}

func (r *memRelTypeRepo) Get(_ context.Context, code string) (*RelationType, error) {
	for _, t := range r.types {
		if t.Code == code {
			return t, nil
		}
	}
	return nil, ErrRelationTypeNotFound
}

type memRelationRepo struct {
	RelationRepo
	mu      sync.Mutex
	rels    map[RelationKey]int64 // (type, direction) → 条数，CountByCI 用
	exists  map[string]bool       // "type|src|dst" → 是否存在
	relList []*memRelation
	// scoped 为真时 scopedRepo 返回带 ScopedRelationRepo 能力的视图。
	scoped bool
	// 最近一次按范围计数 / 展开收到的范围参数（由 scopedMemRelationRepo 记录；不带范围的
	// Neighbors 调用把拓扑那一组清空，便于断言「没有下推范围」）。
	gotCountModels, gotCountEnvs []string
	gotNodesModels, gotNodesEnvs []string
}

func (r *memRelationRepo) Exists(_ context.Context, typeCode, srcID, dstID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.exists[typeCode+"|"+srcID+"|"+dstID], nil
}

func (r *memRelationRepo) CountByCI(_ context.Context, _ string) (map[RelationKey]int64, error) {
	return r.rels, nil
}

// Neighbors 全量展开：每条已登记的关系给出一个对端节点。
func (r *memRelationRepo) Neighbors(_ context.Context, ciID, _ string, _, _ int32) ([]*TopologyNode, []*TopologyEdge, bool, error) {
	r.gotNodesModels, r.gotNodesEnvs = nil, nil
	return r.expand(ciID, nil, nil)
}

// peerVisible 对端是否在范围内（nil = 该维度不限），口径同仓储的 SQL 过滤。
func peerVisible(peer *CI, modelCodes, envs []string) bool {
	if modelCodes != nil && !slices.Contains(modelCodes, peer.ModelCode) {
		return false
	}
	return envs == nil || slices.Contains(envs, peer.Env)
}

func (r *memRelationRepo) expand(ciID string, modelCodes, envs []string) ([]*TopologyNode, []*TopologyEdge, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	root := &TopologyNode{ID: ciID}
	nodes, edges := []*TopologyNode{root}, []*TopologyEdge{}
	seen := map[string]bool{ciID: true}
	for _, rel := range r.relList {
		peerID := rel.DstID
		if rel.DstID == ciID {
			peerID = rel.SrcID
		}
		if seen[peerID] || !peerVisible(rel.Peer, modelCodes, envs) {
			continue
		}
		seen[peerID] = true
		nodes = append(nodes, &TopologyNode{ID: peerID, ModelCode: rel.Peer.ModelCode, Env: rel.Peer.Env, Depth: 1})
		edges = append(edges, &TopologyEdge{ID: rel.ID, TypeCode: rel.TypeCode, SrcID: rel.SrcID, DstID: rel.DstID})
	}
	return nodes, edges, false, nil
}

// scopedMemRelationRepo 在 memRelationRepo 之上补齐 ScopedRelationRepo 能力，并记录收到的范围参数。
// 只有它实现了该能力：memRelationRepo 本身不实现，用来模拟「仓储不具备按范围过滤能力」。
type scopedMemRelationRepo struct {
	*memRelationRepo
}

func (r *scopedMemRelationRepo) CountByCIIn(_ context.Context, _ string, modelCodes, envs []string) (map[RelationKey]int64, error) {
	r.gotCountModels, r.gotCountEnvs = modelCodes, envs
	return r.rels, nil
}

func (r *scopedMemRelationRepo) NeighborsIn(_ context.Context, ciID, _ string, _, _ int32, modelCodes, envs []string) ([]*TopologyNode, []*TopologyEdge, bool, error) {
	r.gotNodesModels, r.gotNodesEnvs = modelCodes, envs
	return r.expand(ciID, modelCodes, envs)
}

func (r *memRelationRepo) scopedRepo() RelationRepo {
	if r.scoped {
		return &scopedMemRelationRepo{memRelationRepo: r}
	}
	return r
}

// memRelation 的 Peer 只供拓扑桩判定可见性。
type memRelation struct {
	Relation
	Peer *CI
}

type memCIRepo struct {
	CIRepo
	mu         sync.Mutex
	items      map[string]*CI
	exists     map[string]bool // "model|id" → ExistsInModel 的返回值
	created    []*CI
	updated    []*CI
	histories  []*History
	nextSeq    int
	lookedUpID []string // GetByIDs 收到的 ID（按调用顺序）
}

func newMemCIRepo() *memCIRepo {
	return &memCIRepo{items: map[string]*CI{}, exists: map[string]bool{}}
}

// seed 预置一条资源，返回落库后的副本。
func (r *memCIRepo) seed(ci *CI) *CI {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *ci
	cp.Attrs = cloneAttrs(ci.Attrs)
	r.items[ci.ID] = &cp
	return &cp
}

// timeForSeq 历史时间随序号递增（倒序读取时 newer 在前）。
func timeForSeq(seq int) time.Time {
	return time.Date(2026, 1, 1, 0, 0, seq, 0, time.UTC)
}

func (r *memCIRepo) Get(_ context.Context, id string) (*CI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ci, ok := r.items[id]
	if !ok {
		return nil, ErrCINotFound
	}
	cp := *ci
	cp.Attrs = cloneAttrs(ci.Attrs)
	return &cp, nil
}

func (r *memCIRepo) GetByIDs(_ context.Context, ids []string) ([]*CI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*CI, 0, len(ids))
	for _, id := range ids {
		r.lookedUpID = append(r.lookedUpID, id)
		if ci, ok := r.items[id]; ok {
			cp := *ci
			cp.Attrs = cloneAttrs(ci.Attrs)
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r *memCIRepo) GetByName(_ context.Context, modelCode, name string) (*CI, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ci := range r.items {
		if ci.ModelCode == modelCode && ci.Name == name {
			cp := *ci
			cp.Attrs = cloneAttrs(ci.Attrs)
			return &cp, nil
		}
	}
	return nil, ErrCINotFound
}

func (r *memCIRepo) ExistsInModel(_ context.Context, modelCode, id string) (bool, error) {
	return r.exists[modelCode+"|"+id], nil
}

func (r *memCIRepo) Create(_ context.Context, ci *CI, h *History) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *ci
	cp.Attrs = cloneAttrs(ci.Attrs)
	r.items[ci.ID] = &cp
	r.created = append(r.created, &cp)
	return r.record(h)
}

func (r *memCIRepo) Update(_ context.Context, ci *CI, expectedVersion int64, h *History) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.items[ci.ID]
	if !ok {
		return ErrCINotFound
	}
	if cur.Version != expectedVersion {
		return ErrCIVersionConflict
	}
	cp := *ci
	cp.Attrs = cloneAttrs(ci.Attrs)
	r.items[ci.ID] = &cp
	r.updated = append(r.updated, &cp)
	return r.record(h)
}

func (r *memCIRepo) record(h *History) error {
	if h == nil {
		return nil
	}
	r.nextSeq++
	seq := r.nextSeq
	cp := *h
	cp.Diff = cloneAttrs(h.Diff)
	cp.CreateTime = timeForSeq(seq)
	r.histories = append(r.histories, &cp)
	return nil
}

func (r *memCIRepo) historyByCI(ciID string) []*History {
	var out []*History
	for i := len(r.histories) - 1; i >= 0; i-- {
		if r.histories[i].CIID == ciID {
			out = append(out, r.histories[i])
		}
	}
	return out
}

// memHistoryRepo 历史仓储：读取 memCIRepo 记下的历史（时间倒序，不分页：单测页宽足够）。
type memHistoryRepo struct {
	HistoryRepo
	store *memCIRepo
}

func (r *memHistoryRepo) ListByCI(_ context.Context, ciID string, _, _ int64) ([]*History, int64, error) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	out := make([]*History, 0, 8)
	for _, h := range r.store.historyByCI(ciID) {
		out = append(out, cloneHistory(h))
	}
	return out, int64(len(out)), nil
}

func (r *memHistoryRepo) List(_ context.Context, _ HistoryFilter, _, _ int64) ([]*History, int64, error) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	out := make([]*History, 0, len(r.store.histories))
	for i := len(r.store.histories) - 1; i >= 0; i-- {
		out = append(out, cloneHistory(r.store.histories[i]))
	}
	return out, int64(len(out)), nil
}

// seedHistory 预置一条历史（含 ID）。
func (r *memHistoryRepo) seedHistory(h *History) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.store.nextSeq++
	r.store.histories = append(r.store.histories, cloneHistory(h))
}

func cloneHistory(h *History) *History {
	cp := *h
	cp.Diff = cloneAttrs(h.Diff)
	return &cp
}

var (
	_ HistoryRepo        = (*memHistoryRepo)(nil)
	_ ScopedRelationRepo = (*scopedMemRelationRepo)(nil)
)

// ---- 测试数据 ----

const (
	testIDRoot   = "018f3e1a-0400-7000-8000-000000000001"
	testIDHost   = "018f3e1a-0400-7000-8000-000000000002"
	testIDApp    = "018f3e1a-0400-7000-8000-000000000003"
	testIDSwitch = "018f3e1a-0400-7000-8000-000000000004"
)

// scopeHostProd 只可见 host + prod 的受限范围。
func scopeHostProd() Scope {
	return Scope{Models: map[string]struct{}{"host": {}}, Envs: map[string]struct{}{"prod": {}}}
}

// ciFixture 一套最小模型：host（ip 必填、secret 敏感、owner_note 人工、app ref→app）、app、
// switch；关系类型 host_runs_app（host → app）、host_uplink（host → switch）。
type ciFixture struct {
	cis       *memCIRepo
	rels      *memRelationRepo
	fields    *memFieldRepo
	types     *memRelTypeRepo
	repoTypes *memRelTypeRepo
}

func newCIFixture() *ciFixture {
	types := []*RelationType{
		{Code: "host_runs_app", SrcModel: "host", DstModel: "app", ForwardName: "运行", BackwardName: "运行于", Cardinality: CardinalityOneToMany},
		{Code: "host_uplink", SrcModel: "host", DstModel: "switch", ForwardName: "上联", BackwardName: "下联", Cardinality: CardinalityManyToMany},
	}
	return &ciFixture{
		cis: newMemCIRepo(),
		rels: &memRelationRepo{exists: map[string]bool{}, rels: map[RelationKey]int64{
			{TypeCode: "host_runs_app", Direction: DirectionOutgoing}: 1,
			{TypeCode: "host_uplink", Direction: DirectionOutgoing}:   1,
			{TypeCode: "host_runs_app", Direction: DirectionIncoming}: 2,
		}},
		fields: &memFieldRepo{byModel: map[string][]*Field{
			"host": {
				{Code: "ip", Type: FieldTypeIP, Required: true, Owner: FieldOwnerBoth},
				{Code: "secret", Type: FieldTypeString, Sensitive: true},
				{Code: "owner_note", Type: FieldTypeString, Owner: FieldOwnerManual},
				{Code: "app", Type: FieldTypeRef, RefModel: "app"},
			},
			"app":    {{Code: "name", Type: FieldTypeString, Required: true}},
			"switch": {{Code: "ip", Type: FieldTypeIP}},
		}},
		types:     &memRelTypeRepo{types: types},
		repoTypes: &memRelTypeRepo{types: types},
	}
}

func (f *ciFixture) usecase() *CIUsecase {
	models := &memModelRepo{models: map[string]*Model{
		"host": {Code: "host", Status: ModelStatusEnabled}, "app": {Code: "app", Status: ModelStatusEnabled}, "switch": {Code: "switch", Status: ModelStatusEnabled},
	}}
	relationUC := NewRelationUsecase(f.repoTypes, f.cis, f.rels.scopedRepo(), f.fields)
	return NewCIUsecase(models, f.fields, f.types, f.cis, f.rels.scopedRepo(), &memHistoryRepo{store: f.cis}, relationUC, nil)
}

// seedHost 一台同步托管的 prod 主机（采集源存活）。
func (f *ciFixture) seedHost() *CI {
	return f.cis.seed(&CI{
		ID: testIDHost, ModelCode: "host", Name: "h1", Status: CIStatusActive, Env: "prod",
		Source: "sync:" + testIDRoot, ExternalID: "i-h1", Version: 1, Attrs: map[string]any{"ip": "10.0.0.1", "secret": "s3", "owner_note": "n"},
	})
}

func (f *ciFixture) seedApp(env string) *CI {
	return f.cis.seed(&CI{
		ID: testIDApp, ModelCode: "app", Name: "a1", Status: CIStatusActive, Env: env, Source: SourceManual, Version: 1,
		Attrs: map[string]any{"name": "a1"},
	})
}

func linkHistory(action, model, targetID, targetName, targetModel string) *History {
	return &History{
		ID: "h-" + strings.Join([]string{action, targetID}, "-"), CIID: testIDHost, ModelCode: model, Action: action,
		Diff: map[string]any{
			"relationType": "host_runs_app", "direction": DirectionOutgoing,
			"target": map[string]any{"id": targetID, "name": targetName, "modelCode": targetModel},
		},
	}
}
