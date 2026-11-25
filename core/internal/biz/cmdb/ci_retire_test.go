package biz

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	kerrors "github.com/go-kratos/kratos/v3/errors"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 退役闭包的等价性测试与基准。
//
// 参照实现 referenceRetireClosure 是改写前 retireClosure（逐类型计数 + 分页列举）的原样拷贝，冻结在这里：
// 新实现的结论一律与它比较，不再依赖生产代码里的退化路径 retireClosureByType。
// 新实现分两段——仓储一条语句取齐出边（biz.RetireGraphRepo），用例层在内存里按同样的遍历顺序重放
// （retireClosureOf）。这里不连库：取边一段由 retireRelationRepo.RetireEdges 按 stmtRetireEdges 的语义
// 模拟，真实 SQL 见 data 包的集成测试。

// retireTestType 测试用关系类型；CreateTime 决定同一资源下各类型的处理顺序（同仓储 ORDER BY create_time, code）。
type retireTestType struct {
	RelationType
	deleted bool
}

type retireTestEdge struct{ typ, src, dst string }

// retireTestGraph 一张测试图：资源（DeleteTime 非空 = 已删除）、关系类型、边。
type retireTestGraph struct {
	cis   map[string]*CI
	types []*retireTestType
	edges []retireTestEdge
}

func newRetireTestGraph() *retireTestGraph {
	return &retireTestGraph{cis: map[string]*CI{}}
}

var retireTestClock = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// addType 追加关系类型；at 为创建偏移（秒），决定处理顺序。
func (g *retireTestGraph) addType(code, src, dst, onDelete string, at int) *retireTestType {
	t := &retireTestType{RelationType: RelationType{
		Code: code, SrcModel: src, DstModel: dst, OnDelete: onDelete, Cardinality: CardinalityManyToMany,
		CreateTime: retireTestClock.Add(time.Duration(at) * time.Second),
	}}
	g.types = append(g.types, t)
	return t
}

func (g *retireTestGraph) ci(id, model, name, status, env string) *CI {
	ci := &CI{ID: id, ModelCode: model, Name: name, Status: status, Env: env, Version: 3, Attrs: map[string]any{}, Tags: []string{}}
	g.cis[id] = ci
	return ci
}

// deleted 标记资源已删除（回收站；关联列表与闭包都不再看到它）。
func (g *retireTestGraph) deleted(ci *CI) *CI {
	ci.DeleteTime = &retireTestClock
	return ci
}

func (g *retireTestGraph) edge(typ, src, dst string) {
	g.edges = append(g.edges, retireTestEdge{typ, src, dst})
}

// ciOf 取资源副本：仓储每次读都返回新对象，用例层改不到图里的数据。
func (g *retireTestGraph) ciOf(id string) *CI {
	ci := *g.cis[id]
	ci.Attrs = cloneAttrs(ci.Attrs)
	ci.Tags = slices.Clone(ci.Tags)
	return &ci
}

// stmtCounter 仓储语句计数（每个假仓储各持一份；按值嵌入，仓储以指针使用，不拷贝）。
type stmtCounter struct {
	mu    sync.Mutex
	total int
}

func (c *stmtCounter) add() {
	c.mu.Lock()
	c.total++
	c.mu.Unlock()
}

func (c *stmtCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// retireRelTypeRepo 模拟 data 层 RelationTypeRepo.List：未删除、适用（任一端匹配或通配），按 (create_time, code) 排序。
type retireRelTypeRepo struct {
	RelationTypeRepo
	g     *retireTestGraph
	calls stmtCounter
}

func (r *retireRelTypeRepo) List(_ context.Context, modelCode string) ([]*RelationType, error) {
	r.calls.add()
	return relationTypesFor(r.g.types, modelCode, false), nil
}

// relationTypesFor 未删除的关系类型里满足条件的子集，按 (create_time, code) 排序。
// anyEnd 为真取任一端匹配或通配的（RelationTypeRepo.List 的口径），否则只取以 modelCode 为源端适用的。
func relationTypesFor(types []*retireTestType, modelCode string, anyEnd bool) []*RelationType {
	var out []*RelationType
	for _, t := range types {
		if t.deleted {
			continue
		}
		if !anyEnd && t.SrcModel != ModelAny && t.SrcModel != modelCode {
			continue
		}
		if anyEnd && t.SrcModel != ModelAny && t.SrcModel != modelCode && t.DstModel != ModelAny && t.DstModel != modelCode {
			continue
		}
		cp := t.RelationType
		out = append(out, &cp)
	}
	slices.SortFunc(out, func(a, b *RelationType) int {
		if c := a.CreateTime.Compare(b.CreateTime); c != 0 {
			return c
		}
		return strings.Compare(a.Code, b.Code)
	})
	return out
}

// retireRelationRepo 模拟 data 层 relationRepo 的读取：
//   - ListRelated：口径同 listRelated（outgoing；对端未删除；先 count 再取页，total 为 0 时只有一次计数；
//     对端按 (name, id) 排序）——参照实现与退化路径走这里；
//   - batch 为真时另实现 biz.RetireGraphRepo，RetireEdges 按 stmtRetireEdges 的语义模拟一条语句取齐出边。
//
// delay 为每条 SQL 语句的固定耗时（基准用），calls 统计语句数。
type retireRelationRepo struct {
	RelationRepo
	g     *retireTestGraph
	calls stmtCounter
	delay time.Duration
	batch bool
	// bySrc 边按源端的索引（图在测试里不变，首次用到时建一次）：模拟取边不必每次全量扫描边，
	// 基准里新实现的耗时才只反映语句延迟与重放本身。
	bySrc map[string][]retireTestEdge
}

// outgoing 以 id 为源端的边。
func (r *retireRelationRepo) outgoing(id string) []retireTestEdge {
	if r.bySrc == nil {
		r.bySrc = make(map[string][]retireTestEdge, len(r.g.cis))
		for _, e := range r.g.edges {
			r.bySrc[e.src] = append(r.bySrc[e.src], e)
		}
	}
	return r.bySrc[id]
}

// stmt 记一次语句（基准下再模拟固定延迟）。
func (r *retireRelationRepo) stmt() {
	r.calls.add()
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
}

// peers 某资源在某关系类型下的出边对端（未删除），按 (name, id) 排序——同 ListRelated 的 ORDER BY。
func (r *retireRelationRepo) peers(ciID, typeCode string) []*CI {
	var out []*CI
	for _, e := range r.outgoing(ciID) {
		if e.typ == typeCode && r.g.cis[e.dst].DeleteTime == nil {
			out = append(out, r.g.ciOf(e.dst))
		}
	}
	slices.SortFunc(out, func(a, b *CI) int {
		if c := strings.Compare(a.Name, b.Name); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return out
}

// ListRelated 模拟 listRelated：count 一条语句，有结果时每页一条。
func (r *retireRelationRepo) ListRelated(_ context.Context, ciID, typeCode, direction, _ string, page, pageSize int64) ([]*RelatedCI, int64, error) {
	if direction != DirectionOutgoing {
		return nil, 0, nil
	}
	items := r.peers(ciID, typeCode)
	total := int64(len(items))
	r.stmt()
	if total == 0 {
		return nil, 0, nil
	}
	r.stmt()
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	out := make([]*RelatedCI, 0, pageSize)
	for _, ci := range items[start:min(start+pageSize, total)] {
		out = append(out, &RelatedCI{CI: ci})
	}
	return out, total, nil
}

// RetireEdges 模拟 stmtRetireEdges：walk = 根 + 沿「未删除、cascade_retire、以资源类型为源端适用」的出边
// 可达且未删除未退役的对端；取 walk 内每个资源的出边（类型未删除且以资源类型为源端适用、对端未删除），
// 行序：资源、类型 (create_time, code)、对端 (name, id)。一条语句。
func (r *retireRelationRepo) RetireEdges(_ context.Context, rootID string) ([]*RetireEdge, error) {
	r.stmt()
	g := r.g
	live := func(id string) (*CI, bool) {
		ci, ok := g.cis[id]
		return ci, ok && ci.DeleteTime == nil
	}
	// srcTypes 资源作为源端适用的未删除关系类型，按 (create_time, code) 排序（每个类型只算一次）。
	srcTypesCache := map[string][]*RelationType{}
	srcTypes := func(modelCode string) []*RelationType {
		if out, ok := srcTypesCache[modelCode]; ok {
			return out
		}
		out := relationTypesFor(g.types, modelCode, false)
		srcTypesCache[modelCode] = out
		return out
	}
	expanded := map[string]bool{rootID: true}
	for queue := []string{rootID}; len(queue) > 0; queue = queue[1:] {
		src, ok := live(queue[0])
		if !ok {
			continue
		}
		for _, e := range r.outgoing(queue[0]) {
			if expanded[e.dst] {
				continue
			}
			if dst, ok := live(e.dst); !ok || dst.Status == CIStatusRetired {
				continue
			}
			if t := findType(srcTypes(src.ModelCode), e.typ); t == nil || t.OnDelete != OnDeleteCascadeRetire {
				continue
			}
			expanded[e.dst] = true
			queue = append(queue, e.dst)
		}
	}
	var out []*RetireEdge
	ids := make([]string, 0, len(expanded))
	for id := range expanded {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		src, ok := live(id)
		if !ok {
			continue
		}
		for _, t := range srcTypes(src.ModelCode) {
			for _, ci := range r.peers(id, t.Code) {
				out = append(out, &RetireEdge{SrcID: id, TypeCode: t.Code, OnDelete: t.OnDelete, CI: ci})
			}
		}
	}
	return out, nil
}

func findType(types []*RelationType, code string) *RelationType {
	for _, t := range types {
		if t.Code == code {
			return t
		}
	}
	return nil
}

// referenceRetireClosure 改写前 retireClosure 的原样拷贝（逐类型遍历），冻结为参照实现。
// 仅把 uc.relTypes / uc.relations 换成入参，其余逐字不变。
func referenceRetireClosure(ctx context.Context, relTypes RelationTypeRepo, relations RelationRepo, scope Scope, root *CI) ([]*CI, error) {
	visited := map[string]struct{}{root.ID: {}}
	typesByModel := map[string][]*RelationType{}
	var deps []*CI
	for queue := []*CI{root}; len(queue) > 0; queue = queue[1:] {
		ci := queue[0]
		types, ok := typesByModel[ci.ModelCode]
		if !ok {
			var err error
			if types, err = relTypes.List(ctx, ci.ModelCode); err != nil {
				return nil, err
			}
			typesByModel[ci.ModelCode] = types
		}
		for _, t := range types {
			if t.SrcModel != ModelAny && t.SrcModel != ci.ModelCode {
				continue
			}
			// 逐页遍历（依赖数可能超过单页上限）。
			for page := int64(1); ; page++ {
				related, total, err := relations.ListRelated(ctx, ci.ID, t.Code, DirectionOutgoing, "", page, paging.MaxPageSize)
				if err != nil {
					return nil, err
				}
				if page == 1 && total > 0 && t.OnDelete != OnDeleteCascadeRetire {
					return nil, detail(ErrCIInUse, "%d resources linked via %s", total, t.Code)
				}
				for _, r := range related {
					if _, ok := visited[r.CI.ID]; ok {
						continue
					}
					visited[r.CI.ID] = struct{}{}
					if r.CI.Status == CIStatusRetired {
						continue
					}
					if !scope.Allows(r.CI.ModelCode, r.CI.Env) {
						return nil, ErrScopeForbidden
					}
					deps = append(deps, r.CI)
					queue = append(queue, r.CI)
				}
				if int64(page*paging.MaxPageSize) >= total {
					break
				}
			}
		}
	}
	return deps, nil
}

// sameError 两个错误是否对调用方等价：哨兵（errors.Is 按 code + reason）与文案都相同。
func sameError(a, b error) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || (errors.Is(a, b) && errors.Is(b, a) && kerrors.FromError(a).Message == kerrors.FromError(b).Message)
}

func retireIDs(deps []*CI) []string {
	out := make([]string, 0, len(deps))
	for _, ci := range deps {
		out = append(out, ci.ID)
	}
	return out
}

// retirePaths 三条路径在同一张图上的执行器：参照实现、生产实现（仓储具备批量取边能力）、
// 生产实现（退化路径：仓储只有逐类型读取）。各路径各自计数语句。
type retirePaths struct {
	g      *retireTestGraph
	scope  Scope
	root   *CI
	types  *retireRelTypeRepo
	perTyp *retireRelationRepo
	batch  *retireRelationRepo
}

func newRetirePaths(g *retireTestGraph, rootID string, scope Scope) *retirePaths {
	return &retirePaths{
		g: g, scope: scope, root: g.ciOf(rootID), types: &retireRelTypeRepo{g: g},
		perTyp: &retireRelationRepo{g: g}, batch: &retireRelationRepo{g: g, batch: true},
	}
}

// reference 参照实现（逐类型计数 + 分页列举）。
func (p *retirePaths) reference() ([]*CI, error) {
	return referenceRetireClosure(context.Background(), p.types, p.perTyp, p.scope, p.root)
}

// fallback 生产实现的退化路径（仓储不具备 RetireGraphRepo）。
func (p *retirePaths) fallback() ([]*CI, error) {
	uc := &CIUsecase{relTypes: p.types, relations: p.perTyp}
	return uc.retireClosure(context.Background(), p.scope, p.root)
}

// batched 生产实现的主路径（仓储一条语句取边）。
func (p *retirePaths) batched() ([]*CI, error) {
	uc := &CIUsecase{relations: p.batch}
	return uc.retireClosure(context.Background(), p.scope, p.root)
}

// checkClosure 断言新实现（与退化路径）和参照实现的结论完全一致；want / wantErr 另外锁住人工推演的预期。
func checkClosure(t *testing.T, p *retirePaths, want []string, wantErr error, wantMsg string) {
	t.Helper()
	ref, refErr := p.reference()
	batched, batchErr := p.batched()
	fallback, fallbackErr := p.fallback()
	if !sameError(refErr, batchErr) || !slices.Equal(retireIDs(ref), retireIDs(batched)) {
		t.Fatalf("batched closure diverges from the reference:\n reference = %v err=%v\n batched   = %v err=%v",
			retireIDs(ref), refErr, retireIDs(batched), batchErr)
	}
	if !sameError(refErr, fallbackErr) || !slices.Equal(retireIDs(ref), retireIDs(fallback)) {
		t.Fatalf("per-type fallback diverges from the reference:\n reference = %v err=%v\n fallback  = %v err=%v",
			retireIDs(ref), refErr, retireIDs(fallback), fallbackErr)
	}
	// 返回的资源带读取时的版本与状态：Transition 按它做乐观锁更新。
	for i, ci := range batched {
		if stored := p.g.cis[ci.ID]; ci.Version != stored.Version || ci.Status != stored.Status || ci.ModelCode != stored.ModelCode || ci.Env != stored.Env {
			t.Fatalf("closure item %d must carry the stored row: %+v", i, ci)
		}
	}
	if wantErr != nil {
		if !errors.Is(batchErr, wantErr) {
			t.Fatalf("error = %v, want %v", batchErr, wantErr)
		}
		if wantMsg != "" && kerrors.FromError(batchErr).Message != wantMsg {
			t.Fatalf("error message = %q, want %q", kerrors.FromError(batchErr).Message, wantMsg)
		}
		return
	}
	if batchErr != nil {
		t.Fatalf("unexpected error: %v", batchErr)
	}
	if want != nil && !slices.Equal(retireIDs(batched), want) {
		t.Fatalf("closure = %v, want %v", retireIDs(batched), want)
	}
}

// 人工构造的图形状，逐项锁住语义（每项同时也是「新旧实现结论相同」的断言）。
func TestRetireClosure_Semantics(t *testing.T) {
	t.Parallel()
	unrestricted := Scope{Unrestricted: true}

	t.Run("diamond and cycle are visited once, breadth-first", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("peer", "vm", "vm", OnDeleteCascadeRetire, 1)
		g.ci("r", "host", "r", CIStatusActive, "prod")
		g.ci("a", "vm", "a", CIStatusActive, "prod")
		g.ci("b", "vm", "b", CIStatusActive, "prod")
		g.ci("c", "vm", "c", CIStatusActive, "prod")
		g.ci("d", "vm", "d", CIStatusActive, "prod")
		g.edge("runs", "r", "a")
		g.edge("peer", "a", "b")
		g.edge("peer", "a", "c")
		g.edge("peer", "b", "d")
		g.edge("peer", "c", "d") // 菱形：d 只入闭包一次
		g.edge("peer", "d", "a") // 成环：a 已访问
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{"a", "b", "c", "d"}, nil, "")
	})

	t.Run("retired peer is skipped and not expanded", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("has", "vm", "disk", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("barrier", "vm", "v", CIStatusRetired, "")
		g.ci("under", "disk", "d", CIStatusActive, "")
		g.edge("runs", "r", "barrier")
		g.edge("has", "barrier", "under")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	t.Run("self loop", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "host", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("a", "host", "a", CIStatusActive, "")
		g.edge("runs", "r", "r")
		g.edge("runs", "r", "a")
		g.edge("runs", "a", "a")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{"a"}, nil, "")
	})

	t.Run("restrict counts retired peers too", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("uplink", "host", "switch", OnDeleteRestrict, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("sw", "switch", "s", CIStatusRetired, "")
		g.edge("uplink", "r", "sw")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), nil, ErrCIInUse, "1 resources linked via uplink")
	})

	t.Run("restrict behind a retired barrier is not checked", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("uses", "vm", "app", OnDeleteRestrict, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("barrier", "vm", "v", CIStatusRetired, "")
		g.ci("app", "app", "p", CIStatusActive, "")
		g.edge("runs", "r", "barrier")
		g.edge("uses", "barrier", "app")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	t.Run("restrict on a deep peer rejects the whole retire", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("child", "vm", "vm", OnDeleteCascadeRetire, 1)
		g.addType("uses", "vm", "app", OnDeleteRestrict, 2)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("a", "vm", "a", CIStatusActive, "")
		g.ci("b", "vm", "b", CIStatusActive, "")
		g.ci("x", "app", "x", CIStatusActive, "")
		g.ci("y", "app", "y", CIStatusActive, "")
		g.edge("runs", "r", "a")
		g.edge("child", "a", "b")
		g.edge("uses", "b", "x")
		g.edge("uses", "b", "y")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), nil, ErrCIInUse, "2 resources linked via uses")
	})

	t.Run("deleted peers are invisible", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("uplink", "host", "switch", OnDeleteRestrict, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.deleted(g.ci("vm-gone", "vm", "v", CIStatusActive, ""))
		g.deleted(g.ci("sw-gone", "switch", "s", CIStatusActive, ""))
		g.edge("runs", "r", "vm-gone")
		g.edge("uplink", "r", "sw-gone") // 已删除的对端不计数：否则会拒绝退役
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	t.Run("wildcard source type follows every model", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("relates", ModelAny, "app", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("x", "app", "x", CIStatusActive, "")
		g.ci("y", "app", "y", CIStatusActive, "")
		g.edge("relates", "r", "x")
		g.edge("relates", "x", "y")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{"x", "y"}, nil, "")
	})

	t.Run("type whose fixed source does not match is ignored", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("has", "vm", "disk", OnDeleteRestrict, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("d", "disk", "d", CIStatusActive, "")
		g.edge("has", "r", "d") // 库里建不出这种边（建关系时校验两端类型）；建得出也不跟、不计数
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	t.Run("deleted relation type is ignored", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("gone", "host", "vm", OnDeleteRestrict, 1).deleted = true
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("v", "vm", "v", CIStatusActive, "")
		g.edge("gone", "r", "v")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	t.Run("incoming edges are not followed", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("other", "host", "o", CIStatusActive, "")
		g.ci("v", "vm", "v", CIStatusActive, "")
		g.edge("runs", "other", "r")
		g.edge("runs", "other", "v")
		checkClosure(t, newRetirePaths(g, "r", unrestricted), []string{}, nil, "")
	})

	// 类型顺序 = (create_time, code)：排在前面的类型先判定，它命中的拒绝原因胜出。
	orderCase := func(cascadeAt, restrictAt int) *retirePaths {
		g := newRetireTestGraph()
		g.addType("z_casc", "host", "vm", OnDeleteCascadeRetire, cascadeAt)
		g.addType("a_block", "host", "switch", OnDeleteRestrict, restrictAt)
		g.ci("r", "host", "r", CIStatusActive, "prod")
		g.ci("sw", "switch", "s", CIStatusActive, "prod")
		g.ci("out", "vm", "o", CIStatusActive, "dev") // 环境不在范围内
		g.edge("z_casc", "r", "out")
		g.edge("a_block", "r", "sw")
		return newRetirePaths(g, "r", Scope{ModelUnrestricted: true, Envs: map[string]struct{}{"prod": {}}})
	}
	t.Run("earlier type wins: restrict", func(t *testing.T) {
		t.Parallel()
		checkClosure(t, orderCase(1, 0), nil, ErrCIInUse, "1 resources linked via a_block")
	})
	t.Run("earlier type wins: scope", func(t *testing.T) {
		t.Parallel()
		checkClosure(t, orderCase(0, 1), nil, ErrScopeForbidden, "")
	})

	t.Run("more peers than one page keep name order", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		want := make([]string, 0, 250)
		for i := 0; i < 250; i++ {
			// 名称有重名（靠 id 决胜）、有大小写：顺序完全由仓储的排序决定。
			id := fmt.Sprintf("vm-%03d", i)
			g.ci(id, "vm", fmt.Sprintf("vm-%d", (i*7919)%100), CIStatusActive, "")
			g.edge("runs", "r", id)
			want = append(want, id)
		}
		slices.SortFunc(want, func(a, b string) int {
			if c := strings.Compare(g.cis[a].Name, g.cis[b].Name); c != 0 {
				return c
			}
			return strings.Compare(a, b)
		})
		checkClosure(t, newRetirePaths(g, "r", unrestricted), want, nil, "")
	})

	t.Run("deep chain is followed to the end: no depth cap", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("child", "vm", "vm", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "")
		g.ci("c000", "vm", "c000", CIStatusActive, "")
		g.edge("runs", "r", "c000")
		want := []string{"c000"}
		for i := 1; i < 150; i++ {
			g.ci(fmt.Sprintf("c%03d", i), "vm", fmt.Sprintf("c%03d", i), CIStatusActive, "")
			g.edge("child", fmt.Sprintf("c%03d", i-1), fmt.Sprintf("c%03d", i))
			want = append(want, fmt.Sprintf("c%03d", i))
		}
		checkClosure(t, newRetirePaths(g, "r", unrestricted), want, nil, "")
	})

	t.Run("out-of-scope peer rejects the whole retire", func(t *testing.T) {
		t.Parallel()
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("has", "vm", "disk", OnDeleteCascadeRetire, 0)
		g.ci("r", "host", "r", CIStatusActive, "prod")
		g.ci("a", "vm", "a", CIStatusActive, "prod")
		g.ci("b", "vm", "b", CIStatusActive, "dev") // 环境外
		g.ci("d", "disk", "d", CIStatusActive, "prod")
		g.edge("runs", "r", "a")
		g.edge("runs", "r", "b")
		g.edge("has", "a", "d")
		scope := Scope{Models: map[string]struct{}{"host": {}, "vm": {}, "disk": {}}, Envs: map[string]struct{}{"prod": {}}}
		// b 按名称排在 a 之后：a 已入闭包，但越权的 b 让整体拒绝（不会部分退役）。
		checkClosure(t, newRetirePaths(g, "r", scope), nil, ErrScopeForbidden, "")
	})
}

// 固定种子的随机有向图（含环、自环、菱形、长链、restrict 类型、已退役 / 已删除节点、通配与已删除的关系类型、
// 随机受限范围）：新实现的退役集合与错误必须与参照实现完全相同。
func TestRetireClosure_MatchesReferenceOnRandomGraphs(t *testing.T) {
	t.Parallel()
	const seeds = 3000
	models := []string{"host", "vm", "disk", "app", "switch"}
	envs := []string{"", "prod", "dev"}
	for seed := uint64(1); seed <= seeds; seed++ {
		rnd := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // G404：固定种子的测试图，不需要密码学随机数
		g := newRetireTestGraph()
		for i := 0; i < 8; i++ {
			src, dst := models[rnd.IntN(len(models))], models[rnd.IntN(len(models))]
			if rnd.IntN(4) == 0 {
				src = ModelAny
			}
			if rnd.IntN(6) == 0 {
				dst = ModelAny
			}
			onDelete := OnDeleteCascadeRetire
			if rnd.IntN(10) < 3 {
				onDelete = OnDeleteRestrict
			}
			g.addType(fmt.Sprintf("t%d", i), src, dst, onDelete, rnd.IntN(3)).deleted = rnd.IntN(10) == 0
		}
		n := 5 + rnd.IntN(60)
		for i := 0; i < n; i++ {
			status := CIStatusActive
			switch rnd.IntN(8) {
			case 0, 1:
				status = CIStatusRetired
			case 2:
				status = CIStatusMaintenance
			}
			ci := g.ci(fmt.Sprintf("n%03d", i), models[rnd.IntN(len(models))], fmt.Sprintf("n%d", rnd.IntN(25)), status, envs[rnd.IntN(len(envs))])
			if rnd.IntN(10) == 0 {
				g.deleted(ci)
			}
		}
		for i := 0; i < 3*n; i++ {
			g.edge(fmt.Sprintf("t%d", rnd.IntN(8)), fmt.Sprintf("n%03d", rnd.IntN(n)), fmt.Sprintf("n%03d", rnd.IntN(n)))
		}
		for i := 1; i < n; i++ { // 一条长链，保证每张图都有深层节点
			if rnd.IntN(3) == 0 {
				g.edge("t0", fmt.Sprintf("n%03d", i-1), fmt.Sprintf("n%03d", i))
			}
		}
		root := g.cis[fmt.Sprintf("n%03d", rnd.IntN(n))]
		root.Status, root.DeleteTime = CIStatusActive, nil // 根在册且未退役（Transition 的状态机保证）
		scope := Scope{Unrestricted: true}
		if rnd.IntN(2) == 0 {
			scope = Scope{Models: map[string]struct{}{}, Envs: map[string]struct{}{}}
			if rnd.IntN(3) > 0 {
				for _, m := range models {
					if rnd.IntN(2) == 0 {
						scope.Models[m] = struct{}{}
					}
				}
			} else {
				scope.ModelUnrestricted = true
			}
			if rnd.IntN(3) > 0 {
				for _, e := range envs {
					if rnd.IntN(2) == 0 {
						scope.Envs[e] = struct{}{}
					}
				}
			} else {
				scope.EnvUnrestricted = true
			}
		}
		p := newRetirePaths(g, root.ID, scope)
		ref, refErr := p.reference()
		batched, batchErr := p.batched()
		if !sameError(refErr, batchErr) || !slices.Equal(retireIDs(ref), retireIDs(batched)) {
			t.Fatalf("seed %d: batched closure diverges from the reference\n graph: %d cis, %d edges, root=%s scope=%+v\n reference = %v err=%v\n batched   = %v err=%v",
				seed, len(g.cis), len(g.edges), root.ID, scope, retireIDs(ref), refErr, retireIDs(batched), batchErr)
		}
	}
}

// Transition 全链路：仓储具备 / 不具备批量取边能力时，更新顺序（依赖在前、根在后）、落库状态与历史完全一致。
func TestTransition_RetireAgreesAcrossPaths(t *testing.T) {
	t.Parallel()
	ctx, actor := context.Background(), Actor{UserID: "u"}
	models := []string{"host", "vm", "disk"}
	for seed := uint64(1); seed <= 300; seed++ {
		rnd := rand.New(rand.NewPCG(seed, 0x9e3779b9)) //nolint:gosec // G404：固定种子的测试图，不需要密码学随机数
		g := newRetireTestGraph()
		g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
		g.addType("has", "vm", "disk", OnDeleteCascadeRetire, 1)
		if seed%2 == 0 {
			g.addType("uses", "vm", "disk", OnDeleteRestrict, rnd.IntN(2))
		}
		if seed%3 == 0 {
			g.addType("relates", ModelAny, "vm", OnDeleteCascadeRetire, 2)
		}
		codes := make([]string, 0, len(g.types))
		for _, tt := range g.types {
			codes = append(codes, tt.Code)
		}
		n := 4 + rnd.IntN(14)
		for i := 0; i < n; i++ {
			status := CIStatusActive
			if rnd.IntN(6) == 0 {
				status = CIStatusRetired
			}
			ci := g.ci(fmt.Sprintf("c%02d", i), models[rnd.IntN(len(models))], fmt.Sprintf("c%d", rnd.IntN(6)), status, "prod")
			ci.Version = int64(1 + rnd.IntN(5))
			if rnd.IntN(9) == 0 {
				g.deleted(ci)
			}
		}
		for i := 0; i < 3*n; i++ {
			g.edge(codes[rnd.IntN(len(codes))], fmt.Sprintf("c%02d", rnd.IntN(n)), fmt.Sprintf("c%02d", rnd.IntN(n)))
		}
		root := g.cis[fmt.Sprintf("c%02d", rnd.IntN(n))]
		root.Status, root.DeleteTime = CIStatusActive, nil

		type outcome struct {
			err      error
			updated  []string
			statuses map[string]string
			versions map[string]int64
			history  []string
		}
		run := func(batched bool) outcome {
			cis := newMemCIRepo()
			for _, ci := range g.cis {
				if ci.DeleteTime == nil {
					cis.seed(ci)
				}
			}
			uc := NewCIUsecase(nil, &memFieldRepo{}, &retireRelTypeRepo{g: g}, cis, &retireRelationRepo{g: g, batch: batched}, &memHistoryRepo{store: cis}, nil, nil)
			_, err := uc.Transition(ctx, actor, Scope{Unrestricted: true}, root.ID, root.Version, CIStatusRetired)
			out := outcome{err: err, statuses: map[string]string{}, versions: map[string]int64{}}
			for _, ci := range cis.updated {
				out.updated = append(out.updated, ci.ID)
			}
			for id, ci := range cis.items {
				out.statuses[id], out.versions[id] = ci.Status, ci.Version
			}
			for _, h := range cis.histories {
				out.history = append(out.history, h.CIID+":"+h.Action)
			}
			return out
		}
		a, b := run(false), run(true)
		if !sameError(a.err, b.err) || !slices.Equal(a.updated, b.updated) || !slices.Equal(a.history, b.history) {
			t.Fatalf("seed %d: Transition diverges between the per-type and the batched path\n per-type: updated=%v history=%v err=%v\n batched:  updated=%v history=%v err=%v",
				seed, a.updated, a.history, a.err, b.updated, b.history, b.err)
		}
		if a.err != nil {
			if len(a.updated) != 0 || len(a.history) != 0 {
				t.Fatalf("seed %d: a rejected retire must not write: %v %v", seed, a.updated, a.history)
			}
			continue
		}
		if len(a.updated) == 0 || a.updated[len(a.updated)-1] != root.ID {
			t.Fatalf("seed %d: root must be written last (dependencies first): %v", seed, a.updated)
		}
		for _, id := range a.updated {
			if a.statuses[id] != CIStatusRetired || a.versions[id] != g.cis[id].Version+1 {
				t.Fatalf("seed %d: %s must be retired with version+1: %s %d", seed, id, a.statuses[id], a.versions[id])
			}
		}
	}
}

// 一条语句取边的查询次数与规模无关：参照实现 / 退化路径随「资源 × 适用类型」增长。
func TestRetireClosure_QueryCount(t *testing.T) {
	t.Parallel()
	for _, n := range []int{10, 100, 1000} {
		g, rootID := retireBenchmarkGraph(n)
		p := newRetirePaths(g, rootID, Scope{Unrestricted: true})
		want := make([]string, 0, n-1)
		for i := 0; i < n/2; i++ { // 广度优先：先全部虚拟机，再各虚拟机的磁盘
			want = append(want, fmt.Sprintf("vm%04d", i))
		}
		for i := 0; i < n/2-1; i++ {
			want = append(want, fmt.Sprintf("disk%04d", i))
		}
		checkClosure(t, p, want, nil, "")
		perType := p.types.calls.count() + p.perTyp.calls.count()
		if got := p.batch.calls.count(); got != 1 {
			t.Fatalf("nodes=%d: batched path must issue exactly one statement, got %d", n, got)
		}
		if n == 1000 && perType < n {
			t.Fatalf("per-type path is expected to scale with nodes x types: %d statements for %d nodes", perType, n)
		}
		t.Logf("nodes=%d: per-type path %d statements, batched path 1 statement", n, perType)
	}
}

// retireBenchmarkGraph 基准图（n 须为偶数）：根 → n/2 台虚拟机（cascade），前 n/2-1 台虚拟机各 → 1 块磁盘
// （cascade）；虚拟机另有 1 个没有边的 restrict 类型。共 n 个资源、n-1 条边，闭包 = 除根外的 n-1 个资源。
func retireBenchmarkGraph(n int) (*retireTestGraph, string) {
	g := newRetireTestGraph()
	g.addType("runs", "host", "vm", OnDeleteCascadeRetire, 0)
	g.addType("has", "vm", "disk", OnDeleteCascadeRetire, 0)
	g.addType("uses", "vm", "app", OnDeleteRestrict, 0)
	g.ci("root", "host", "root", CIStatusActive, "prod")
	for i := 0; i < n/2; i++ {
		vm := fmt.Sprintf("vm%04d", i)
		g.ci(vm, "vm", vm, CIStatusActive, "prod")
		g.edge("runs", "root", vm)
		if i < n/2-1 {
			g.ci(fmt.Sprintf("disk%04d", i), "disk", fmt.Sprintf("disk%04d", i), CIStatusActive, "prod")
			g.edge("has", vm, fmt.Sprintf("disk%04d", i))
		}
	}
	return g, "root"
}

// 基准：每次仓储语句固定延迟（模拟一次数据库往返），比较参照实现（逐类型计数 + 分页）与
// 新实现（一条语句取边后重放）的耗时与语句数。不断言性能阈值。
func BenchmarkRetireClosure(b *testing.B) {
	const delay = 200 * time.Microsecond
	for _, n := range []int{10, 100, 1000} {
		g, rootID := retireBenchmarkGraph(n)
		for _, path := range []string{"per-type", "batched"} {
			b.Run(fmt.Sprintf("nodes=%d/%s", n, path), func(b *testing.B) {
				ctx, scope := context.Background(), Scope{Unrestricted: true}
				types := &retireRelTypeRepo{g: g}
				rels := &retireRelationRepo{g: g, delay: delay, batch: path == "batched"}
				uc := &CIUsecase{relTypes: types, relations: rels}
				var deps []*CI
				var err error
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if rels.batch {
						deps, err = uc.retireClosure(ctx, scope, g.ciOf(rootID))
					} else {
						deps, err = referenceRetireClosure(ctx, types, rels, scope, g.ciOf(rootID))
					}
				}
				b.StopTimer()
				if err != nil || len(deps) != n-1 {
					b.Fatalf("closure = %d err=%v, want %d", len(deps), err, n-1)
				}
				b.ReportMetric(float64(types.calls.count()+rels.calls.count())/float64(b.N), "calls/op")
			})
		}
	}
}
