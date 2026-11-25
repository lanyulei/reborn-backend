package biz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// linkRelRepo Link 用到的关系仓储桩：只有逐个查询的能力（Exists / CountSide）。
type linkRelRepo struct {
	RelationRepo
	mu         sync.Mutex
	rels       [][3]string // (type, src, dst)
	created    []*Relation
	histories  []*History
	existsN    int
	countSideN int
}

func (r *linkRelRepo) Exists(_ context.Context, typeCode, srcID, dstID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.existsN++
	return r.has(typeCode, srcID, dstID), nil
}

func (r *linkRelRepo) has(typeCode, srcID, dstID string) bool {
	for _, rel := range r.rels {
		if rel == [3]string{typeCode, srcID, dstID} {
			return true
		}
	}
	return false
}

func (r *linkRelRepo) CountSide(_ context.Context, typeCode, direction, id string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.countSideN++
	return r.count(typeCode, direction, id), nil
}

func (r *linkRelRepo) count(typeCode, direction, id string) int64 {
	var n int64
	for _, rel := range r.rels {
		if rel[0] == typeCode && ((direction == DirectionOutgoing && rel[1] == id) || (direction == DirectionIncoming && rel[2] == id)) {
			n++
		}
	}
	return n
}

func (r *linkRelRepo) CreateBatch(_ context.Context, rels []*Relation, histories []*History) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.created = append(r.created, rels...)
	r.histories = append(r.histories, histories...)
	return nil
}

// batchLinkRelRepo 在 linkRelRepo 之上提供 RelationPrecheckRepo（口径与逐个查询一致）。
type batchLinkRelRepo struct {
	*linkRelRepo
	existingPeersN, countSidesN int
}

func (r *batchLinkRelRepo) ExistingPeers(_ context.Context, typeCode, direction, ciID string, peerIDs []string) (map[string]struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.existingPeersN++
	out := map[string]struct{}{}
	for _, peer := range peerIDs {
		src, dst := orient(ciID, peer, direction)
		if r.has(typeCode, src, dst) {
			out[peer] = struct{}{}
		}
	}
	return out, nil
}

func (r *batchLinkRelRepo) CountSides(_ context.Context, typeCode, direction string, ids []string) (map[string]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.countSidesN++
	out := map[string]int64{}
	for _, id := range ids {
		if n := r.count(typeCode, direction, id); n > 0 {
			out[id] = n
		}
	}
	return out, nil
}

// linkOutcome 一次 Link 的可观察结果：返回值、错误文案、写入的关系三元组与历史归属。
func linkOutcome(linked int32, err error, repo *linkRelRepo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "linked=%d err=%v", linked, err)
	for _, rel := range repo.created {
		fmt.Fprintf(&b, " rel(%s %s→%s %s)", rel.TypeCode, rel.SrcID, rel.DstID, rel.Source)
	}
	for _, h := range repo.histories {
		fmt.Fprintf(&b, " hist(%s %s %v)", h.CIID, h.Action, h.Diff)
	}
	return b.String()
}

// Link 的批量预检与逐个查询结果完全一致：同一组输入分别走两条路径，比较返回值、错误与写入内容；
// 批量路径不再逐个目标查询。
func TestLink_BatchPrecheckMatchesPerTarget(t *testing.T) {
	t.Parallel()
	types := []*RelationType{
		{Code: "m2m", SrcModel: "host", DstModel: "app", Cardinality: CardinalityManyToMany},
		{Code: "o2m", SrcModel: "host", DstModel: "app", Cardinality: CardinalityOneToMany},
		{Code: "o2o", SrcModel: "host", DstModel: "app", Cardinality: CardinalityOneToOne},
		{Code: "any", SrcModel: ModelAny, DstModel: ModelAny, Cardinality: CardinalityOneToOne},
	}
	cases := []struct {
		name      string
		rels      [][3]string
		ci, typ   string
		direction string
		targets   []string
		want      string // 期望结果的片段（防止两条路径「一致地错」）
	}{
		{name: "m2m 跳过已存在", rels: [][3]string{{"m2m", "h1", "a2"}}, ci: "h1", typ: "m2m", direction: DirectionOutgoing, targets: []string{"a1", "a2", "a3"}, want: "linked=2 err=<nil>"},
		{name: "m2m 全部已存在", rels: [][3]string{{"m2m", "h1", "a1"}, {"m2m", "h1", "a2"}}, ci: "h1", typ: "m2m", direction: DirectionOutgoing, targets: []string{"a1", "a2"}, want: "linked=0 err=<nil>"},
		{name: "m2m incoming", rels: [][3]string{{"m2m", "h2", "a1"}}, ci: "a1", typ: "m2m", direction: DirectionIncoming, targets: []string{"h1", "h2", "h3"}, want: "linked=2 err=<nil>"},
		{name: "o2m outgoing 全部可建", ci: "h1", typ: "o2m", direction: DirectionOutgoing, targets: []string{"a1", "a2", "a3"}, want: "linked=3 err=<nil>"},
		{name: "o2m outgoing 目标已属于别的 src", rels: [][3]string{{"o2m", "h2", "a2"}}, ci: "h1", typ: "o2m", direction: DirectionOutgoing, targets: []string{"a1", "a2", "a3"}, want: `"app-a2" already has a o2m relation`},
		{name: "o2m outgoing 已存在的跳过、不算基数冲突", rels: [][3]string{{"o2m", "h1", "a2"}}, ci: "h1", typ: "o2m", direction: DirectionOutgoing, targets: []string{"a1", "a2", "a3"}, want: "linked=2 err=<nil>"},
		{name: "o2m incoming 第二个目标批内冲突", ci: "a1", typ: "o2m", direction: DirectionIncoming, targets: []string{"h1", "h2"}, want: `"app-a1" already has a o2m relation`},
		{name: "o2m incoming 当前资源已有 src", rels: [][3]string{{"o2m", "h3", "a1"}}, ci: "a1", typ: "o2m", direction: DirectionIncoming, targets: []string{"h1", "h2"}, want: `"app-a1" already has a o2m relation`},
		{name: "o2o outgoing 第二个目标批内冲突", ci: "h1", typ: "o2o", direction: DirectionOutgoing, targets: []string{"a1", "a2"}, want: `"host-h1" already has a o2o relation`},
		{name: "o2o outgoing 目标端已占用", rels: [][3]string{{"o2o", "h2", "a1"}}, ci: "h1", typ: "o2o", direction: DirectionOutgoing, targets: []string{"a1", "a2"}, want: `"app-a1" already has a o2o relation`},
		{name: "o2o incoming 目标端已占用", rels: [][3]string{{"o2o", "h2", "a3"}}, ci: "a1", typ: "o2o", direction: DirectionIncoming, targets: []string{"h1", "h2"}, want: `"app-a1" already has a o2o relation`},
		{name: "o2o incoming 第二个目标端已占用（先于批内冲突被发现的是当前资源端）", rels: [][3]string{{"o2o", "h1", "a3"}}, ci: "a1", typ: "o2o", direction: DirectionIncoming, targets: []string{"h1", "h2"}, want: `"host-h1" already has a o2o relation`},
		{name: "o2o 两端同类型，目标同时出现在另一端", rels: [][3]string{{"any", "a2", "h3"}}, ci: "h1", typ: "any", direction: DirectionOutgoing, targets: []string{"a2", "a3"}, want: `"host-h1" already has a any relation`},
		{name: "类型不符", ci: "h1", typ: "o2m", direction: DirectionOutgoing, targets: []string{"a1", "h2"}, want: "o2m requires host → app, got host → host"},
		{name: "含自身", ci: "h1", typ: "any", direction: DirectionOutgoing, targets: []string{"a1", "h1"}, want: "cannot link a resource to itself"},
		{name: "目标不存在", ci: "h1", typ: "m2m", direction: DirectionOutgoing, targets: []string{"a1", "a2", "missing"}, want: "linked=0 err=error:"},
		{name: "单个目标不预取", rels: [][3]string{{"o2m", "h2", "a1"}}, ci: "h1", typ: "o2m", direction: DirectionOutgoing, targets: []string{"a1"}, want: `"app-a1" already has a o2m relation`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := func(batch bool) (string, *linkRelRepo, *batchLinkRelRepo) {
				cis := newMemCIRepo()
				for _, id := range []string{"h1", "h2", "h3"} {
					cis.seed(&CI{ID: id, ModelCode: "host", Name: "host-" + id})
				}
				for _, id := range []string{"a1", "a2", "a3"} {
					cis.seed(&CI{ID: id, ModelCode: "app", Name: "app-" + id})
				}
				plain := &linkRelRepo{rels: tc.rels}
				var repo RelationRepo = plain
				var batched *batchLinkRelRepo
				if batch {
					batched = &batchLinkRelRepo{linkRelRepo: plain}
					repo = batched
				}
				uc := NewRelationUsecase(&memRelTypeRepo{types: types}, cis, repo, nil)
				linked, err := uc.Link(context.Background(), Actor{UserID: "u1", RequestID: "req"}, tc.ci, tc.typ, tc.direction, tc.targets)
				// 关系与历史的 ID / 时间每次生成都不同，不参与比较。
				return linkOutcome(linked, err, plain), plain, batched
			}
			perTarget, _, _ := run(false)
			got, plain, batched := run(true)
			if got != perTarget {
				t.Fatalf("batch precheck diverges from per-target precheck:\n per-target: %s\n batch:      %s", perTarget, got)
			}
			if !strings.Contains(got, tc.want) {
				t.Fatalf("outcome = %s, want it to contain %q", got, tc.want)
			}
			if len(tc.targets) < 2 {
				if batched.existingPeersN != 0 || batched.countSidesN != 0 {
					t.Fatalf("single target must not prefetch: peers=%d sides=%d", batched.existingPeersN, batched.countSidesN)
				}
				return
			}
			// 批量路径：不再逐个目标查 Exists；CountSide 只剩当前资源自己那一端（至多两次）。
			if plain.existsN != 0 || plain.countSideN > 2 || batched.existingPeersN > 1 || batched.countSidesN > 1 {
				t.Fatalf("batch path still queries per target: exists=%d countSide=%d peers=%d sides=%d",
					plain.existsN, plain.countSideN, batched.existingPeersN, batched.countSidesN)
			}
		})
	}
}
