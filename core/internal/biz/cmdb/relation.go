package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 关系方向（以当前资源视角）。
const (
	DirectionOutgoing = cmdbcontract.DirectionOutgoing // 当前资源为 src
	DirectionIncoming = cmdbcontract.DirectionIncoming // 当前资源为 dst
)

// Relation 关系实例（有向）。
type Relation struct {
	ID         string
	TypeCode   string
	SrcID      string
	DstID      string
	Source     string
	CreateUser string
	CreateTime time.Time
}

// RelatedCI 关联资源条目。
type RelatedCI struct {
	RelationID string
	CI         *CI
}

// TopologyNode 拓扑节点（Depth 距中心层数，中心为 0）。
type TopologyNode struct {
	ID        string
	Name      string
	ModelCode string
	ModelName string
	Status    string
	Env       string
	Depth     int32
}

// TopologyEdge 拓扑边（有向 src → dst）。
type TopologyEdge struct {
	ID       string
	TypeCode string
	Name     string
	SrcID    string
	DstID    string
}

// 拓扑上限（设计 §10：层数 ≤3、节点 ≤500）。
const (
	TopologyMaxDepth = 3
	TopologyMaxNodes = 500
)

// RelationKey 关联统计键（关系类型 + 方向）。
type RelationKey struct {
	TypeCode  string
	Direction string
}

// RelationRepo 关系仓储接口。
type RelationRepo interface {
	// Create 建立关系并同事务写入两端历史；已存在返回 ErrRelationExists。
	Create(ctx context.Context, rel *Relation, histories []*History) error
	// CreateBatch 同事务建立多条关系并写入历史（任一失败整体回滚）。
	// 基数在事务内加锁复核（并发关联不得突破），违反返回 ErrRelationCardinality；Move 同。
	CreateBatch(ctx context.Context, rels []*Relation, histories []*History) error
	// Exists 报告 (type, src, dst) 关系是否存在。
	Exists(ctx context.Context, typeCode, srcID, dstID string) (bool, error)
	// Delete 解除关系并同事务写入两端历史；不存在返回 ErrRelationNotFound。
	Delete(ctx context.Context, typeCode, srcID, dstID string, histories []*History) error
	// Move 同事务：解除 (type, oldSrc, oldDst) 并建立 (type, newSrc, newDst)。
	Move(ctx context.Context, typeCode string, old, next *Relation, histories []*History) error
	ListRelated(ctx context.Context, ciID, typeCode, direction, keyword string, page, pageSize int64) ([]*RelatedCI, int64, error)
	// Neighbors 以 ciID 为中心按层展开（两个方向），节点数达 maxNodes 即停止并报告截断。
	Neighbors(ctx context.Context, ciID string, depth, maxNodes int32) (nodes []*TopologyNode, edges []*TopologyEdge, truncated bool, err error)
	// CountByCI 当前资源各关系类型两个方向的关联数。
	CountByCI(ctx context.Context, ciID string) (map[RelationKey]int64, error)
	// CountSide 某关系类型下以 id 作为 src（side=outgoing）或 dst（side=incoming）的关系数。
	CountSide(ctx context.Context, typeCode, direction, id string) (int64, error)
}

// RelationUsecase 关系用例：类型/方向/基数校验，关联、解除、迁移、关联查询。
type RelationUsecase struct {
	relTypes  RelationTypeRepo
	cis       CIRepo
	relations RelationRepo
	fields    FieldRepo
}

// NewRelationUsecase new a RelationUsecase.
func NewRelationUsecase(relTypes RelationTypeRepo, cis CIRepo, relations RelationRepo, fields FieldRepo) *RelationUsecase {
	return &RelationUsecase{relTypes: relTypes, cis: cis, relations: relations, fields: fields}
}

// Link 把当前资源与目标资源批量建立关系：先逐个校验（类型、基数、已存在则跳过），
// 全部合规后单事务写入，任一目标不合规整体失败、不产生部分关联。
func (uc *RelationUsecase) Link(ctx context.Context, actor Actor, ciID, typeCode, direction string, targetIDs []string) (int32, error) {
	t, ci, err := uc.load(ctx, ciID, typeCode)
	if err != nil {
		return 0, err
	}
	// 批内计数：同一批里对同一端的多次关联也要受基数约束。
	batch := map[string]int64{}
	rels := make([]*Relation, 0, len(targetIDs))
	histories := make([]*History, 0, len(targetIDs)*2)
	var linked int32
	for _, targetID := range targetIDs {
		if targetID == ciID {
			return 0, detail(ErrRelationNotAllowed, "cannot link a resource to itself")
		}
		target, err := uc.cis.Get(ctx, targetID)
		if err != nil {
			return 0, err
		}
		rel, err := uc.buildRelation(ctx, actor, t, ci, target, direction, batch)
		if err != nil {
			return 0, err
		}
		if rel == nil {
			continue // 已存在
		}
		h, err := linkHistories(actor, ActionLink, t, rel, ci, target, direction)
		if err != nil {
			return 0, err
		}
		rels = append(rels, rel)
		histories = append(histories, h...)
		linked++
	}
	if len(rels) == 0 {
		return 0, nil
	}
	if err := uc.relations.CreateBatch(ctx, rels, histories); err != nil {
		return 0, err
	}
	return linked, nil
}

// Unlink 解除当前资源与目标资源的关系。
func (uc *RelationUsecase) Unlink(ctx context.Context, actor Actor, ciID, typeCode, direction, targetID string) error {
	t, ci, err := uc.load(ctx, ciID, typeCode)
	if err != nil {
		return err
	}
	target, err := uc.cis.Get(ctx, targetID)
	if err != nil {
		return err
	}
	src, dst := orient(ciID, targetID, direction)
	rel := &Relation{TypeCode: typeCode, SrcID: src, DstID: dst}
	histories, err := linkHistories(actor, ActionUnlink, t, rel, ci, target, direction)
	if err != nil {
		return err
	}
	return uc.relations.Delete(ctx, typeCode, src, dst, histories)
}

// Move 关系迁移：当前资源从 from 迁到 to（同事务），基数校验以 to 为准。
func (uc *RelationUsecase) Move(ctx context.Context, actor Actor, ciID, typeCode, direction, fromID, toID string) error {
	if fromID == toID {
		return nil
	}
	t, ci, err := uc.load(ctx, ciID, typeCode)
	if err != nil {
		return err
	}
	from, err := uc.cis.Get(ctx, fromID)
	if err != nil {
		return err
	}
	to, err := uc.cis.Get(ctx, toID)
	if err != nil {
		return err
	}
	if toID == ciID {
		return detail(ErrRelationNotAllowed, "cannot link a resource to itself")
	}
	oldSrc, oldDst := orient(ciID, fromID, direction)
	old := &Relation{TypeCode: typeCode, SrcID: oldSrc, DstID: oldDst}
	// 迁移目标校验时把旧关系视为已解除：用临时批计数抵消。
	batch := map[string]int64{}
	if direction == DirectionOutgoing {
		batch["src:"+ciID] = -1
	} else {
		batch["dst:"+ciID] = -1
	}
	next, err := uc.buildRelation(ctx, actor, t, ci, to, direction, batch)
	if err != nil {
		return err
	}
	if next == nil {
		return ErrRelationExists
	}
	h1, err := linkHistories(actor, ActionUnlink, t, old, ci, from, direction)
	if err != nil {
		return err
	}
	h2, err := linkHistories(actor, ActionLink, t, next, ci, to, direction)
	if err != nil {
		return err
	}
	return uc.relations.Move(ctx, typeCode, old, next, append(h1, h2...))
}

// ListRelated 分页查询关联资源。
func (uc *RelationUsecase) ListRelated(ctx context.Context, ciID, typeCode, direction, keyword string, page, pageSize int64) ([]*RelatedCI, int64, error) {
	if _, _, err := uc.load(ctx, ciID, typeCode); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.relations.ListRelated(ctx, ciID, typeCode, direction, keyword, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	// 对端属性脱敏（对端类型固定时只取一次字段定义）。
	byModel := map[string][]string{}
	for _, r := range items {
		codes, ok := byModel[r.CI.ModelCode]
		if !ok {
			fields, err := uc.fields.ListByModel(ctx, r.CI.ModelCode)
			if err != nil {
				return nil, 0, err
			}
			codes = sensitiveCodes(fields)
			byModel[r.CI.ModelCode] = codes
		}
		MaskAttrs(r.CI.Attrs, codes)
	}
	return items, total, nil
}

// Neighbors 资源拓扑：depth/maxNodes 非法值回退缺省（2 / 200）并封顶。
func (uc *RelationUsecase) Neighbors(ctx context.Context, ciID string, depth, maxNodes int32) ([]*TopologyNode, []*TopologyEdge, bool, error) {
	if _, err := uc.cis.Get(ctx, ciID); err != nil {
		return nil, nil, false, err
	}
	if depth <= 0 {
		depth = 2
	}
	if maxNodes <= 0 {
		maxNodes = 200
	}
	return uc.relations.Neighbors(ctx, ciID, min(depth, TopologyMaxDepth), min(maxNodes, TopologyMaxNodes))
}

func (uc *RelationUsecase) load(ctx context.Context, ciID, typeCode string) (*RelationType, *CI, error) {
	t, err := uc.relTypes.Get(ctx, typeCode)
	if err != nil {
		return nil, nil, err
	}
	ci, err := uc.cis.Get(ctx, ciID)
	if err != nil {
		return nil, nil, err
	}
	return t, ci, nil
}

// buildRelation 校验两端类型与基数并构造关系；已存在返回 (nil, nil)。
func (uc *RelationUsecase) buildRelation(ctx context.Context, actor Actor, t *RelationType, ci, target *CI, direction string, batch map[string]int64) (*Relation, error) {
	var src, dst *CI
	if direction == DirectionOutgoing {
		src, dst = ci, target
	} else {
		src, dst = target, ci
	}
	if !matchModel(t.SrcModel, src.ModelCode) || !matchModel(t.DstModel, dst.ModelCode) {
		return nil, detail(ErrRelationNotAllowed, "%s requires %s → %s, got %s → %s",
			t.Code, t.SrcModel, t.DstModel, src.ModelCode, dst.ModelCode)
	}
	if exists, err := uc.exists(ctx, t.Code, src.ID, dst.ID); err != nil {
		return nil, err
	} else if exists {
		return nil, nil
	}
	// 基数：one_to_one 两端各 ≤1；one_to_many 每个 dst 只属一个 src。
	if t.Cardinality == CardinalityOneToOne || t.Cardinality == CardinalityOneToMany {
		if err := uc.checkSide(ctx, t, DirectionIncoming, dst.ID, batch, "dst:"+dst.ID, dst.Name); err != nil {
			return nil, err
		}
	}
	if t.Cardinality == CardinalityOneToOne {
		if err := uc.checkSide(ctx, t, DirectionOutgoing, src.ID, batch, "src:"+src.ID, src.Name); err != nil {
			return nil, err
		}
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	return &Relation{
		ID: id, TypeCode: t.Code, SrcID: src.ID, DstID: dst.ID,
		Source: actor.source(), CreateUser: actor.UserID, CreateTime: time.Now(),
	}, nil
}

// checkSide 某端已有关系数（含批内增量）须为 0，否则基数冲突。
// 此处为无锁预检（给出具体提示）；并发安全由仓储写入事务内的加锁复核保证。
func (uc *RelationUsecase) checkSide(ctx context.Context, t *RelationType, side, id string, batch map[string]int64, key, name string) error {
	n, err := uc.relations.CountSide(ctx, t.Code, side, id)
	if err != nil {
		return err
	}
	if n+batch[key] > 0 {
		return detail(ErrRelationCardinality, "%q already has a %s relation", name, t.Code)
	}
	batch[key]++
	return nil
}

// exists 关系是否已存在（三元组唯一约束兜底）。
func (uc *RelationUsecase) exists(ctx context.Context, typeCode, srcID, dstID string) (bool, error) {
	return uc.relations.Exists(ctx, typeCode, srcID, dstID)
}

func matchModel(pattern, code string) bool {
	return pattern == ModelAny || pattern == code
}

// orient 按方向把 (当前, 目标) 排成 (src, dst)。
func orient(ciID, targetID, direction string) (src, dst string) {
	if direction == DirectionOutgoing {
		return ciID, targetID
	}
	return targetID, ciID
}

// linkHistories 关系动作在两端资源各写一条历史。
func linkHistories(actor Actor, action string, t *RelationType, rel *Relation, ci, target *CI, direction string) ([]*History, error) {
	reverse := DirectionIncoming
	if direction == DirectionIncoming {
		reverse = DirectionOutgoing
	}
	h1, err := newHistory(actor, ci.ID, action, map[string]any{
		"relationType": t.Code, "direction": direction, "target": map[string]any{"id": target.ID, "name": target.Name, "modelCode": target.ModelCode},
	})
	if err != nil {
		return nil, err
	}
	h2, err := newHistory(actor, target.ID, action, map[string]any{
		"relationType": t.Code, "direction": reverse, "target": map[string]any{"id": ci.ID, "name": ci.Name, "modelCode": ci.ModelCode},
	})
	if err != nil {
		return nil, err
	}
	_ = rel
	return []*History{h1, h2}, nil
}
