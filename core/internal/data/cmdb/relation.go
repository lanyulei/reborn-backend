package data

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.RelationRepo = (*relationRepo)(nil)

type relationRepo struct {
	data *basedata.Data
}

// NewRelationRepo new a relationRepo.
func NewRelationRepo(data *basedata.Data) *relationRepo { return &relationRepo{data: data} }

func insertRelation(ctx context.Context, tx pgx.Tx, rel *biz.Relation) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO cmdb_relation (id, type_code, src_id, dst_id, source, create_user, create_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rel.ID, rel.TypeCode, rel.SrcID, rel.DstID, rel.Source, dbx.NilIfEmpty(rel.CreateUser), rel.CreateTime)
	if err != nil {
		if isUniqueViolation(err, "uq_cmdb_relation") {
			return biz.ErrRelationExists
		}
		if isForeignKeyViolation(err) {
			return biz.ErrCINotFound
		}
		return fmt.Errorf("insert cmdb relation: %w", err)
	}
	return nil
}

func deleteRelation(ctx context.Context, tx pgx.Tx, typeCode, srcID, dstID string) error {
	tag, err := tx.Exec(ctx, "DELETE FROM cmdb_relation WHERE type_code = $1 AND src_id = $2 AND dst_id = $3", typeCode, srcID, dstID)
	if err != nil {
		return fmt.Errorf("delete cmdb relation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrRelationNotFound
	}
	return nil
}

// relationSide 受基数约束的一端：某关系类型下 col（src_id / dst_id）= id 的关系至多一条。
type relationSide struct {
	typeCode, col, id string
}

// lockCardinalitySides 事务内对待写关系的受约束端加事务级 advisory lock（键排序，防死锁），
// 返回这些端供写入后复核。one_to_one 两端受约束；one_to_many 仅 dst 端。
func lockCardinalitySides(ctx context.Context, tx pgx.Tx, rels []*biz.Relation) ([]relationSide, error) {
	cardinality := map[string]string{}
	var sides []relationSide
	for _, rel := range rels {
		c, ok := cardinality[rel.TypeCode]
		if !ok {
			err := tx.QueryRow(ctx, "SELECT cardinality FROM cmdb_relation_type WHERE code = $1", rel.TypeCode).Scan(&c)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("load cmdb relation type cardinality: %w", err)
			}
			cardinality[rel.TypeCode] = c
		}
		if c == biz.CardinalityOneToOne || c == biz.CardinalityOneToMany {
			sides = append(sides, relationSide{rel.TypeCode, "dst_id", rel.DstID})
		}
		if c == biz.CardinalityOneToOne {
			sides = append(sides, relationSide{rel.TypeCode, "src_id", rel.SrcID})
		}
	}
	key := func(s relationSide) string { return "cmdb_relation:" + s.typeCode + ":" + s.col + ":" + s.id }
	slices.SortFunc(sides, func(a, b relationSide) int { return strings.Compare(key(a), key(b)) })
	sides = slices.Compact(sides)
	for _, s := range sides {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", dbx.LockID(key(s))); err != nil {
			return nil, fmt.Errorf("lock cmdb relation side: %w", err)
		}
	}
	return sides, nil
}

// checkCardinalitySides 写入后在同一事务（持锁）内复核：受约束端的关系数不得超过 1。
func checkCardinalitySides(ctx context.Context, tx pgx.Tx, sides []relationSide) error {
	for _, s := range sides {
		var n int64
		if err := tx.QueryRow(ctx,
			"SELECT count(*) FROM cmdb_relation WHERE type_code = $1 AND "+s.col+" = $2", s.typeCode, s.id).Scan(&n); err != nil {
			return fmt.Errorf("count cmdb relation side: %w", err)
		}
		if n > 1 {
			return biz.ErrRelationCardinality
		}
	}
	return nil
}

// Create implements biz.RelationRepo.
func (r *relationRepo) Create(ctx context.Context, rel *biz.Relation, histories []*biz.History) error {
	return r.CreateBatch(ctx, []*biz.Relation{rel}, histories)
}

// CreateBatch implements biz.RelationRepo（单事务）。
func (r *relationRepo) CreateBatch(ctx context.Context, rels []*biz.Relation, histories []*biz.History) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		sides, err := lockCardinalitySides(ctx, tx, rels)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if err := insertRelation(ctx, tx, rel); err != nil {
				return err
			}
		}
		if err := checkCardinalitySides(ctx, tx, sides); err != nil {
			return err
		}
		return insertHistories(ctx, tx, histories)
	})
}

// Exists implements biz.RelationRepo.
func (r *relationRepo) Exists(ctx context.Context, typeCode, srcID, dstID string) (bool, error) {
	var exists bool
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM cmdb_relation WHERE type_code = $1 AND src_id = $2 AND dst_id = $3)",
		typeCode, srcID, dstID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check cmdb relation: %w", err)
	}
	return exists, nil
}

// Delete implements biz.RelationRepo.
func (r *relationRepo) Delete(ctx context.Context, typeCode, srcID, dstID string, histories []*biz.History) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := deleteRelation(ctx, tx, typeCode, srcID, dstID); err != nil {
			return err
		}
		return insertHistories(ctx, tx, histories)
	})
}

// Move implements biz.RelationRepo（解除 + 建立同事务）。
func (r *relationRepo) Move(ctx context.Context, typeCode string, old, next *biz.Relation, histories []*biz.History) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		sides, err := lockCardinalitySides(ctx, tx, []*biz.Relation{next})
		if err != nil {
			return err
		}
		if err := deleteRelation(ctx, tx, typeCode, old.SrcID, old.DstID); err != nil {
			return err
		}
		if err := insertRelation(ctx, tx, next); err != nil {
			return err
		}
		if err := checkCardinalitySides(ctx, tx, sides); err != nil {
			return err
		}
		return insertHistories(ctx, tx, histories)
	})
}

// ListRelated implements biz.RelationRepo：按方向连接对端资源，对端名称关键字过滤。
func (r *relationRepo) ListRelated(ctx context.Context, ciID, typeCode, direction, keyword string, page, pageSize int64) ([]*biz.RelatedCI, int64, error) {
	self, other := "r.src_id", "r.dst_id"
	if direction == biz.DirectionIncoming {
		self, other = "r.dst_id", "r.src_id"
	}
	where := "r.type_code = $1 AND " + self + " = $2 AND c.delete_time IS NULL"
	args := []any{typeCode, ciID}
	if keyword != "" {
		args = append(args, dbx.LikeContains(keyword))
		where += " AND c.name ILIKE $" + strconv.Itoa(len(args))
	}
	from := " FROM cmdb_relation r JOIN cmdb_ci c ON c.id = " + other + " JOIN cmdb_model m ON m.code = c.model_code" + ciJoinSource + "WHERE "
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+from+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb related: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT r.id, "+ciSelectColumns+from+where+
		" ORDER BY c.name, c.id LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb related: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.RelatedCI, 0, pageSize)
	for rows.Next() {
		var relID string
		ci, err := scanCI(func(dest ...any) error {
			return rows.Scan(append([]any{&relID}, dest...)...)
		})
		if err != nil {
			return nil, 0, fmt.Errorf("scan cmdb related: %w", err)
		}
		items = append(items, &biz.RelatedCI{RelationID: relID, CI: ci})
	}
	return items, total, rows.Err()
}

// CountByCI implements biz.RelationRepo.
func (r *relationRepo) CountByCI(ctx context.Context, ciID string) (map[biz.RelationKey]int64, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT type_code, 'outgoing', count(*) FROM cmdb_relation WHERE src_id = $1 GROUP BY type_code
		 UNION ALL
		 SELECT type_code, 'incoming', count(*) FROM cmdb_relation WHERE dst_id = $1 GROUP BY type_code`, ciID)
	if err != nil {
		return nil, fmt.Errorf("count cmdb relations: %w", err)
	}
	defer rows.Close()
	out := map[biz.RelationKey]int64{}
	for rows.Next() {
		var key biz.RelationKey
		var n int64
		if err := rows.Scan(&key.TypeCode, &key.Direction, &n); err != nil {
			return nil, fmt.Errorf("scan cmdb relation count: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}

// CountSide implements biz.RelationRepo.
func (r *relationRepo) CountSide(ctx context.Context, typeCode, direction, id string) (int64, error) {
	col := "src_id"
	if direction == biz.DirectionIncoming {
		col = "dst_id"
	}
	var n int64
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM cmdb_relation WHERE type_code = $1 AND "+col+" = $2", typeCode, id).Scan(&n); err != nil {
		return 0, fmt.Errorf("count cmdb relation side: %w", err)
	}
	return n, nil
}

// neighborsRowsPerNode 拓扑每层 SQL 行数上限 = maxNodes × 此系数（行 = 与当前层相连的边）；
// 超出即报告截断，避免超级节点把整层边全部拉回。
const neighborsRowsPerNode = 4

// Neighbors implements biz.RelationRepo：逐层 BFS（每层一条 SQL，层数 ≤3），两个方向同时展开；
// 边按关系 ID 去重，节点达上限或单层行数达上限即停止并报告截断。
func (r *relationRepo) Neighbors(ctx context.Context, ciID string, depth, maxNodes int32) ([]*biz.TopologyNode, []*biz.TopologyEdge, bool, error) {
	var root biz.TopologyNode
	if err := r.data.Pool.QueryRow(ctx,
		`SELECT c.id, c.name, c.model_code, m.name, c.status, c.env FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code
		 WHERE c.id = $1 AND c.delete_time IS NULL`, ciID).
		Scan(&root.ID, &root.Name, &root.ModelCode, &root.ModelName, &root.Status, &root.Env); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, false, biz.ErrCINotFound
		}
		return nil, nil, false, fmt.Errorf("topology root: %w", err)
	}
	nodes := []*biz.TopologyNode{&root}
	seenNode := map[string]struct{}{ciID: {}}
	seenEdge := map[string]struct{}{}
	var edges []*biz.TopologyEdge
	frontier := []string{ciID}
	truncated := false
	rowLimit := int(maxNodes) * neighborsRowsPerNode
	for level := int32(1); level <= depth && len(frontier) > 0 && !truncated; level++ {
		rows, err := r.data.Pool.Query(ctx,
			`SELECT r.id, r.type_code, t.forward_name, r.src_id, r.dst_id, c.id, c.name, c.model_code, m.name, c.status, c.env
			 FROM cmdb_relation r
			 JOIN cmdb_relation_type t ON t.code = r.type_code
			 JOIN cmdb_ci c ON c.id = CASE WHEN r.src_id = ANY($1::uuid[]) THEN r.dst_id ELSE r.src_id END
			 JOIN cmdb_model m ON m.code = c.model_code
			 WHERE (r.src_id = ANY($1::uuid[]) OR r.dst_id = ANY($1::uuid[])) AND c.delete_time IS NULL
			 ORDER BY c.name, r.id LIMIT $2`, dbx.TextArray(frontier), rowLimit+1)
		if err != nil {
			return nil, nil, false, fmt.Errorf("topology level %d: %w", level, err)
		}
		var next []string
		for n := 0; rows.Next(); n++ {
			if n >= rowLimit {
				truncated = true // 第 rowLimit+1 行仅用于探测截断
				break
			}
			var e biz.TopologyEdge
			var n biz.TopologyNode
			if err := rows.Scan(&e.ID, &e.TypeCode, &e.Name, &e.SrcID, &e.DstID, &n.ID, &n.Name, &n.ModelCode, &n.ModelName, &n.Status, &n.Env); err != nil {
				rows.Close()
				return nil, nil, false, fmt.Errorf("scan topology: %w", err)
			}
			if _, ok := seenEdge[e.ID]; !ok {
				seenEdge[e.ID] = struct{}{}
				edges = append(edges, &e)
			}
			if _, ok := seenNode[n.ID]; ok {
				continue
			}
			if len(nodes) >= int(maxNodes) {
				truncated = true
				continue
			}
			n.Depth = level
			seenNode[n.ID] = struct{}{}
			nodes = append(nodes, &n)
			next = append(next, n.ID)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, nil, false, err
		}
		frontier = next
	}
	// 只保留两端都在节点集合内的边（截断时对端可能未纳入）。
	kept := edges[:0]
	for _, e := range edges {
		if _, ok := seenNode[e.SrcID]; !ok {
			continue
		}
		if _, ok := seenNode[e.DstID]; !ok {
			continue
		}
		kept = append(kept, e)
	}
	if kept == nil {
		kept = []*biz.TopologyEdge{}
	}
	return nodes, kept, truncated, nil
}
