package data

import (
	"context"
	"errors"
	"math"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var _ biz.SpaceRepo = (*spaceRepo)(nil)

// spaceRepo 知识空间仓储。
type spaceRepo struct {
	data *basedata.Data
}

// NewSpaceRepo new a spaceRepo.
func NewSpaceRepo(data *basedata.Data) *spaceRepo {
	return &spaceRepo{data: data}
}

const spaceColumns = `id, code, name, type, description, visibility, owner_id, embedding_enabled, review_required, review_days, create_user, create_time, update_time`

func scanSpace(scan func(dest ...any) error) (*biz.Space, error) {
	var s biz.Space
	var visibility int16
	if err := scan(&s.ID, &s.Code, &s.Name, &s.Type, &s.Description, &visibility, &s.OwnerID, &s.EmbeddingEnabled, &s.ReviewRequired,
		&s.ReviewDays, &s.CreateUser, &s.CreateTime, &s.UpdateTime); err != nil {
		return nil, err
	}
	s.Visibility = int32(visibility)
	return &s, nil
}

// Create implements biz.SpaceRepo.
func (r *spaceRepo) Create(ctx context.Context, s *biz.Space) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO knowledge_space (`+spaceColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		s.ID, s.Code, s.Name, s.Type, s.Description, s.Visibility, s.OwnerID, s.EmbeddingEnabled, s.ReviewRequired, s.ReviewDays,
		s.CreateUser, s.CreateTime, s.UpdateTime)
	if isUniqueViolation(err, "uq_knowledge_space_code") {
		return biz.ErrSpaceCodeExists
	}
	if err != nil {
		return wrap("insert knowledge space", err)
	}
	return nil
}

// Update implements biz.SpaceRepo.
func (r *spaceRepo) Update(ctx context.Context, s *biz.Space) error {
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE knowledge_space SET name=$2, type=$3, description=$4, visibility=$5, owner_id=$6, embedding_enabled=$7,
		 review_required=$8, review_days=$9, update_time=$10 WHERE id=$1 AND delete_time IS NULL`,
		s.ID, s.Name, s.Type, s.Description, s.Visibility, s.OwnerID, s.EmbeddingEnabled, s.ReviewRequired, s.ReviewDays, s.UpdateTime)
	if err != nil {
		return wrap("update knowledge space", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSpaceNotFound
	}
	return nil
}

// Get implements biz.SpaceRepo.
func (r *spaceRepo) Get(ctx context.Context, id string) (*biz.Space, error) {
	row := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx, `SELECT `+spaceColumns+` FROM knowledge_space WHERE id=$1 AND delete_time IS NULL`, id)
	s, err := scanSpace(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrSpaceNotFound
	}
	if err != nil {
		return nil, wrap("get knowledge space", err)
	}
	return s, nil
}

// List implements biz.SpaceRepo.
func (r *spaceRepo) List(ctx context.Context, keyword, typ string) ([]*biz.Space, error) {
	var a sqlArgs
	where := "delete_time IS NULL"
	if keyword != "" {
		p := a.add(dbx.LikeContains(keyword))
		where += " AND (name ILIKE " + p + " OR code ILIKE " + p + ")"
	}
	if typ != "" {
		where += " AND type = " + a.add(typ)
	}
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx, `SELECT `+spaceColumns+` FROM knowledge_space WHERE `+where+` ORDER BY name, id`, a.args...)
	if err != nil {
		return nil, wrap("list knowledge spaces", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.Space, error) { return scanSpace(row.Scan) })
	if err != nil {
		return nil, wrap("scan knowledge space", err)
	}
	return items, nil
}

// SoftDelete implements biz.SpaceRepo：单语句条件更新，仍有未删除文档时不命中。
func (r *spaceRepo) SoftDelete(ctx context.Context, id string, at time.Time) error {
	var exists, hasDocs bool
	err := r.data.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM knowledge_space WHERE id=$1 AND delete_time IS NULL),
		        EXISTS (SELECT 1 FROM knowledge_document WHERE space_id=$1 AND delete_time IS NULL)`, id).Scan(&exists, &hasDocs)
	if err != nil {
		return wrap("check knowledge space", err)
	}
	if !exists {
		return biz.ErrSpaceNotFound
	}
	if hasDocs {
		return biz.ErrSpaceNotEmpty
	}
	tag, err := r.data.Pool.Exec(ctx,
		`UPDATE knowledge_space SET delete_time=$2 WHERE id=$1 AND delete_time IS NULL
		 AND NOT EXISTS (SELECT 1 FROM knowledge_document WHERE space_id=$1 AND delete_time IS NULL)`, id, at)
	if err != nil {
		return wrap("delete knowledge space", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrSpaceNotEmpty
	}
	return nil
}

// DocumentCounts implements biz.SpaceRepo.
func (r *spaceRepo) DocumentCounts(ctx context.Context) (map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx, `SELECT space_id, count(*) FROM knowledge_document WHERE delete_time IS NULL AND status = 2 GROUP BY space_id`)
	if err != nil {
		return nil, wrap("count knowledge documents", err)
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, wrap("scan knowledge document count", err)
		}
		out[id] = int32(min(n, math.MaxInt32)) //nolint:gosec // 已限定上限。
	}
	return out, rows.Err()
}

// ListAcl implements biz.SpaceRepo.
func (r *spaceRepo) ListAcl(ctx context.Context, spaceID string) ([]*biz.AclEntry, error) {
	rows, err := r.data.Pool.Query(ctx, `SELECT subject_type, subject_id, level FROM knowledge_space_acl WHERE space_id=$1 ORDER BY subject_type, level DESC, subject_id`, spaceID)
	if err != nil {
		return nil, wrap("list knowledge acl", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.AclEntry, error) {
		var e biz.AclEntry
		var st, lv int16
		if err := row.Scan(&st, &e.SubjectID, &lv); err != nil {
			return nil, err
		}
		e.SubjectType, e.Level = int32(st), int32(lv)
		return &e, nil
	})
	if err != nil {
		return nil, wrap("scan knowledge acl", err)
	}
	return items, nil
}

// ReplaceAcl implements biz.SpaceRepo：删旧插新同事务。
func (r *spaceRepo) ReplaceAcl(ctx context.Context, spaceID string, entries []*biz.AclEntry, userID string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM knowledge_space_acl WHERE space_id=$1`, spaceID); err != nil {
			return wrap("clear knowledge acl", err)
		}
		if len(entries) == 0 {
			return nil
		}
		b := &pgx.Batch{}
		for _, e := range entries {
			b.Queue(`INSERT INTO knowledge_space_acl (space_id, subject_type, subject_id, level, update_user, update_time) VALUES ($1,$2,$3,$4,$5,now())`,
				spaceID, e.SubjectType, e.SubjectID, e.Level, userID)
		}
		if err := tx.SendBatch(ctx, b).Close(); err != nil {
			return wrap("insert knowledge acl", err)
		}
		return nil
	})
}

// Levels implements biz.SpaceRepo.
func (r *spaceRepo) Levels(ctx context.Context, userID string, roleIDs []string) (map[string]int32, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT s.id, GREATEST(
		    CASE WHEN s.visibility = 1 THEN 1 ELSE 0 END,
		    CASE WHEN s.owner_id = $1 THEN 3 ELSE 0 END,
		    COALESCE((SELECT max(a.level) FROM knowledge_space_acl a WHERE a.space_id = s.id
		              AND ((a.subject_type IN (2, 3) AND a.subject_id = $1) OR (a.subject_type = 1 AND a.subject_id = ANY($2::uuid[])))), 0)
		 ) AS level FROM knowledge_space s WHERE s.delete_time IS NULL`, userID, dbx.TextArray(roleIDs))
	if err != nil {
		return nil, wrap("resolve knowledge levels", err)
	}
	defer rows.Close()
	out := map[string]int32{}
	for rows.Next() {
		var id string
		var lv int32
		if err := rows.Scan(&id, &lv); err != nil {
			return nil, wrap("scan knowledge level", err)
		}
		if lv > 0 {
			out[id] = lv
		}
	}
	return out, rows.Err()
}
