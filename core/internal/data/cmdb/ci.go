package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.CIRepo = (*ciRepo)(nil)

// ciSelectColumns 资源查询列序（c = cmdb_ci，m = cmdb_model）。
const ciSelectColumns = `c.id, c.model_code, m.name, c.name, c.status, c.env, c.owner, c.tags, c.source, c.external_id, c.unique_key,
	c.attrs, c.last_synced_at, c.stale, c.version, c.create_user, c.update_user, c.create_time, c.update_time,
	COALESCE(ss.name, ''), ss.delete_time IS NOT NULL`

// ciJoinSource 同步来源采集源（source = sync:<uuid>）；manual 不命中。
const ciJoinSource = " LEFT JOIN cmdb_sync_source ss ON c.source LIKE 'sync:%' AND ss.id::text = substr(c.source, 6) "

const ciFrom = " FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code " + ciJoinSource

func scanCI(scan func(dest ...any) error) (*biz.CI, error) {
	var ci biz.CI
	var attrs []byte
	var createUser, updateUser *string
	if err := scan(&ci.ID, &ci.ModelCode, &ci.ModelName, &ci.Name, &ci.Status, &ci.Env, &ci.Owner, &ci.Tags, &ci.Source,
		&ci.ExternalID, &ci.UniqueKey, &attrs, &ci.LastSyncedAt, &ci.Stale, &ci.Version, &createUser, &updateUser,
		&ci.CreateTime, &ci.UpdateTime, &ci.SourceName, &ci.SourceDeleted); err != nil {
		return nil, err
	}
	ci.Attrs = map[string]any{}
	if len(attrs) > 0 {
		if err := json.Unmarshal(attrs, &ci.Attrs); err != nil {
			return nil, fmt.Errorf("decode attrs of ci %s: %w", ci.ID, err)
		}
	}
	if createUser != nil {
		ci.CreateUser = *createUser
	}
	if updateUser != nil {
		ci.UpdateUser = *updateUser
	}
	if ci.Tags == nil {
		ci.Tags = []string{}
	}
	return &ci, nil
}

// mapCIUniqueViolation 名称/唯一键冲突映射为业务错误。
func mapCIUniqueViolation(err error) error {
	if isUniqueViolation(err, "uq_cmdb_ci_model_name") || isUniqueViolation(err, "uq_cmdb_ci_model_unique_key") {
		return biz.ErrCIDuplicate
	}
	if isForeignKeyViolation(err) {
		return biz.ErrModelNotFound
	}
	return err
}

type ciRepo struct {
	data *basedata.Data
}

// NewCIRepo new a ciRepo.
func NewCIRepo(data *basedata.Data) *ciRepo { return &ciRepo{data: data} }

// Create implements biz.CIRepo（资源 + 历史同事务）。
func (r *ciRepo) Create(ctx context.Context, ci *biz.CI, h *biz.History) error {
	attrs, err := json.Marshal(ci.Attrs)
	if err != nil {
		return fmt.Errorf("marshal attrs: %w", err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO cmdb_ci (id, model_code, name, status, env, owner, tags, source, external_id, unique_key, attrs,
			   version, create_user, update_user, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			ci.ID, ci.ModelCode, ci.Name, ci.Status, ci.Env, ci.Owner, dbx.TextArray(ci.Tags), ci.Source, ci.ExternalID,
			ci.UniqueKey, attrs, ci.Version, dbx.NilIfEmpty(ci.CreateUser), dbx.NilIfEmpty(ci.UpdateUser), ci.CreateTime, ci.UpdateTime); err != nil {
			if mapped := mapCIUniqueViolation(err); mapped != err {
				return mapped
			}
			return fmt.Errorf("insert cmdb ci: %w", err)
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
}

// Get implements biz.CIRepo.
func (r *ciRepo) Get(ctx context.Context, id string) (*biz.CI, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+ciSelectColumns+ciFrom+"WHERE c.id = $1 AND c.delete_time IS NULL", id)
	ci, err := scanCI(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrCINotFound
		}
		return nil, fmt.Errorf("get cmdb ci: %w", err)
	}
	return ci, nil
}

// GetByName implements biz.CIRepo.
func (r *ciRepo) GetByName(ctx context.Context, modelCode, name string) (*biz.CI, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+ciSelectColumns+ciFrom+"WHERE c.model_code = $1 AND c.name = $2 AND c.delete_time IS NULL", modelCode, name)
	ci, err := scanCI(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrCINotFound
		}
		return nil, fmt.Errorf("get cmdb ci by name: %w", err)
	}
	return ci, nil
}

// List implements biz.CIRepo：固定列过滤 + tags 包含 + attrs 等值（jsonb @>，走 GIN）。
func (r *ciRepo) List(ctx context.Context, filter biz.CIFilter, page, pageSize int64) ([]*biz.CI, int64, error) {
	where := []string{"c.delete_time IS NULL"}
	args := make([]any, 0, 8)
	add := func(cond string, value any) {
		args = append(args, value)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if filter.ModelCode != "" {
		add("c.model_code = ?", filter.ModelCode)
	}
	if len(filter.ModelCodes) > 0 {
		add("c.model_code = ANY(?)", dbx.TextArray(filter.ModelCodes))
	}
	if len(filter.Envs) > 0 {
		add("c.env = ANY(?)", dbx.TextArray(filter.Envs))
	}
	if filter.Keyword != "" {
		add("(c.name ILIKE ? OR c.external_id ILIKE ?)", dbx.LikeContains(filter.Keyword))
	}
	if filter.Status != "" {
		add("c.status = ?", filter.Status)
	}
	if filter.Env != "" {
		add("c.env = ?", filter.Env)
	}
	if filter.Tag != "" {
		add("? = ANY(c.tags)", filter.Tag)
	}
	if len(filter.Attrs) > 0 {
		// 标量等值走 attrs ->> code = value（可命中 indexed 字段的表达式索引）；复合值走 @>（GIN）。
		complex := map[string]any{}
		for key, v := range filter.Attrs {
			switch v.(type) {
			case string, float64, float32, int, int32, int64, bool:
				if !identPattern.MatchString(key) {
					return nil, 0, fmt.Errorf("invalid attr filter key %q", key)
				}
				add("(c.attrs ->> '"+key+"') = ?", fmt.Sprint(v))
			default:
				complex[key] = v
			}
		}
		if len(complex) > 0 {
			raw, err := json.Marshal(complex)
			if err != nil {
				return nil, 0, fmt.Errorf("marshal attrs filter: %w", err)
			}
			add("c.attrs @> ?::jsonb", raw)
		}
	}
	if filter.RelType != "" {
		self := "r.src_id"
		if filter.RelDirection == biz.DirectionIncoming {
			self = "r.dst_id"
		}
		other := "r.dst_id"
		if self == "r.dst_id" {
			other = "r.src_id"
		}
		switch {
		case filter.RelTargetID != "":
			args = append(args, filter.RelType, filter.RelTargetID)
			where = append(where, fmt.Sprintf("EXISTS (SELECT 1 FROM cmdb_relation r WHERE r.type_code = $%d AND %s = c.id AND %s = $%d)",
				len(args)-1, self, other, len(args)))
		case filter.RelNone:
			add("NOT EXISTS (SELECT 1 FROM cmdb_relation r WHERE r.type_code = ? AND "+self+" = c.id)", filter.RelType)
		default:
			add("EXISTS (SELECT 1 FROM cmdb_relation r WHERE r.type_code = ? AND "+self+" = c.id)", filter.RelType)
		}
	}
	whereClause := strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+ciFrom+"WHERE "+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb cis: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	pageClause := ""
	if filter.Cursor != "" {
		// 键集分页：(update_time, id) 严格小于游标（与排序一致）。
		at, id, err := biz.DecodeCursor(filter.Cursor)
		if err != nil {
			return nil, 0, err
		}
		args = append(args, at, id)
		whereClause += fmt.Sprintf(" AND (c.update_time, c.id) < ($%d, $%d::uuid)", len(args)-1, len(args))
		args = append(args, pageSize)
		pageClause = " LIMIT $" + strconv.Itoa(len(args))
	} else {
		args = append(args, pageSize, (page-1)*pageSize)
		pageClause = " LIMIT $" + strconv.Itoa(len(args)-1) + " OFFSET $" + strconv.Itoa(len(args))
	}
	rows, err := r.data.Pool.Query(ctx, "SELECT "+ciSelectColumns+ciFrom+"WHERE "+whereClause+
		" ORDER BY c.update_time DESC, c.id DESC"+pageClause, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb cis: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.CI, 0, pageSize)
	for rows.Next() {
		ci, err := scanCI(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan cmdb ci: %w", err)
		}
		items = append(items, ci)
	}
	return items, total, rows.Err()
}

// Update implements biz.CIRepo：乐观锁条件更新 + 历史同事务。
func (r *ciRepo) Update(ctx context.Context, ci *biz.CI, expectedVersion int64, h *biz.History) error {
	attrs, err := json.Marshal(ci.Attrs)
	if err != nil {
		return fmt.Errorf("marshal attrs: %w", err)
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE cmdb_ci SET name = $2, status = $3, env = $4, owner = $5, tags = $6, external_id = $7, unique_key = $8,
			   attrs = $9, version = $10, update_user = $11, update_time = $12
			 WHERE id = $1 AND version = $13 AND delete_time IS NULL`,
			ci.ID, ci.Name, ci.Status, ci.Env, ci.Owner, dbx.TextArray(ci.Tags), ci.ExternalID, ci.UniqueKey,
			attrs, ci.Version, dbx.NilIfEmpty(ci.UpdateUser), ci.UpdateTime, expectedVersion)
		if err != nil {
			if mapped := mapCIUniqueViolation(err); mapped != err {
				return mapped
			}
			return fmt.Errorf("update cmdb ci: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM cmdb_ci WHERE id = $1 AND delete_time IS NULL)", ci.ID).Scan(&exists); err != nil {
				return fmt.Errorf("check cmdb ci: %w", err)
			}
			if !exists {
				return biz.ErrCINotFound
			}
			return biz.ErrCIVersionConflict
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
}

// SoftDelete implements biz.CIRepo.
func (r *ciRepo) SoftDelete(ctx context.Context, id string, at time.Time, h *biz.History) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE cmdb_ci SET delete_time = $2 WHERE id = $1 AND delete_time IS NULL", id, at)
		if err != nil {
			return fmt.Errorf("soft delete cmdb ci: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrCINotFound
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
}

// ExistsInModel implements biz.CIRepo.
func (r *ciRepo) ExistsInModel(ctx context.Context, modelCode, id string) (bool, error) {
	var exists bool
	// id 的 UUID 格式由 biz 先行校验，此处的错误即基础设施错误。
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM cmdb_ci WHERE id = $1::uuid AND model_code = $2 AND delete_time IS NULL)", id, modelCode).Scan(&exists); err != nil {
		return false, fmt.Errorf("check cmdb ci: %w", err)
	}
	return exists, nil
}

// Search implements biz.CIRepo：名称 / 外部标识 / 标签 / 属性值子串匹配（全部参数绑定）；
// 命中字段按 name → externalId → tags → 属性键 的优先级给出，属性键经 LATERAL 取第一个命中。
func (r *ciRepo) Search(ctx context.Context, filter biz.SearchFilter) ([]*biz.SearchHit, error) {
	like := dbx.LikeContains(filter.Query)
	args := []any{like, dbx.TextArray(filter.ModelCodes), dbx.TextArray(filter.Envs), filter.Limit}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT `+ciSelectColumns+`,
		   CASE WHEN c.name ILIKE $1 THEN 'name' WHEN c.external_id ILIKE $1 THEN 'externalId'
		        WHEN tg.tag IS NOT NULL THEN 'tags' ELSE COALESCE(a.key, '') END AS field,
		   CASE WHEN c.name ILIKE $1 THEN c.name WHEN c.external_id ILIKE $1 THEN c.external_id
		        WHEN tg.tag IS NOT NULL THEN tg.tag ELSE COALESCE(a.value, '') END AS value
		 FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code`+ciJoinSource+`
		 LEFT JOIN LATERAL (SELECT t AS tag FROM unnest(c.tags) AS t WHERE t ILIKE $1 LIMIT 1) tg ON true
		 LEFT JOIN LATERAL (SELECT key, value FROM jsonb_each_text(c.attrs) WHERE value ILIKE $1 ORDER BY key LIMIT 1) a ON true
		 WHERE c.delete_time IS NULL
		   AND (c.name ILIKE $1 OR c.external_id ILIKE $1 OR tg.tag IS NOT NULL OR a.key IS NOT NULL)
		   AND (cardinality($2::text[]) = 0 OR c.model_code = ANY($2::text[]))
		   AND (cardinality($3::text[]) = 0 OR c.env = ANY($3::text[]))
		 ORDER BY (c.name ILIKE $1) DESC, c.update_time DESC, c.id DESC LIMIT $4`, args...)
	if err != nil {
		return nil, fmt.Errorf("search cmdb ci: %w", err)
	}
	defer rows.Close()
	hits := make([]*biz.SearchHit, 0, filter.Limit)
	for rows.Next() {
		var field, value string
		ci, err := scanCI(func(dest ...any) error {
			return rows.Scan(append(dest, &field, &value)...)
		})
		if err != nil {
			return nil, fmt.Errorf("scan cmdb search hit: %w", err)
		}
		hits = append(hits, &biz.SearchHit{CI: ci, Field: field, Value: value})
	}
	return hits, rows.Err()
}
