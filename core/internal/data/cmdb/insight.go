package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.GroupRepo       = (*groupRepo)(nil)
	_ biz.FavoriteRepo    = (*favoriteRepo)(nil)
	_ biz.SchemaAuditRepo = (*schemaAuditRepo)(nil)
	_ biz.OverviewRepo    = (*overviewRepo)(nil)
)

// ---- 模型分组 ----

// groupSelectColumns 分组查询列序（含分组下未删除的模型数子查询）。
const groupSelectColumns = `g.code, g.name, g.sort, g.builtin,
	(SELECT count(*) FROM cmdb_model m WHERE m.category = g.code AND m.delete_time IS NULL), g.create_time, g.update_time`

func scanGroup(scan func(dest ...any) error) (*biz.ModelGroup, error) {
	var g biz.ModelGroup
	if err := scan(&g.Code, &g.Name, &g.Sort, &g.Builtin, &g.ModelCount, &g.CreateTime, &g.UpdateTime); err != nil {
		return nil, err
	}
	return &g, nil
}

type groupRepo struct {
	data *basedata.Data
}

// NewGroupRepo new a groupRepo.
func NewGroupRepo(data *basedata.Data) *groupRepo { return &groupRepo{data: data} }

// List implements biz.GroupRepo.
func (r *groupRepo) List(ctx context.Context) ([]*biz.ModelGroup, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+groupSelectColumns+" FROM cmdb_model_group g ORDER BY g.sort, g.code")
	if err != nil {
		return nil, fmt.Errorf("list cmdb model groups: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.ModelGroup, 0, 8)
	for rows.Next() {
		g, err := scanGroup(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan cmdb model group: %w", err)
		}
		items = append(items, g)
	}
	return items, rows.Err()
}

// Get implements biz.GroupRepo.
func (r *groupRepo) Get(ctx context.Context, code string) (*biz.ModelGroup, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+groupSelectColumns+" FROM cmdb_model_group g WHERE g.code = $1", code)
	g, err := scanGroup(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrGroupNotFound
		}
		return nil, fmt.Errorf("get cmdb model group: %w", err)
	}
	return g, nil
}

// Create implements biz.GroupRepo.
func (r *groupRepo) Create(ctx context.Context, g *biz.ModelGroup) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_model_group (code, name, sort, builtin, create_time, update_time) VALUES ($1, $2, $3, $4, $5, $6)`,
		g.Code, g.Name, g.Sort, g.Builtin, g.CreateTime, g.UpdateTime)
	if err != nil {
		if isUniqueViolation(err, "cmdb_model_group_pkey") {
			return biz.ErrGroupCodeExists
		}
		return fmt.Errorf("insert cmdb model group: %w", err)
	}
	return nil
}

// Update implements biz.GroupRepo.
func (r *groupRepo) Update(ctx context.Context, code string, upd biz.ModelGroupUpdate) error {
	sets := make([]string, 0, 2)
	args := make([]any, 0, 3)
	if upd.Name != nil {
		args = append(args, *upd.Name)
		sets = append(sets, "name = $"+strconv.Itoa(len(args)))
	}
	if upd.Sort != nil {
		args = append(args, *upd.Sort)
		sets = append(sets, "sort = $"+strconv.Itoa(len(args)))
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, code)
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_model_group SET "+strings.Join(sets, ", ")+
		", update_time = now() WHERE code = $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return fmt.Errorf("update cmdb model group: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrGroupNotFound
	}
	return nil
}

// Delete implements biz.GroupRepo（物理删除；仍有模型的校验在 biz，这里以条件删除兜底并发新增）。
func (r *groupRepo) Delete(ctx context.Context, code string) error {
	tag, err := r.data.Pool.Exec(ctx,
		`DELETE FROM cmdb_model_group g WHERE g.code = $1 AND NOT g.builtin
		   AND NOT EXISTS (SELECT 1 FROM cmdb_model m WHERE m.category = g.code AND m.delete_time IS NULL)`, code)
	if err != nil {
		return fmt.Errorf("delete cmdb model group: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrGroupInUse
	}
	return nil
}

// ---- 模型收藏 ----

type favoriteRepo struct {
	data *basedata.Data
}

// NewFavoriteRepo new a favoriteRepo.
func NewFavoriteRepo(data *basedata.Data) *favoriteRepo { return &favoriteRepo{data: data} }

// List implements biz.FavoriteRepo.
func (r *favoriteRepo) List(ctx context.Context, userID string) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT f.model_code FROM cmdb_model_favorite f
		   JOIN cmdb_model m ON m.code = f.model_code AND m.delete_time IS NULL
		  WHERE f.user_id = $1 ORDER BY f.create_time, f.model_code`, userID)
	if err != nil {
		return nil, fmt.Errorf("list cmdb favorites: %w", err)
	}
	defer rows.Close()
	codes := make([]string, 0, 8)
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, fmt.Errorf("scan cmdb favorite: %w", err)
		}
		codes = append(codes, code)
	}
	return codes, rows.Err()
}

// Set implements biz.FavoriteRepo.
func (r *favoriteRepo) Set(ctx context.Context, userID, modelCode string, favorite bool) error {
	var err error
	if favorite {
		_, err = r.data.Pool.Exec(ctx,
			`INSERT INTO cmdb_model_favorite (user_id, model_code) VALUES ($1, $2) ON CONFLICT (user_id, model_code) DO NOTHING`,
			userID, modelCode)
	} else {
		_, err = r.data.Pool.Exec(ctx,
			`DELETE FROM cmdb_model_favorite WHERE user_id = $1 AND model_code = $2`, userID, modelCode)
	}
	if err != nil {
		return fmt.Errorf("set cmdb favorite: %w", err)
	}
	return nil
}

// ---- 模型变更审计 ----

type schemaAuditRepo struct {
	data *basedata.Data
}

// NewSchemaAuditRepo new a schemaAuditRepo.
func NewSchemaAuditRepo(data *basedata.Data) *schemaAuditRepo { return &schemaAuditRepo{data: data} }

// Insert implements biz.SchemaAuditRepo.
func (r *schemaAuditRepo) Insert(ctx context.Context, a *biz.SchemaAudit) error {
	diff, err := json.Marshal(a.Diff)
	if err != nil {
		return fmt.Errorf("marshal schema audit diff: %w", err)
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_schema_audit (id, target_type, target_code, target_name, model_code, action, diff, operator, request_id, create_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		a.ID, a.TargetType, a.TargetCode, a.TargetName, a.ModelCode, a.Action, diff, a.Operator, a.RequestID, a.CreateTime); err != nil {
		return fmt.Errorf("insert cmdb schema audit: %w", err)
	}
	return nil
}

// List implements biz.SchemaAuditRepo（时间倒序）。
func (r *schemaAuditRepo) List(ctx context.Context, filter biz.SchemaAuditFilter, page, pageSize int64) ([]*biz.SchemaAudit, int64, error) {
	where := []string{"true"}
	args := make([]any, 0, 8)
	add := func(cond string, value any) {
		args = append(args, value)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if filter.TargetType != "" {
		add("a.target_type = ?", filter.TargetType)
	}
	if filter.ModelCode != "" {
		add("a.model_code = ?", filter.ModelCode)
	}
	if filter.Action != "" {
		add("a.action = ?", filter.Action)
	}
	if filter.Operator != "" {
		add("a.operator = ?", filter.Operator)
	}
	if filter.Keyword != "" {
		add("(a.target_code ILIKE ? OR a.target_name ILIKE ?)", dbx.LikeContains(filter.Keyword))
	}
	if filter.From != nil {
		add("a.create_time >= ?", *filter.From)
	}
	if filter.To != nil {
		add("a.create_time <= ?", *filter.To)
	}
	from := " FROM cmdb_schema_audit a WHERE " + strings.Join(where, " AND ")
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+from, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count cmdb schema audit: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}
	args = append(args, pageSize, (page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx,
		"SELECT a.id, a.target_type, a.target_code, a.target_name, a.model_code, a.action, a.diff, a.operator, a.request_id, a.create_time"+from+
			" ORDER BY a.create_time DESC, a.id DESC LIMIT $"+strconv.Itoa(len(args)-1)+" OFFSET $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list cmdb schema audit: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.SchemaAudit, 0, pageSize)
	for rows.Next() {
		var a biz.SchemaAudit
		var diff []byte
		if err := rows.Scan(&a.ID, &a.TargetType, &a.TargetCode, &a.TargetName, &a.ModelCode, &a.Action, &diff,
			&a.Operator, &a.RequestID, &a.CreateTime); err != nil {
			return nil, 0, fmt.Errorf("scan cmdb schema audit: %w", err)
		}
		if len(diff) > 0 {
			if err := json.Unmarshal(diff, &a.Diff); err != nil {
				return nil, 0, fmt.Errorf("decode schema audit diff: %w", err)
			}
		}
		items = append(items, &a)
	}
	return items, total, rows.Err()
}

// ---- 运营概览 ----

type overviewRepo struct {
	data *basedata.Data
}

// NewOverviewRepo new an overviewRepo.
func NewOverviewRepo(data *basedata.Data) *overviewRepo { return &overviewRepo{data: data} }

// scopeConds 以资源表别名 c 生成数据范围条件（nil = 该维度不限），占位符从 len(args)+1 起。
func scopeConds(filter biz.OverviewFilter, args []any) (string, []any) {
	conds := ""
	if filter.ModelCodes != nil {
		args = append(args, dbx.TextArray(filter.ModelCodes))
		conds += " AND c.model_code = ANY($" + strconv.Itoa(len(args)) + ")"
	}
	if filter.Envs != nil {
		args = append(args, dbx.TextArray(filter.Envs))
		conds += " AND c.env = ANY($" + strconv.Itoa(len(args)) + ")"
	}
	return conds, args
}

// Stats implements biz.OverviewRepo：全部聚合均为单次 GROUP BY，资源数量级下走既有索引。
func (r *overviewRepo) Stats(ctx context.Context, filter biz.OverviewFilter) (*biz.Overview, error) {
	out := &biz.Overview{}
	scope, args := scopeConds(filter, nil)
	live := " FROM cmdb_ci c WHERE c.delete_time IS NULL" + scope

	// 可见模型数。
	modelSQL, modelArgs := "SELECT count(*) FROM cmdb_model m WHERE m.delete_time IS NULL", []any(nil)
	if filter.ModelCodes != nil {
		modelSQL += " AND m.code = ANY($1)"
		modelArgs = []any{dbx.TextArray(filter.ModelCodes)}
	}
	if err := r.data.Pool.QueryRow(ctx, modelSQL, modelArgs...).Scan(&out.ModelCount); err != nil {
		return nil, fmt.Errorf("count cmdb models: %w", err)
	}
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*)"+live, args...).Scan(&out.CICount); err != nil {
		return nil, fmt.Errorf("count cmdb cis: %w", err)
	}
	if err := r.data.Pool.QueryRow(ctx,
		"SELECT count(*) FROM cmdb_relation r JOIN cmdb_ci c ON c.id = r.src_id WHERE c.delete_time IS NULL"+scope, args...).
		Scan(&out.RelationCount); err != nil {
		return nil, fmt.Errorf("count cmdb relations: %w", err)
	}

	var err error
	if out.ByModel, err = r.counts(ctx,
		"SELECT c.model_code, m.name, count(*) FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code WHERE c.delete_time IS NULL"+
			scope+" GROUP BY c.model_code, m.name ORDER BY count(*) DESC, c.model_code", args); err != nil {
		return nil, err
	}
	if out.ByStatus, err = r.counts(ctx, "SELECT c.status, '', count(*)"+live+" GROUP BY c.status ORDER BY c.status", args); err != nil {
		return nil, err
	}
	if out.ByEnv, err = r.counts(ctx, "SELECT c.env, '', count(*)"+live+" GROUP BY c.env ORDER BY count(*) DESC, c.env", args); err != nil {
		return nil, err
	}
	// 来源：sync:<采集源ID> 归并为 sync，其余原样（manual / assistant …）。
	const sourceExpr = "CASE WHEN c.source LIKE 'sync:%' THEN 'sync' ELSE c.source END"
	if out.BySource, err = r.counts(ctx, "SELECT "+sourceExpr+", '', count(*)"+live+" GROUP BY 1 ORDER BY count(*) DESC, 1", args); err != nil {
		return nil, err
	}

	// 变更趋势：历史 join 资源取范围（已删除资源的历史仍计入）。
	trendArgs := append([]any{filter.Since}, args...)
	trendScope := scope
	for i := len(args); i >= 1; i-- { // 占位符整体后移一位（$1 让给 since）
		trendScope = strings.ReplaceAll(trendScope, "$"+strconv.Itoa(i), "$"+strconv.Itoa(i+1))
	}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT to_char(date_trunc('day', h.create_time), 'YYYY-MM-DD'), h.action, count(*)
		   FROM cmdb_ci_history h JOIN cmdb_ci c ON c.id = h.ci_id
		  WHERE h.create_time >= $1`+trendScope+` GROUP BY 1, 2 ORDER BY 1, 2`, trendArgs...)
	if err != nil {
		return nil, fmt.Errorf("cmdb change trend: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var p biz.OverviewTrendPoint
		if err := rows.Scan(&p.Date, &p.Action, &p.Count); err != nil {
			return nil, fmt.Errorf("scan cmdb change trend: %w", err)
		}
		out.ChangeCount += p.Count
		out.Trend = append(out.Trend, p)
	}
	return out, rows.Err()
}

// counts 执行 (key, name, count) 三列聚合查询。
func (r *overviewRepo) counts(ctx context.Context, sql string, args []any) ([]biz.OverviewCount, error) {
	rows, err := r.data.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("cmdb overview counts: %w", err)
	}
	defer rows.Close()
	items := make([]biz.OverviewCount, 0, 8)
	for rows.Next() {
		var c biz.OverviewCount
		if err := rows.Scan(&c.Key, &c.Name, &c.Count); err != nil {
			return nil, fmt.Errorf("scan cmdb overview count: %w", err)
		}
		items = append(items, c)
	}
	return items, rows.Err()
}
