package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.CIRepo         = (*ciRepo)(nil)
	_ biz.CIBatchUpdater = (*ciRepo)(nil)
	_ biz.OwnerDirectory = (*ciRepo)(nil)
)

// ciSelectColumns 资源查询列序（c = cmdb_ci，m = cmdb_model）。
const ciSelectColumns = `c.id, c.model_code, m.name, c.name, c.status, c.env, c.owner, c.tags, c.source, c.external_id,
	c.attrs, c.last_synced_at, c.stale, c.version, c.create_user, c.update_user, c.create_time, c.update_time,
	COALESCE(ss.name, ''), ss.delete_time IS NOT NULL,
	COALESCE(NULLIF(ou.nickname, ''), ou.username, ''), c.delete_time`

// ciJoinSource 同步来源采集源（source = sync:<uuid>）；manual 不命中。
// 负责人（owner 存用户 ID 时）取显示名；已删除的用户不解析（展示 owner 原文）。
const ciJoinSource = " LEFT JOIN cmdb_sync_source ss ON c.source LIKE 'sync:%' AND ss.id::text = substr(c.source, 6) " +
	// owner 形如 UUID 时才转型比较（CASE 惰性求值，自由文本不会转型报错）：等值落在 ou.id 上，可走主键。
	"LEFT JOIN \"system_user\" ou ON ou.delete_time IS NULL AND ou.id = CASE WHEN c.owner ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' THEN c.owner::uuid END "

const ciFrom = " FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code " + ciJoinSource

func scanCI(scan func(dest ...any) error) (*biz.CI, error) {
	var ci biz.CI
	var attrs []byte
	var createUser, updateUser *string
	if err := scan(&ci.ID, &ci.ModelCode, &ci.ModelName, &ci.Name, &ci.Status, &ci.Env, &ci.Owner, &ci.Tags, &ci.Source,
		&ci.ExternalID, &attrs, &ci.LastSyncedAt, &ci.Stale, &ci.Version, &createUser, &updateUser,
		&ci.CreateTime, &ci.UpdateTime, &ci.SourceName, &ci.SourceDeleted, &ci.OwnerName, &ci.DeleteTime); err != nil {
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

// errCIUniqueRuleViolated 事务内标记：资源违反某条唯一规则（cmdb_ci 触发器写 cmdb_ci_unique
// 时撞唯一索引）。事务回滚后由 explainDuplicate 查出具体规则再转成业务错误。
var errCIUniqueRuleViolated = errors.New("cmdb ci violates a unique rule")

// mapCIUniqueViolation 名称 / 唯一规则冲突映射为业务错误。
func mapCIUniqueViolation(err error) error {
	if isUniqueViolation(err, "uq_cmdb_ci_model_name") {
		return biz.DuplicateName()
	}
	if isUniqueViolation(err, "uq_cmdb_ci_unique_rule_key") {
		return errCIUniqueRuleViolated
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
	err = dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// 唯一键由 cmdb_ci 的触发器按当前唯一规则写入 cmdb_ci_unique（migrations 0049）。
		if _, err := tx.Exec(ctx,
			`INSERT INTO cmdb_ci (id, model_code, name, status, env, owner, tags, source, external_id, attrs,
			   version, create_user, update_user, create_time, update_time)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
			ci.ID, ci.ModelCode, ci.Name, ci.Status, ci.Env, ci.Owner, dbx.TextArray(ci.Tags), ci.Source, ci.ExternalID,
			attrs, ci.Version, dbx.NilIfEmpty(ci.CreateUser), dbx.NilIfEmpty(ci.UpdateUser), ci.CreateTime, ci.UpdateTime); err != nil {
			if mapped := mapCIUniqueViolation(err); mapped != err {
				return mapped
			}
			return fmt.Errorf("insert cmdb ci: %w", err)
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
	return r.explainDuplicate(ctx, err, ci)
}

// explainDuplicate 把"违反唯一规则"的事务内标记换成指明规则的业务错误：事务已回滚，
// 按拟写入的属性查出被哪条规则、哪个已有资源占用。只返回规则名与字段，不含对方资源信息。
// 查不到（对方已并发删除 / 改值）时退回通用的重复错误。其它错误原样返回。
func (r *ciRepo) explainDuplicate(ctx context.Context, err error, items ...*biz.CI) error {
	if !errors.Is(err, errCIUniqueRuleViolated) {
		return err
	}
	for _, ci := range items {
		attrs, mErr := json.Marshal(ci.Attrs)
		if mErr != nil {
			continue
		}
		var name string
		var codes []string
		qErr := r.data.Pool.QueryRow(ctx,
			`SELECT r.name, r.field_codes
			 FROM cmdb_model_unique_rule r
			 JOIN cmdb_ci_unique u ON u.rule_id = r.id AND u.key = cmdb_unique_key($2::jsonb, r.field_codes)
			 WHERE r.model_code = $1 AND u.ci_id <> $3
			 ORDER BY r.name LIMIT 1`, ci.ModelCode, attrs, ci.ID).Scan(&name, &codes)
		if qErr == nil {
			return biz.DuplicateByRule(name, codes)
		}
	}
	return biz.ErrCIDuplicate
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
	if filter.Deleted {
		where[0] = "c.delete_time IS NOT NULL"
	}
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
	if filter.Owner != "" {
		add("c.owner = ?", filter.Owner)
	}
	if cond, needTime := qualityCond(filter.Issue); cond != "" {
		// 与数据质量看板同口径：只看在册（未退役）资源。
		add("c.status <> ?", biz.CIStatusRetired)
		if needTime {
			add(cond, time.Now().AddDate(0, 0, -biz.QualityOutdatedDays))
		} else {
			where = append(where, cond)
		}
	}
	if len(filter.Attrs) > 0 {
		// 敏感字段不可过滤（防取值探测）：过滤键在该行所属类型下为敏感字段的行一律不命中。
		keys := make([]string, 0, len(filter.Attrs))
		for key := range filter.Attrs {
			keys = append(keys, key)
		}
		add("NOT EXISTS (SELECT 1 FROM cmdb_model_field sf WHERE sf.model_code = c.model_code AND sf.sensitive AND sf.code = ANY(?))", dbx.TextArray(keys))
		// 标量等值走 attrs ->> code = value（可命中 indexed 字段的表达式索引）；复合值走 @>（GIN）。
		complex := map[string]any{}
		for key, v := range filter.Attrs {
			switch v.(type) {
			case string, float64, float32, int, int32, int64, bool:
				if !identPattern.MatchString(key) {
					return nil, 0, fmt.Errorf("invalid attr filter key %q", key)
				}
				add("(c.attrs ->> '"+key+"') = ?", scalarText(v))
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
	total := int64(-1)
	if !filter.NoCount {
		// 计数不需要类型名：只在 cmdb_ci 上数，省掉与模型表的 join
		if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci c WHERE "+whereClause, args...).Scan(&total); err != nil {
			return nil, 0, fmt.Errorf("count cmdb cis: %w", err)
		}
		if total == 0 {
			return nil, 0, nil
		}
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

// scalarText 标量过滤值 → 与 jsonb ->> 文本一致的形式（浮点不走科学计数法）。
func scalarText(v any) string {
	switch n := v.(type) {
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(n), 'f', -1, 32)
	default:
		return fmt.Sprint(v)
	}
}

// Update implements biz.CIRepo：乐观锁条件更新 + 历史同事务。
func (r *ciRepo) Update(ctx context.Context, ci *biz.CI, expectedVersion int64, h *biz.History) error {
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		return updateCI(ctx, tx, ci, expectedVersion, h)
	})
	return r.explainDuplicate(ctx, err, ci)
}

// UpdateBatch implements biz.CIBatchUpdater：多条乐观锁更新 + 历史同一事务
// （按 ID 排序写入，避免并发批次互相死锁）。
func (r *ciRepo) UpdateBatch(ctx context.Context, items []*biz.CI, histories []*biz.History) error {
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return strings.Compare(items[a].ID, items[b].ID) })
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		for _, i := range order {
			if err := updateCI(ctx, tx, items[i], items[i].Version-1, histories[i]); err != nil {
				return err
			}
		}
		return nil
	})
	return r.explainDuplicate(ctx, err, items...)
}

// updateCI 事务内的单条乐观锁更新 + 历史。
func updateCI(ctx context.Context, tx pgx.Tx, ci *biz.CI, expectedVersion int64, h *biz.History) error {
	attrs, err := json.Marshal(ci.Attrs)
	if err != nil {
		return fmt.Errorf("marshal attrs: %w", err)
	}
	tag, err := tx.Exec(ctx,
		`UPDATE cmdb_ci SET name = $2, status = $3, env = $4, owner = $5, tags = $6, external_id = $7,
		   attrs = $8, version = $9, update_user = $10, update_time = $11
		 WHERE id = $1 AND version = $12 AND delete_time IS NULL`,
		ci.ID, ci.Name, ci.Status, ci.Env, ci.Owner, dbx.TextArray(ci.Tags), ci.ExternalID,
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
}

// SoftDelete implements biz.CIRepo.
func (r *ciRepo) SoftDelete(ctx context.Context, id string, at time.Time, h *biz.History) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// update_time 一并置为删除时刻：回收站按它倒序（最近删除的在前），与索引、游标一致。
		tag, err := tx.Exec(ctx, "UPDATE cmdb_ci SET delete_time = $2, update_time = $2 WHERE id = $1 AND delete_time IS NULL", id, at)
		if err != nil {
			return fmt.Errorf("soft delete cmdb ci: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrCINotFound
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
}

// GetDeleted implements biz.CIRepo.
func (r *ciRepo) GetDeleted(ctx context.Context, id string) (*biz.CI, error) {
	row := r.data.Pool.QueryRow(ctx, "SELECT "+ciSelectColumns+ciFrom+"WHERE c.id = $1 AND c.delete_time IS NOT NULL", id)
	ci, err := scanCI(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrCINotFound
		}
		return nil, fmt.Errorf("get deleted cmdb ci: %w", err)
	}
	return ci, nil
}

// Restore implements biz.CIRepo：清除删除标记（触发器随之按当前唯一规则重建键，冲突即失败）。
func (r *ciRepo) Restore(ctx context.Context, ci *biz.CI, at time.Time, h *biz.History) error {
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// 同步资源：删除期间同一外部标识可能已被重新发现并新建，恢复会造成对账锚点重复。
		// 先取类型级排他键锁——对账事务全程持有同一把锁的共享模式（见数据面 reconcile），
		// 两者因此串行，检查与恢复之间不会插进一条同锚点的新资源；等锁超时返回可重试错误。
		if ci.ExternalID != "" && strings.HasPrefix(ci.Source, "sync:") {
			if err := lockWithTimeout(ctx, tx, ci.ModelCode, uniqueRuleLockSQL); err != nil {
				return err
			}
			var taken bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM cmdb_ci WHERE model_code = $1 AND source = $2 AND external_id = $3
				   AND external_id <> '' AND delete_time IS NULL)`, ci.ModelCode, ci.Source, ci.ExternalID).Scan(&taken); err != nil {
				return fmt.Errorf("check external id: %w", err)
			}
			if taken {
				return biz.DuplicateExternalID()
			}
		}
		tag, err := tx.Exec(ctx,
			`UPDATE cmdb_ci SET delete_time = NULL, version = version + 1, update_user = $2, update_time = $3
			 WHERE id = $1 AND delete_time IS NOT NULL`, ci.ID, dbx.NilIfEmpty(ci.UpdateUser), at)
		if err != nil {
			if mapped := mapCIUniqueViolation(err); mapped != err {
				return mapped
			}
			return fmt.Errorf("restore cmdb ci: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrCINotFound
		}
		return insertHistories(ctx, tx, []*biz.History{h})
	})
	return r.explainDuplicate(ctx, err, ci)
}

// ListOwnerCandidates implements biz.OwnerDirectory（只取 ID 与显示名）。
func (r *ciRepo) ListOwnerCandidates(ctx context.Context, keyword string, limit int) ([]biz.OwnerCandidate, error) {
	rows, err := r.data.Pool.Query(ctx,
		// 只按展示出来的名称匹配：不让关键字成为探测登录名的手段。
		`SELECT id::text, COALESCE(NULLIF(nickname, ''), username) AS name FROM "system_user"
		 WHERE delete_time IS NULL AND status = 1 AND ($1 = '' OR COALESCE(NULLIF(nickname, ''), username) ILIKE $2)
		 ORDER BY name, id LIMIT $3`, keyword, dbx.LikeContains(keyword), limit)
	if err != nil {
		return nil, fmt.Errorf("list owner candidates: %w", err)
	}
	defer rows.Close()
	items := make([]biz.OwnerCandidate, 0, limit)
	for rows.Next() {
		var c biz.OwnerCandidate
		if err := rows.Scan(&c.ID, &c.Name); err != nil {
			return nil, fmt.Errorf("scan owner candidate: %w", err)
		}
		items = append(items, c)
	}
	return items, rows.Err()
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

// Search implements biz.CIRepo：名称 / 外部标识 / 标签 / 属性值子串匹配（全部参数绑定；敏感字段的值不参与匹配）；
// 命中字段按 name → externalId → tags → 属性键 的优先级给出，属性键经 LATERAL 取第一个命中。
func (r *ciRepo) Search(ctx context.Context, filter biz.SearchFilter) ([]*biz.SearchHit, error) {
	like := dbx.LikeContains(filter.Query)
	args := []any{like, dbx.TextArray(filter.ModelCodes), dbx.TextArray(filter.Envs), filter.Limit}
	rows, err := r.data.Pool.Query(ctx,
		`SELECT `+ciSelectColumns+`,
		   CASE WHEN c.name ILIKE $1 THEN 'name' WHEN c.external_id ILIKE $1 THEN 'externalId'
		        WHEN tg.tag IS NOT NULL THEN 'tags' ELSE COALESCE(a.key, '') END AS field,
		   CASE WHEN c.name ILIKE $1 THEN c.name WHEN c.external_id ILIKE $1 THEN c.external_id
		        WHEN tg.tag IS NOT NULL THEN tg.tag ELSE COALESCE(a.value, '') END AS value,
		   NOT (c.name ILIKE $1 OR c.external_id ILIKE $1 OR tg.tag IS NOT NULL) AS attr
		 FROM cmdb_ci c JOIN cmdb_model m ON m.code = c.model_code`+ciJoinSource+`
		 LEFT JOIN LATERAL (SELECT t AS tag FROM unnest(c.tags) AS t WHERE t ILIKE $1 LIMIT 1) tg ON true
		 LEFT JOIN LATERAL (SELECT kv.key, kv.value FROM jsonb_each_text(c.attrs) kv WHERE kv.value ILIKE $1
		     AND NOT EXISTS (SELECT 1 FROM cmdb_model_field sf WHERE sf.model_code = c.model_code AND sf.code = kv.key AND sf.sensitive)
		     ORDER BY kv.key LIMIT 1) a ON true
		 WHERE c.delete_time IS NULL
		   -- 索引预筛（0047 的三元组表达式索引）：候选行再由下一行做精确命中判定与敏感值排除
		   AND cmdb_ci_search_text(c.name, c.external_id, c.tags, c.attrs) ILIKE $1
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
		var attr bool
		ci, err := scanCI(func(dest ...any) error {
			return rows.Scan(append(dest, &field, &value, &attr)...)
		})
		if err != nil {
			return nil, fmt.Errorf("scan cmdb search hit: %w", err)
		}
		hits = append(hits, &biz.SearchHit{CI: ci, Field: field, Value: value, Attr: attr})
	}
	return hits, rows.Err()
}
