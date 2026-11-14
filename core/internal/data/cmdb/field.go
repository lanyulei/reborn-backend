package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var _ biz.FieldRepo = (*fieldRepo)(nil)

const fieldSelectColumns = `id, model_code, code, name, type, required, "unique", enum_values, enum_labels, ref_model, rule,
	owner, list_visible, sort, description, deprecated, indexed, sensitive, create_time, update_time`

func scanField(scan func(dest ...any) error) (*biz.Field, error) {
	var f biz.Field
	var enumRaw, labelsRaw, ruleRaw []byte
	if err := scan(&f.ID, &f.ModelCode, &f.Code, &f.Name, &f.Type, &f.Required, &f.Unique, &enumRaw, &labelsRaw, &f.RefModel, &ruleRaw,
		&f.Owner, &f.ListVisible, &f.Sort, &f.Description, &f.Deprecated, &f.Indexed, &f.Sensitive, &f.CreateTime, &f.UpdateTime); err != nil {
		return nil, err
	}
	if len(enumRaw) > 0 {
		if err := json.Unmarshal(enumRaw, &f.EnumValues); err != nil {
			return nil, fmt.Errorf("decode enum_values of field %s: %w", f.ID, err)
		}
	}
	if len(labelsRaw) > 0 {
		if err := json.Unmarshal(labelsRaw, &f.EnumLabels); err != nil {
			return nil, fmt.Errorf("decode enum_labels of field %s: %w", f.ID, err)
		}
	}
	if len(ruleRaw) > 0 {
		if err := json.Unmarshal(ruleRaw, &f.Rule); err != nil {
			return nil, fmt.Errorf("decode rule of field %s: %w", f.ID, err)
		}
	}
	return &f, nil
}

func marshalEnum(values []string) (any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	return json.Marshal(values)
}

func marshalEnumLabels(labels map[string]string) (any, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	return json.Marshal(labels)
}

func marshalRule(rule biz.FieldRule) (any, error) {
	if rule == (biz.FieldRule{}) {
		return nil, nil
	}
	return json.Marshal(rule)
}

type fieldRepo struct {
	data  *basedata.Data
	cache fieldCache
}

// NewFieldRepo new a fieldRepo.
func NewFieldRepo(data *basedata.Data) *fieldRepo { return &fieldRepo{data: data} }

// Create implements biz.FieldRepo.
func (r *fieldRepo) Create(ctx context.Context, f *biz.Field) error {
	r.cache.invalidate(f.ModelCode)
	enumRaw, err := marshalEnum(f.EnumValues)
	if err != nil {
		return err
	}
	labelsRaw, err := marshalEnumLabels(f.EnumLabels)
	if err != nil {
		return err
	}
	ruleRaw, err := marshalRule(f.Rule)
	if err != nil {
		return err
	}
	_, err = r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_model_field (id, model_code, code, name, type, required, "unique", enum_values, enum_labels, ref_model, rule,
		   owner, list_visible, sort, description, indexed, sensitive, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`,
		// "unique" 为派生标记（被唯一规则引用），新字段恒为 false，由规则变更维护。
		f.ID, f.ModelCode, f.Code, f.Name, f.Type, f.Required, false, enumRaw, labelsRaw, f.RefModel, ruleRaw,
		f.Owner, f.ListVisible, f.Sort, f.Description, f.Indexed, f.Sensitive, f.CreateTime, f.UpdateTime)
	if err != nil {
		if isUniqueViolation(err, "uq_cmdb_model_field_code") {
			return biz.ErrFieldCodeExists
		}
		if isForeignKeyViolation(err) {
			return biz.ErrModelNotFound
		}
		return fmt.Errorf("insert cmdb field: %w", err)
	}
	r.cache.invalidate(f.ModelCode) // 写后再失效：防并发读把旧定义重新缓存
	return nil
}

// Get implements biz.FieldRepo.
func (r *fieldRepo) Get(ctx context.Context, modelCode, id string) (*biz.Field, error) {
	row := r.data.Pool.QueryRow(ctx,
		"SELECT "+fieldSelectColumns+" FROM cmdb_model_field WHERE model_code = $1 AND id = $2", modelCode, id)
	f, err := scanField(row.Scan)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrFieldNotFound
		}
		return nil, fmt.Errorf("get cmdb field: %w", err)
	}
	return f, nil
}

// ListByModel implements biz.FieldRepo（含已废弃；排序由 biz 负责）。
func (r *fieldRepo) ListByModel(ctx context.Context, modelCode string) ([]*biz.Field, error) {
	if items, ok := r.cache.get(modelCode); ok {
		return items, nil
	}
	items, err := r.listByModel(ctx, modelCode)
	if err != nil {
		return nil, err
	}
	r.cache.put(modelCode, items)
	return items, nil
}

func (r *fieldRepo) listByModel(ctx context.Context, modelCode string) ([]*biz.Field, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT "+fieldSelectColumns+" FROM cmdb_model_field WHERE model_code = $1 ORDER BY sort, code", modelCode)
	if err != nil {
		return nil, fmt.Errorf("list cmdb fields: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Field, 0, 16)
	for rows.Next() {
		f, err := scanField(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan cmdb field: %w", err)
		}
		items = append(items, f)
	}
	return items, rows.Err()
}

// Update implements biz.FieldRepo.
func (r *fieldRepo) Update(ctx context.Context, modelCode, id string, upd biz.FieldUpdate) error {
	sets := make([]string, 0, 8)
	args := make([]any, 0, 10)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, column+" = $"+strconv.Itoa(len(args)))
	}
	if upd.Name != nil {
		add("name", *upd.Name)
	}
	if upd.Required != nil {
		add("required", *upd.Required)
	}
	if upd.EnumValues != nil {
		raw, err := marshalEnum(*upd.EnumValues)
		if err != nil {
			return err
		}
		add("enum_values", raw)
	}
	if upd.EnumLabels != nil {
		raw, err := marshalEnumLabels(*upd.EnumLabels)
		if err != nil {
			return err
		}
		add("enum_labels", raw)
	}
	if upd.Rule != nil {
		raw, err := marshalRule(*upd.Rule)
		if err != nil {
			return err
		}
		add("rule", raw)
	}
	if upd.Owner != nil {
		add("owner", *upd.Owner)
	}
	if upd.ListVisible != nil {
		add("list_visible", *upd.ListVisible)
	}
	if upd.Sort != nil {
		add("sort", *upd.Sort)
	}
	if upd.Indexed != nil {
		add("indexed", *upd.Indexed)
	}
	if upd.Sensitive != nil {
		add("sensitive", *upd.Sensitive)
	}
	if upd.Description != nil {
		add("description", *upd.Description)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, modelCode, id)
	r.cache.invalidate(modelCode)
	defer r.cache.invalidate(modelCode) // 写后再失效：防并发读把旧定义重新缓存
	stmt := "UPDATE cmdb_model_field SET " + strings.Join(sets, ", ") +
		", update_time = now() WHERE model_code = $" + strconv.Itoa(len(args)-1) + " AND id = $" + strconv.Itoa(len(args))
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		// 设为敏感：与唯一规则变更互斥，锁内确认该字段未被规则引用（敏感字段不可唯一）。
		if upd.Sensitive != nil && *upd.Sensitive {
			if err := lockUniqueRuleDef(ctx, tx, modelCode); err != nil {
				return err
			}
			if err := checkFieldNotInUniqueRule(ctx, tx, modelCode, id); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, stmt, args...)
		if err != nil {
			return fmt.Errorf("update cmdb field: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrFieldNotFound
		}
		return nil
	})
}

// checkFieldNotInUniqueRule 字段被任一唯一规则引用时返回 ErrFieldInUse（调用方已持有定义锁）。
func checkFieldNotInUniqueRule(ctx context.Context, tx pgx.Tx, modelCode, fieldID string) error {
	var used bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		     SELECT 1 FROM cmdb_model_unique_rule r JOIN cmdb_model_field f ON f.model_code = r.model_code AND f.code = ANY (r.field_codes)
		     WHERE f.model_code = $1 AND f.id = $2)`, modelCode, fieldID).Scan(&used); err != nil {
		return fmt.Errorf("check cmdb field usage: %w", err)
	}
	if used {
		return biz.ErrFieldInUse
	}
	return nil
}

// Deprecate implements biz.FieldRepo：与唯一规则变更互斥，被规则引用的字段拒绝废弃。
func (r *fieldRepo) Deprecate(ctx context.Context, modelCode, id string) error {
	r.cache.invalidate(modelCode)
	defer r.cache.invalidate(modelCode) // 写后再失效：防并发读把旧定义重新缓存
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockUniqueRuleDef(ctx, tx, modelCode); err != nil {
			return err
		}
		if err := checkFieldNotInUniqueRule(ctx, tx, modelCode, id); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`UPDATE cmdb_model_field SET deprecated = true, update_time = now() WHERE model_code = $1 AND id = $2`, modelCode, id)
		if err != nil {
			return fmt.Errorf("deprecate cmdb field: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return biz.ErrFieldNotFound
		}
		return nil
	})
}

// fieldCacheTTL 字段定义进程内只读缓存有效期：多实例下其它实例的字段变更最多延迟此时长可见
// （AGENTS §8：进程内只允许带 TTL 的只读缓存，正确性不依赖各实例一致——校验口径最多晚 5s）。
const fieldCacheTTL = 5 * time.Second

type fieldCacheEntry struct {
	items   []*biz.Field
	expires time.Time
}

// fieldCache 按类型缓存字段列表；本进程写操作立即失效。
type fieldCache struct {
	mu      sync.Mutex
	entries map[string]fieldCacheEntry
}

func (c *fieldCache) get(modelCode string) ([]*biz.Field, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[modelCode]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return cloneFields(e.items), true
}

func (c *fieldCache) put(modelCode string, items []*biz.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]fieldCacheEntry{}
	}
	c.entries[modelCode] = fieldCacheEntry{items: cloneFields(items), expires: time.Now().Add(fieldCacheTTL)}
}

func (c *fieldCache) invalidate(modelCode string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, modelCode)
}

// cloneFields 深拷贝（调用方可能排序/修改切片）。
func cloneFields(items []*biz.Field) []*biz.Field {
	out := make([]*biz.Field, 0, len(items))
	for _, f := range items {
		cp := *f
		cp.EnumValues = append([]string(nil), f.EnumValues...)
		if f.EnumLabels != nil {
			cp.EnumLabels = maps.Clone(f.EnumLabels)
		}
		out = append(out, &cp)
	}
	return out
}
