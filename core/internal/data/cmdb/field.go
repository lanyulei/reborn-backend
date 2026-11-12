package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/jackc/pgx/v5"
)

var _ biz.FieldRepo = (*fieldRepo)(nil)

const fieldSelectColumns = `id, model_code, code, name, type, required, "unique", enum_values, ref_model, rule,
	owner, list_visible, sort, description, deprecated, indexed, sensitive, create_time, update_time`

func scanField(scan func(dest ...any) error) (*biz.Field, error) {
	var f biz.Field
	var enumRaw, ruleRaw []byte
	if err := scan(&f.ID, &f.ModelCode, &f.Code, &f.Name, &f.Type, &f.Required, &f.Unique, &enumRaw, &f.RefModel, &ruleRaw,
		&f.Owner, &f.ListVisible, &f.Sort, &f.Description, &f.Deprecated, &f.Indexed, &f.Sensitive, &f.CreateTime, &f.UpdateTime); err != nil {
		return nil, err
	}
	if len(enumRaw) > 0 {
		if err := json.Unmarshal(enumRaw, &f.EnumValues); err != nil {
			return nil, fmt.Errorf("decode enum_values of field %s: %w", f.ID, err)
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
	ruleRaw, err := marshalRule(f.Rule)
	if err != nil {
		return err
	}
	_, err = r.data.Pool.Exec(ctx,
		`INSERT INTO cmdb_model_field (id, model_code, code, name, type, required, "unique", enum_values, ref_model, rule,
		   owner, list_visible, sort, description, indexed, sensitive, create_time, update_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`,
		f.ID, f.ModelCode, f.Code, f.Name, f.Type, f.Required, f.Unique, enumRaw, f.RefModel, ruleRaw,
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
	if upd.Unique != nil {
		add(`"unique"`, *upd.Unique)
	}
	if upd.EnumValues != nil {
		raw, err := marshalEnum(*upd.EnumValues)
		if err != nil {
			return err
		}
		add("enum_values", raw)
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
	tag, err := r.data.Pool.Exec(ctx, "UPDATE cmdb_model_field SET "+strings.Join(sets, ", ")+
		", update_time = now() WHERE model_code = $"+strconv.Itoa(len(args)-1)+" AND id = $"+strconv.Itoa(len(args)), args...)
	if err != nil {
		return fmt.Errorf("update cmdb field: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrFieldNotFound
	}
	return nil
}

// Deprecate implements biz.FieldRepo.
func (r *fieldRepo) Deprecate(ctx context.Context, modelCode, id string) error {
	r.cache.invalidate(modelCode)
	tag, err := r.data.Pool.Exec(ctx,
		"UPDATE cmdb_model_field SET deprecated = true, update_time = now() WHERE model_code = $1 AND id = $2", modelCode, id)
	if err != nil {
		return fmt.Errorf("deprecate cmdb field: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrFieldNotFound
	}
	return nil
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
		out = append(out, &cp)
	}
	return out
}
