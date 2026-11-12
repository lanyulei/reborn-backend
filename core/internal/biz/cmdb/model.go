package biz

import (
	"context"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 资源类型状态。
const (
	ModelStatusEnabled  int32 = 1
	ModelStatusDisabled int32 = 2
)

// 字段数据类型。
const (
	FieldTypeString   = "string"
	FieldTypeInt      = "int"
	FieldTypeFloat    = "float"
	FieldTypeBool     = "bool"
	FieldTypeEnum     = "enum"
	FieldTypeDatetime = "datetime"
	FieldTypeJSON     = "json"
	FieldTypeIP       = "ip"
	FieldTypeRef      = "ref"
)

// 字段归属。
const (
	FieldOwnerManual = cmdbcontract.FieldOwnerManual
	FieldOwnerSync   = cmdbcontract.FieldOwnerSync
	FieldOwnerBoth   = cmdbcontract.FieldOwnerBoth
)

// 关系基数与删除策略。
const (
	CardinalityOneToOne   = "one_to_one"
	CardinalityOneToMany  = "one_to_many"
	CardinalityManyToMany = "many_to_many"
	OnDeleteRestrict      = cmdbcontract.OnDeleteRestrict
	OnDeleteCascadeRetire = cmdbcontract.OnDeleteCascadeRetire
	// ModelAny 关系端通配。
	ModelAny = cmdbcontract.ModelAny
)

// Model 资源类型（元数据）。
type Model struct {
	Code        string
	Name        string
	Category    string
	Icon        string
	Description string
	Status      int32
	Builtin     bool
	CICount     int64
	CreateTime  time.Time
	UpdateTime  time.Time
}

// ModelFilter 类型列表过滤；零值不过滤。
type ModelFilter struct {
	Keyword  string
	Category string
	Status   *int32
}

// ModelUpdate 类型部分更新；nil 不更新。
type ModelUpdate struct {
	Name        *string
	Category    *string
	Icon        *string
	Description *string
	Status      *int32
}

// FieldRule 字段校验规则（jsonb 持久化，json 标签即契约）。
type FieldRule struct {
	Pattern *string  `json:"pattern,omitempty"`
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	MinLen  *int32   `json:"minLen,omitempty"`
	MaxLen  *int32   `json:"maxLen,omitempty"`
}

// Field 类型字段定义。
type Field struct {
	ID          string
	ModelCode   string
	Code        string
	Name        string
	Type        string
	Required    bool
	Unique      bool
	EnumValues  []string
	RefModel    string
	Rule        FieldRule
	Owner       string
	ListVisible bool
	Sort        int32
	Description string
	Deprecated  bool
	// Indexed 为 attrs->>code 建表达式索引（data 层维护 DDL）。
	Indexed bool
	// Sensitive 敏感属性：读取、历史、导出一律脱敏。
	Sensitive  bool
	CreateTime time.Time
	UpdateTime time.Time
}

// FieldUpdate 字段部分更新；nil 不更新（EnumValues/Rule 非 nil 整组替换）。
type FieldUpdate struct {
	Name        *string
	Required    *bool
	Unique      *bool
	EnumValues  *[]string
	Rule        *FieldRule
	Owner       *string
	ListVisible *bool
	Sort        *int32
	Description *string
	Indexed     *bool
	Sensitive   *bool
}

// AttrIndexer 属性表达式索引维护（data 层以 DDL 实现；幂等）。
type AttrIndexer interface {
	EnsureAttrIndex(ctx context.Context, modelCode, fieldCode string) error
	DropAttrIndex(ctx context.Context, modelCode, fieldCode string) error
}

// RelationType 关系类型（有向 src → dst）。
type RelationType struct {
	Code          string
	Name          string
	ForwardName   string
	BackwardName  string
	SrcModel      string
	DstModel      string
	Cardinality   string
	OnDelete      string
	Description   string
	RelationCount int64
	CreateTime    time.Time
	UpdateTime    time.Time
}

// RelationTypeUpdate 关系类型部分更新。
type RelationTypeUpdate struct {
	Name         *string
	ForwardName  *string
	BackwardName *string
	OnDelete     *string
	Description  *string
}

// AppliesTo 报告关系类型是否适用于某资源类型（任一端匹配或通配）。
func (t *RelationType) AppliesTo(modelCode string) bool {
	return t.SrcModel == ModelAny || t.SrcModel == modelCode || t.DstModel == ModelAny || t.DstModel == modelCode
}

// ModelRepo 资源类型仓储接口（data 层实现）。
type ModelRepo interface {
	Create(ctx context.Context, m *Model) error
	Get(ctx context.Context, code string) (*Model, error)
	List(ctx context.Context, filter ModelFilter, page, pageSize int64) ([]*Model, int64, error)
	Update(ctx context.Context, code string, upd ModelUpdate) error
	SoftDelete(ctx context.Context, code string, at time.Time) error
}

// FieldRepo 类型字段仓储接口。
type FieldRepo interface {
	Create(ctx context.Context, f *Field) error
	Get(ctx context.Context, modelCode, id string) (*Field, error)
	ListByModel(ctx context.Context, modelCode string) ([]*Field, error)
	Update(ctx context.Context, modelCode, id string, upd FieldUpdate) error
	Deprecate(ctx context.Context, modelCode, id string) error
}

// RelationTypeRepo 关系类型仓储接口。
type RelationTypeRepo interface {
	Create(ctx context.Context, t *RelationType) error
	Get(ctx context.Context, code string) (*RelationType, error)
	// List 全部关系类型；modelCode 非空时只返回适用于该类型的。
	List(ctx context.Context, modelCode string) ([]*RelationType, error)
	Update(ctx context.Context, code string, upd RelationTypeUpdate) error
	SoftDelete(ctx context.Context, code string, at time.Time) error
}

// ModelUsecase 资源模型用例：类型、字段、关系类型的维护与校验。
type ModelUsecase struct {
	models    ModelRepo
	fields    FieldRepo
	relTypes  RelationTypeRepo
	relations RelationRepo
	views     ViewRepo
	indexer   AttrIndexer
	groups    GroupRepo
	audits    SchemaAuditRepo
}

// NewModelUsecase new a ModelUsecase.
func NewModelUsecase(models ModelRepo, fields FieldRepo, relTypes RelationTypeRepo, relations RelationRepo, views ViewRepo, indexer AttrIndexer, groups GroupRepo, audits SchemaAuditRepo) *ModelUsecase {
	return &ModelUsecase{models: models, fields: fields, relTypes: relTypes, relations: relations, views: views, indexer: indexer, groups: groups, audits: audits}
}

// CreateModel 创建资源类型（缺省启用）；所属分组须存在。
func (uc *ModelUsecase) CreateModel(ctx context.Context, m *Model) (*Model, error) {
	if _, err := uc.groups.Get(ctx, m.Category); err != nil {
		return nil, err
	}
	now := time.Now()
	m.Status = ModelStatusEnabled
	m.Builtin = false
	m.CreateTime, m.UpdateTime = now, now
	if err := uc.models.Create(ctx, m); err != nil {
		return nil, err
	}
	created, err := uc.models.Get(ctx, m.Code)
	if err != nil {
		return nil, err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetModel, created.Code, created.Name, created.Code, AuditActionCreate, nil, modelAuditMap(created))
	return created, nil
}

// GetModel 查询类型详情：类型 + 字段（按 sort/code 排序）+ 适用关系类型。
func (uc *ModelUsecase) GetModel(ctx context.Context, code string) (*Model, []*Field, []*RelationType, error) {
	m, err := uc.models.Get(ctx, code)
	if err != nil {
		return nil, nil, nil, err
	}
	fields, err := uc.fields.ListByModel(ctx, code)
	if err != nil {
		return nil, nil, nil, err
	}
	sortFields(fields)
	types, err := uc.relTypes.List(ctx, code)
	if err != nil {
		return nil, nil, nil, err
	}
	return m, fields, types, nil
}

// ListModels 分页查询类型。
func (uc *ModelUsecase) ListModels(ctx context.Context, filter ModelFilter, page, pageSize int64) ([]*Model, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.models.List(ctx, filter, page, pageSize)
}

// UpdateModel 更新类型并返回最新值；变更分组时目标分组须存在。
func (uc *ModelUsecase) UpdateModel(ctx context.Context, code string, upd ModelUpdate) (*Model, error) {
	before, err := uc.models.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	if upd.Category != nil && *upd.Category != before.Category {
		if _, err := uc.groups.Get(ctx, *upd.Category); err != nil {
			return nil, err
		}
	}
	if err := uc.models.Update(ctx, code, upd); err != nil {
		return nil, err
	}
	after, err := uc.models.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	if b, a := diffAuditMaps(modelAuditMap(before), modelAuditMap(after)); len(a) > 0 {
		recordSchemaAudit(ctx, uc.audits, AuditTargetModel, code, after.Name, code, AuditActionUpdate, b, a)
	}
	return after, nil
}

// DeleteModel 软删类型：内置不可删；仍有资源或被关系类型显式引用时拒绝。
func (uc *ModelUsecase) DeleteModel(ctx context.Context, code string) error {
	m, err := uc.models.Get(ctx, code)
	if err != nil {
		return err
	}
	if m.Builtin {
		return detail(ErrModelInUse, "builtin model")
	}
	if m.CICount > 0 {
		return detail(ErrModelInUse, "%d resources", m.CICount)
	}
	types, err := uc.relTypes.List(ctx, "")
	if err != nil {
		return err
	}
	for _, t := range types {
		if t.SrcModel == code || t.DstModel == code {
			return detail(ErrModelInUse, "referenced by relation type %s", t.Code)
		}
	}
	if err := uc.models.SoftDelete(ctx, code, time.Now()); err != nil {
		return err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetModel, code, m.Name, code, AuditActionDelete, modelAuditMap(m), nil)
	return nil
}

// CreateField 新增字段：类型存在、定义自洽（enum 有值、ref 指向存在的类型、规则与类型匹配）。
func (uc *ModelUsecase) CreateField(ctx context.Context, f *Field) (*Field, error) {
	if _, err := uc.models.Get(ctx, f.ModelCode); err != nil {
		return nil, err
	}
	if f.Owner == "" {
		f.Owner = FieldOwnerManual
	}
	if err := uc.validateFieldDefinition(ctx, f.Type, f.EnumValues, f.RefModel, f.Rule); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	f.ID, f.CreateTime, f.UpdateTime = id, now, now
	if err := uc.fields.Create(ctx, f); err != nil {
		return nil, err
	}
	if f.Indexed {
		if err := uc.indexer.EnsureAttrIndex(ctx, f.ModelCode, f.Code); err != nil {
			return nil, err
		}
	}
	created, err := uc.fields.Get(ctx, f.ModelCode, f.ID)
	if err != nil {
		return nil, err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetField, created.Code, created.Name, created.ModelCode, AuditActionCreate, nil, fieldAuditMap(created))
	return created, nil
}

// UpdateField 更新字段（编码与数据类型不可改）。
func (uc *ModelUsecase) UpdateField(ctx context.Context, modelCode, id string, upd FieldUpdate) (*Field, error) {
	cur, err := uc.fields.Get(ctx, modelCode, id)
	if err != nil {
		return nil, err
	}
	enumValues, rule := cur.EnumValues, cur.Rule
	if upd.EnumValues != nil {
		enumValues = *upd.EnumValues
	}
	if upd.Rule != nil {
		rule = *upd.Rule
	}
	if err := uc.validateFieldDefinition(ctx, cur.Type, enumValues, cur.RefModel, rule); err != nil {
		return nil, err
	}
	if err := uc.fields.Update(ctx, modelCode, id, upd); err != nil {
		return nil, err
	}
	if upd.Indexed != nil && *upd.Indexed != cur.Indexed {
		var err error
		if *upd.Indexed {
			err = uc.indexer.EnsureAttrIndex(ctx, modelCode, cur.Code)
		} else {
			err = uc.indexer.DropAttrIndex(ctx, modelCode, cur.Code)
		}
		if err != nil {
			return nil, err
		}
	}
	after, err := uc.fields.Get(ctx, modelCode, id)
	if err != nil {
		return nil, err
	}
	if b, a := diffAuditMaps(fieldAuditMap(cur), fieldAuditMap(after)); len(a) > 0 {
		recordSchemaAudit(ctx, uc.audits, AuditTargetField, after.Code, after.Name, modelCode, AuditActionUpdate, b, a)
	}
	return after, nil
}

// DeprecateField 废弃字段（数据保留，不再展示与校验）。
func (uc *ModelUsecase) DeprecateField(ctx context.Context, modelCode, id string) error {
	cur, err := uc.fields.Get(ctx, modelCode, id)
	if err != nil {
		return err
	}
	if err := uc.fields.Deprecate(ctx, modelCode, id); err != nil {
		return err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetField, cur.Code, cur.Name, modelCode, AuditActionDelete, fieldAuditMap(cur), nil)
	return nil
}

// validateFieldDefinition 字段定义自洽性校验。
func (uc *ModelUsecase) validateFieldDefinition(ctx context.Context, typ string, enumValues []string, refModel string, rule FieldRule) error {
	switch typ {
	case FieldTypeEnum:
		if len(enumValues) == 0 {
			return detail(ErrFieldInvalid, "enum field requires enum_values")
		}
		if len(slices.Compact(slices.Sorted(slices.Values(enumValues)))) != len(enumValues) {
			return detail(ErrFieldInvalid, "duplicate enum values")
		}
	case FieldTypeRef:
		if refModel == "" {
			return detail(ErrFieldInvalid, "ref field requires ref_model")
		}
		if _, err := uc.models.Get(ctx, refModel); err != nil {
			return detail(ErrFieldInvalid, "ref_model %s not found", refModel)
		}
	case FieldTypeString, FieldTypeInt, FieldTypeFloat, FieldTypeBool, FieldTypeDatetime, FieldTypeJSON, FieldTypeIP:
	default:
		return detail(ErrFieldInvalid, "unsupported type %s", typ)
	}
	if rule.Pattern != nil && *rule.Pattern != "" {
		if typ != FieldTypeString {
			return detail(ErrFieldInvalid, "pattern only applies to string fields")
		}
		if _, err := compilePattern(*rule.Pattern); err != nil {
			return detail(ErrFieldInvalid, "invalid pattern: %v", err)
		}
	}
	if (rule.Min != nil || rule.Max != nil) && typ != FieldTypeInt && typ != FieldTypeFloat {
		return detail(ErrFieldInvalid, "min/max only apply to numeric fields")
	}
	if rule.Min != nil && rule.Max != nil && *rule.Min > *rule.Max {
		return detail(ErrFieldInvalid, "min greater than max")
	}
	if (rule.MinLen != nil || rule.MaxLen != nil) && typ != FieldTypeString {
		return detail(ErrFieldInvalid, "min_len/max_len only apply to string fields")
	}
	if rule.MinLen != nil && rule.MaxLen != nil && *rule.MinLen > *rule.MaxLen {
		return detail(ErrFieldInvalid, "min_len greater than max_len")
	}
	return nil
}

// ListRelationTypes 查询关系类型；modelCode 非空时过滤适用项。
func (uc *ModelUsecase) ListRelationTypes(ctx context.Context, modelCode string) ([]*RelationType, error) {
	return uc.relTypes.List(ctx, modelCode)
}

// CreateRelationType 创建关系类型：两端类型须存在（或通配），两端不得同为通配。
func (uc *ModelUsecase) CreateRelationType(ctx context.Context, t *RelationType) (*RelationType, error) {
	if t.SrcModel == ModelAny && t.DstModel == ModelAny {
		return nil, detail(ErrFieldInvalid, "src_model and dst_model cannot both be *")
	}
	for _, code := range []string{t.SrcModel, t.DstModel} {
		if code == ModelAny {
			continue
		}
		if _, err := uc.models.Get(ctx, code); err != nil {
			return nil, err
		}
	}
	if t.OnDelete == "" {
		t.OnDelete = OnDeleteRestrict
	}
	now := time.Now()
	t.CreateTime, t.UpdateTime = now, now
	if err := uc.relTypes.Create(ctx, t); err != nil {
		return nil, err
	}
	created, err := uc.relTypes.Get(ctx, t.Code)
	if err != nil {
		return nil, err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetRelationType, created.Code, created.Name, created.SrcModel, AuditActionCreate, nil, relationTypeAuditMap(created))
	return created, nil
}

// UpdateRelationType 更新关系类型（编码与两端类型不可改）。
func (uc *ModelUsecase) UpdateRelationType(ctx context.Context, code string, upd RelationTypeUpdate) (*RelationType, error) {
	before, err := uc.relTypes.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	if err := uc.relTypes.Update(ctx, code, upd); err != nil {
		return nil, err
	}
	after, err := uc.relTypes.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	if b, a := diffAuditMaps(relationTypeAuditMap(before), relationTypeAuditMap(after)); len(a) > 0 {
		recordSchemaAudit(ctx, uc.audits, AuditTargetRelationType, code, after.Name, after.SrcModel, AuditActionUpdate, b, a)
	}
	return after, nil
}

// DeleteRelationType 软删关系类型；仍有关系实例时拒绝。
func (uc *ModelUsecase) DeleteRelationType(ctx context.Context, code string) error {
	t, err := uc.relTypes.Get(ctx, code)
	if err != nil {
		return err
	}
	if t.RelationCount > 0 {
		return detail(ErrRelationTypeInUse, "%d relations", t.RelationCount)
	}
	if err := uc.relTypes.SoftDelete(ctx, code, time.Now()); err != nil {
		return err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetRelationType, code, t.Name, t.SrcModel, AuditActionDelete, relationTypeAuditMap(t), nil)
	return nil
}

// sortFields 按 sort 升序、code 字典序稳定排序。
func sortFields(fields []*Field) {
	slices.SortStableFunc(fields, func(a, b *Field) int {
		if a.Sort != b.Sort {
			return int(a.Sort - b.Sort)
		}
		return compareString(a.Code, b.Code)
	})
}

func compareString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
