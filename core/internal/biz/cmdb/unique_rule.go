package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 唯一规则上限：每类型的规则数、每规则的字段数；预检 / 冲突报告的样例数。
const (
	MaxUniqueRulesPerModel  = 8
	MaxUniqueRuleFields     = 5
	UniqueConflictSampleMax = 5
)

// uniqueRuleFieldTypes 可作为唯一字段的数据类型：取值有稳定文本形式的标量。
// float（精度）、bool（只有两个取值）、json（非标量）不允许。
var uniqueRuleFieldTypes = map[string]bool{
	FieldTypeString: true, FieldTypeInt: true, FieldTypeEnum: true,
	FieldTypeDatetime: true, FieldTypeIP: true, FieldTypeRef: true,
}

// UniqueRule 唯一规则：FieldCodes 这组字段的取值在类型内不可重复
// （1 个字段 = 单字段唯一，多个 = 联合唯一）。任一字段为空的资源不受该规则约束。
type UniqueRule struct {
	ID        string
	ModelCode string
	Name      string
	// FieldCodes 字段编码，字典序、不重复。
	FieldCodes []string
	CreateTime time.Time
	UpdateTime time.Time
}

// UniqueConflict 一组在某字段组上取值相同的资源。
type UniqueConflict struct {
	Count int64
	// Names 资源名称样例（至多 UniqueConflictSampleMax 个）。
	Names []string
}

// UniqueRuleRepo 唯一规则仓储接口。写操作各自一个事务：取类型级排他锁（与资源写入互斥），
// 在事务内复核字段可用性与数量上限、重算该规则下全部资源的键，并同步字段的 unique 派生标记；
// 存量资源已有重复时返回 ErrUniqueRuleConflict（事务回滚，不留半成品）。
type UniqueRuleRepo interface {
	ListByModel(ctx context.Context, modelCode string) ([]*UniqueRule, error)
	Get(ctx context.Context, modelCode, id string) (*UniqueRule, error)
	// Create 新增规则并为存量资源建键；超出每类型上限返回 ErrUniqueRuleInvalid。
	Create(ctx context.Context, r *UniqueRule, maxPerModel int) error
	// Update 按需更新：rename 为真写名称，rekey 为真（字段组变化）写字段组并重算该规则的全部键。
	// 未要求的部分不写（调用方手里的值可能已被并发变更取代）。
	Update(ctx context.Context, r *UniqueRule, rename, rekey bool) error
	Delete(ctx context.Context, modelCode, id string) error
	// Conflicts 统计存量资源在给定字段组上的重复：组数 + 至多 limit 组样例（只读）。
	Conflicts(ctx context.Context, modelCode string, fieldCodes []string, limit int) (int64, []UniqueConflict, error)
	// ListByField 引用了该字段的规则（废弃 / 设为敏感前的占用检查）。
	ListByField(ctx context.Context, modelCode, fieldCode string) ([]*UniqueRule, error)
}

// UniqueRuleUpdate 规则部分更新；nil 不更新（FieldCodes 非 nil 整组替换）。
type UniqueRuleUpdate struct {
	Name       *string
	FieldCodes *[]string
}

// ListUniqueRules 类型的全部唯一规则（按名称排序）。
func (uc *ModelUsecase) ListUniqueRules(ctx context.Context, modelCode string) ([]*UniqueRule, error) {
	return uc.uniqueRules.ListByModel(ctx, modelCode)
}

// CreateUniqueRule 新增唯一规则：字段须存在、未废弃、非敏感且类型受支持；存量资源重复时拒绝。
func (uc *ModelUsecase) CreateUniqueRule(ctx context.Context, r *UniqueRule) (*UniqueRule, error) {
	if _, err := uc.models.Get(ctx, r.ModelCode); err != nil {
		return nil, err
	}
	name, err := normalizeUniqueRuleName(r.Name)
	if err != nil {
		return nil, err
	}
	codes, err := uc.validateUniqueRuleFields(ctx, r.ModelCode, r.FieldCodes)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	rule := &UniqueRule{ID: id, ModelCode: r.ModelCode, Name: name, FieldCodes: codes, CreateTime: now, UpdateTime: now}
	if err := uc.uniqueRules.Create(ctx, rule, MaxUniqueRulesPerModel); err != nil {
		return nil, err
	}
	created, err := uc.uniqueRules.Get(ctx, rule.ModelCode, rule.ID)
	if err != nil {
		return nil, err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetUniqueRule, created.ID, created.Name, created.ModelCode, AuditActionCreate, nil, uniqueRuleAuditMap(created))
	return created, nil
}

// UpdateUniqueRule 更新规则名称 / 字段组；字段组变化时重算键（存量重复则拒绝）。
func (uc *ModelUsecase) UpdateUniqueRule(ctx context.Context, modelCode, id string, upd UniqueRuleUpdate) (*UniqueRule, error) {
	cur, err := uc.uniqueRules.Get(ctx, modelCode, id)
	if err != nil {
		return nil, err
	}
	next := *cur
	if upd.Name != nil {
		name, err := normalizeUniqueRuleName(*upd.Name)
		if err != nil {
			return nil, err
		}
		next.Name = name
	}
	rekey := false
	if upd.FieldCodes != nil {
		// 字段组与当前相同（仅顺序不同也算）即不变：不校验、不重算——存量迁移来的规则可能
		// 不满足新建规则的限制，回传原字段组的改名请求不应因此被拒。
		if same := slices.Sorted(slices.Values(*upd.FieldCodes)); !slices.Equal(same, cur.FieldCodes) {
			codes, err := uc.validateUniqueRuleFields(ctx, modelCode, *upd.FieldCodes)
			if err != nil {
				return nil, err
			}
			rekey = true
			next.FieldCodes = codes
		}
	}
	rename := next.Name != cur.Name
	if !rename && !rekey {
		return cur, nil
	}
	next.UpdateTime = time.Now()
	if err := uc.uniqueRules.Update(ctx, &next, rename, rekey); err != nil {
		return nil, err
	}
	after, err := uc.uniqueRules.Get(ctx, modelCode, id)
	if err != nil {
		return nil, err
	}
	if b, a := diffAuditMaps(uniqueRuleAuditMap(cur), uniqueRuleAuditMap(after)); len(a) > 0 {
		recordSchemaAudit(ctx, uc.audits, AuditTargetUniqueRule, id, after.Name, modelCode, AuditActionUpdate, b, a)
	}
	return after, nil
}

// DeleteUniqueRule 删除规则（解除该组字段的唯一约束）。
func (uc *ModelUsecase) DeleteUniqueRule(ctx context.Context, modelCode, id string) error {
	cur, err := uc.uniqueRules.Get(ctx, modelCode, id)
	if err != nil {
		return err
	}
	if err := uc.uniqueRules.Delete(ctx, modelCode, id); err != nil {
		return err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetUniqueRule, id, cur.Name, modelCode, AuditActionDelete, uniqueRuleAuditMap(cur), nil)
	return nil
}

// CheckUniqueRule 预检：存量资源在给定字段组上的重复情况（只读）。
func (uc *ModelUsecase) CheckUniqueRule(ctx context.Context, modelCode string, fieldCodes []string) (int64, []UniqueConflict, error) {
	if _, err := uc.models.Get(ctx, modelCode); err != nil {
		return 0, nil, err
	}
	codes, err := uc.validateUniqueRuleFields(ctx, modelCode, fieldCodes)
	if err != nil {
		return 0, nil, err
	}
	return uc.uniqueRules.Conflicts(ctx, modelCode, codes, UniqueConflictSampleMax)
}

func normalizeUniqueRuleName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if l := len([]rune(name)); l == 0 || l > 64 {
		return "", detail(ErrUniqueRuleInvalid, "name must be 1-64 characters")
	}
	return name, nil
}

// validateUniqueRuleFields 校验并归一化字段组（去重、字典序）。仓储在事务内再复核一次
// 字段可用性（防与并发的字段废弃 / 敏感标记竞争）；此处负责给出具体原因。
func (uc *ModelUsecase) validateUniqueRuleFields(ctx context.Context, modelCode string, fieldCodes []string) ([]string, error) {
	codes := slices.Clone(fieldCodes)
	slices.Sort(codes)
	if len(slices.Compact(slices.Clone(codes))) != len(codes) {
		return nil, detail(ErrUniqueRuleInvalid, "duplicate fields")
	}
	if len(codes) == 0 || len(codes) > MaxUniqueRuleFields {
		return nil, detail(ErrUniqueRuleInvalid, "a rule takes 1-%d fields", MaxUniqueRuleFields)
	}
	fields, err := uc.fields.ListByModel(ctx, modelCode)
	if err != nil {
		return nil, err
	}
	byCode := make(map[string]*Field, len(fields))
	for _, f := range fields {
		byCode[f.Code] = f
	}
	for _, code := range codes {
		f, ok := byCode[code]
		switch {
		case !ok:
			return nil, detail(ErrUniqueRuleInvalid, "unknown field %q", code)
		case f.Deprecated:
			return nil, detail(ErrUniqueRuleInvalid, "field %q is deprecated", code)
		case f.Sensitive:
			// 敏感字段的唯一性会成为取值探测手段（冲突与否即泄露是否存在）。
			return nil, detail(ErrUniqueRuleInvalid, "field %q is sensitive and cannot be unique", code)
		case !uniqueRuleFieldTypes[f.Type]:
			return nil, detail(ErrUniqueRuleInvalid, "field %q of type %s cannot be unique", code, f.Type)
		}
	}
	return codes, nil
}

// DuplicateByRule 资源违反某条唯一规则的错误（errors.Is 仍匹配 ErrCIDuplicate）。
// 只说明规则与字段，不含对方资源的任何信息。
func DuplicateByRule(ruleName string, fieldCodes []string) error {
	return detail(ErrCIDuplicate, "unique rule %q violated: another resource has the same %s", ruleName, strings.Join(fieldCodes, " + "))
}

// UniqueRuleConflictError 存量资源在规则下已有重复的错误（附冲突规模；样例由预检接口给出）。
func UniqueRuleConflictError(groups int64) error {
	return detail(ErrUniqueRuleConflict, "%d group(s) of existing resources share the same values; deduplicate them first", groups)
}

// UniqueRuleLimitError 超出每类型规则数上限。
func UniqueRuleLimitError(limit int) error {
	return detail(ErrUniqueRuleInvalid, "a model takes at most %d unique rules", limit)
}

// UniqueRuleFieldsChangedError 规则字段在提交期间被并发废弃 / 设为敏感（锁内复核未通过）。
func UniqueRuleFieldsChangedError() error {
	return detail(ErrUniqueRuleInvalid, "some fields are no longer usable (deprecated or sensitive); reload and retry")
}

// MaxCITags 单个资源的标签数上限（与创建接口的校验一致）。
const MaxCITags = 32

// TooManyTagsError 标签数超过上限。
func TooManyTagsError() error {
	return detail(ErrCIInvalid, "a resource takes at most %d tags", MaxCITags)
}

// DuplicateExternalID 同一来源下外部标识已被其它资源占用（恢复同步资源时）。
func DuplicateExternalID() error {
	return detail(ErrCIDuplicate, "another resource from the same source already uses this external id")
}

// DuplicateName 资源名称在类型内重复的错误（errors.Is 仍匹配 ErrCIDuplicate）。
func DuplicateName() error {
	return detail(ErrCIDuplicate, "resource name already exists in this model")
}

func uniqueRuleAuditMap(r *UniqueRule) map[string]any {
	return map[string]any{"name": r.Name, "fields": slices.Clone(r.FieldCodes)}
}
