package testfake

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
)

// 编译期断言：视图类型实现 cmdb 域全部仓储接口。
var (
	_ biz.ModelRepo        = cmdbModelView{}
	_ biz.FieldRepo        = cmdbFieldView{}
	_ biz.RelationTypeRepo = cmdbRelationTypeView{}
	_ biz.CIRepo           = cmdbCIView{}
	_ biz.RelationRepo     = cmdbRelationView{}
	_ biz.HistoryRepo      = cmdbHistoryView{}
	_ biz.ViewRepo         = cmdbViewView{}
	_ biz.ScopeRepo        = cmdbScopeView{}
)

// CmdbStore 是 cmdb 域六个仓储接口的内存实现，语义对齐 data 层：唯一约束、
// 外键、乐观锁、软删、历史同"事务"（顺序写入）。经各 *View 方法暴露视图。
type CmdbStore struct {
	mu        sync.RWMutex
	models    map[string]*cmdbModelEntry
	fields    map[string]*biz.Field
	relTypes  map[string]*cmdbRelationTypeEntry
	cis       map[string]*cmdbCIEntry
	relations map[string]*biz.Relation
	histories []*biz.History
	views     map[string]*biz.ModelView
	scopes    map[string]*biz.RoleScope
	// 洞察相关：模型分组（预置内置分组）、收藏（userID -> 模型编码）、模型变更审计。
	groups       map[string]*biz.ModelGroup
	favorites    map[string][]string
	schemaAudits []*biz.SchemaAudit
}

type cmdbModelEntry struct {
	model   biz.Model
	deleted bool
}

type cmdbRelationTypeEntry struct {
	t       biz.RelationType
	deleted bool
}

type cmdbCIEntry struct {
	ci      biz.CI
	deleted bool
}

// NewCmdbStore new a CmdbStore。
func NewCmdbStore() *CmdbStore {
	return &CmdbStore{
		models:    make(map[string]*cmdbModelEntry),
		fields:    make(map[string]*biz.Field),
		relTypes:  make(map[string]*cmdbRelationTypeEntry),
		cis:       make(map[string]*cmdbCIEntry),
		relations: make(map[string]*biz.Relation),
		views:     make(map[string]*biz.ModelView),
		scopes:    make(map[string]*biz.RoleScope),
		groups:    builtinGroups(),
		favorites: make(map[string][]string),
	}
}

// Models 返回 biz.ModelRepo 视图。
func (s *CmdbStore) Models() biz.ModelRepo { return cmdbModelView{s} }

// Fields 返回 biz.FieldRepo 视图。
func (s *CmdbStore) Fields() biz.FieldRepo { return cmdbFieldView{s} }

// RelationTypes 返回 biz.RelationTypeRepo 视图。
func (s *CmdbStore) RelationTypes() biz.RelationTypeRepo { return cmdbRelationTypeView{s} }

// CIs 返回 biz.CIRepo 视图。
func (s *CmdbStore) CIs() biz.CIRepo { return cmdbCIView{s} }

// Relations 返回 biz.RelationRepo 视图。
func (s *CmdbStore) Relations() biz.RelationRepo { return cmdbRelationView{s} }

// Histories 返回 biz.HistoryRepo 视图。
func (s *CmdbStore) Histories() biz.HistoryRepo { return cmdbHistoryView{s} }

func (s *CmdbStore) ciCount(modelCode string) int64 {
	var n int64
	for _, e := range s.cis {
		if !e.deleted && e.ci.ModelCode == modelCode {
			n++
		}
	}
	return n
}

func (s *CmdbStore) relationCount(typeCode string) int64 {
	var n int64
	for _, r := range s.relations {
		if r.TypeCode == typeCode {
			n++
		}
	}
	return n
}

func cloneCI(ci *biz.CI) *biz.CI {
	cp := *ci
	cp.Tags = slices.Clone(ci.Tags)
	cp.Attrs = make(map[string]any, len(ci.Attrs))
	for k, v := range ci.Attrs {
		cp.Attrs[k] = v
	}
	return &cp
}

// ---- ModelRepo ----

type cmdbModelView struct{ s *CmdbStore }

func (v cmdbModelView) Create(_ context.Context, m *biz.Model) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.models[m.Code]; ok {
		return biz.ErrModelCodeExists
	}
	v.s.models[m.Code] = &cmdbModelEntry{model: *m}
	return nil
}

func (v cmdbModelView) Get(_ context.Context, code string) (*biz.Model, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	e, ok := v.s.models[code]
	if !ok || e.deleted {
		return nil, biz.ErrModelNotFound
	}
	m := e.model
	m.CICount = v.s.ciCount(code)
	return &m, nil
}

func (v cmdbModelView) List(_ context.Context, filter biz.ModelFilter, page, pageSize int64) ([]*biz.Model, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.Model, 0, len(v.s.models))
	for code, e := range v.s.models {
		if e.deleted {
			continue
		}
		m := e.model
		if filter.Keyword != "" && !strings.Contains(m.Code, filter.Keyword) && !strings.Contains(m.Name, filter.Keyword) {
			continue
		}
		if filter.Category != "" && m.Category != filter.Category {
			continue
		}
		if filter.Status != nil && m.Status != *filter.Status {
			continue
		}
		m.CICount = v.s.ciCount(code)
		items = append(items, &m)
	}
	v.s.mu.RUnlock()
	slices.SortFunc(items, func(a, b *biz.Model) int { return strings.Compare(a.Code, b.Code) })
	return pageSlice(items, page, pageSize)
}

func (v cmdbModelView) Update(_ context.Context, code string, upd biz.ModelUpdate) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.models[code]
	if !ok || e.deleted {
		return biz.ErrModelNotFound
	}
	if upd.Name != nil {
		e.model.Name = *upd.Name
	}
	if upd.Category != nil {
		e.model.Category = *upd.Category
	}
	if upd.Icon != nil {
		e.model.Icon = *upd.Icon
	}
	if upd.Description != nil {
		e.model.Description = *upd.Description
	}
	if upd.Status != nil {
		e.model.Status = *upd.Status
	}
	e.model.UpdateTime = time.Now()
	return nil
}

func (v cmdbModelView) SoftDelete(_ context.Context, code string, _ time.Time) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.models[code]
	if !ok || e.deleted {
		return biz.ErrModelNotFound
	}
	e.deleted = true
	return nil
}

// ---- FieldRepo ----

type cmdbFieldView struct{ s *CmdbStore }

func (v cmdbFieldView) Create(_ context.Context, f *biz.Field) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if e, ok := v.s.models[f.ModelCode]; !ok || e.deleted {
		return biz.ErrModelNotFound
	}
	for _, existing := range v.s.fields {
		if existing.ModelCode == f.ModelCode && existing.Code == f.Code {
			return biz.ErrFieldCodeExists
		}
	}
	cp := *f
	v.s.fields[f.ID] = &cp
	return nil
}

func (v cmdbFieldView) Get(_ context.Context, modelCode, id string) (*biz.Field, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	f, ok := v.s.fields[id]
	if !ok || f.ModelCode != modelCode {
		return nil, biz.ErrFieldNotFound
	}
	cp := *f
	return &cp, nil
}

func (v cmdbFieldView) ListByModel(_ context.Context, modelCode string) ([]*biz.Field, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	items := make([]*biz.Field, 0, 8)
	for _, f := range v.s.fields {
		if f.ModelCode == modelCode {
			cp := *f
			items = append(items, &cp)
		}
	}
	slices.SortFunc(items, func(a, b *biz.Field) int {
		if a.Sort != b.Sort {
			return int(a.Sort - b.Sort)
		}
		return strings.Compare(a.Code, b.Code)
	})
	return items, nil
}

func (v cmdbFieldView) Update(_ context.Context, modelCode, id string, upd biz.FieldUpdate) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	f, ok := v.s.fields[id]
	if !ok || f.ModelCode != modelCode {
		return biz.ErrFieldNotFound
	}
	if upd.Name != nil {
		f.Name = *upd.Name
	}
	if upd.Required != nil {
		f.Required = *upd.Required
	}
	if upd.Unique != nil {
		f.Unique = *upd.Unique
	}
	if upd.EnumValues != nil {
		f.EnumValues = slices.Clone(*upd.EnumValues)
	}
	if upd.Rule != nil {
		f.Rule = *upd.Rule
	}
	if upd.Owner != nil {
		f.Owner = *upd.Owner
	}
	if upd.ListVisible != nil {
		f.ListVisible = *upd.ListVisible
	}
	if upd.Sort != nil {
		f.Sort = *upd.Sort
	}
	if upd.Indexed != nil {
		f.Indexed = *upd.Indexed
	}
	if upd.Sensitive != nil {
		f.Sensitive = *upd.Sensitive
	}
	if upd.Description != nil {
		f.Description = *upd.Description
	}
	return nil
}

func (v cmdbFieldView) Deprecate(_ context.Context, modelCode, id string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	f, ok := v.s.fields[id]
	if !ok || f.ModelCode != modelCode {
		return biz.ErrFieldNotFound
	}
	f.Deprecated = true
	return nil
}

// ---- RelationTypeRepo ----

type cmdbRelationTypeView struct{ s *CmdbStore }

func (v cmdbRelationTypeView) Create(_ context.Context, t *biz.RelationType) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if _, ok := v.s.relTypes[t.Code]; ok {
		return biz.ErrRelationTypeCodeExists
	}
	v.s.relTypes[t.Code] = &cmdbRelationTypeEntry{t: *t}
	return nil
}

func (v cmdbRelationTypeView) Get(_ context.Context, code string) (*biz.RelationType, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	e, ok := v.s.relTypes[code]
	if !ok || e.deleted {
		return nil, biz.ErrRelationTypeNotFound
	}
	t := e.t
	t.RelationCount = v.s.relationCount(code)
	return &t, nil
}

func (v cmdbRelationTypeView) List(_ context.Context, modelCode string) ([]*biz.RelationType, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	items := make([]*biz.RelationType, 0, len(v.s.relTypes))
	for code, e := range v.s.relTypes {
		if e.deleted || (modelCode != "" && !e.t.AppliesTo(modelCode)) {
			continue
		}
		t := e.t
		t.RelationCount = v.s.relationCount(code)
		items = append(items, &t)
	}
	slices.SortFunc(items, func(a, b *biz.RelationType) int { return strings.Compare(a.Code, b.Code) })
	return items, nil
}

func (v cmdbRelationTypeView) Update(_ context.Context, code string, upd biz.RelationTypeUpdate) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.relTypes[code]
	if !ok || e.deleted {
		return biz.ErrRelationTypeNotFound
	}
	if upd.Name != nil {
		e.t.Name = *upd.Name
	}
	if upd.ForwardName != nil {
		e.t.ForwardName = *upd.ForwardName
	}
	if upd.BackwardName != nil {
		e.t.BackwardName = *upd.BackwardName
	}
	if upd.OnDelete != nil {
		e.t.OnDelete = *upd.OnDelete
	}
	if upd.Description != nil {
		e.t.Description = *upd.Description
	}
	return nil
}

func (v cmdbRelationTypeView) SoftDelete(_ context.Context, code string, _ time.Time) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.relTypes[code]
	if !ok || e.deleted {
		return biz.ErrRelationTypeNotFound
	}
	e.deleted = true
	return nil
}

// ---- CIRepo ----

type cmdbCIView struct{ s *CmdbStore }

// duplicate 同类型内名称或唯一键重复（排除自身）。
func (s *CmdbStore) duplicate(ci *biz.CI) bool {
	for id, e := range s.cis {
		if e.deleted || id == ci.ID || e.ci.ModelCode != ci.ModelCode {
			continue
		}
		if e.ci.Name == ci.Name || (ci.UniqueKey != "" && e.ci.UniqueKey == ci.UniqueKey) {
			return true
		}
	}
	return false
}

func (v cmdbCIView) Create(_ context.Context, ci *biz.CI, h *biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.models[ci.ModelCode]
	if !ok || e.deleted {
		return biz.ErrModelNotFound
	}
	if v.s.duplicate(ci) {
		return biz.ErrCIDuplicate
	}
	cp := cloneCI(ci)
	cp.ModelName = e.model.Name
	v.s.cis[ci.ID] = &cmdbCIEntry{ci: *cp}
	v.s.histories = append(v.s.histories, h)
	return nil
}

func (v cmdbCIView) Get(_ context.Context, id string) (*biz.CI, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	e, ok := v.s.cis[id]
	if !ok || e.deleted {
		return nil, biz.ErrCINotFound
	}
	return cloneCI(&e.ci), nil
}

func (v cmdbCIView) GetByName(_ context.Context, modelCode, name string) (*biz.CI, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	for _, e := range v.s.cis {
		if !e.deleted && e.ci.ModelCode == modelCode && e.ci.Name == name {
			return cloneCI(&e.ci), nil
		}
	}
	return nil, biz.ErrCINotFound
}

func (v cmdbCIView) List(_ context.Context, filter biz.CIFilter, page, pageSize int64) ([]*biz.CI, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.CI, 0, len(v.s.cis))
	for _, e := range v.s.cis {
		ci := e.ci
		if e.deleted || (filter.ModelCode != "" && ci.ModelCode != filter.ModelCode) ||
			(len(filter.ModelCodes) > 0 && !slices.Contains(filter.ModelCodes, ci.ModelCode)) ||
			(len(filter.Envs) > 0 && !slices.Contains(filter.Envs, ci.Env)) ||
			(filter.Status != "" && ci.Status != filter.Status) || (filter.Env != "" && ci.Env != filter.Env) ||
			(filter.Tag != "" && !slices.Contains(ci.Tags, filter.Tag)) {
			continue
		}
		if filter.Keyword != "" && !strings.Contains(ci.Name, filter.Keyword) && !strings.Contains(ci.ExternalID, filter.Keyword) {
			continue
		}
		if filter.RelType != "" && !v.s.matchRelationFilter(ci.ID, filter) {
			continue
		}
		match := true
		for k, want := range filter.Attrs {
			if got, ok := ci.Attrs[k]; !ok || !reflect.DeepEqual(normalizeNumber(got), normalizeNumber(want)) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		items = append(items, cloneCI(&ci))
	}
	v.s.mu.RUnlock()
	slices.SortFunc(items, func(a, b *biz.CI) int {
		if !a.UpdateTime.Equal(b.UpdateTime) {
			return b.UpdateTime.Compare(a.UpdateTime)
		}
		return strings.Compare(b.ID, a.ID)
	})
	if filter.Cursor != "" {
		at, id, err := biz.DecodeCursor(filter.Cursor)
		if err != nil {
			return nil, 0, err
		}
		total := int64(len(items))
		rest := make([]*biz.CI, 0, len(items))
		for _, ci := range items {
			if ci.UpdateTime.Before(at) || (ci.UpdateTime.Equal(at) && ci.ID < id) {
				rest = append(rest, ci)
			}
		}
		if int64(len(rest)) > pageSize {
			rest = rest[:pageSize]
		}
		return rest, total, nil
	}
	return pageSlice(items, page, pageSize)
}

// matchRelationFilter 关联条件（语义对齐 data 层）。
func (s *CmdbStore) matchRelationFilter(ciID string, filter biz.CIFilter) bool {
	has := false
	for _, r := range s.relations {
		if r.TypeCode != filter.RelType {
			continue
		}
		var self, other string
		if filter.RelDirection == biz.DirectionIncoming {
			self, other = r.DstID, r.SrcID
		} else {
			self, other = r.SrcID, r.DstID
		}
		if self != ciID {
			continue
		}
		if filter.RelTargetID != "" {
			if other == filter.RelTargetID {
				return true
			}
			continue
		}
		has = true
	}
	if filter.RelTargetID != "" {
		return false
	}
	if filter.RelNone {
		return !has
	}
	return has
}

func normalizeNumber(v any) any {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	default:
		return v
	}
}

func (v cmdbCIView) Update(_ context.Context, ci *biz.CI, expectedVersion int64, h *biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.cis[ci.ID]
	if !ok || e.deleted {
		return biz.ErrCINotFound
	}
	if e.ci.Version != expectedVersion {
		return biz.ErrCIVersionConflict
	}
	if v.s.duplicate(ci) {
		return biz.ErrCIDuplicate
	}
	cp := cloneCI(ci)
	cp.ModelName = e.ci.ModelName
	e.ci = *cp
	v.s.histories = append(v.s.histories, h)
	return nil
}

func (v cmdbCIView) SoftDelete(_ context.Context, id string, _ time.Time, h *biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.cis[id]
	if !ok || e.deleted {
		return biz.ErrCINotFound
	}
	e.deleted = true
	v.s.histories = append(v.s.histories, h)
	return nil
}

func (v cmdbCIView) ExistsInModel(_ context.Context, modelCode, id string) (bool, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	e, ok := v.s.cis[id]
	return ok && !e.deleted && e.ci.ModelCode == modelCode, nil
}

// ---- RelationRepo ----

type cmdbRelationView struct{ s *CmdbStore }

func (s *CmdbStore) findRelation(typeCode, srcID, dstID string) (string, bool) {
	for id, r := range s.relations {
		if r.TypeCode == typeCode && r.SrcID == srcID && r.DstID == dstID {
			return id, true
		}
	}
	return "", false
}

func (s *CmdbStore) insertRelation(rel *biz.Relation) error {
	for _, id := range []string{rel.SrcID, rel.DstID} {
		if e, ok := s.cis[id]; !ok || e.deleted {
			return biz.ErrCINotFound
		}
	}
	if _, ok := s.findRelation(rel.TypeCode, rel.SrcID, rel.DstID); ok {
		return biz.ErrRelationExists
	}
	cp := *rel
	s.relations[rel.ID] = &cp
	return nil
}

func (s *CmdbStore) deleteRelation(typeCode, srcID, dstID string) error {
	id, ok := s.findRelation(typeCode, srcID, dstID)
	if !ok {
		return biz.ErrRelationNotFound
	}
	delete(s.relations, id)
	return nil
}

func (v cmdbRelationView) Create(_ context.Context, rel *biz.Relation, histories []*biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.s.insertRelation(rel); err != nil {
		return err
	}
	v.s.histories = append(v.s.histories, histories...)
	return nil
}

func (v cmdbRelationView) CreateBatch(_ context.Context, rels []*biz.Relation, histories []*biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	inserted := make([]string, 0, len(rels))
	for _, rel := range rels {
		if err := v.s.insertRelation(rel); err != nil {
			for _, id := range inserted { // 回滚本批已插入的
				delete(v.s.relations, id)
			}
			return err
		}
		inserted = append(inserted, rel.ID)
	}
	v.s.histories = append(v.s.histories, histories...)
	return nil
}

func (v cmdbRelationView) Exists(_ context.Context, typeCode, srcID, dstID string) (bool, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	_, ok := v.s.findRelation(typeCode, srcID, dstID)
	return ok, nil
}

func (v cmdbRelationView) Delete(_ context.Context, typeCode, srcID, dstID string, histories []*biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.s.deleteRelation(typeCode, srcID, dstID); err != nil {
		return err
	}
	v.s.histories = append(v.s.histories, histories...)
	return nil
}

func (v cmdbRelationView) Move(_ context.Context, typeCode string, old, next *biz.Relation, histories []*biz.History) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.s.deleteRelation(typeCode, old.SrcID, old.DstID); err != nil {
		return err
	}
	if err := v.s.insertRelation(next); err != nil {
		return err
	}
	v.s.histories = append(v.s.histories, histories...)
	return nil
}

func (v cmdbRelationView) ListRelated(_ context.Context, ciID, typeCode, direction, keyword string, page, pageSize int64) ([]*biz.RelatedCI, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.RelatedCI, 0, 8)
	for id, r := range v.s.relations {
		if r.TypeCode != typeCode {
			continue
		}
		var other string
		switch {
		case direction == biz.DirectionOutgoing && r.SrcID == ciID:
			other = r.DstID
		case direction == biz.DirectionIncoming && r.DstID == ciID:
			other = r.SrcID
		default:
			continue
		}
		e, ok := v.s.cis[other]
		if !ok || e.deleted || (keyword != "" && !strings.Contains(e.ci.Name, keyword)) {
			continue
		}
		items = append(items, &biz.RelatedCI{RelationID: id, CI: cloneCI(&e.ci)})
	}
	v.s.mu.RUnlock()
	slices.SortFunc(items, func(a, b *biz.RelatedCI) int { return strings.Compare(a.CI.Name, b.CI.Name) })
	return pageSlice(items, page, pageSize)
}

func (v cmdbRelationView) CountByCI(_ context.Context, ciID string) (map[biz.RelationKey]int64, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	out := map[biz.RelationKey]int64{}
	for _, r := range v.s.relations {
		if r.SrcID == ciID {
			out[biz.RelationKey{TypeCode: r.TypeCode, Direction: biz.DirectionOutgoing}]++
		}
		if r.DstID == ciID {
			out[biz.RelationKey{TypeCode: r.TypeCode, Direction: biz.DirectionIncoming}]++
		}
	}
	return out, nil
}

func (v cmdbRelationView) CountSide(_ context.Context, typeCode, direction, id string) (int64, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	var n int64
	for _, r := range v.s.relations {
		if r.TypeCode != typeCode {
			continue
		}
		if (direction == biz.DirectionOutgoing && r.SrcID == id) || (direction == biz.DirectionIncoming && r.DstID == id) {
			n++
		}
	}
	return n, nil
}

// ---- ViewRepo / ScopeRepo ----

type cmdbViewView struct{ s *CmdbStore }

// Views 返回 biz.ViewRepo 视图。
func (s *CmdbStore) Views() biz.ViewRepo { return cmdbViewView{s} }

func (v cmdbViewView) Get(_ context.Context, modelCode string) (*biz.ModelView, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	mv, ok := v.s.views[modelCode]
	if !ok {
		return nil, nil
	}
	cp := *mv
	cp.Sections = slices.Clone(mv.Sections)
	return &cp, nil
}

func (v cmdbViewView) Upsert(_ context.Context, view *biz.ModelView) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if e, ok := v.s.models[view.ModelCode]; !ok || e.deleted {
		return biz.ErrModelNotFound
	}
	cp := *view
	cp.Sections = slices.Clone(view.Sections)
	v.s.views[view.ModelCode] = &cp
	return nil
}

type cmdbScopeView struct{ s *CmdbStore }

// Scopes 返回 biz.ScopeRepo 视图。
func (s *CmdbStore) Scopes() biz.ScopeRepo { return cmdbScopeView{s} }

func (v cmdbScopeView) ListByRoles(_ context.Context, roleIDs []string) ([]*biz.RoleScope, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	out := make([]*biz.RoleScope, 0, len(roleIDs))
	for _, id := range roleIDs {
		if rs, ok := v.s.scopes[id]; ok {
			cp := *rs
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (v cmdbScopeView) ListAll(_ context.Context) ([]*biz.RoleScope, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	out := make([]*biz.RoleScope, 0, len(v.s.scopes))
	for _, rs := range v.s.scopes {
		cp := *rs
		out = append(out, &cp)
	}
	return out, nil
}

func (v cmdbScopeView) Upsert(_ context.Context, rs *biz.RoleScope) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	cp := *rs
	cp.ModelCodes, cp.Envs = slices.Clone(rs.ModelCodes), slices.Clone(rs.Envs)
	v.s.scopes[rs.RoleID] = &cp
	return nil
}

// ---- HistoryRepo ----

type cmdbHistoryView struct{ s *CmdbStore }

func (v cmdbHistoryView) ListByCI(_ context.Context, ciID string, page, pageSize int64) ([]*biz.History, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.History, 0, 8)
	for _, h := range v.s.histories {
		if h != nil && h.CIID == ciID {
			cp := *h
			items = append(items, &cp)
		}
	}
	v.s.mu.RUnlock()
	slices.Reverse(items)
	return pageSlice(items, page, pageSize)
}

// pageSlice 内存分页。
func pageSlice[T any](items []T, page, pageSize int64) ([]T, int64, error) {
	total := int64(len(items))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	return items[start:min(start+pageSize, total)], total, nil
}

// ---- 同步控制面：采集源 / 任务 / 运行记录 / 凭据加密 ----

var (
	_ biz.SyncSourceRepo   = cmdbSyncSourceView{}
	_ biz.SyncTaskRepo     = cmdbSyncTaskView{}
	_ biz.SyncRunRepo      = cmdbSyncRunView{}
	_ biz.CredentialSealer = (*FakeSealer)(nil)
)

// FakeSealer 凭据加密桩：Enabled=false 模拟主密钥未配置；密文 = "sealed:" + 明文。
type FakeSealer struct {
	Enabled bool
}

// Seal implements biz.CredentialSealer.
func (f *FakeSealer) Seal(plaintext string) ([]byte, error) {
	if !f.Enabled {
		return nil, biz.ErrSecretUnavailable
	}
	return []byte("sealed:" + plaintext), nil
}

type cmdbSyncSourceEntry struct {
	src        biz.SyncSource
	credential []byte
	webhook    []byte
	deleted    bool
}

// WebhookHash 返回采集源当前回调令牌哈希（测试断言只存哈希）。
func (s *SyncStore) WebhookHash(id string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.sources[id]; ok {
		return slices.Clone(e.webhook)
	}
	return nil
}

// FakeIndexer 表达式索引桩：记录调用。
type FakeIndexer struct {
	mu      sync.Mutex
	Ensured []string
	Dropped []string
}

// EnsureAttrIndex implements biz.AttrIndexer.
func (f *FakeIndexer) EnsureAttrIndex(_ context.Context, modelCode, fieldCode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Ensured = append(f.Ensured, modelCode+"."+fieldCode)
	return nil
}

// DropAttrIndex implements biz.AttrIndexer.
func (f *FakeIndexer) DropAttrIndex(_ context.Context, modelCode, fieldCode string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Dropped = append(f.Dropped, modelCode+"."+fieldCode)
	return nil
}

// SyncStore 同步控制面三个仓储接口的内存实现（与 CmdbStore 分离：数据面不经 core）。
type SyncStore struct {
	mu      sync.RWMutex
	sources map[string]*cmdbSyncSourceEntry
	tasks   map[string]*biz.SyncTask
	runs    []*biz.SyncRun
}

// NewSyncStore new a SyncStore。
func NewSyncStore() *SyncStore {
	return &SyncStore{sources: map[string]*cmdbSyncSourceEntry{}, tasks: map[string]*biz.SyncTask{}}
}

// Sources 返回 biz.SyncSourceRepo 视图。
func (s *SyncStore) Sources() biz.SyncSourceRepo { return cmdbSyncSourceView{s} }

// Tasks 返回 biz.SyncTaskRepo 视图。
func (s *SyncStore) Tasks() biz.SyncTaskRepo { return cmdbSyncTaskView{s} }

// Runs 返回 biz.SyncRunRepo 视图。
func (s *SyncStore) Runs() biz.SyncRunRepo { return cmdbSyncRunView{s} }

// Credential 返回采集源当前密文（测试断言"只写不读"与轮换）。
func (s *SyncStore) Credential(id string) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.sources[id]; ok {
		return slices.Clone(e.credential)
	}
	return nil
}

// FinishTask 模拟数据面完成任务并写一条运行记录（控制面只读运行记录）。
func (s *SyncStore) FinishTask(taskID string, run *biz.SyncRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[taskID]; ok {
		t.Status = run.Status
	}
	cp := *run
	s.runs = append(s.runs, &cp)
}

func (s *SyncStore) activeTask(sourceID string) bool {
	for _, t := range s.tasks {
		if t.SourceID == sourceID && (t.Status == "pending" || t.Status == "running") {
			return true
		}
	}
	return false
}

type cmdbSyncSourceView struct{ s *SyncStore }

func (v cmdbSyncSourceView) Create(_ context.Context, src *biz.SyncSource, credential []byte) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	for _, e := range v.s.sources {
		if !e.deleted && e.src.Name == src.Name {
			return biz.ErrSourceNameExists
		}
	}
	cp := *src
	cp.Config = slices.Clone(src.Config)
	v.s.sources[src.ID] = &cmdbSyncSourceEntry{src: cp, credential: slices.Clone(credential)}
	return nil
}

func (v cmdbSyncSourceView) get(id string) (*biz.SyncSource, error) {
	e, ok := v.s.sources[id]
	if !ok || e.deleted {
		return nil, biz.ErrSourceNotFound
	}
	cp := e.src
	cp.Config = slices.Clone(e.src.Config)
	cp.HasCredential = len(e.credential) > 0
	cp.WebhookEnabled = len(e.webhook) > 0
	cp.ActiveTask = v.s.activeTask(id)
	return &cp, nil
}

func (v cmdbSyncSourceView) SetWebhookHash(_ context.Context, id string, hash []byte) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.sources[id]
	if !ok || e.deleted {
		return biz.ErrSourceNotFound
	}
	e.webhook = slices.Clone(hash)
	return nil
}

func (v cmdbSyncSourceView) Get(_ context.Context, id string) (*biz.SyncSource, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	return v.get(id)
}

func (v cmdbSyncSourceView) List(_ context.Context, filter biz.SyncSourceFilter, page, pageSize int64) ([]*biz.SyncSource, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.SyncSource, 0, len(v.s.sources))
	for id, e := range v.s.sources {
		if e.deleted || (filter.Provider != "" && e.src.Provider != filter.Provider) ||
			(filter.Keyword != "" && !strings.Contains(strings.ToLower(e.src.Name), strings.ToLower(filter.Keyword))) {
			continue
		}
		src, _ := v.get(id)
		items = append(items, src)
	}
	v.s.mu.RUnlock()
	slices.SortFunc(items, func(a, b *biz.SyncSource) int { return b.CreateTime.Compare(a.CreateTime) })
	return pageSlice(items, page, pageSize)
}

func (v cmdbSyncSourceView) Update(_ context.Context, src *biz.SyncSource, expectedVersion int64, credential []byte) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.sources[src.ID]
	if !ok || e.deleted || e.src.Version != expectedVersion {
		return biz.ErrSourceVersionConflict
	}
	for id, o := range v.s.sources {
		if id != src.ID && !o.deleted && o.src.Name == src.Name {
			return biz.ErrSourceNameExists
		}
	}
	cp := *src
	cp.Config = slices.Clone(src.Config)
	cp.Version = expectedVersion + 1
	e.src = cp
	if len(credential) > 0 {
		e.credential = slices.Clone(credential)
	}
	return nil
}

func (v cmdbSyncSourceView) SoftDelete(_ context.Context, id string, now time.Time) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	e, ok := v.s.sources[id]
	if !ok || e.deleted {
		return biz.ErrSourceNotFound
	}
	e.deleted, e.src.Enabled, e.src.UpdateTime = true, false, now
	return nil
}

type cmdbSyncTaskView struct{ s *SyncStore }

func (v cmdbSyncTaskView) Create(_ context.Context, task *biz.SyncTask) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if e, ok := v.s.sources[task.SourceID]; !ok || e.deleted {
		return biz.ErrSourceNotFound
	}
	if v.s.activeTask(task.SourceID) {
		return biz.ErrSyncBusy
	}
	cp := *task
	v.s.tasks[task.ID] = &cp
	return nil
}

type cmdbSyncRunView struct{ s *SyncStore }

func (v cmdbSyncRunView) ListBySource(_ context.Context, sourceID string, page, pageSize int64) ([]*biz.SyncRun, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.SyncRun, 0, 4)
	for _, r := range v.s.runs {
		if r.SourceID == sourceID {
			cp := *r
			items = append(items, &cp)
		}
	}
	v.s.mu.RUnlock()
	slices.Reverse(items)
	return pageSlice(items, page, pageSize)
}

// Search implements biz.CIRepo（名称/外部标识/标签/属性值子串匹配，名称命中优先）。
func (v cmdbCIView) Search(_ context.Context, filter biz.SearchFilter) ([]*biz.SearchHit, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	q := strings.ToLower(filter.Query)
	hits := make([]*biz.SearchHit, 0, 8)
	for _, e := range v.s.cis {
		ci := e.ci
		if e.deleted || (len(filter.ModelCodes) > 0 && !slices.Contains(filter.ModelCodes, ci.ModelCode)) ||
			(len(filter.Envs) > 0 && !slices.Contains(filter.Envs, ci.Env)) {
			continue
		}
		var field, value string
		switch {
		case strings.Contains(strings.ToLower(ci.Name), q):
			field, value = "name", ci.Name
		case strings.Contains(strings.ToLower(ci.ExternalID), q):
			field, value = "externalId", ci.ExternalID
		default:
			for _, tag := range ci.Tags {
				if strings.Contains(strings.ToLower(tag), q) {
					field, value = "tags", tag
					break
				}
			}
			if field == "" {
				keys := make([]string, 0, len(ci.Attrs))
				for k := range ci.Attrs {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				for _, k := range keys {
					if s := fmt.Sprint(ci.Attrs[k]); strings.Contains(strings.ToLower(s), q) {
						field, value = k, s
						break
					}
				}
			}
		}
		if field == "" {
			continue
		}
		hits = append(hits, &biz.SearchHit{CI: cloneCI(&ci), Field: field, Value: value})
	}
	slices.SortFunc(hits, func(a, b *biz.SearchHit) int {
		an, bn := a.Field == "name", b.Field == "name"
		if an != bn {
			if an {
				return -1
			}
			return 1
		}
		return b.CI.UpdateTime.Compare(a.CI.UpdateTime)
	})
	if len(hits) > int(filter.Limit) {
		hits = hits[:filter.Limit]
	}
	return hits, nil
}

// Neighbors implements biz.RelationRepo（BFS，语义对齐 data 层）。
func (v cmdbRelationView) Neighbors(_ context.Context, ciID string, depth, maxNodes int32) ([]*biz.TopologyNode, []*biz.TopologyEdge, bool, error) {
	v.s.mu.RLock()
	defer v.s.mu.RUnlock()
	node := func(id string, d int32) *biz.TopologyNode {
		e, ok := v.s.cis[id]
		if !ok || e.deleted {
			return nil
		}
		name := ""
		if m, ok := v.s.models[e.ci.ModelCode]; ok {
			name = m.model.Name
		}
		return &biz.TopologyNode{ID: id, Name: e.ci.Name, ModelCode: e.ci.ModelCode, ModelName: name, Status: e.ci.Status, Env: e.ci.Env, Depth: d}
	}
	root := node(ciID, 0)
	if root == nil {
		return nil, nil, false, biz.ErrCINotFound
	}
	nodes := []*biz.TopologyNode{root}
	seen := map[string]struct{}{ciID: {}}
	seenEdge := map[string]struct{}{}
	edges := []*biz.TopologyEdge{}
	frontier := []string{ciID}
	truncated := false
	relIDs := make([]string, 0, len(v.s.relations))
	for id := range v.s.relations {
		relIDs = append(relIDs, id)
	}
	slices.Sort(relIDs)
	for level := int32(1); level <= depth && len(frontier) > 0 && !truncated; level++ {
		var next []string
		for _, id := range relIDs {
			r := v.s.relations[id]
			var other string
			switch {
			case slices.Contains(frontier, r.SrcID):
				other = r.DstID
			case slices.Contains(frontier, r.DstID):
				other = r.SrcID
			default:
				continue
			}
			if _, ok := seenEdge[id]; !ok {
				seenEdge[id] = struct{}{}
				name := ""
				if t, ok := v.s.relTypes[r.TypeCode]; ok {
					name = t.t.ForwardName
				}
				edges = append(edges, &biz.TopologyEdge{ID: id, TypeCode: r.TypeCode, Name: name, SrcID: r.SrcID, DstID: r.DstID})
			}
			if _, ok := seen[other]; ok {
				continue
			}
			if len(nodes) >= int(maxNodes) {
				truncated = true
				continue
			}
			n := node(other, level)
			if n == nil {
				continue
			}
			seen[other] = struct{}{}
			nodes = append(nodes, n)
			next = append(next, other)
		}
		frontier = next
	}
	kept := make([]*biz.TopologyEdge, 0, len(edges))
	for _, e := range edges {
		if _, ok := seen[e.SrcID]; !ok {
			continue
		}
		if _, ok := seen[e.DstID]; !ok {
			continue
		}
		kept = append(kept, e)
	}
	return nodes, kept, truncated, nil
}

// List implements biz.HistoryRepo（全局审计）。
func (v cmdbHistoryView) List(_ context.Context, filter biz.HistoryFilter, page, pageSize int64) ([]*biz.History, int64, error) {
	v.s.mu.RLock()
	items := make([]*biz.History, 0, 16)
	for _, h := range v.s.histories {
		if h == nil {
			continue
		}
		e, ok := v.s.cis[h.CIID]
		if !ok {
			continue
		}
		ci := e.ci
		if (len(filter.ModelCodes) > 0 && !slices.Contains(filter.ModelCodes, ci.ModelCode)) ||
			(len(filter.Envs) > 0 && !slices.Contains(filter.Envs, ci.Env)) ||
			(filter.ModelCode != "" && ci.ModelCode != filter.ModelCode) ||
			(filter.Action != "" && h.Action != filter.Action) ||
			(filter.Operator != "" && h.Operator != filter.Operator) ||
			(filter.Keyword != "" && !strings.Contains(strings.ToLower(ci.Name), strings.ToLower(filter.Keyword))) ||
			(filter.From != nil && h.CreateTime.Before(*filter.From)) ||
			(filter.To != nil && h.CreateTime.After(*filter.To)) {
			continue
		}
		cp := *h
		cp.CIName, cp.ModelCode = ci.Name, ci.ModelCode
		cp.Diff = cloneDiff(h.Diff)
		items = append(items, &cp)
	}
	v.s.mu.RUnlock()
	slices.Reverse(items)
	return pageSlice(items, page, pageSize)
}

// cloneDiff 深拷贝 diff 的两层（脱敏时会原地修改）。
func cloneDiff(diff map[string]any) map[string]any {
	out := make(map[string]any, len(diff))
	for k, v := range diff {
		if m, ok := v.(map[string]any); ok {
			cm := make(map[string]any, len(m))
			for k2, v2 := range m {
				if attrs, ok := v2.(map[string]any); ok {
					ca := make(map[string]any, len(attrs))
					for k3, v3 := range attrs {
						ca[k3] = v3
					}
					cm[k2] = ca
				} else {
					cm[k2] = v2
				}
			}
			out[k] = cm
		} else {
			out[k] = v
		}
	}
	return out
}

// CIAttr 读取资源的原始属性值（测试断言脱敏不污染存储）。
func (s *CmdbStore) CIAttr(id, code string) any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.cis[id]; ok {
		return e.ci.Attrs[code]
	}
	return nil
}
