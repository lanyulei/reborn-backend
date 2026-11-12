package biz

import (
	"context"
	"encoding/base64"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 资源生命周期状态（字面量由 kit/cmdbcontract 统一，数据面对账写入同一取值）。
const (
	CIStatusPlanning    = cmdbcontract.CIStatusPlanning
	CIStatusActive      = cmdbcontract.CIStatusActive
	CIStatusMaintenance = cmdbcontract.CIStatusMaintenance
	CIStatusRetired     = cmdbcontract.CIStatusRetired
)

// 历史动作。
const (
	ActionCreate     = cmdbcontract.ActionCreate
	ActionUpdate     = cmdbcontract.ActionUpdate
	ActionTransition = cmdbcontract.ActionTransition
	ActionDelete     = cmdbcontract.ActionDelete
	ActionLink       = cmdbcontract.ActionLink
	ActionUnlink     = cmdbcontract.ActionUnlink
)

// transitions 状态机：允许的 from → to。
var transitions = map[string][]string{
	CIStatusPlanning:    {CIStatusActive, CIStatusRetired},
	CIStatusActive:      {CIStatusMaintenance, CIStatusRetired},
	CIStatusMaintenance: {CIStatusActive, CIStatusRetired},
	CIStatusRetired:     {},
}

// CanTransition 报告状态流转是否被状态机允许。
func CanTransition(from, to string) bool {
	return slices.Contains(transitions[from], to)
}

// CI 资源实例领域模型。
type CI struct {
	ID           string
	ModelCode    string
	ModelName    string
	Name         string
	Status       string
	Env          string
	Owner        string
	Tags         []string
	Source       string
	ExternalID   string
	UniqueKey    string
	Attrs        map[string]any
	LastSyncedAt *time.Time
	// SourceName / SourceDeleted 同步来源的采集源名称与是否已删除（manual 为空）。
	SourceName    string
	SourceDeleted bool
	Stale         bool
	Version       int64
	CreateUser    string
	UpdateUser    string
	CreateTime    time.Time
	UpdateTime    time.Time
}

// CIFilter 资源列表过滤；零值不过滤，Attrs 为属性等值过滤。
type CIFilter struct {
	// Cursor 游标分页（上一页 NextCursor）；非空时忽略 page（按 update_time, id 键集）。
	Cursor string
	// RelType / RelDirection / RelTargetID / RelNone 关联条件：
	// RelTargetID 非空 = 关联到该资源；RelNone = 该方向上无任何关联；两者皆空 = 有任意关联。
	RelType      string
	RelDirection string
	RelTargetID  string
	RelNone      bool
	ModelCode    string
	// ModelCodes 可见类型集合（数据范围守卫叠加；与 ModelCode 同时存在时取交集语义由守卫保证）。
	ModelCodes []string
	Keyword    string
	Status     string
	Env        string
	// Envs 可见环境集合（数据范围守卫叠加）。
	Envs  []string
	Tag   string
	Attrs map[string]any
}

// HistoryFilter 全局变更审计条件（ModelCodes / Envs 由数据范围守卫叠加）。
type HistoryFilter struct {
	ModelCodes []string
	Envs       []string
	ModelCode  string
	Action     string
	Operator   string
	Keyword    string
	From       *time.Time
	To         *time.Time
}

// MaskedValue 敏感属性读取占位值。
const MaskedValue = cmdbcontract.MaskedValue

// EncodeCursor 由资源构造下一页游标（update_time 纳秒 + id，base64）。
func EncodeCursor(ci *CI) string {
	if ci == nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(ci.UpdateTime.UnixNano(), 10) + "|" + ci.ID))
}

// DecodeCursor 解析游标；非法返回 ErrCIInvalid。
func DecodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", detail(ErrCIInvalid, "invalid cursor")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || !uuidPattern.MatchString(parts[1]) {
		return time.Time{}, "", detail(ErrCIInvalid, "invalid cursor")
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", detail(ErrCIInvalid, "invalid cursor")
	}
	return time.Unix(0, nanos), parts[1], nil
}

// SearchFilter 全局搜索条件（ModelCodes / Envs 由数据范围守卫叠加，空 = 不限）。
type SearchFilter struct {
	Query      string
	ModelCodes []string
	Envs       []string
	Limit      int32
}

// SearchHit 搜索命中：Field 为 name / externalId / tags 或属性字段编码。
type SearchHit struct {
	CI    *CI
	Field string
	Value string
}

// SearchMaxLimit 全局搜索返回上限。
const SearchMaxLimit = 100

// CIUpdate 资源部分更新；nil 不更新。Attrs 按键合并，值为 nil 的键清空。
type CIUpdate struct {
	Name       *string
	Env        *string
	Owner      *string
	Tags       *[]string
	ExternalID *string
	Attrs      map[string]any
}

// History 变更历史（append-only）。
type History struct {
	ID         string
	CIID       string
	CIName     string
	ModelCode  string
	Action     string
	Diff       map[string]any
	Operator   string
	Source     string
	RequestID  string
	CreateTime time.Time
}

// RelationSummary 资源在某关系类型某方向上的关联统计。
type RelationSummary struct {
	RelationType string
	Direction    string
	Name         string
	TargetModel  string
	Count        int64
}

// CIRepo 资源仓储接口：写操作与历史同事务。
type CIRepo interface {
	Create(ctx context.Context, ci *CI, h *History) error
	Get(ctx context.Context, id string) (*CI, error)
	// GetByName 按类型 + 名称取未删除资源；不存在返回 ErrCINotFound。
	GetByName(ctx context.Context, modelCode, name string) (*CI, error)
	List(ctx context.Context, filter CIFilter, page, pageSize int64) ([]*CI, int64, error)
	// Update 以 expectedVersion 条件更新全部可变列并 +1 版本；不匹配返回 ErrCIVersionConflict。
	Update(ctx context.Context, ci *CI, expectedVersion int64, h *History) error
	SoftDelete(ctx context.Context, id string, at time.Time, h *History) error
	// ExistsInModel 报告资源是否存在且属于该类型（ref 字段校验）。
	ExistsInModel(ctx context.Context, modelCode, id string) (bool, error)
	// Search 名称 / 外部标识 / 标签 / 属性值子串匹配；名称命中优先，其次更新时间倒序。
	Search(ctx context.Context, filter SearchFilter) ([]*SearchHit, error)
}

// HistoryRepo 变更历史只读接口（写入随资源/关系操作同事务）。
type HistoryRepo interface {
	ListByCI(ctx context.Context, ciID string, page, pageSize int64) ([]*History, int64, error)
	// List 全局审计（join 资源取名称/类型；时间倒序）。
	List(ctx context.Context, filter HistoryFilter, page, pageSize int64) ([]*History, int64, error)
}

// CIUsecase 资源用例：属性校验、唯一键、乐观锁、状态机、历史。
type CIUsecase struct {
	models     ModelRepo
	fields     FieldRepo
	relTypes   RelationTypeRepo
	cis        CIRepo
	relations  RelationRepo
	history    HistoryRepo
	relationUC *RelationUsecase
}

// NewCIUsecase new a CIUsecase.
func NewCIUsecase(models ModelRepo, fields FieldRepo, relTypes RelationTypeRepo, cis CIRepo, relations RelationRepo, history HistoryRepo, relationUC *RelationUsecase) *CIUsecase {
	return &CIUsecase{models: models, fields: fields, relTypes: relTypes, cis: cis, relations: relations, history: history, relationUC: relationUC}
}

// Create 创建资源：类型须启用；属性按字段定义全量校验；ref 字段指向存在的资源。
func (uc *CIUsecase) Create(ctx context.Context, actor Actor, ci *CI) (*CI, error) {
	m, err := uc.models.Get(ctx, ci.ModelCode)
	if err != nil {
		return nil, err
	}
	if m.Status != ModelStatusEnabled {
		return nil, detail(ErrCIInvalid, "model %s is disabled", m.Code)
	}
	fields, err := uc.fields.ListByModel(ctx, ci.ModelCode)
	if err != nil {
		return nil, err
	}
	attrs, err := ValidateAttrs(fields, ci.Attrs, false)
	if err != nil {
		return nil, err
	}
	if err := uc.checkRefs(ctx, fields, attrs); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if ci.Status == "" {
		ci.Status = CIStatusActive
	}
	ci.ID, ci.Attrs, ci.UniqueKey = id, attrs, UniqueKey(fields, attrs)
	ci.Source, ci.Version = actor.source(), 1
	ci.CreateUser, ci.UpdateUser, ci.CreateTime, ci.UpdateTime = actor.UserID, actor.UserID, now, now
	if ci.Tags == nil {
		ci.Tags = []string{}
	}
	h, err := newHistory(actor, ci.ID, ActionCreate, map[string]any{"after": snapshot(ci)})
	if err != nil {
		return nil, err
	}
	if err := uc.cis.Create(ctx, ci, h); err != nil {
		return nil, err
	}
	return uc.cis.Get(ctx, ci.ID)
}

// GetByID 资源详情（不含关联统计；守卫与编排用）。
func (uc *CIUsecase) GetByID(ctx context.Context, id string) (*CI, error) {
	return uc.cis.Get(ctx, id)
}

// Get 资源详情 + 各适用关系类型两个方向的关联统计。
func (uc *CIUsecase) Get(ctx context.Context, id string) (*CI, []RelationSummary, error) {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if err := uc.mask(ctx, ci); err != nil {
		return nil, nil, err
	}
	types, err := uc.relTypes.List(ctx, ci.ModelCode)
	if err != nil {
		return nil, nil, err
	}
	counts, err := uc.relations.CountByCI(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	summaries := make([]RelationSummary, 0, len(types)*2)
	for _, t := range types {
		if t.SrcModel == ModelAny || t.SrcModel == ci.ModelCode {
			summaries = append(summaries, RelationSummary{
				RelationType: t.Code, Direction: DirectionOutgoing, Name: t.ForwardName,
				TargetModel: t.DstModel, Count: counts[RelationKey{t.Code, DirectionOutgoing}],
			})
		}
		if t.DstModel == ModelAny || t.DstModel == ci.ModelCode {
			summaries = append(summaries, RelationSummary{
				RelationType: t.Code, Direction: DirectionIncoming, Name: t.BackwardName,
				TargetModel: t.SrcModel, Count: counts[RelationKey{t.Code, DirectionIncoming}],
			})
		}
	}
	return ci, summaries, nil
}

// Search 全局搜索（limit 非法时取 20，上限 SearchMaxLimit）。
func (uc *CIUsecase) Search(ctx context.Context, filter SearchFilter) ([]*SearchHit, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	filter.Limit = min(filter.Limit, SearchMaxLimit)
	hits, err := uc.cis.Search(ctx, filter)
	if err != nil {
		return nil, err
	}
	cis := make([]*CI, 0, len(hits))
	for _, h := range hits {
		cis = append(cis, h.CI)
	}
	if err := uc.maskAll(ctx, cis); err != nil {
		return nil, err
	}
	for _, h := range hits {
		if h.Field != "name" && h.Field != "externalId" && h.Field != "tags" && h.CI.Attrs[h.Field] == MaskedValue {
			h.Value = MaskedValue
		}
	}
	return hits, nil
}

// List 分页查询资源（filter.Cursor 非空时为键集分页，返回值 nextCursor 由 service 取末行构造）。
func (uc *CIUsecase) List(ctx context.Context, filter CIFilter, page, pageSize int64) ([]*CI, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	if filter.Cursor != "" {
		if _, _, err := DecodeCursor(filter.Cursor); err != nil {
			return nil, 0, err
		}
	}
	items, total, err := uc.cis.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if err := uc.maskAll(ctx, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListHistory 全局变更审计（diff 中的敏感属性脱敏）。
func (uc *CIUsecase) ListHistory(ctx context.Context, filter HistoryFilter, page, pageSize int64) ([]*History, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.history.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if err := uc.maskHistories(ctx, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// mask 按类型敏感字段把属性值替换为占位值。
func (uc *CIUsecase) mask(ctx context.Context, ci *CI) error {
	return uc.maskAll(ctx, []*CI{ci})
}

// maskAll 批量脱敏（每类型只取一次字段定义）。
func (uc *CIUsecase) maskAll(ctx context.Context, items []*CI) error {
	byModel := map[string][]string{}
	for _, ci := range items {
		if ci == nil {
			continue
		}
		codes, ok := byModel[ci.ModelCode]
		if !ok {
			fields, err := uc.fields.ListByModel(ctx, ci.ModelCode)
			if err != nil {
				return err
			}
			codes = sensitiveCodes(fields)
			byModel[ci.ModelCode] = codes
		}
		MaskAttrs(ci.Attrs, codes)
	}
	return nil
}

// maskHistories 历史 diff 的 before/after.attrs 中敏感键脱敏。
func (uc *CIUsecase) maskHistories(ctx context.Context, items []*History) error {
	byModel := map[string][]string{}
	for _, h := range items {
		if h == nil || h.ModelCode == "" {
			continue
		}
		codes, ok := byModel[h.ModelCode]
		if !ok {
			fields, err := uc.fields.ListByModel(ctx, h.ModelCode)
			if err != nil {
				return err
			}
			codes = sensitiveCodes(fields)
			byModel[h.ModelCode] = codes
		}
		if len(codes) == 0 {
			continue
		}
		for _, side := range []string{"before", "after"} {
			if m, ok := h.Diff[side].(map[string]any); ok {
				if attrs, ok := m["attrs"].(map[string]any); ok {
					MaskAttrs(attrs, codes)
				}
			}
		}
	}
	return nil
}

func sensitiveCodes(fields []*Field) []string {
	var codes []string
	for _, f := range fields {
		if f.Sensitive {
			codes = append(codes, f.Code)
		}
	}
	return codes
}

// MaskAttrs 把 attrs 中存在且非空的敏感键替换为 MaskedValue。
func MaskAttrs(attrs map[string]any, codes []string) {
	for _, code := range codes {
		if v, ok := attrs[code]; ok && v != nil {
			attrs[code] = MaskedValue
		}
	}
}

// dropMaskedValues 写入前移除值等于占位符的敏感键（表单回显的脱敏值不得覆盖真实值）。
func dropMaskedValues(fields []*Field, patch map[string]any) map[string]any {
	if len(patch) == 0 {
		return patch
	}
	for _, f := range fields {
		if f.Sensitive {
			if v, ok := patch[f.Code]; ok && v == MaskedValue {
				delete(patch, f.Code)
			}
		}
	}
	return patch
}

// Update 更新资源：乐观锁；属性按键合并后全量重校验；只记录真实变化。
func (uc *CIUsecase) Update(ctx context.Context, actor Actor, id string, expectedVersion int64, upd CIUpdate) (*CI, error) {
	cur, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.Version != expectedVersion {
		return nil, ErrCIVersionConflict
	}
	fields, err := uc.fields.ListByModel(ctx, cur.ModelCode)
	if err != nil {
		return nil, err
	}
	next := *cur
	next.Attrs = cloneAttrs(cur.Attrs)
	if upd.Name != nil {
		next.Name = *upd.Name
	}
	if upd.Env != nil {
		next.Env = *upd.Env
	}
	if upd.Owner != nil {
		next.Owner = *upd.Owner
	}
	if upd.ExternalID != nil {
		next.ExternalID = *upd.ExternalID
	}
	if upd.Tags != nil {
		next.Tags = slices.Clone(*upd.Tags)
	}
	if upd.Attrs != nil {
		upd.Attrs = dropMaskedValues(fields, cloneAttrs(upd.Attrs))
		// 同步只读字段不接受人工修改（同步来源可写）。
		if actor.source() == SourceManual {
			if err := rejectSyncOwned(fields, cur.Attrs, upd.Attrs); err != nil {
				return nil, err
			}
		}
		// 增量校验改动的键（partial），再合并；清空的键从结果移除。
		patch, err := ValidateAttrs(fields, upd.Attrs, true)
		if err != nil {
			return nil, err
		}
		for key, v := range upd.Attrs {
			if v == nil {
				delete(next.Attrs, key)
			}
		}
		for key, v := range patch {
			next.Attrs[key] = v
		}
		// 合并后全量校验必填（防止清空必填字段）。
		if _, err := ValidateAttrs(fields, next.Attrs, false); err != nil {
			return nil, err
		}
		if err := uc.checkRefs(ctx, fields, patch); err != nil {
			return nil, err
		}
	}
	before, after := diffCI(cur, &next)
	if len(after) == 0 {
		return cur, nil
	}
	next.UniqueKey = UniqueKey(fields, next.Attrs)
	next.Version = cur.Version + 1
	next.UpdateUser, next.UpdateTime = actor.UserID, time.Now()
	h, err := newHistory(actor, id, ActionUpdate, map[string]any{"before": before, "after": after})
	if err != nil {
		return nil, err
	}
	if err := uc.cis.Update(ctx, &next, expectedVersion, h); err != nil {
		return nil, err
	}
	return uc.cis.Get(ctx, id)
}

// Transition 状态流转：状态机校验；退役时按关系类型策略级联退役 dst 资源（restrict 则拒绝）。
func (uc *CIUsecase) Transition(ctx context.Context, actor Actor, id string, expectedVersion int64, to string) (*CI, error) {
	cur, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.Version != expectedVersion {
		return nil, ErrCIVersionConflict
	}
	if !CanTransition(cur.Status, to) {
		return nil, detail(ErrCITransitionInvalid, "%s → %s", cur.Status, to)
	}
	if to == CIStatusRetired {
		if err := uc.retireDependents(ctx, actor, cur); err != nil {
			return nil, err
		}
	}
	next := *cur
	next.Status = to
	next.Version = cur.Version + 1
	next.UpdateUser, next.UpdateTime = actor.UserID, time.Now()
	h, err := newHistory(actor, id, ActionTransition, map[string]any{
		"before": map[string]any{"status": cur.Status}, "after": map[string]any{"status": to},
	})
	if err != nil {
		return nil, err
	}
	if err := uc.cis.Update(ctx, &next, expectedVersion, h); err != nil {
		return nil, err
	}
	return uc.cis.Get(ctx, id)
}

// retireDependents 退役前处理 outgoing 关系：cascade_retire 的 dst 逐个退役
// （未退役者），restrict 且仍有关联则拒绝。
func (uc *CIUsecase) retireDependents(ctx context.Context, actor Actor, ci *CI) error {
	types, err := uc.relTypes.List(ctx, ci.ModelCode)
	if err != nil {
		return err
	}
	for _, t := range types {
		if t.SrcModel != ModelAny && t.SrcModel != ci.ModelCode {
			continue
		}
		// 逐页遍历（依赖数可能超过单页上限）。
		for page := int64(1); ; page++ {
			related, total, err := uc.relations.ListRelated(ctx, ci.ID, t.Code, DirectionOutgoing, "", page, paging.MaxPageSize)
			if err != nil {
				return err
			}
			if page == 1 && total > 0 && t.OnDelete != OnDeleteCascadeRetire {
				return detail(ErrCIInUse, "%d resources linked via %s", total, t.Code)
			}
			for _, r := range related {
				if r.CI.Status == CIStatusRetired {
					continue
				}
				if _, err := uc.Transition(ctx, actor, r.CI.ID, r.CI.Version, CIStatusRetired); err != nil {
					return err
				}
			}
			if int64(page*paging.MaxPageSize) >= total {
				break
			}
		}
	}
	return nil
}

// Delete 软删资源：仍有任何关系时拒绝（先解除关系）。
func (uc *CIUsecase) Delete(ctx context.Context, actor Actor, id string) error {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return err
	}
	counts, err := uc.relations.CountByCI(ctx, id)
	if err != nil {
		return err
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	if total > 0 {
		return detail(ErrCIInUse, "%d relations", total)
	}
	h, err := newHistory(actor, id, ActionDelete, map[string]any{"before": snapshot(ci)})
	if err != nil {
		return err
	}
	return uc.cis.SoftDelete(ctx, id, time.Now(), h)
}

// History 分页查询资源变更历史。
func (uc *CIUsecase) History(ctx context.Context, id string, page, pageSize int64) ([]*History, int64, error) {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.history.ListByCI(ctx, id, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	for _, h := range items {
		if h.ModelCode == "" {
			h.ModelCode, h.CIName = ci.ModelCode, ci.Name
		}
	}
	if err := uc.maskHistories(ctx, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// checkRefs ref 字段的值必须是对应类型下存在的资源 ID。
func (uc *CIUsecase) checkRefs(ctx context.Context, fields []*Field, attrs map[string]any) error {
	for _, f := range fields {
		if f.Type != FieldTypeRef || f.Deprecated {
			continue
		}
		v, ok := attrs[f.Code]
		if !ok || v == nil {
			continue
		}
		id, _ := v.(string)
		if !uuidPattern.MatchString(id) {
			return detail(ErrCIInvalid, "field %q must be a resource id", f.Code)
		}
		exists, err := uc.cis.ExistsInModel(ctx, f.RefModel, id)
		if err != nil {
			return err
		}
		if !exists {
			return detail(ErrCIInvalid, "field %q references a missing %s resource", f.Code, f.RefModel)
		}
	}
	return nil
}

// uuidPattern ref 字段值须为 UUID 文本（避免非法文本直达 uuid 列报 500）。
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// rejectSyncOwned 人工更新不得改动 owner=sync 的字段（值不变允许回传）。
func rejectSyncOwned(fields []*Field, cur, patch map[string]any) error {
	for _, f := range fields {
		if f.Owner != FieldOwnerSync || f.Deprecated {
			continue
		}
		v, ok := patch[f.Code]
		if !ok {
			continue
		}
		if !reflect.DeepEqual(normalizeNumber(v), normalizeNumber(cur[f.Code])) {
			return detail(ErrCIInvalid, "field %q is maintained by sync and cannot be edited", f.Code)
		}
	}
	return nil
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

// newHistory 构造历史记录（ID 由此生成）。
func newHistory(actor Actor, ciID, action string, diff map[string]any) (*History, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	return &History{
		ID: id, CIID: ciID, Action: action, Diff: diff,
		Operator: actor.UserID, Source: actor.source(), RequestID: actor.RequestID, CreateTime: time.Now(),
	}, nil
}

// snapshot 资源的可审计快照（固定列 + attrs）。
func snapshot(ci *CI) map[string]any {
	return map[string]any{
		"name": ci.Name, "status": ci.Status, "env": ci.Env, "owner": ci.Owner,
		"tags": slices.Clone(ci.Tags), "externalId": ci.ExternalID, "attrs": cloneAttrs(ci.Attrs),
	}
}

// diffCI 计算固定列与属性的差异（仅变化的键）。
func diffCI(cur, next *CI) (before, after map[string]any) {
	before, after = map[string]any{}, map[string]any{}
	mark := func(key string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			before[key], after[key] = a, b
		}
	}
	mark("name", cur.Name, next.Name)
	mark("env", cur.Env, next.Env)
	mark("owner", cur.Owner, next.Owner)
	mark("externalId", cur.ExternalID, next.ExternalID)
	if !slices.Equal(cur.Tags, next.Tags) {
		before["tags"], after["tags"] = slices.Clone(cur.Tags), slices.Clone(next.Tags)
	}
	ab, aa := map[string]any{}, map[string]any{}
	for key, v := range cur.Attrs {
		if nv, ok := next.Attrs[key]; !ok || !reflect.DeepEqual(v, nv) {
			ab[key] = v
			if ok {
				aa[key] = nv
			} else {
				aa[key] = nil
			}
		}
	}
	for key, nv := range next.Attrs {
		if _, ok := cur.Attrs[key]; !ok {
			ab[key] = nil
			aa[key] = nv
		}
	}
	if len(aa) > 0 {
		before["attrs"], after["attrs"] = ab, aa
	}
	return before, after
}

func cloneAttrs(attrs map[string]any) map[string]any {
	out := make(map[string]any, len(attrs))
	for k, v := range attrs {
		out[k] = v
	}
	return out
}
