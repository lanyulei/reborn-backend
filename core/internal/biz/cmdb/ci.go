package biz

import (
	"context"
	"encoding/base64"
	"reflect"
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
	ActionRestore    = cmdbcontract.ActionRestore
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
	Attrs        map[string]any
	LastSyncedAt *time.Time
	// SourceName / SourceDeleted 同步来源的采集源名称与是否已删除（manual 为空）。
	SourceName    string
	SourceDeleted bool
	// OwnerName 负责人为系统用户（Owner 存用户 ID）时的显示名；否则为空。
	OwnerName string
	// DeleteTime 删除时间（仅回收站查询返回）。
	DeleteTime *time.Time
	Stale      bool
	Version    int64
	CreateUser string
	UpdateUser string
	CreateTime time.Time
	UpdateTime time.Time
}

// CIFilter 资源列表过滤；零值不过滤，Attrs 为属性等值过滤。
type CIFilter struct {
	// Cursor 游标分页（上一页 NextCursor）；非空时忽略 page（按 update_time, id 键集）。
	Cursor string
	// NoCount 不统计总数（返回 total = -1）：内部批量遍历（导出）用，省掉逐页 count(*)。
	NoCount bool
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
	// Owner 负责人精确匹配（用户 ID 或原文）。
	Owner string
	// Deleted 为真查询回收站（已删除的资源），否则只查未删除。
	Deleted bool
	// Issue 数据质量问题（Issue* 常量）；与数据质量看板同口径，只含在册（未退役）资源。
	Issue string
}

// 数据质量问题类别。
const (
	IssueMissingRequired = "missing_required"
	IssueNoOwner         = "no_owner"
	IssueOrphan          = "orphan"
	IssueStale           = "stale"
	IssueOutdated        = "outdated"
	// QualityOutdatedDays "长期未更新"的天数阈值。
	QualityOutdatedDays = 180
)

// HistoryFilter 全局变更审计条件（ModelCodes / Envs 由数据范围守卫叠加）。
type HistoryFilter struct {
	// CIIDs 限定资源（故障管理的变更关联用）；为空不限。
	CIIDs      []string
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
	if len(parts) != 2 || !idgen.IsUUIDFold(parts[1]) {
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
	// Attr 命中来自属性值（Field 为属性字段编码；与固定列同名的字段编码据此区分）。
	Attr bool
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
	// GetByIDs 批量取未删除资源（一条语句）；不存在的 ID 静默略过，顺序不保证。
	GetByIDs(ctx context.Context, ids []string) ([]*CI, error)
	// GetByName 按类型 + 名称取未删除资源；不存在返回 ErrCINotFound。
	GetByName(ctx context.Context, modelCode, name string) (*CI, error)
	List(ctx context.Context, filter CIFilter, page, pageSize int64) ([]*CI, int64, error)
	// Update 以 expectedVersion 条件更新全部可变列并 +1 版本；不匹配返回 ErrCIVersionConflict。
	Update(ctx context.Context, ci *CI, expectedVersion int64, h *History) error
	// SoftDelete 软删资源：事务内锁住资源行后数关系，仍有关系返回 ErrCIInUse（带条数）；不存在返回 ErrCINotFound。
	SoftDelete(ctx context.Context, id string, at time.Time, h *History) error
	// Unclaim 解除同步托管：来源置为 manual、清空外部标识与消失 / 陈旧标记，版本 +1 并写历史
	// （与对账互斥）。资源不由采集源维护时返回 ErrNotSyncManaged。
	Unclaim(ctx context.Context, ci *CI, actor Actor, at time.Time, historyID string) error
	// GetDeleted 取回收站中的资源；不存在或未删除返回 ErrCINotFound。
	GetDeleted(ctx context.Context, id string) (*CI, error)
	// Restore 恢复已删除的资源（版本 +1，历史同事务）：名称 / 唯一规则 / 同源外部标识已被
	// 其它未删除资源占用时返回 ErrCIDuplicate。
	Restore(ctx context.Context, ci *CI, at time.Time, h *History) error
	// ExistsInModel 报告资源是否存在且属于该类型（ref 字段校验）。
	ExistsInModel(ctx context.Context, modelCode, id string) (bool, error)
	// Search 名称 / 外部标识 / 标签 / 属性值子串匹配；名称命中优先，其次更新时间倒序。
	Search(ctx context.Context, filter SearchFilter) ([]*SearchHit, error)
}

// CIBatchUpdater 仓储可选能力：多条资源的乐观锁更新与历史在同一事务内完成
// （items[i].Version 为新版本，期望版本 = Version-1；任一失败整体回滚）。
// 级联退役使用；仓储未实现时退化为逐条 Update。
type CIBatchUpdater interface {
	UpdateBatch(ctx context.Context, items []*CI, histories []*History) error
}

// CIBatchLookup 仓储可选能力：GetByName 的批量版本，一条语句取某类型下多个名称对应的已存在资源。
// 导入用它一次取齐本批名称与关系目标（此前逐行 GetByName）；结果口径须与 GetByName 完全一致
// （同一组列、不含已删除）。仓储不具备该能力或查询失败时导入退回逐行查询。
type CIBatchLookup interface {
	// GetByNames 返回 modelCode 下名称在 names 内的未删除资源；不存在的名称不在结果里，顺序不保证。
	GetByNames(ctx context.Context, modelCode string, names []string) ([]*CI, error)
}

// RetireEdge 退役闭包展开用的一条出边：SrcID 经关系类型 TypeCode（OnDelete 为其删除策略）指向未删除的对端。
type RetireEdge struct {
	SrcID    string
	TypeCode string
	OnDelete string
	CI       *CI
}

// RetireGraphRepo 关系仓储的可选能力：一条语句取齐退役闭包展开要读的全部出边。此前闭包内每个
// 资源 × 每个适用的关系类型都要计数 + 分页列举（见 retireClosureByType），上千台虚拟机的宿主机
// 退役即数千次往返。生产仓储必须实现（编译期断言）；仓储不具备该能力时（内存桩）退回逐类型遍历。
type RetireGraphRepo interface {
	// RetireEdges 取以 rootID 为根的闭包的出边。会被展开的资源 = 根，加上沿「关系类型未删除、
	// cascade_retire、以该资源的类型为源端适用」的出边可达、且未删除未退役的对端（已退役 / 已删除的
	// 对端不再展开）。返回这些资源的全部出边：关系类型未删除且以该资源的类型为源端适用、对端未删除
	// （任何删除策略；已退役的对端也包含在内，是否跳过由用例层判定）。
	// 行序：资源、关系类型（create_time, code）、对端（name, id）升序——用例层据此重放逐类型遍历的
	// 判定顺序（先遇到的拒绝原因胜出、闭包内资源的顺序），见 retireClosureOf。
	RetireEdges(ctx context.Context, rootID string) ([]*RetireEdge, error)
}

// HistoryRepo 变更历史只读接口（写入随资源/关系操作同事务）。
type HistoryRepo interface {
	ListByCI(ctx context.Context, ciID string, page, pageSize int64) ([]*History, int64, error)
	// ListAfter 资源在 after 之后的变更（时间倒序），至多 limit 条。
	ListAfter(ctx context.Context, ciID string, after time.Time, limit int) ([]*History, error)
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
	owners     OwnerDirectory
}

// OwnerCandidate 可选负责人（系统用户的最小投影：不含邮箱、手机号等）。
type OwnerCandidate struct {
	ID   string
	Name string
}

// OwnerCandidateLimit 可选负责人单次返回上限。
const OwnerCandidateLimit = 50

// OwnerDirectory 负责人目录（data 层读系统用户表实现）。
type OwnerDirectory interface {
	// ListOwnerCandidates 启用且未删除的用户，按显示名排序；keyword 匹配用户名 / 昵称。
	ListOwnerCandidates(ctx context.Context, keyword string, limit int) ([]OwnerCandidate, error)
}

// NewCIUsecase new a CIUsecase.
func NewCIUsecase(models ModelRepo, fields FieldRepo, relTypes RelationTypeRepo, cis CIRepo, relations RelationRepo, history HistoryRepo, relationUC *RelationUsecase, owners OwnerDirectory) *CIUsecase {
	return &CIUsecase{models: models, fields: fields, relTypes: relTypes, cis: cis, relations: relations, history: history, relationUC: relationUC, owners: owners}
}

// Create 创建资源：类型须启用；属性按字段定义全量校验；ref 字段指向存在且在调用者数据范围
// 内的资源（scope）。资源本身的环境是否在范围内由调用方（守卫）先行校验。
// 返回值重新读取并脱敏（与 Get / List 口径一致）。
func (uc *CIUsecase) Create(ctx context.Context, actor Actor, scope Scope, ci *CI) (*CI, error) {
	created, err := uc.create(ctx, actor, scope, ci)
	if err != nil {
		return nil, err
	}
	return uc.getMasked(ctx, created.ID)
}

// create Create 的校验与写入，返回写入后的资源（未回读、未脱敏）。导入走这里：它只用到资源 ID，
// 每行写入后不必再读一遍（也不存在「已写入、回读失败、这一行却被报成 error」的窗口）。
func (uc *CIUsecase) create(ctx context.Context, actor Actor, scope Scope, ci *CI) (*CI, error) {
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
	if err := uc.checkRefs(ctx, scope, fields, attrs); err != nil {
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
	ci.ID, ci.Attrs = id, attrs
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
	return ci, nil
}

// getMasked 读取资源并脱敏（写接口的返回值；与 Get / List 口径一致）。
func (uc *CIUsecase) getMasked(ctx context.Context, id string) (*CI, error) {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := uc.mask(ctx, ci); err != nil {
		return nil, err
	}
	return ci, nil
}

// GetByID 资源详情（不含关联统计；守卫与编排用）。
func (uc *CIUsecase) GetByID(ctx context.Context, id string) (*CI, error) {
	return uc.cis.Get(ctx, id)
}

// GetByIDs 按 ID 批量读取未删除资源（未脱敏；不存在的 ID 静默略过，调用方按需核对）。
// 供守卫与其它域（故障卡片等）一次取齐多条资源，代替逐条 GetByID。
func (uc *CIUsecase) GetByIDs(ctx context.Context, ids []string) ([]*CI, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return uc.cis.GetByIDs(ctx, ids)
}

// Get 资源详情 + 各适用关系类型两个方向的关联统计。受限调用者（scope）的统计只计范围内的
// 对端，与关联列表同口径；资源本身是否在范围内由调用方（守卫）校验。
func (uc *CIUsecase) Get(ctx context.Context, scope Scope, id string) (*CI, []RelationSummary, error) {
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
	counts, err := uc.relationCounts(ctx, scope, id)
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

// relationCounts 当前资源各关系类型两个方向的关联数。受限调用者只计范围内的对端：关联列表
// （ListRelatedScoped）对范围外对端「不泄露名称与数量」，统计若仍按全部对端计，数字减去
// 列表条数就是范围外对端数。仓储不具备按范围计数的能力（仅内存桩）时不下发统计——
// 宁可少显示，也不退回不按范围计数。
func (uc *CIUsecase) relationCounts(ctx context.Context, scope Scope, ciID string) (map[RelationKey]int64, error) {
	if scope.Unrestricted {
		return uc.relations.CountByCI(ctx, ciID)
	}
	r, ok := uc.relations.(ScopedRelationRepo)
	if !ok {
		return map[RelationKey]int64{}, nil
	}
	modelCodes, envs := scopeLists(scope)
	return r.CountByCIIn(ctx, ciID, modelCodes, envs)
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
		// 以"是否属性命中"判定（敏感字段编码恰为 name / tags 时也须脱敏）；
		// 未标注 Attr 的仓储按字段名兜底。
		attr := h.Attr || (h.Field != "name" && h.Field != "externalId" && h.Field != "tags")
		if attr && h.CI.Attrs[h.Field] == MaskedValue {
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
	// 敏感字段不可作为过滤条件（等值过滤即取值探测）。未指定类型时由仓储按行所属类型排除。
	if len(filter.Attrs) > 0 && filter.ModelCode != "" {
		fields, err := uc.fields.ListByModel(ctx, filter.ModelCode)
		if err != nil {
			return nil, 0, err
		}
		for _, code := range sensitiveCodes(fields) {
			if _, ok := filter.Attrs[code]; ok {
				return nil, 0, detail(ErrCIInvalid, "field %q is sensitive and cannot be used as a filter", code)
			}
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

// ListHistory 全局变更审计，不按调用者数据范围裁剪（系统链路用：故障管理的变更关联等）。
// 面向用户的查询走 ListHistoryScoped。
func (uc *CIUsecase) ListHistory(ctx context.Context, filter HistoryFilter, page, pageSize int64) ([]*History, int64, error) {
	return uc.ListHistoryScoped(ctx, Scope{Unrestricted: true}, filter, page, pageSize)
}

// ListHistoryScoped 全局变更审计（diff 中的敏感属性脱敏；受限调用者的关联历史抹去范围外的对端，
// 见 maskHistories）。filter 的 ModelCodes / Envs 由数据范围守卫叠加。
func (uc *CIUsecase) ListHistoryScoped(ctx context.Context, scope Scope, filter HistoryFilter, page, pageSize int64) ([]*History, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.history.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if err := uc.maskHistories(ctx, scope, items); err != nil {
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

// maskHistories 历史 diff 的脱敏：before/after.attrs 中的敏感键替换为占位值；受限调用者（scope）
// 的 link / unlink 记录里抹去范围外对端的 target（见 maskHistoryTargets）。
func (uc *CIUsecase) maskHistories(ctx context.Context, scope Scope, items []*History) error {
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
	return uc.maskHistoryTargets(ctx, scope, items)
}

// maskHistoryTargets 受限调用者的关联历史：target 带着对端的 id / name / modelCode，范围内资源
// 的历史会带出范围外对端的名称与 ID。对端类型不在范围内的直接抹去；环境受限时对端环境要回查
// （一页一次批量取），已删除或环境不在范围内的同样抹去——不存在与越权同形，也不泄露数量。
// 记录本身保留：动作、关系类型与方向是当前资源自己的历史。
func (uc *CIUsecase) maskHistoryTargets(ctx context.Context, scope Scope, items []*History) error {
	if scope.Unrestricted {
		return nil
	}
	needEnv := !scope.EnvUnrestricted
	ids := make([]string, 0, len(items))
	for _, h := range items {
		if h == nil || (h.Action != ActionLink && h.Action != ActionUnlink) {
			continue
		}
		target, ok := h.Diff["target"].(map[string]any)
		if !ok {
			continue
		}
		id, _ := target["id"].(string)
		model, _ := target["modelCode"].(string)
		if !scope.AllowsModel(model) || !idgen.IsUUIDFold(id) {
			delete(h.Diff, "target")
			continue
		}
		if needEnv {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	peers, err := uc.cis.GetByIDs(ctx, ids)
	if err != nil {
		return err
	}
	visible := make(map[string]bool, len(peers))
	for _, p := range peers {
		visible[p.ID] = scope.AllowsEnv(p.Env)
	}
	for _, h := range items {
		if h == nil || (h.Action != ActionLink && h.Action != ActionUnlink) {
			continue
		}
		target, ok := h.Diff["target"].(map[string]any)
		if !ok {
			continue
		}
		if id, _ := target["id"].(string); !visible[id] {
			delete(h.Diff, "target")
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

// rejectExternalIDChange 同步托管资源的外部标识是采集对账的锚点（对账按它匹配已存在的资源）：
// 人工改掉它之后，下一次同步会匹配不上——该资源被判「消失」、宽限期后退役，采集行则作为新资源重建
// （历史断裂）。要改先解除同步托管（Unclaim）；同步来源自己写入时不受限。
func rejectExternalIDChange(actor Actor, cur *CI, upd CIUpdate) error {
	if upd.ExternalID == nil || *upd.ExternalID == cur.ExternalID {
		return nil
	}
	if actor.source() != SourceManual || !cmdbcontract.IsSyncSource(cur.Source) {
		return nil
	}
	return detail(ErrCIInvalid, "external id of a sync-managed resource is the reconciliation anchor; unclaim it before changing it")
}

// Update 更新资源：乐观锁；属性按键合并后全量重校验；只记录真实变化。校验与合并见 applyUpdate
// （导入试运行的预演走同一实现）；scope 为调用者数据范围，用于 ref 字段引用的范围校验。
// 返回值：有变化时重新读取并脱敏，无变化时返回当前资源（脱敏）。
func (uc *CIUsecase) Update(ctx context.Context, actor Actor, scope Scope, id string, expectedVersion int64, upd CIUpdate) (*CI, error) {
	next, changed, err := uc.update(ctx, actor, scope, id, expectedVersion, upd)
	if err != nil {
		return nil, err
	}
	if !changed {
		if err := uc.mask(ctx, next); err != nil {
			return nil, err
		}
		return next, nil
	}
	return uc.getMasked(ctx, id)
}

// update Update 的校验、合并与写入：返回合并后的资源（未回读、未脱敏）与是否有真实变化。
func (uc *CIUsecase) update(ctx context.Context, actor Actor, scope Scope, id string, expectedVersion int64, upd CIUpdate) (next *CI, changed bool, err error) {
	cur, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if cur.Version != expectedVersion {
		return nil, false, ErrCIVersionConflict
	}
	return uc.updateCurrent(ctx, actor, scope, cur, upd)
}

// updateCurrent 对刚读取的当前资源做校验、合并与写入（乐观锁条件 = 该读取的版本），返回合并后的资源
// （未回读、未脱敏）与是否有真实变化。导入走这里：它读一次当下状态后判定 skip / update，
// 不必像 Update 那样再读一遍做版本比对（导入行本来就不携带期望版本）。
func (uc *CIUsecase) updateCurrent(ctx context.Context, actor Actor, scope Scope, cur *CI, upd CIUpdate) (next *CI, changed bool, err error) {
	fields, err := uc.fields.ListByModel(ctx, cur.ModelCode)
	if err != nil {
		return nil, false, err
	}
	next, _, err = uc.applyUpdate(ctx, actor, scope, cur, fields, upd)
	if err != nil {
		return nil, false, err
	}
	before, after := diffCI(cur, next)
	if len(after) == 0 {
		return cur, false, nil
	}
	next.Version = cur.Version + 1
	next.UpdateUser, next.UpdateTime = actor.UserID, time.Now()
	h, err := newHistory(actor, cur.ID, ActionUpdate, map[string]any{"before": before, "after": after})
	if err != nil {
		return nil, false, err
	}
	if err := uc.cis.Update(ctx, next, cur.Version, h); err != nil {
		return nil, false, err
	}
	return next, true, nil
}

// Transition 状态流转：状态机校验；退役时按关系类型策略级联退役 dst 资源（restrict 则拒绝）。
// 级联闭包先整体算出，再与当前资源在同一事务内写入；scope 为调用者数据范围，
// 闭包内任一资源越权则整体拒绝。
func (uc *CIUsecase) Transition(ctx context.Context, actor Actor, scope Scope, id string, expectedVersion int64, to string) (*CI, error) {
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
	targets := []*CI{cur}
	if to == CIStatusRetired {
		deps, err := uc.retireClosure(ctx, scope, cur)
		if err != nil {
			return nil, err
		}
		targets = append(targets, deps...)
	}
	now := time.Now()
	items := make([]*CI, 0, len(targets))
	histories := make([]*History, 0, len(targets))
	for _, t := range targets {
		next := *t
		next.Status = to
		next.Version = t.Version + 1
		next.UpdateUser, next.UpdateTime = actor.UserID, now
		h, err := newHistory(actor, t.ID, ActionTransition, map[string]any{
			"before": map[string]any{"status": t.Status}, "after": map[string]any{"status": to},
		})
		if err != nil {
			return nil, err
		}
		items = append(items, &next)
		histories = append(histories, h)
	}
	if err := uc.updateAll(ctx, items, histories); err != nil {
		return nil, err
	}
	return uc.getMasked(ctx, id)
}

// updateAll 写入一组乐观锁更新：单条走 Update；多条优先同事务（CIBatchUpdater），
// 仓储不支持时自后向前逐条更新（依赖在前、根在后）。
func (uc *CIUsecase) updateAll(ctx context.Context, items []*CI, histories []*History) error {
	if len(items) > 1 {
		if b, ok := uc.cis.(CIBatchUpdater); ok {
			return b.UpdateBatch(ctx, items, histories)
		}
	}
	for i := len(items) - 1; i >= 0; i-- {
		if err := uc.cis.Update(ctx, items[i], items[i].Version-1, histories[i]); err != nil {
			return err
		}
	}
	return nil
}

// retireClosure 计算退役 root 时需级联退役的资源闭包（不含 root）：沿 outgoing 关系
// 广度优先遍历，visited 去重（环与菱形各只处理一次）；cascade_retire 的未退役 dst 纳入
// 闭包并继续展开，restrict 且仍有关联则拒绝；已退役的 dst 跳过；越权的 dst 整体拒绝。
// 返回的资源带读取时的当前版本。
// 展开要读的边由仓储一条语句取齐（RetireGraphRepo），判定在内存里按同样的遍历顺序重放
// （retireClosureOf）；仓储不具备该能力时退回逐类型分页遍历（retireClosureByType）。
func (uc *CIUsecase) retireClosure(ctx context.Context, scope Scope, root *CI) ([]*CI, error) {
	repo, ok := uc.relations.(RetireGraphRepo)
	if !ok {
		return uc.retireClosureByType(ctx, scope, root)
	}
	edges, err := repo.RetireEdges(ctx, root.ID)
	if err != nil {
		return nil, err
	}
	return retireClosureOf(scope, root, edges)
}

// retireOutgoing 一个资源的出边按关系类型分组；order 为类型首次出现的顺序（即仓储行序里
// 关系类型的顺序）。
type retireOutgoing struct {
	order  []*retireGroup
	byType map[string]*retireGroup
}

// retireGroup 某资源在一个关系类型下的出边（对端按仓储行序）。
type retireGroup struct {
	code, onDelete string
	cis            []*CI
}

// retireClosureOf 在取齐的出边上重放逐类型遍历，判定与 retireClosureByType 逐条一致：每个资源
// 依次处理它的各关系类型；非 cascade_retire 的类型只要有未删除的对端就拒绝整个退役（报错带条数），
// 处理时机同逐类型遍历——排在前面的类型先判定，所以同时命中多种拒绝原因时返回的错误相同；
// cascade_retire 的对端按行序 visited 去重，已退役的跳过且不展开，越权的整体拒绝，其余入闭包。
func retireClosureOf(scope Scope, root *CI, edges []*RetireEdge) ([]*CI, error) {
	outgoing := make(map[string]*retireOutgoing)
	for _, e := range edges {
		out, ok := outgoing[e.SrcID]
		if !ok {
			out = &retireOutgoing{byType: map[string]*retireGroup{}}
			outgoing[e.SrcID] = out
		}
		group, ok := out.byType[e.TypeCode]
		if !ok {
			group = &retireGroup{code: e.TypeCode, onDelete: e.OnDelete}
			out.byType[e.TypeCode] = group
			out.order = append(out.order, group)
		}
		group.cis = append(group.cis, e.CI)
	}
	visited := map[string]struct{}{root.ID: {}}
	var deps []*CI
	for queue := []*CI{root}; len(queue) > 0; queue = queue[1:] {
		ci := queue[0]
		out, ok := outgoing[ci.ID]
		if !ok {
			continue
		}
		for _, group := range out.order {
			if group.onDelete != OnDeleteCascadeRetire {
				return nil, detail(ErrCIInUse, "%d resources linked via %s", len(group.cis), group.code)
			}
			for _, peer := range group.cis {
				if _, ok := visited[peer.ID]; ok {
					continue
				}
				visited[peer.ID] = struct{}{}
				if peer.Status == CIStatusRetired {
					continue
				}
				if !scope.Allows(peer.ModelCode, peer.Env) {
					return nil, ErrScopeForbidden
				}
				deps = append(deps, peer)
				queue = append(queue, peer)
			}
		}
	}
	return deps, nil
}

// retireClosureByType 逐类型分页遍历的闭包计算（RetireGraphRepo 的退化路径，语义见 retireClosure）：
// 每个资源 × 每个适用的关系类型一次计数 + 逐页列举。
func (uc *CIUsecase) retireClosureByType(ctx context.Context, scope Scope, root *CI) ([]*CI, error) {
	visited := map[string]struct{}{root.ID: {}}
	typesByModel := map[string][]*RelationType{}
	var deps []*CI
	for queue := []*CI{root}; len(queue) > 0; queue = queue[1:] {
		ci := queue[0]
		types, ok := typesByModel[ci.ModelCode]
		if !ok {
			var err error
			if types, err = uc.relTypes.List(ctx, ci.ModelCode); err != nil {
				return nil, err
			}
			typesByModel[ci.ModelCode] = types
		}
		for _, t := range types {
			if t.SrcModel != ModelAny && t.SrcModel != ci.ModelCode {
				continue
			}
			// 逐页遍历（依赖数可能超过单页上限）。
			for page := int64(1); ; page++ {
				related, total, err := uc.relations.ListRelated(ctx, ci.ID, t.Code, DirectionOutgoing, "", page, paging.MaxPageSize)
				if err != nil {
					return nil, err
				}
				if page == 1 && total > 0 && t.OnDelete != OnDeleteCascadeRetire {
					return nil, detail(ErrCIInUse, "%d resources linked via %s", total, t.Code)
				}
				for _, r := range related {
					if _, ok := visited[r.CI.ID]; ok {
						continue
					}
					visited[r.CI.ID] = struct{}{}
					if r.CI.Status == CIStatusRetired {
						continue
					}
					if !scope.Allows(r.CI.ModelCode, r.CI.Env) {
						return nil, ErrScopeForbidden
					}
					deps = append(deps, r.CI)
					queue = append(queue, r.CI)
				}
				if int64(page*paging.MaxPageSize) >= total {
					break
				}
			}
		}
	}
	return deps, nil
}

// Delete 软删资源：仍有任何关系时拒绝（先解除关系）。关系数在仓储的删除事务内、锁住资源行之后判定，
// 与并发的关联写入互斥（这里不做无锁预检）。
func (uc *CIUsecase) Delete(ctx context.Context, actor Actor, id string) error {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return err
	}
	h, err := newHistory(actor, id, ActionDelete, map[string]any{"before": snapshot(ci)})
	if err != nil {
		return err
	}
	return uc.cis.SoftDelete(ctx, id, time.Now(), h)
}

// Unclaim 解除同步托管：资源改回人工维护，同步不再更新或退役它。
// 采集源里若仍有这台设备，下次同步会把它当作新资源（重名时进入同步冲突清单）。
// 它身上由同步建立的关联转为人工关联保留。关联类型上的"级联退役"不区分来源，对它照常生效
// （与人工资源一致）；之后被重新认领时，源端仍声明的关联由同步收回维护（见数据面 stmtRelationsAdopt）。
func (uc *CIUsecase) Unclaim(ctx context.Context, actor Actor, id string) (*CI, error) {
	ci, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !cmdbcontract.IsSyncSource(ci.Source) {
		return nil, ErrNotSyncManaged
	}
	hid, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	if err := uc.cis.Unclaim(ctx, ci, actor, time.Now(), hid); err != nil {
		return nil, err
	}
	return uc.getMasked(ctx, id)
}

// GetDeleted 回收站中的资源（守卫与恢复用，未脱敏）。
func (uc *CIUsecase) GetDeleted(ctx context.Context, id string) (*CI, error) {
	return uc.cis.GetDeleted(ctx, id)
}

// Restore 从回收站恢复资源：类型须仍存在；名称 / 唯一规则冲突由仓储判定。
// 恢复后不重校验属性（字段定义可能已变化，与"存量值不重校验"同口径）。
func (uc *CIUsecase) Restore(ctx context.Context, actor Actor, id string) (*CI, error) {
	ci, err := uc.cis.GetDeleted(ctx, id)
	if err != nil {
		return nil, err
	}
	m, err := uc.models.Get(ctx, ci.ModelCode)
	if err != nil {
		return nil, err
	}
	// 与新建同口径：停用的类型不接受新的在册资源。
	if m.Status != ModelStatusEnabled {
		return nil, detail(ErrCIInvalid, "model %s is disabled", m.Code)
	}
	now := time.Now()
	ci.Version++
	ci.UpdateUser, ci.UpdateTime = actor.UserID, now
	h, err := newHistory(actor, id, ActionRestore, map[string]any{"after": snapshot(ci)})
	if err != nil {
		return nil, err
	}
	if err := uc.cis.Restore(ctx, ci, now, h); err != nil {
		return nil, err
	}
	return uc.getMasked(ctx, id)
}

// ListOwnerCandidates 可选负责人。
func (uc *CIUsecase) ListOwnerCandidates(ctx context.Context, keyword string) ([]OwnerCandidate, error) {
	return uc.owners.ListOwnerCandidates(ctx, strings.TrimSpace(keyword), OwnerCandidateLimit)
}

// StateAtMaxChanges 时间点回溯最多倒推的变更条数（超过则拒绝：回溯跨度过大）。
const StateAtMaxChanges = 2000

// StateAtMaxAge 可回溯的最远时间：与变更历史的保留期（数据面维护任务，365 天）一致。
const StateAtMaxAge = 365 * 24 * time.Hour

// StateAt 时间点回溯：从当前状态出发，按时间倒序撤销 at 之后的每条变更，得到资源在 at 时刻的
// 基本信息与属性（不含关联）。at 早于创建时间返回 ErrCINotFound；返回的 deleted 表示该时刻
// 资源处于已删除状态。结果按当前字段定义脱敏。
func (uc *CIUsecase) StateAt(ctx context.Context, id string, at time.Time) (*CI, bool, error) {
	cur, err := uc.cis.Get(ctx, id)
	if err != nil {
		return nil, false, err
	}
	// 早于创建时间：不存在。早于历史保留期：更早的变更记录已清理，倒推结果不可信，拒绝。
	if at.Before(cur.CreateTime) {
		return nil, false, detail(ErrCINotFound, "resource did not exist at that time")
	}
	if at.Before(time.Now().Add(-StateAtMaxAge)) {
		return nil, false, detail(ErrCIInvalid, "change history is only kept for %d days; pick a more recent time", int(StateAtMaxAge.Hours()/24))
	}
	changes, err := uc.history.ListAfter(ctx, id, at, StateAtMaxChanges+1)
	if err != nil {
		return nil, false, err
	}
	if len(changes) > StateAtMaxChanges {
		return nil, false, detail(ErrCIInvalid, "more than %d changes since that time; pick a more recent time", StateAtMaxChanges)
	}
	state := *cur
	state.Attrs, state.Tags = cloneAttrs(cur.Attrs), slices.Clone(cur.Tags)
	deleted := false
	for _, h := range changes {
		before, _ := h.Diff["before"].(map[string]any)
		switch h.Action {
		case ActionCreate:
			return nil, false, detail(ErrCINotFound, "resource did not exist at that time")
		case ActionRestore:
			deleted = true // 恢复之前处于已删除状态
		case ActionDelete:
			deleted = false // 删除之前在册；before 为删除时的完整快照
			revertFields(&state, before, true)
		case ActionUpdate, ActionTransition:
			revertFields(&state, before, false)
		}
	}
	// 审计字段在历史时刻无定义。
	state.Version, state.UpdateUser, state.Stale, state.LastSyncedAt = 0, "", false, nil
	state.UpdateTime, state.OwnerName = at, ""
	// 来源可能已回退（如认领之前为人工）：采集源名称不再对应。
	if state.Source != cur.Source {
		state.SourceName, state.SourceDeleted = "", false
	}
	if err := uc.mask(ctx, &state); err != nil {
		return nil, false, err
	}
	return &state, deleted, nil
}

// revertFields 把一条变更的 before 值写回状态。full = before 为完整快照（属性整体替换），
// 否则只含变化的键（属性按键回退，nil 表示当时不存在该键）。
func revertFields(state *CI, before map[string]any, full bool) {
	if before == nil {
		return
	}
	str := func(key string, dst *string) {
		if v, ok := before[key].(string); ok {
			*dst = v
		}
	}
	str("name", &state.Name)
	str("status", &state.Status)
	str("env", &state.Env)
	str("owner", &state.Owner)
	str("externalId", &state.ExternalID)
	str("source", &state.Source)
	if raw, ok := before["tags"]; ok {
		tags := []string{}
		switch list := raw.(type) {
		case []any:
			for _, v := range list {
				if s, ok := v.(string); ok {
					tags = append(tags, s)
				}
			}
		case []string:
			tags = slices.Clone(list)
		}
		state.Tags = tags
	}
	attrs, ok := before["attrs"].(map[string]any)
	if !ok {
		return
	}
	if full {
		state.Attrs = cloneAttrs(attrs)
		return
	}
	for key, v := range attrs {
		if v == nil {
			delete(state.Attrs, key)
		} else {
			state.Attrs[key] = v
		}
	}
}

// History 分页查询资源变更历史（diff 脱敏；受限调用者的关联历史抹去范围外对端，见 maskHistories）。
func (uc *CIUsecase) History(ctx context.Context, scope Scope, id string, page, pageSize int64) ([]*History, int64, error) {
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
	if err := uc.maskHistories(ctx, scope, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// checkRefs ref 字段的值必须是对应类型下存在的资源 ID。受限调用者（scope）还要求被引用的资源
// 在其数据范围内：范围外的引用与不存在返回同一文案——否则成功与否就成了对范围外资源的存在性探测。
func (uc *CIUsecase) checkRefs(ctx context.Context, scope Scope, fields []*Field, attrs map[string]any) error {
	// 受限路径的被引用资源：首个需要回查的字段触发、一次取齐（见 refTargets），键为小写 ID。
	var targets map[string]*CI
	for _, f := range fields {
		id, ok, err := refValue(f, attrs)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if scope.Unrestricted {
			exists, err := uc.cis.ExistsInModel(ctx, f.RefModel, id)
			if err != nil {
				return err
			}
			if !exists {
				return missingRef(f)
			}
			continue
		}
		if targets == nil {
			if targets, err = uc.refTargets(ctx, fields, attrs); err != nil {
				return err
			}
		}
		// GetByIDs 只返回未删除的资源：不存在、已删除、类型不符、范围外，一律按「不存在」报错。
		target, found := targets[strings.ToLower(id)]
		if !found || target.ModelCode != f.RefModel || !scope.Allows(target.ModelCode, target.Env) {
			return missingRef(f)
		}
	}
	return nil
}

// refValue 取 ref 字段本次写入的资源 ID：不是 ref 字段 / 已废弃 / 未给值返回 ok=false；值不是合法 ID 报错。
func refValue(f *Field, attrs map[string]any) (id string, ok bool, err error) {
	if f.Type != FieldTypeRef || f.Deprecated {
		return "", false, nil
	}
	v, present := attrs[f.Code]
	if !present || v == nil {
		return "", false, nil
	}
	id, _ = v.(string)
	if !idgen.IsUUIDFold(id) {
		return "", false, detail(ErrCIInvalid, "field %q must be a resource id", f.Code)
	}
	return id, true, nil
}

// refTargets 把本次写入的全部 ref 字段指向的资源一次取齐（此前每个 ref 字段各查一次）。
// 格式不合法的值跳过——它所在的字段轮到时由 checkRefs 报错，报错顺序与逐个查询时一致。
// 键统一为小写：库里返回的 ID 是小写，而入参允许大写。
func (uc *CIUsecase) refTargets(ctx context.Context, fields []*Field, attrs map[string]any) (map[string]*CI, error) {
	ids := make([]string, 0, 2)
	for _, f := range fields {
		if id, ok, err := refValue(f, attrs); err == nil && ok && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	cis, err := uc.cis.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*CI, len(cis))
	for _, ci := range cis {
		out[strings.ToLower(ci.ID)] = ci
	}
	return out, nil
}

func missingRef(f *Field) error {
	return detail(ErrCIInvalid, "field %q references a missing %s resource", f.Code, f.RefModel)
}

// syncMaintained 资源是否仍由采集源维护：来源是同步来源，且该采集源尚未删除。采集源删除后
// 不会再有同步写这些字段，owner=sync 的字段应放回人工维护——否则其下资源的同步字段永久不可
// 编辑，只能逐台解除托管。
func syncMaintained(ci *CI) bool {
	return cmdbcontract.IsSyncSource(ci.Source) && !ci.SourceDeleted
}

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
		// 数值归一后比较：存量经 JSON 读回为 float64，新值归一为 int64，值相等不算变化。
		if nv, ok := next.Attrs[key]; !ok || !reflect.DeepEqual(normalizeNumber(v), normalizeNumber(nv)) {
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
