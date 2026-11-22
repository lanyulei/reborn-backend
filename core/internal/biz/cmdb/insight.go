package biz

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/lanyulei/reborn/backend/kit/paging"
)

// DefaultGroupSort 新建分组的缺省顺序：位于内置分组之后、"其他"（900）之前。
const DefaultGroupSort int32 = 500

// OverviewDefaultDays / OverviewMaxDays 运营概览变更趋势的回看天数。
const (
	OverviewDefaultDays int32 = 30
	OverviewMaxDays     int32 = 90
)

// ModelGroup 模型分组（cmdb_model.category 的取值）。
type ModelGroup struct {
	Code       string
	Name       string
	Sort       int32
	Builtin    bool
	ModelCount int64
	CreateTime time.Time
	UpdateTime time.Time
}

// ModelGroupUpdate 分组部分更新；nil 不更新（编码不可改）。
type ModelGroupUpdate struct {
	Name *string
	Sort *int32
}

// GroupRepo 模型分组仓储接口。
type GroupRepo interface {
	// List 全部分组（sort、code 升序），含分组下未删除的模型数。
	List(ctx context.Context) ([]*ModelGroup, error)
	// Get 不存在返回 ErrGroupNotFound。
	Get(ctx context.Context, code string) (*ModelGroup, error)
	// Create 编码冲突返回 ErrGroupCodeExists。
	Create(ctx context.Context, g *ModelGroup) error
	Update(ctx context.Context, code string, upd ModelGroupUpdate) error
	Delete(ctx context.Context, code string) error
}

// FavoriteRepo 模型收藏仓储接口（按用户）。
type FavoriteRepo interface {
	// List 用户收藏的模型编码（收藏时间升序；已删除模型不返回）。
	List(ctx context.Context, userID string) ([]string, error)
	// Set 收藏 / 取消收藏（幂等）。
	Set(ctx context.Context, userID, modelCode string, favorite bool) error
}

// CIView 保存的资源列表视图（按用户 + 模型；Filters 为前端筛选表单的值，后端只存取不解释）。
type CIView struct {
	Name    string
	Filters map[string]any
}

// 保存的视图的上限：每用户每模型的个数、单个视图筛选条件序列化后的字节数。
const (
	MaxCIViewsPerModel = 20
	MaxCIViewBytes     = 4096
)

// CIViewRepo 保存的视图仓储接口。
type CIViewRepo interface {
	List(ctx context.Context, userID, modelCode string) ([]*CIView, error)
	// Save 同名覆盖；新增时超过 limit 个返回 ErrCIViewLimit（计数与写入同事务）。
	Save(ctx context.Context, userID, modelCode string, view *CIView, limit int) error
	Delete(ctx context.Context, userID, modelCode, name string) error
}

// OverviewFilter 运营概览条件：ModelCodes / Envs 为 nil 表示该维度不限（数据范围守卫叠加）。
type OverviewFilter struct {
	ModelCodes []string
	Envs       []string
	// Since 变更趋势起点。
	Since time.Time
}

// OverviewCount 一个维度取值上的资源数。
type OverviewCount struct {
	Key   string
	Name  string
	Count int64
}

// OverviewTrendPoint 某天某动作的变更次数（Date 为 YYYY-MM-DD）。
type OverviewTrendPoint struct {
	Date   string
	Action string
	Count  int64
}

// Overview 运营概览统计。
type Overview struct {
	ModelCount    int64
	CICount       int64
	RelationCount int64
	ChangeCount   int64
	ByModel       []OverviewCount
	ByStatus      []OverviewCount
	ByEnv         []OverviewCount
	BySource      []OverviewCount
	Trend         []OverviewTrendPoint
	Days          int32
}

// OverviewRepo 运营概览只读仓储接口（聚合查询）。
type OverviewRepo interface {
	Stats(ctx context.Context, filter OverviewFilter) (*Overview, error)
	// Quality 按模型的数据质量计数（只统计未删除、未退役的资源）。
	Quality(ctx context.Context, filter OverviewFilter, outdatedBefore time.Time) ([]*QualityRow, error)
}

// QualityRow 一个模型的数据质量计数。
type QualityRow struct {
	ModelCode       string
	ModelName       string
	Total           int64
	MissingRequired int64
	NoOwner         int64
	Orphan          int64
	Stale           int64
	Outdated        int64
}

// InsightUsecase 资源目录与运营视角用例：模型分组、收藏、概览统计、模型变更审计查询。
type InsightUsecase struct {
	groups    GroupRepo
	favorites FavoriteRepo
	overview  OverviewRepo
	audits    SchemaAuditRepo
	models    ModelRepo
	ciViews   CIViewRepo

	// 数据质量统计的短缓存（见 Quality）。
	qualityMu sync.Mutex
	quality   map[string]qualityCacheEntry
	// qualityFlight 合并同一范围的并发计算。
	qualityFlight singleflight.Group
	// 运营概览的短缓存（见 Overview），口径同数据质量，键多一个回看天数。
	overviewMu     sync.Mutex
	overviewCache  map[string]overviewCacheEntry
	overviewFlight singleflight.Group
}

type overviewCacheEntry struct {
	at  time.Time
	out *Overview
}

// NewInsightUsecase new an InsightUsecase.
func NewInsightUsecase(groups GroupRepo, favorites FavoriteRepo, overview OverviewRepo, audits SchemaAuditRepo, models ModelRepo, views CIViewRepo) *InsightUsecase {
	return &InsightUsecase{groups: groups, favorites: favorites, overview: overview, audits: audits, models: models, ciViews: views}
}

// ListGroups 全部模型分组。
func (uc *InsightUsecase) ListGroups(ctx context.Context) ([]*ModelGroup, error) {
	return uc.groups.List(ctx)
}

// CreateGroup 创建模型分组（非内置；Sort 为 0 时取缺省顺序）。
func (uc *InsightUsecase) CreateGroup(ctx context.Context, g *ModelGroup) (*ModelGroup, error) {
	now := time.Now()
	g.Builtin = false
	if g.Sort == 0 {
		g.Sort = DefaultGroupSort
	}
	g.CreateTime, g.UpdateTime = now, now
	if err := uc.groups.Create(ctx, g); err != nil {
		return nil, err
	}
	created, err := uc.groups.Get(ctx, g.Code)
	if err != nil {
		return nil, err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetGroup, created.Code, created.Name, "", AuditActionCreate, nil, groupAuditMap(created))
	return created, nil
}

// UpdateGroup 更新分组名称与顺序（内置分组同样可改）。
func (uc *InsightUsecase) UpdateGroup(ctx context.Context, code string, upd ModelGroupUpdate) (*ModelGroup, error) {
	before, err := uc.groups.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	if err := uc.groups.Update(ctx, code, upd); err != nil {
		return nil, err
	}
	after, err := uc.groups.Get(ctx, code)
	if err != nil {
		return nil, err
	}
	b, a := diffAuditMaps(groupAuditMap(before), groupAuditMap(after))
	if len(a) > 0 {
		recordSchemaAudit(ctx, uc.audits, AuditTargetGroup, code, after.Name, "", AuditActionUpdate, b, a)
	}
	return after, nil
}

// DeleteGroup 删除分组：内置分组、仍有模型的分组不可删。
func (uc *InsightUsecase) DeleteGroup(ctx context.Context, code string) error {
	g, err := uc.groups.Get(ctx, code)
	if err != nil {
		return err
	}
	if g.Builtin {
		return detail(ErrGroupInUse, "builtin group")
	}
	if g.ModelCount > 0 {
		return detail(ErrGroupInUse, "%d models", g.ModelCount)
	}
	if err := uc.groups.Delete(ctx, code); err != nil {
		return err
	}
	recordSchemaAudit(ctx, uc.audits, AuditTargetGroup, code, g.Name, "", AuditActionDelete, groupAuditMap(g), nil)
	return nil
}

// ListFavorites 用户收藏的模型编码。
func (uc *InsightUsecase) ListFavorites(ctx context.Context, userID string) ([]string, error) {
	return uc.favorites.List(ctx, userID)
}

// SetFavorite 收藏 / 取消收藏；收藏时模型须存在。
func (uc *InsightUsecase) SetFavorite(ctx context.Context, userID, modelCode string, favorite bool) error {
	if favorite {
		if _, err := uc.models.Get(ctx, modelCode); err != nil {
			return err
		}
	}
	return uc.favorites.Set(ctx, userID, modelCode, favorite)
}

// Overview 运营概览：days 越界时规整到 [1, OverviewMaxDays]，0 取缺省。
func (uc *InsightUsecase) Overview(ctx context.Context, filter OverviewFilter, days int32) (*Overview, error) {
	if days <= 0 {
		days = OverviewDefaultDays
	}
	if days > OverviewMaxDays {
		days = OverviewMaxDays
	}
	// 结果按（数据范围，回看天数）缓存 qualityCacheTTL：概览是十来条聚合查询，多人同时打开看板、
	// 前端轮询都只扫一遍；看板允许分钟级延迟。并发未命中合并为一次计算。
	key := qualityCacheKey(filter) + "/" + strconv.Itoa(int(days))
	uc.overviewMu.Lock()
	if e, ok := uc.overviewCache[key]; ok && time.Since(e.at) < qualityCacheTTL {
		uc.overviewMu.Unlock()
		return cloneOverview(e.out), nil
	}
	uc.overviewMu.Unlock()
	v, err, _ := uc.overviewFlight.Do(key, func() (any, error) {
		// 回看窗口含今天：起点为 (days-1) 天前的零点。
		now := time.Now()
		filter.Since = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).AddDate(0, 0, -int(days-1))
		octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), qualityQueryTimeout)
		defer cancel()
		out, err := uc.overview.Stats(octx, filter)
		if err != nil {
			return nil, err
		}
		out.Days = days
		uc.overviewMu.Lock()
		if uc.overviewCache == nil || len(uc.overviewCache) >= qualityCacheMaxKeys {
			uc.overviewCache = make(map[string]overviewCacheEntry, 8)
		}
		uc.overviewCache[key] = overviewCacheEntry{at: time.Now(), out: out}
		uc.overviewMu.Unlock()
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return cloneOverview(v.(*Overview)), nil
}

// cloneOverview 返回副本（切片独立，调用方改写不污染缓存）。
func cloneOverview(o *Overview) *Overview {
	cp := *o
	cp.ByModel = slices.Clone(o.ByModel)
	cp.ByStatus = slices.Clone(o.ByStatus)
	cp.ByEnv = slices.Clone(o.ByEnv)
	cp.BySource = slices.Clone(o.BySource)
	cp.Trend = slices.Clone(o.Trend)
	return &cp
}

// Quality 数据质量：按模型统计各类问题的资源数。
//
// 性能：统计要扫一遍全部在册资源（实测 50 万资源约 1.5–2s，随资源数线性增长），因此：
//   - 结果按数据范围缓存 qualityCacheTTL（进程内只读 TTL 缓存；看板允许分钟级延迟）；
//   - 计算脱离请求取消并自带上限：即使首次请求因服务端时限（5s）先返回超时，计算仍会完成并
//     写入缓存，随后的请求直接命中。
func (uc *InsightUsecase) Quality(ctx context.Context, filter OverviewFilter) ([]*QualityRow, error) {
	key := qualityCacheKey(filter)
	uc.qualityMu.Lock()
	if e, ok := uc.quality[key]; ok && time.Since(e.at) < qualityCacheTTL {
		uc.qualityMu.Unlock()
		return cloneQualityRows(e.rows), nil
	}
	uc.qualityMu.Unlock()
	// 同一范围的并发未命中合并为一次计算（多人同时打开看板、缓存到期的瞬间都只扫一遍）。
	v, err, _ := uc.qualityFlight.Do(key, func() (any, error) {
		qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), qualityQueryTimeout)
		defer cancel()
		rows, err := uc.overview.Quality(qctx, filter, time.Now().AddDate(0, 0, -QualityOutdatedDays))
		if err != nil {
			return nil, err
		}
		uc.qualityMu.Lock()
		if uc.quality == nil || len(uc.quality) >= qualityCacheMaxKeys {
			uc.quality = make(map[string]qualityCacheEntry, 8) // 范围组合过多时整体重建，避免无界增长
		}
		uc.quality[key] = qualityCacheEntry{at: time.Now(), rows: rows}
		uc.qualityMu.Unlock()
		return rows, nil
	})
	if err != nil {
		return nil, err
	}
	return cloneQualityRows(v.([]*QualityRow)), nil
}

const (
	qualityCacheTTL     = 60 * time.Second
	qualityQueryTimeout = 60 * time.Second
	qualityCacheMaxKeys = 256
)

type qualityCacheEntry struct {
	at   time.Time
	rows []*QualityRow
}

// qualityCacheKey 数据范围的规范化表示（nil 与空集合含义不同：不限 / 什么都看不到）。
func qualityCacheKey(f OverviewFilter) string {
	// JSON 编码：null（不限）与 []（什么都看不到）天然区分，取值里的任何字符都不会造成歧义。
	norm := func(set []string) []string {
		if set == nil {
			return nil
		}
		// 空集合须保持为非 nil（slices.Sorted 对空输入返回 nil，会与"不限"混同）。
		return append([]string{}, slices.Sorted(slices.Values(set))...)
	}
	raw, _ := json.Marshal([2][]string{norm(f.ModelCodes), norm(f.Envs)})
	return string(raw)
}

// cloneQualityRows 返回副本（调用方会排序 / 改写切片）。
func cloneQualityRows(rows []*QualityRow) []*QualityRow {
	out := make([]*QualityRow, 0, len(rows))
	for _, r := range rows {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

// ListSchemaAudit 分页查询模型变更审计（时间倒序）。
func (uc *InsightUsecase) ListSchemaAudit(ctx context.Context, filter SchemaAuditFilter, page, pageSize int64) ([]*SchemaAudit, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.audits.List(ctx, filter, page, pageSize)
}

// ListCIViews 用户在某模型下保存的视图。
func (uc *InsightUsecase) ListCIViews(ctx context.Context, userID, modelCode string) ([]*CIView, error) {
	return uc.ciViews.List(ctx, userID, modelCode)
}

// SaveCIView 保存视图（同名覆盖）：模型须存在；名称去首尾空白；筛选条件有大小上限。
func (uc *InsightUsecase) SaveCIView(ctx context.Context, userID, modelCode string, view *CIView) error {
	if _, err := uc.models.Get(ctx, modelCode); err != nil {
		return err
	}
	view.Name = strings.TrimSpace(view.Name)
	if view.Name == "" {
		return detail(ErrViewInvalid, "view name is required")
	}
	raw, err := json.Marshal(view.Filters)
	if err != nil {
		return detail(ErrViewInvalid, "filters are not serialisable")
	}
	if len(raw) > MaxCIViewBytes {
		return detail(ErrViewInvalid, "filters are too large (max %d bytes)", MaxCIViewBytes)
	}
	return uc.ciViews.Save(ctx, userID, modelCode, view, MaxCIViewsPerModel)
}

// DeleteCIView 删除视图（幂等）。
func (uc *InsightUsecase) DeleteCIView(ctx context.Context, userID, modelCode, name string) error {
	return uc.ciViews.Delete(ctx, userID, modelCode, strings.TrimSpace(name))
}

// CIViewLimitError 视图个数超过上限。
func CIViewLimitError(limit int) error {
	return detail(ErrViewInvalid, "at most %d saved views per model", limit)
}
