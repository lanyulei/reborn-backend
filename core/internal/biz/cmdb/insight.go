package biz

import (
	"context"
	"time"

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
}

// InsightUsecase 资源目录与运营视角用例：模型分组、收藏、概览统计、模型变更审计查询。
type InsightUsecase struct {
	groups    GroupRepo
	favorites FavoriteRepo
	overview  OverviewRepo
	audits    SchemaAuditRepo
	models    ModelRepo
}

// NewInsightUsecase new an InsightUsecase.
func NewInsightUsecase(groups GroupRepo, favorites FavoriteRepo, overview OverviewRepo, audits SchemaAuditRepo, models ModelRepo) *InsightUsecase {
	return &InsightUsecase{groups: groups, favorites: favorites, overview: overview, audits: audits, models: models}
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
	// 回看窗口含今天：起点为 (days-1) 天前的零点。
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).AddDate(0, 0, -int(days-1))
	filter.Since = start
	out, err := uc.overview.Stats(ctx, filter)
	if err != nil {
		return nil, err
	}
	out.Days = days
	return out, nil
}

// ListSchemaAudit 分页查询模型变更审计（时间倒序）。
func (uc *InsightUsecase) ListSchemaAudit(ctx context.Context, filter SchemaAuditFilter, page, pageSize int64) ([]*SchemaAudit, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.audits.List(ctx, filter, page, pageSize)
}
