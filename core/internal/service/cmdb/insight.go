package service

import (
	"context"
	"slices"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// withActor 把操作者写入上下文，供 biz 记录模型变更审计（取不到身份时原样返回，
// 审计操作者为空；接口鉴权由中间件保证）。
func withActor(ctx context.Context) context.Context {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return ctx
	}
	return biz.WithActor(ctx, actor)
}

// CmdbInsightService 实现 CmdbInsightServiceServer：模型分组、收藏、运营概览、模型变更审计。
type CmdbInsightService struct {
	uc    *biz.InsightUsecase
	guard scopeGuard
}

// NewCmdbInsightService new a CmdbInsightService.
func NewCmdbInsightService(uc *biz.InsightUsecase, scopes *biz.ScopeUsecase) *CmdbInsightService {
	return &CmdbInsightService{uc: uc, guard: newScopeGuard(scopes)}
}

// ListModelGroups implements proto CmdbInsightService/ListModelGroups.
func (s *CmdbInsightService) ListModelGroups(ctx context.Context, _ *v1.ListModelGroupsRequest) (*v1.ListModelGroupsResponse, error) {
	items, err := s.uc.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListModelGroupsResponse{Items: make([]*v1.CmdbModelGroup, 0, len(items))}
	for _, g := range items {
		resp.Items = append(resp.Items, toProtoGroup(g))
	}
	return resp, nil
}

// CreateModelGroup implements proto CmdbInsightService/CreateModelGroup.
func (s *CmdbInsightService) CreateModelGroup(ctx context.Context, req *v1.CreateModelGroupRequest) (*v1.CreateModelGroupResponse, error) {
	g, err := s.uc.CreateGroup(withActor(ctx), &biz.ModelGroup{Code: req.GetCode(), Name: req.GetName(), Sort: req.GetSort()})
	if err != nil {
		return nil, err
	}
	return &v1.CreateModelGroupResponse{Group: toProtoGroup(g)}, nil
}

// UpdateModelGroup implements proto CmdbInsightService/UpdateModelGroup.
func (s *CmdbInsightService) UpdateModelGroup(ctx context.Context, req *v1.UpdateModelGroupRequest) (*v1.UpdateModelGroupResponse, error) {
	g, err := s.uc.UpdateGroup(withActor(ctx), req.GetCode(), biz.ModelGroupUpdate{Name: req.Name, Sort: req.Sort})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateModelGroupResponse{Group: toProtoGroup(g)}, nil
}

// DeleteModelGroup implements proto CmdbInsightService/DeleteModelGroup.
func (s *CmdbInsightService) DeleteModelGroup(ctx context.Context, req *v1.DeleteModelGroupRequest) (*v1.DeleteModelGroupResponse, error) {
	if err := s.uc.DeleteGroup(withActor(ctx), req.GetCode()); err != nil {
		return nil, err
	}
	return &v1.DeleteModelGroupResponse{}, nil
}

// ListFavorites implements proto CmdbInsightService/ListFavorites.
func (s *CmdbInsightService) ListFavorites(ctx context.Context, _ *v1.ListFavoritesRequest) (*v1.ListFavoritesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	codes, err := s.uc.ListFavorites(ctx, userID)
	if err != nil {
		return nil, err
	}
	if codes == nil {
		codes = []string{}
	}
	return &v1.ListFavoritesResponse{ModelCodes: codes}, nil
}

// SetFavorite implements proto CmdbInsightService/SetFavorite：收藏的模型须在数据范围内。
func (s *CmdbInsightService) SetFavorite(ctx context.Context, req *v1.SetFavoriteRequest) (*v1.SetFavoriteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetFavorite() {
		scope, err := s.guard.resolve(ctx)
		if err != nil {
			return nil, err
		}
		if !scope.AllowsModel(req.GetModelCode()) {
			return nil, biz.ErrScopeForbidden
		}
	}
	if err := s.uc.SetFavorite(ctx, userID, req.GetModelCode(), req.GetFavorite()); err != nil {
		return nil, err
	}
	return &v1.SetFavoriteResponse{}, nil
}

// GetOverview implements proto CmdbInsightService/GetOverview：受限用户只统计可见类型与环境。
func (s *CmdbInsightService) GetOverview(ctx context.Context, req *v1.GetOverviewRequest) (*v1.GetOverviewResponse, error) {
	scope, err := s.guard.resolve(ctx)
	if err != nil {
		return nil, err
	}
	filter := biz.OverviewFilter{}
	if !scope.Unrestricted {
		filter.ModelCodes = keys(scope.Models) // 非 nil：空集合即"什么都看不到"
		if !scope.EnvUnrestricted {
			filter.Envs = keys(scope.Envs)
		}
	}
	ov, err := s.uc.Overview(ctx, filter, req.GetDays())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetOverviewResponse{
		ModelCount: ov.ModelCount, CiCount: ov.CICount, RelationCount: ov.RelationCount, ChangeCount: ov.ChangeCount,
		ByModel: toProtoCounts(ov.ByModel), ByStatus: toProtoCounts(ov.ByStatus),
		ByEnv: toProtoCounts(ov.ByEnv), BySource: toProtoCounts(ov.BySource),
		Trend: make([]*v1.OverviewTrendPoint, 0, len(ov.Trend)), Days: ov.Days,
	}
	for _, p := range ov.Trend {
		resp.Trend = append(resp.Trend, &v1.OverviewTrendPoint{Date: p.Date, Action: p.Action, Count: p.Count})
	}
	return resp, nil
}

// GetQuality implements proto CmdbInsightService/GetQuality（数据范围同 GetOverview）。
func (s *CmdbInsightService) GetQuality(ctx context.Context, _ *v1.GetQualityRequest) (*v1.GetQualityResponse, error) {
	scope, err := s.guard.resolve(ctx)
	if err != nil {
		return nil, err
	}
	filter := biz.OverviewFilter{}
	if !scope.Unrestricted {
		filter.ModelCodes = keys(scope.Models)
		if !scope.EnvUnrestricted {
			filter.Envs = keys(scope.Envs)
		}
	}
	rows, err := s.uc.Quality(ctx, filter)
	if err != nil {
		return nil, err
	}
	// 问题多的模型在前。
	issues := func(q *biz.QualityRow) int64 { return q.MissingRequired + q.NoOwner + q.Orphan + q.Stale + q.Outdated }
	slices.SortFunc(rows, func(a, b *biz.QualityRow) int {
		if d := issues(b) - issues(a); d != 0 {
			return int(min(max(d, -1), 1))
		}
		return strings.Compare(a.ModelCode, b.ModelCode)
	})
	resp := &v1.GetQualityResponse{Items: make([]*v1.QualityRow, 0, len(rows)), OutdatedDays: biz.QualityOutdatedDays}
	for _, q := range rows {
		resp.Items = append(resp.Items, &v1.QualityRow{
			ModelCode: q.ModelCode, ModelName: q.ModelName, Total: q.Total, MissingRequired: q.MissingRequired,
			NoOwner: q.NoOwner, Orphan: q.Orphan, Stale: q.Stale, Outdated: q.Outdated,
		})
	}
	return resp, nil
}

// ListSchemaAudit implements proto CmdbInsightService/ListSchemaAudit。
// 模型元数据对持有接口权限者全量可见（与 ListModels 一致），不叠加数据范围。
func (s *CmdbInsightService) ListSchemaAudit(ctx context.Context, req *v1.ListSchemaAuditRequest) (*v1.ListSchemaAuditResponse, error) {
	filter := biz.SchemaAuditFilter{
		TargetType: req.GetTargetType(), ModelCode: req.GetModelCode(), Action: req.GetAction(),
		Operator: req.GetOperator(), Keyword: req.GetKeyword(),
	}
	if req.From != nil {
		from := req.GetFrom().AsTime()
		filter.From = &from
	}
	if req.To != nil {
		to := req.GetTo().AsTime()
		filter.To = &to
	}
	items, total, err := s.uc.ListSchemaAudit(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListSchemaAuditResponse{Items: make([]*v1.CmdbSchemaAudit, 0, len(items)), Total: total}
	for _, a := range items {
		diff, err := jsonStruct(a.Diff)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, &v1.CmdbSchemaAudit{
			Id: a.ID, TargetType: a.TargetType, TargetCode: a.TargetCode, TargetName: a.TargetName, ModelCode: a.ModelCode,
			Action: a.Action, Diff: diff, Operator: a.Operator, RequestId: a.RequestID, CreateTime: timestamppb.New(a.CreateTime),
		})
	}
	return resp, nil
}

func toProtoGroup(g *biz.ModelGroup) *v1.CmdbModelGroup {
	return &v1.CmdbModelGroup{Code: g.Code, Name: g.Name, Sort: g.Sort, Builtin: g.Builtin, ModelCount: g.ModelCount}
}

func toProtoCounts(items []biz.OverviewCount) []*v1.OverviewCount {
	out := make([]*v1.OverviewCount, 0, len(items))
	for _, c := range items {
		out = append(out, &v1.OverviewCount{Key: c.Key, Name: c.Name, Count: c.Count})
	}
	return out
}
