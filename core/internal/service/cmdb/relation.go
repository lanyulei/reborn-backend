package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// CmdbRelationService 实现 CmdbRelationServiceServer。
type CmdbRelationService struct {
	uc    *biz.RelationUsecase
	cis   *biz.CIUsecase
	guard scopeGuard
}

// NewCmdbRelationService new a CmdbRelationService.
func NewCmdbRelationService(uc *biz.RelationUsecase, cis *biz.CIUsecase, scopes *biz.ScopeUsecase) *CmdbRelationService {
	return &CmdbRelationService{uc: uc, cis: cis, guard: newScopeGuard(scopes)}
}

// requireAll 当前资源与全部目标资源都须在范围内。
func (s *CmdbRelationService) requireAll(ctx context.Context, ids ...string) error {
	for _, id := range ids {
		if _, err := s.guard.requireCI(ctx, s.cis.GetByID, id); err != nil {
			return err
		}
	}
	return nil
}

// LinkCIs implements proto CmdbRelationService/LinkCIs.
func (s *CmdbRelationService) LinkCIs(ctx context.Context, req *v1.LinkCIsRequest) (*v1.LinkCIsResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAll(ctx, append([]string{req.GetCiId()}, req.GetTargetIds()...)...); err != nil {
		return nil, err
	}
	linked, err := s.uc.Link(ctx, actor, req.GetCiId(), req.GetRelationType(), req.GetDirection(), req.GetTargetIds())
	if err != nil {
		return nil, err
	}
	return &v1.LinkCIsResponse{Linked: linked}, nil
}

// UnlinkCI implements proto CmdbRelationService/UnlinkCI.
func (s *CmdbRelationService) UnlinkCI(ctx context.Context, req *v1.UnlinkCIRequest) (*v1.UnlinkCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAll(ctx, req.GetCiId(), req.GetTargetId()); err != nil {
		return nil, err
	}
	if err := s.uc.Unlink(ctx, actor, req.GetCiId(), req.GetRelationType(), req.GetDirection(), req.GetTargetId()); err != nil {
		return nil, err
	}
	return &v1.UnlinkCIResponse{}, nil
}

// MoveCI implements proto CmdbRelationService/MoveCI.
func (s *CmdbRelationService) MoveCI(ctx context.Context, req *v1.MoveCIRequest) (*v1.MoveCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAll(ctx, req.GetCiId(), req.GetFromId(), req.GetToId()); err != nil {
		return nil, err
	}
	if err := s.uc.Move(ctx, actor, req.GetCiId(), req.GetRelationType(), req.GetDirection(), req.GetFromId(), req.GetToId()); err != nil {
		return nil, err
	}
	return &v1.MoveCIResponse{}, nil
}

// Neighbors implements proto CmdbRelationService/Neighbors：范围外节点及其边一并隐藏。
func (s *CmdbRelationService) Neighbors(ctx context.Context, req *v1.NeighborsRequest) (*v1.NeighborsResponse, error) {
	if _, err := s.guard.requireCI(ctx, s.cis.GetByID, req.GetCiId()); err != nil {
		return nil, err
	}
	scope, err := s.guard.resolve(ctx)
	if err != nil {
		return nil, err
	}
	nodes, edges, truncated, err := s.uc.Neighbors(ctx, req.GetCiId(), req.GetDepth(), req.GetMaxNodes())
	if err != nil {
		return nil, err
	}
	resp := &v1.NeighborsResponse{Nodes: make([]*v1.TopologyNode, 0, len(nodes)), Edges: make([]*v1.TopologyEdge, 0, len(edges)), Truncated: truncated}
	visible := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if !scope.Allows(n.ModelCode, n.Env) {
			continue
		}
		visible[n.ID] = struct{}{}
		resp.Nodes = append(resp.Nodes, &v1.TopologyNode{Id: n.ID, Name: n.Name, ModelCode: n.ModelCode, ModelName: n.ModelName, Status: n.Status, Env: n.Env, Depth: n.Depth})
	}
	for _, e := range edges {
		if _, ok := visible[e.SrcID]; !ok {
			continue
		}
		if _, ok := visible[e.DstID]; !ok {
			continue
		}
		resp.Edges = append(resp.Edges, &v1.TopologyEdge{Id: e.ID, RelationType: e.TypeCode, Name: e.Name, SrcId: e.SrcID, DstId: e.DstID})
	}
	return resp, nil
}

// ListRelated implements proto CmdbRelationService/ListRelated.
func (s *CmdbRelationService) ListRelated(ctx context.Context, req *v1.ListRelatedRequest) (*v1.ListRelatedResponse, error) {
	if _, err := s.guard.requireCI(ctx, s.cis.GetByID, req.GetCiId()); err != nil {
		return nil, err
	}
	scope, err := s.guard.resolve(ctx)
	if err != nil {
		return nil, err
	}
	var items []*biz.RelatedCI
	var total int64
	if scope.Unrestricted {
		items, total, err = s.uc.ListRelated(ctx, req.GetCiId(), req.GetRelationType(), req.GetDirection(), req.GetKeyword(),
			req.GetPage(), int64(req.GetPageSize()))
	} else {
		items, total, err = s.listRelatedInScope(ctx, scope, req)
	}
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRelatedResponse{Items: make([]*v1.RelatedCI, 0, len(items)), Total: total}
	for _, r := range items {
		p, err := toProtoCI(r.CI)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, &v1.RelatedCI{RelationId: r.RelationID, Ci: p})
	}
	return resp, nil
}

// listRelatedInScope 受限用户的关联列表：范围外的对端不展示（不泄露名称与数量）。
// 仓储分页不带范围条件，这里取全量后按范围过滤、再内存分页，保证各页 Total 一致。
func (s *CmdbRelationService) listRelatedInScope(ctx context.Context, scope biz.Scope, req *v1.ListRelatedRequest) ([]*biz.RelatedCI, int64, error) {
	visible := make([]*biz.RelatedCI, 0, 16)
	for page := int64(1); ; page++ {
		items, total, err := s.uc.ListRelated(ctx, req.GetCiId(), req.GetRelationType(), req.GetDirection(), req.GetKeyword(), page, paging.MaxPageSize)
		if err != nil {
			return nil, 0, err
		}
		for _, r := range items {
			if scope.Allows(r.CI.ModelCode, r.CI.Env) {
				visible = append(visible, r)
			}
		}
		if len(items) == 0 || page*paging.MaxPageSize >= total {
			break
		}
	}
	page, pageSize := paging.Normalize(req.GetPage(), int64(req.GetPageSize()))
	total := int64(len(visible))
	start := min((page-1)*pageSize, total)
	return visible[start:min(start+pageSize, total)], total, nil
}
