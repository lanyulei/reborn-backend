package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ScopeService 实现 K8SScopeServiceHTTPServer。
type ScopeService struct {
	uc *biz.ScopeUsecase
}

// NewScopeService new a ScopeService.
func NewScopeService(uc *biz.ScopeUsecase) *ScopeService { return &ScopeService{uc: uc} }

func toProtoScopes(items []*biz.RoleScope) []*v1.RoleScopeItem {
	out := make([]*v1.RoleScopeItem, 0, len(items))
	for _, it := range items {
		out = append(out, &v1.RoleScopeItem{ClusterId: it.ClusterID, ClusterName: it.ClusterName, Namespace: it.Namespace, Level: it.Level})
	}
	return out
}

// GetRoleScope implements proto K8sScopeService/GetRoleScope.
func (s *ScopeService) GetRoleScope(ctx context.Context, req *v1.GetRoleScopeRequest) (*v1.GetRoleScopeResponse, error) {
	items, err := s.uc.Get(ctx, req.GetRoleId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRoleScopeResponse{Items: toProtoScopes(items)}, nil
}

// ReplaceRoleScope implements proto K8sScopeService/ReplaceRoleScope.
func (s *ScopeService) ReplaceRoleScope(ctx context.Context, req *v1.ReplaceRoleScopeRequest) (*v1.ReplaceRoleScopeResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*biz.RoleScope, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, &biz.RoleScope{ClusterID: it.GetClusterId(), Namespace: it.GetNamespace(), Level: it.GetLevel()})
	}
	saved, err := s.uc.Replace(ctx, caller, req.GetRoleId(), items)
	if err != nil {
		return nil, err
	}
	return &v1.ReplaceRoleScopeResponse{Items: toProtoScopes(saved)}, nil
}

// AuditService 实现 K8SAuditServiceHTTPServer。
type AuditService struct {
	uc *biz.AuditUsecase
}

// NewAuditService new an AuditService.
func NewAuditService(uc *biz.AuditUsecase) *AuditService { return &AuditService{uc: uc} }

func toProtoAudit(a *biz.Audit) *v1.Audit {
	return &v1.Audit{
		Id: a.ID, CreateTime: timestamppb.New(a.CreateTime), UserId: a.UserID, Username: a.Username, ClientIp: a.ClientIP,
		ClusterId: a.ClusterID, ClusterName: a.ClusterName, Namespace: a.Namespace, ApiGroup: a.APIGroup, ApiVersion: a.APIVersion,
		Resource: a.Resource, Kind: a.Kind, Name: a.Name, Action: a.Action, Result: a.Result, Error: a.Error,
		Emergency: a.Emergency, ChangeId: a.ChangeID, Truncated: a.Truncated,
	}
}

// ListAudits implements proto K8sAuditService/ListAudits.
func (s *AuditService) ListAudits(ctx context.Context, req *v1.ListAuditsRequest) (*v1.ListAuditsResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.List(ctx, caller, biz.AuditFilter{
		ClusterID: req.GetClusterId(), Namespace: req.GetNamespace(), Resource: req.GetResource(), Name: req.GetName(),
		Action: req.GetAction(), UserID: req.GetUserId(), Result: req.Result, Keyword: req.GetKeyword(),
	}, optTime(req.GetCreateTimeStart()), optTime(req.GetCreateTimeEnd()), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListAuditsResponse{Items: make([]*v1.Audit, 0, len(items)), Total: total}
	for _, a := range items {
		resp.Items = append(resp.Items, toProtoAudit(a))
	}
	return resp, nil
}

// GetAudit implements proto K8sAuditService/GetAudit。对象渲染为 YAML，改动保持 JSON 文本。
func (s *AuditService) GetAudit(ctx context.Context, req *v1.GetAuditRequest) (*v1.GetAuditResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	a, err := s.uc.Get(ctx, caller, req.GetId())
	if err != nil {
		return nil, err
	}
	out := toProtoAudit(a)
	out.BeforeYaml, out.AfterYaml, out.Patch = biz.ObjectYAML(a.BeforeObj), biz.ObjectYAML(a.AfterObj), string(a.Patch)
	return &v1.GetAuditResponse{Audit: out}, nil
}

// ListExecSessions implements proto K8sAuditService/ListExecSessions.
func (s *AuditService) ListExecSessions(ctx context.Context, req *v1.ListExecSessionsRequest) (*v1.ListExecSessionsResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListExecSessions(ctx, caller, biz.ExecSessionFilter{
		ClusterID: req.GetClusterId(), UserID: req.GetUserId(), Keyword: req.GetKeyword(),
	}, optTime(req.GetStartTimeStart()), optTime(req.GetStartTimeEnd()), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListExecSessionsResponse{Items: make([]*v1.ExecSession, 0, len(items)), Total: total}
	for _, e := range items {
		resp.Items = append(resp.Items, &v1.ExecSession{
			Id: e.ID, UserId: e.UserID, Username: e.Username, ClientIp: e.ClientIP, ClusterId: e.ClusterID, ClusterName: e.ClusterName,
			Namespace: e.Namespace, Pod: e.Pod, Container: e.Container, Kind: e.Kind, StartTime: timestamppb.New(e.StartTime),
			EndTime: protoTime(e.EndTime), EndReason: e.EndReason, BytesIn: e.BytesIn, BytesOut: e.BytesOut, Recorded: e.Recorded,
		})
	}
	return resp, nil
}
