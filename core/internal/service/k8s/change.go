package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ChangeService 实现 K8SChangeServiceHTTPServer（变更审批）。
type ChangeService struct {
	uc *biz.ChangeUsecase
}

// NewChangeService new a ChangeService.
func NewChangeService(uc *biz.ChangeUsecase) *ChangeService { return &ChangeService{uc: uc} }

func toProtoChange(c *biz.Change, withDetail bool) *v1.Change {
	out := &v1.Change{
		Id: c.ID, Kind: c.Kind, ClusterId: c.ClusterID, ClusterName: c.ClusterName, Namespace: c.Namespace,
		ApiGroup: c.APIGroup, ApiVersion: c.APIVersion, Resource: c.Resource, ResKind: c.ResKind, Name: c.Name,
		Action: c.Action, Summary: c.Summary, Status: c.Status, RequesterId: c.RequesterID, RequesterName: c.RequesterName,
		TicketId: c.TicketID, TicketNumber: c.TicketNumber, Message: c.Message, CreateTime: timestamppb.New(c.CreateTime),
		ApproveTime: protoTime(c.ApproveTime), ExpireTime: protoTime(c.ExpireTime), FinishTime: protoTime(c.FinishTime),
	}
	if !withDetail {
		return out
	}
	if c.Display != nil {
		for _, it := range c.Display.Items {
			item := &v1.ChangeItem{
				ApiGroup: it.Ref.Group, ApiVersion: it.Ref.Version, Resource: it.Ref.Resource, Namespace: it.Ref.Namespace, Name: it.Ref.Name,
				Kind: it.Kind, Action: it.Action, BeforeYaml: it.BeforeYAML, AfterYaml: it.AfterYAML,
			}
			for _, f := range it.Fields {
				item.Fields = append(item.Fields, &v1.ChangeField{Path: f.Path, Before: f.Before, After: f.After})
			}
			out.Items = append(out.Items, item)
		}
	}
	for _, r := range c.Result.Applied {
		name := r.Name
		if r.Namespace != "" {
			name = r.Namespace + "/" + r.Name
		}
		out.Applied = append(out.Applied, r.Resource+" "+name)
	}
	if len(c.Params) > 0 && string(c.Params) != "{}" {
		out.Params = string(c.Params)
	}
	return out
}

// ListChanges implements proto K8sChangeService/ListChanges.
func (s *ChangeService) ListChanges(ctx context.Context, req *v1.ListChangesRequest) (*v1.ListChangesResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	filter := biz.ChangeFilter{ClusterID: req.GetClusterId(), Status: req.Status, Keyword: req.GetKeyword()}
	items, total, err := s.uc.List(ctx, caller, req.GetScope() != "all", filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListChangesResponse{Total: total, Items: make([]*v1.Change, 0, len(items))}
	for _, c := range items {
		resp.Items = append(resp.Items, toProtoChange(c, false))
	}
	return resp, nil
}

// GetChange implements proto K8sChangeService/GetChange.
func (s *ChangeService) GetChange(ctx context.Context, req *v1.GetChangeRequest) (*v1.GetChangeResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.Get(ctx, caller, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetChangeResponse{Change: toProtoChange(c, true)}, nil
}

// CancelChange implements proto K8sChangeService/CancelChange.
func (s *ChangeService) CancelChange(ctx context.Context, req *v1.CancelChangeRequest) (*v1.CancelChangeResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Cancel(ctx, caller, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.CancelChangeResponse{}, nil
}

// CheckChangeBypass implements proto K8sChangeService/CheckChangeBypass：能调通即持有免审批权限
// （授权中间件已判定）。
func (s *ChangeService) CheckChangeBypass(context.Context, *v1.CheckChangeBypassRequest) (*v1.CheckChangeBypassResponse, error) {
	return &v1.CheckChangeBypassResponse{}, nil
}

// GetChangeSettings implements proto K8sChangeService/GetChangeSettings.
func (s *ChangeService) GetChangeSettings(ctx context.Context, _ *v1.GetChangeSettingsRequest) (*v1.GetChangeSettingsResponse, error) {
	ns, perm, err := s.uc.Settings(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetChangeSettingsResponse{Settings: &v1.ChangeSettings{NamespaceRequestTemplateId: ns, PermissionRequestTemplateId: perm}}, nil
}

// UpdateChangeSettings implements proto K8sChangeService/UpdateChangeSettings.
func (s *ChangeService) UpdateChangeSettings(ctx context.Context, req *v1.UpdateChangeSettingsRequest) (*v1.UpdateChangeSettingsResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	in := req.GetSettings()
	if err := s.uc.UpdateSettings(ctx, caller, in.GetNamespaceRequestTemplateId(), in.GetPermissionRequestTemplateId()); err != nil {
		return nil, err
	}
	return &v1.UpdateChangeSettingsResponse{Settings: in}, nil
}

// ListRequestClusters implements proto K8sChangeService/ListRequestClusters.
func (s *ChangeService) ListRequestClusters(ctx context.Context, _ *v1.ListRequestClustersRequest) (*v1.ListRequestClustersResponse, error) {
	items, err := s.uc.RequestClusters(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRequestClustersResponse{Items: make([]*v1.RequestCluster, 0, len(items))}
	for _, o := range items {
		resp.Items = append(resp.Items, &v1.RequestCluster{Id: o.ID, Name: o.Name, DisplayName: o.DisplayName, Env: o.Env})
	}
	return resp, nil
}

// RequestNamespace implements proto K8sChangeService/RequestNamespace.
func (s *ChangeService) RequestNamespace(ctx context.Context, req *v1.RequestNamespaceRequest) (*v1.RequestNamespaceResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	err = s.uc.RequestNamespace(ctx, caller, req.GetClusterId(), kc.NamespaceRequest{
		Name: req.GetName(), Quota: req.GetQuota(), LimitDefault: req.GetLimitDefault(), LimitDefaultRequest: req.GetLimitDefaultRequest(),
	}, req.GetReason())
	changeID, ticketID, number, ok := biz.Submitted(err)
	if !ok {
		return nil, err
	}
	return &v1.RequestNamespaceResponse{ChangeId: changeID, TicketId: ticketID, TicketNumber: number}, nil
}

// RequestPermission implements proto K8sChangeService/RequestPermission.
func (s *ChangeService) RequestPermission(ctx context.Context, req *v1.RequestPermissionRequest) (*v1.RequestPermissionResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	err = s.uc.RequestPermission(ctx, caller, req.GetClusterId(), kc.PermissionRequest{
		Namespace: req.GetNamespace(), Level: req.GetLevel(), Reason: req.GetReason(),
	})
	changeID, ticketID, number, ok := biz.Submitted(err)
	if !ok {
		return nil, err
	}
	return &v1.RequestPermissionResponse{ChangeId: changeID, TicketId: ticketID, TicketNumber: number}, nil
}
