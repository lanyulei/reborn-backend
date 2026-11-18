package service

import (
	"context"
	"encoding/json"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ClusterService 实现 K8SClusterServiceHTTPServer。
type ClusterService struct {
	uc *biz.ClusterUsecase
}

// NewClusterService new a ClusterService.
func NewClusterService(uc *biz.ClusterUsecase) *ClusterService { return &ClusterService{uc: uc} }

func toClusterInput(s *v1.ClusterSpec) biz.ClusterInput {
	return biz.ClusterInput{
		Name: s.GetName(), DisplayName: s.GetDisplayName(), Env: s.GetEnv(), AccessMode: s.GetAccessMode(),
		AgentLocation: s.GetAgentLocation(), APIServer: s.GetApiServer(), ProxyURL: s.GetProxyUrl(), CACert: s.GetCaCert(),
		ExecDisabled: s.GetExecDisabled(), Description: s.GetDescription(), Enabled: s.GetEnabled(),
		DatasourceID: s.GetDatasourceId(), EventReasons: s.GetEventReasons(), ApprovalTemplateID: s.GetApprovalTemplateId(),
	}
}

// toCredentialInput 未传凭据返回 nil（编辑时表示沿用已保存的）。
func toCredentialInput(c *v1.ClusterCredential) *biz.CredentialInput {
	if c == nil {
		return nil
	}
	return &biz.CredentialInput{Token: c.GetToken(), ClientCert: c.GetClientCert(), ClientKey: c.GetClientKey()}
}

func toProtoCluster(c *biz.Cluster) *v1.Cluster {
	return &v1.Cluster{
		Id: c.ID,
		Spec: &v1.ClusterSpec{
			Name: c.Name, DisplayName: c.DisplayName, Env: c.Env, AccessMode: c.AccessMode, AgentLocation: c.AgentLocation,
			ApiServer: c.APIServer, ProxyUrl: biz.MaskProxyURL(c.ProxyURL), CaCert: c.CACert, ExecDisabled: c.ExecDisabled,
			Description: c.Description, Enabled: c.Enabled, DatasourceId: c.DatasourceID, EventReasons: c.EventReasons,
			ApprovalTemplateId: c.ApprovalTemplateID,
		},
		HasCredential: c.HasCredential,
		Runtime: &v1.ClusterRuntime{
			Status: c.Runtime.Status, StatusSince: protoTime(c.Runtime.StatusSince), K8SVersion: c.Runtime.K8sVersion,
			NodeTotal: c.Runtime.NodeTotal, NodeReady: c.Runtime.NodeReady, AgentVersion: c.Runtime.AgentVersion,
			AgentLastSeen: protoTime(c.Runtime.AgentLastSeen), LastError: c.Runtime.LastError, ProbeTime: protoTime(c.Runtime.ProbeTime),
			ScopeError: c.Runtime.ScopeError, ScopeSynced: c.Runtime.ScopeSynced,
		},
		AgentTokens: c.AgentTokens, Version: c.Version,
		CreateTime: timestamppb.New(c.CreateTime), UpdateTime: timestamppb.New(c.UpdateTime),
	}
}

// ListClusters implements proto K8sClusterService/ListClusters.
func (s *ClusterService) ListClusters(ctx context.Context, req *v1.ListClustersRequest) (*v1.ListClustersResponse, error) {
	items, total, err := s.uc.List(ctx, biz.ClusterFilter{Keyword: req.GetKeyword(), Env: req.Env, Status: req.Status},
		req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListClustersResponse{Items: make([]*v1.Cluster, 0, len(items)), Total: total}
	for _, c := range items {
		resp.Items = append(resp.Items, toProtoCluster(c))
	}
	return resp, nil
}

// GetCluster implements proto K8sClusterService/GetCluster.
func (s *ClusterService) GetCluster(ctx context.Context, req *v1.GetClusterRequest) (*v1.GetClusterResponse, error) {
	c, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetClusterResponse{Cluster: toProtoCluster(c)}, nil
}

// CreateCluster implements proto K8sClusterService/CreateCluster.
func (s *ClusterService) CreateCluster(ctx context.Context, req *v1.CreateClusterRequest) (*v1.CreateClusterResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.Create(ctx, caller, toClusterInput(req.GetSpec()), toCredentialInput(req.GetCredential()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateClusterResponse{Cluster: toProtoCluster(c)}, nil
}

// UpdateCluster implements proto K8sClusterService/UpdateCluster.
func (s *ClusterService) UpdateCluster(ctx context.Context, req *v1.UpdateClusterRequest) (*v1.UpdateClusterResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.Update(ctx, caller, req.GetId(), toClusterInput(req.GetSpec()), toCredentialInput(req.GetCredential()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateClusterResponse{Cluster: toProtoCluster(c)}, nil
}

// DeleteCluster implements proto K8sClusterService/DeleteCluster.
func (s *ClusterService) DeleteCluster(ctx context.Context, req *v1.DeleteClusterRequest) (*v1.DeleteClusterResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, caller, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteClusterResponse{}, nil
}

// ParseKubeconfig implements proto K8sClusterService/ParseKubeconfig。解析出的凭据就是请求里已有的内容，
// 回填表单用；不落库。
func (s *ClusterService) ParseKubeconfig(_ context.Context, req *v1.ParseKubeconfigRequest) (*v1.ParseKubeconfigResponse, error) {
	k, err := biz.ParseKubeconfig(req.GetKubeconfig(), req.GetContext())
	if err != nil {
		return nil, err
	}
	return &v1.ParseKubeconfigResponse{
		Contexts: k.Contexts, Context: k.Context, ApiServer: k.APIServer, CaCert: k.CACert, ProxyUrl: k.ProxyURL,
		Credential: &v1.ClusterCredential{Token: k.Credential.Token, ClientCert: k.Credential.ClientCert, ClientKey: k.Credential.ClientKey},
	}, nil
}

// TestCluster implements proto K8sClusterService/TestCluster.
func (s *ClusterService) TestCluster(ctx context.Context, req *v1.TestClusterRequest) (*v1.TestClusterResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.uc.Test(ctx, caller, biz.TestInput{
		ClusterID: req.GetId(), APIServer: req.GetApiServer(), ProxyURL: req.GetProxyUrl(), CACert: req.GetCaCert(),
		Credential: toCredentialInput(req.GetCredential()),
	})
	if err != nil {
		return nil, err
	}
	var out kc.ClusterTestResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, v1.ErrorK8sUpstreamError("data plane returned an unreadable test result")
	}
	resp := &v1.TestClusterResponse{Ok: out.OK, Version: out.Version, DurationMs: out.DurationMS, Error: out.Error}
	for _, c := range out.Checks {
		resp.Checks = append(resp.Checks, &v1.SelfCheckItem{Name: c.Name, Ok: c.OK, Message: c.Message})
	}
	return resp, nil
}

// InstallAgent implements proto K8sClusterService/InstallAgent。安装物里含一次性的注册令牌，只在这里返回。
func (s *ClusterService) InstallAgent(ctx context.Context, req *v1.InstallAgentRequest) (*v1.InstallAgentResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	out, err := s.uc.InstallAgent(ctx, caller, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.InstallAgentResponse{
		Location: out.Location, Manifest: out.Manifest, Command: out.Command, ExpireTime: timestamppb.New(out.ExpireTime),
	}, nil
}

// RevokeAgent implements proto K8sClusterService/RevokeAgent.
func (s *ClusterService) RevokeAgent(ctx context.Context, req *v1.RevokeAgentRequest) (*v1.RevokeAgentResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RevokeAgent(ctx, caller, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.RevokeAgentResponse{}, nil
}

// ListClusterOptions implements proto K8sClusterService/ListClusterOptions.
func (s *ClusterService) ListClusterOptions(ctx context.Context, _ *v1.ListClusterOptionsRequest) (*v1.ListClusterOptionsResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.Options(ctx, caller.UserID)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListClusterOptionsResponse{Items: make([]*v1.ClusterOption, 0, len(items))}
	for _, o := range items {
		resp.Items = append(resp.Items, &v1.ClusterOption{
			Id: o.ID, Name: o.Name, DisplayName: o.DisplayName, Env: o.Env, Status: o.Status, LastError: o.LastError,
			ExecDisabled: o.ExecDisabled, K8SVersion: o.K8sVersion, Level: o.Level, ApprovalRequired: o.ApprovalRequired,
		})
	}
	return resp, nil
}
