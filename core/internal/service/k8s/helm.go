package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HelmRepoService 实现 K8SHelmRepoServiceHTTPServer。
type HelmRepoService struct {
	uc *biz.HelmUsecase
}

// NewHelmRepoService new a HelmRepoService.
func NewHelmRepoService(uc *biz.HelmUsecase) *HelmRepoService { return &HelmRepoService{uc: uc} }

func toHelmRepoInput(s *v1.HelmRepoSpec) biz.HelmRepoInput {
	return biz.HelmRepoInput{
		Name: s.GetName(), Type: s.GetType(), URL: s.GetUrl(), Username: s.GetUsername(), CACert: s.GetCaCert(),
		Insecure: s.GetInsecureSkipVerify(), Description: s.GetDescription(), Enabled: s.GetEnabled(),
	}
}

func toProtoHelmRepo(r *biz.HelmRepo) *v1.HelmRepo {
	return &v1.HelmRepo{
		Id: r.ID,
		Spec: &v1.HelmRepoSpec{
			Name: r.Name, Type: r.Type, Url: r.URL, Username: r.Username, CaCert: r.CACert,
			InsecureSkipVerify: r.Insecure, Description: r.Description, Enabled: r.Enabled,
		},
		HasSecret: r.HasSecret, Version: r.Version,
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime),
	}
}

// ListHelmRepos implements proto K8sHelmRepoService/ListHelmRepos.
func (s *HelmRepoService) ListHelmRepos(ctx context.Context, req *v1.ListHelmReposRequest) (*v1.ListHelmReposResponse, error) {
	items, total, err := s.uc.List(ctx, req.GetKeyword(), req.GetPage(), req.GetPageSize())
	if err != nil {
		return nil, err
	}
	out := &v1.ListHelmReposResponse{Items: make([]*v1.HelmRepo, 0, len(items)), Total: total}
	for _, r := range items {
		out.Items = append(out.Items, toProtoHelmRepo(r))
	}
	return out, nil
}

// CreateHelmRepo implements proto K8sHelmRepoService/CreateHelmRepo.
func (s *HelmRepoService) CreateHelmRepo(ctx context.Context, req *v1.CreateHelmRepoRequest) (*v1.CreateHelmRepoResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Create(ctx, caller, toHelmRepoInput(req.GetSpec()), req.Secret)
	if err != nil {
		return nil, err
	}
	return &v1.CreateHelmRepoResponse{Repo: toProtoHelmRepo(r)}, nil
}

// UpdateHelmRepo implements proto K8sHelmRepoService/UpdateHelmRepo.
func (s *HelmRepoService) UpdateHelmRepo(ctx context.Context, req *v1.UpdateHelmRepoRequest) (*v1.UpdateHelmRepoResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Update(ctx, caller, req.GetId(), toHelmRepoInput(req.GetSpec()), req.Secret, req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateHelmRepoResponse{Repo: toProtoHelmRepo(r)}, nil
}

// DeleteHelmRepo implements proto K8sHelmRepoService/DeleteHelmRepo.
func (s *HelmRepoService) DeleteHelmRepo(ctx context.Context, req *v1.DeleteHelmRepoRequest) (*v1.DeleteHelmRepoResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, caller, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteHelmRepoResponse{}, nil
}

// ListHelmRepoOptions implements proto K8sHelmRepoService/ListHelmRepoOptions.
func (s *HelmRepoService) ListHelmRepoOptions(ctx context.Context, _ *v1.ListHelmRepoOptionsRequest) (*v1.ListHelmRepoOptionsResponse, error) {
	items, err := s.uc.Options(ctx)
	if err != nil {
		return nil, err
	}
	out := &v1.ListHelmRepoOptionsResponse{Items: make([]*v1.HelmRepoOption, 0, len(items))}
	for _, r := range items {
		out.Items = append(out.Items, &v1.HelmRepoOption{Id: r.ID, Name: r.Name, Type: r.Type, Url: r.URL})
	}
	return out, nil
}

// TestHelmRepo implements proto K8sHelmRepoService/TestHelmRepo.
func (s *HelmRepoService) TestHelmRepo(ctx context.Context, req *v1.TestHelmRepoRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.uc.Test(ctx, caller, req.GetId(), toHelmRepoInput(req.GetSpec()), req.Secret, req.GetChart())
	if err != nil {
		return nil, err
	}
	return jsonBody(raw), nil
}

// ListHelmCharts implements proto K8sHelmRepoService/ListHelmCharts.
func (s *HelmRepoService) ListHelmCharts(ctx context.Context, req *v1.ListHelmChartsRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.uc.Charts(ctx, caller, kc.HelmChartsRequest{RepoID: req.GetRepoId(), Keyword: req.GetKeyword(), Chart: req.GetChart(), Refresh: req.GetRefresh()})
	if err != nil {
		return nil, err
	}
	return jsonBody(raw), nil
}

// GetHelmChart implements proto K8sHelmRepoService/GetHelmChart.
func (s *HelmRepoService) GetHelmChart(ctx context.Context, req *v1.GetHelmChartRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := s.uc.Chart(ctx, caller, kc.HelmChartRequest{RepoID: req.GetRepoId(), Chart: req.GetChart(), Version: req.GetVersion()})
	if err != nil {
		return nil, err
	}
	return jsonBody(raw), nil
}

// HelmReleaseService 实现 K8SHelmReleaseServiceHTTPServer（全部转发给数据面，以调用者身份访问集群）。
type HelmReleaseService struct {
	forwarder
}

// NewHelmReleaseService new a HelmReleaseService.
func NewHelmReleaseService(planes *biz.PlaneUsecase) *HelmReleaseService {
	return &HelmReleaseService{forwarder{planes: planes}}
}

// ListHelmReleases implements proto K8sHelmReleaseService/ListHelmReleases.
func (s *HelmReleaseService) ListHelmReleases(ctx context.Context, req *v1.ListHelmReleasesRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpHelmList, kc.HelmListRequest{Namespace: req.GetNamespace(), Keyword: req.GetKeyword()})
}

// GetHelmRelease implements proto K8sHelmReleaseService/GetHelmRelease.
func (s *HelmReleaseService) GetHelmRelease(ctx context.Context, req *v1.GetHelmReleaseRequest) (*httpbody.HttpBody, error) {
	if err := biz.CheckRelease(req.GetName(), "", ""); err != nil {
		return nil, err
	}
	return s.call(ctx, req.GetClusterId(), kc.OpHelmGet, kc.HelmReleaseRequest{Namespace: req.GetNamespace(), Name: req.GetName()})
}

type helmApply interface {
	GetNamespace() string
	GetName() string
	GetRepoId() string
	GetChart() string
	GetVersion() string
	GetValues() string
}

func (s *HelmReleaseService) apply(ctx context.Context, clusterID, op string, req helmApply, upgrade bool) (*httpbody.HttpBody, error) {
	if err := biz.CheckRelease(req.GetName(), req.GetChart(), req.GetVersion()); err != nil {
		return nil, err
	}
	return s.call(ctx, clusterID, op, kc.HelmApplyRequest{
		Namespace: req.GetNamespace(), Name: req.GetName(), RepoID: req.GetRepoId(), Chart: req.GetChart(),
		Version: req.GetVersion(), Values: req.GetValues(), Upgrade: upgrade,
	})
}

// PreviewHelmRelease implements proto K8sHelmReleaseService/PreviewHelmRelease.
func (s *HelmReleaseService) PreviewHelmRelease(ctx context.Context, req *v1.PreviewHelmReleaseRequest) (*httpbody.HttpBody, error) {
	return s.apply(ctx, req.GetClusterId(), kc.OpHelmPreview, req, req.GetUpgrade())
}

// InstallHelmRelease implements proto K8sHelmReleaseService/InstallHelmRelease.
func (s *HelmReleaseService) InstallHelmRelease(ctx context.Context, req *v1.InstallHelmReleaseRequest) (*httpbody.HttpBody, error) {
	return s.apply(ctx, req.GetClusterId(), kc.OpHelmInstall, req, false)
}

// UpgradeHelmRelease implements proto K8sHelmReleaseService/UpgradeHelmRelease.
func (s *HelmReleaseService) UpgradeHelmRelease(ctx context.Context, req *v1.UpgradeHelmReleaseRequest) (*httpbody.HttpBody, error) {
	return s.apply(ctx, req.GetClusterId(), kc.OpHelmUpgrade, req, true)
}

// RollbackHelmRelease implements proto K8sHelmReleaseService/RollbackHelmRelease.
func (s *HelmReleaseService) RollbackHelmRelease(ctx context.Context, req *v1.RollbackHelmReleaseRequest) (*httpbody.HttpBody, error) {
	if err := biz.CheckRelease(req.GetName(), "", ""); err != nil {
		return nil, err
	}
	return s.call(ctx, req.GetClusterId(), kc.OpHelmRollback, kc.HelmRollbackRequest{Namespace: req.GetNamespace(), Name: req.GetName(), Revision: int(req.GetRevision())})
}

// UninstallHelmRelease implements proto K8sHelmReleaseService/UninstallHelmRelease.
func (s *HelmReleaseService) UninstallHelmRelease(ctx context.Context, req *v1.UninstallHelmReleaseRequest) (*httpbody.HttpBody, error) {
	if err := biz.CheckRelease(req.GetName(), "", ""); err != nil {
		return nil, err
	}
	return s.call(ctx, req.GetClusterId(), kc.OpHelmUninstall, kc.HelmReleaseRequest{Namespace: req.GetNamespace(), Name: req.GetName()})
}

// ListNamespaceQuotas implements proto K8sHelmReleaseService/ListNamespaceQuotas.
func (s *HelmReleaseService) ListNamespaceQuotas(ctx context.Context, req *v1.ListNamespaceQuotasRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpNamespaceQuota, kc.NamespaceQuotaRequest{Namespace: req.GetNamespace()})
}
