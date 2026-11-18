package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/genproto/googleapis/api/httpbody"
)

// forwarder 把一个已转换好的契约请求转给数据面，响应体原样透传。
type forwarder struct {
	planes *biz.PlaneUsecase
}

func (f forwarder) call(ctx context.Context, clusterID, op string, req any) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := f.planes.Call(ctx, caller, clusterID, op, req)
	if err != nil {
		return nil, err
	}
	return jsonBody(raw), nil
}

// toRef proto 对象引用 → 契约 Ref。
func toRef(r *v1.ResourceRef) kc.Ref {
	return kc.Ref{Group: r.GetGroup(), Version: r.GetVersion(), Resource: r.GetResource(), Namespace: r.GetNamespace(), Name: r.GetName()}
}

// ResourceService 实现 K8SResourceServiceHTTPServer（通用资源引擎，全部转发给数据面）。
type ResourceService struct {
	forwarder
}

// NewResourceService new a ResourceService.
func NewResourceService(planes *biz.PlaneUsecase) *ResourceService {
	return &ResourceService{forwarder{planes: planes}}
}

// ListAPIResources implements proto K8sResourceService/ListAPIResources.
func (s *ResourceService) ListAPIResources(ctx context.Context, req *v1.ListAPIResourcesRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpAPIResources, kc.APIResourcesRequest{Refresh: req.GetRefresh()})
}

// ListNamespaceOptions implements proto K8sResourceService/ListNamespaceOptions.
func (s *ResourceService) ListNamespaceOptions(ctx context.Context, req *v1.ListNamespaceOptionsRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpNamespaces, kc.Empty{})
}

// toListRequest 列表请求：state / node / owner 收进契约的 Filters。
func toListRequest(req *v1.ListResourcesRequest) kc.ListRequest {
	out := kc.ListRequest{
		Group: req.GetGroup(), Version: req.GetVersion(), Resource: req.GetResource(), Namespace: req.GetNamespace(),
		Keyword: req.GetKeyword(), LabelSelector: req.GetLabelSelector(), SortBy: req.GetSortBy(), SortDesc: req.GetSortDesc(),
		Page: int(req.GetPage()), PageSize: int(req.GetPageSize()), Continue: req.GetContinue(),
	}
	filters := map[string]string{}
	if v := req.GetState(); v != "" {
		filters[kc.FilterState] = v
	}
	if v := req.GetNode(); v != "" {
		filters[kc.FilterNode] = v
	}
	if v := req.GetOwner(); v != "" {
		filters[kc.FilterOwner] = v
	}
	if len(filters) > 0 {
		out.Filters = filters
	}
	return out
}

// ListResources implements proto K8sResourceService/ListResources.
func (s *ResourceService) ListResources(ctx context.Context, req *v1.ListResourcesRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpList, toListRequest(req))
}

// GetObject implements proto K8sResourceService/GetObject.
func (s *ResourceService) GetObject(ctx context.Context, req *v1.GetObjectRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpGet, kc.ObjectRequest{Ref: kc.Ref{
		Group: req.GetGroup(), Version: req.GetVersion(), Resource: req.GetResource(), Namespace: req.GetNamespace(), Name: req.GetName(),
	}})
}

// ListObjectEvents implements proto K8sResourceService/ListObjectEvents.
func (s *ResourceService) ListObjectEvents(ctx context.Context, req *v1.ListObjectEventsRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpEvents, kc.ObjectRequest{Ref: kc.Ref{
		Group: req.GetGroup(), Version: req.GetVersion(), Resource: req.GetResource(), Namespace: req.GetNamespace(), Name: req.GetName(),
	}})
}

// PreviewObject implements proto K8sResourceService/PreviewObject.
func (s *ResourceService) PreviewObject(ctx context.Context, req *v1.PreviewObjectRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpPreview,
		kc.ApplyRequest{Ref: toRef(req.GetRef()), YAML: req.GetYaml(), ResourceVersion: req.GetResourceVersion()})
}

// ApplyObject implements proto K8sResourceService/ApplyObject.
func (s *ResourceService) ApplyObject(ctx context.Context, req *v1.ApplyObjectRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpApply,
		kc.ApplyRequest{Ref: toRef(req.GetRef()), YAML: req.GetYaml(), ResourceVersion: req.GetResourceVersion()})
}

// DeleteObject implements proto K8sResourceService/DeleteObject.
func (s *ResourceService) DeleteObject(ctx context.Context, req *v1.DeleteObjectRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpDelete,
		kc.DeleteRequest{Ref: toRef(req.GetRef()), UID: req.GetUid(), Propagation: req.GetPropagation()})
}

// PreviewManifest implements proto K8sResourceService/PreviewManifest.
func (s *ResourceService) PreviewManifest(ctx context.Context, req *v1.PreviewManifestRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpManifestPreview, kc.ManifestRequest{YAML: req.GetYaml(), Namespace: req.GetNamespace()})
}

// ApplyManifest implements proto K8sResourceService/ApplyManifest.
func (s *ResourceService) ApplyManifest(ctx context.Context, req *v1.ApplyManifestRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpManifestApply, kc.ManifestRequest{YAML: req.GetYaml(), Namespace: req.GetNamespace()})
}

// RevealSecret implements proto K8sResourceService/RevealSecret。明文只经过内存，不落日志；审计由数据面写。
func (s *ResourceService) RevealSecret(ctx context.Context, req *v1.RevealSecretRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpSecretReveal, kc.ObjectRequest{Ref: kc.Ref{
		Group: kc.GVRSecret.Group, Version: kc.GVRSecret.Version, Resource: kc.GVRSecret.Resource,
		Namespace: req.GetNamespace(), Name: req.GetName(),
	}})
}
