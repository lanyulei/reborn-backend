package service

import (
	"context"
	"encoding/json"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/genproto/googleapis/api/httpbody"
)

// InsightService 实现 K8SInsightServiceHTTPServer（可观测，只读）。总览 / 事件 / 指标 / 主机关联由 core
// 生成 JSON；选择器、关联关系、诊断摘要转发给数据面。
type InsightService struct {
	forwarder
	uc *biz.InsightUsecase
}

// NewInsightService new an InsightService.
func NewInsightService(uc *biz.InsightUsecase, planes *biz.PlaneUsecase) *InsightService {
	return &InsightService{forwarder: forwarder{planes: planes}, uc: uc}
}

func jsonOf(v any) (*httpbody.HttpBody, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return jsonBody(raw), nil
}

// GetOverview implements proto K8sInsightService/GetOverview.
func (s *InsightService) GetOverview(ctx context.Context, _ *v1.GetOverviewRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	out, err := s.uc.Overview(ctx, caller)
	if err != nil {
		return nil, err
	}
	return jsonOf(out)
}

// ListEvents implements proto K8sInsightService/ListEvents.
func (s *InsightService) ListEvents(ctx context.Context, req *v1.ListEventsRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	filter := biz.EventFilter{
		ClusterID: req.ClusterId, Namespace: req.Namespace, Kind: req.Kind, Name: req.Name, Reason: req.Reason, Keyword: req.GetKeyword(),
	}
	items, total, err := s.uc.ListEvents(ctx, caller, filter, optTime(req.GetLastTimeStart()), optTime(req.GetLastTimeEnd()), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []*biz.Event{}
	}
	return jsonOf(map[string]any{"items": items, "total": total})
}

// QueryMetrics implements proto K8sInsightService/QueryMetrics.
func (s *InsightService) QueryMetrics(ctx context.Context, req *v1.QueryMetricsRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	t := req.GetTarget()
	out, err := s.uc.QueryMetrics(ctx, caller, req.GetClusterId(),
		biz.MetricsTarget{Kind: t.GetKind(), Namespace: t.GetNamespace(), Name: t.GetName(), WorkloadKind: t.GetWorkloadKind()},
		req.GetPanels(), optTime(req.GetStart()), optTime(req.GetEnd()))
	if err != nil {
		return nil, err
	}
	return jsonOf(out)
}

// LocateHost implements proto K8sInsightService/LocateHost.
func (s *InsightService) LocateHost(ctx context.Context, req *v1.LocateHostRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.LocateHost(ctx, caller, req.GetIp())
	if err != nil {
		return nil, err
	}
	return jsonOf(map[string]any{"items": items})
}

// GetNodeHost implements proto K8sInsightService/GetNodeHost.
func (s *InsightService) GetNodeHost(ctx context.Context, req *v1.GetNodeHostRequest) (*httpbody.HttpBody, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.NodeHost(ctx, caller, req.GetClusterId(), req.GetIp())
	if err != nil {
		return nil, err
	}
	return jsonOf(map[string]any{"items": items})
}

type refFields interface {
	GetGroup() string
	GetVersion() string
	GetResource() string
	GetNamespace() string
	GetName() string
}

func objectRequest(r refFields) kc.ObjectRequest {
	return kc.ObjectRequest{Ref: kc.Ref{Group: r.GetGroup(), Version: r.GetVersion(), Resource: r.GetResource(), Namespace: r.GetNamespace(), Name: r.GetName()}}
}

// GetPodSelector implements proto K8sInsightService/GetPodSelector.
func (s *InsightService) GetPodSelector(ctx context.Context, req *v1.GetPodSelectorRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpSelector, objectRequest(req))
}

// GetRelations implements proto K8sInsightService/GetRelations.
func (s *InsightService) GetRelations(ctx context.Context, req *v1.GetRelationsRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpRelations, objectRequest(req))
}

// DiagnosePod implements proto K8sInsightService/DiagnosePod.
func (s *InsightService) DiagnosePod(ctx context.Context, req *v1.DiagnosePodRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpDiagnosePod, kc.ObjectRequest{Ref: kc.Ref{
		Group: kc.GVRPod.Group, Version: kc.GVRPod.Version, Resource: kc.GVRPod.Resource, Namespace: req.GetNamespace(), Name: req.GetName(),
	}})
}

// DiagnoseWorkload implements proto K8sInsightService/DiagnoseWorkload.
func (s *InsightService) DiagnoseWorkload(ctx context.Context, req *v1.DiagnoseWorkloadRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpDiagnoseWorkload, objectRequest(req))
}
