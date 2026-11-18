package service

import (
	"context"
	"net/http"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// GovernanceService 实现 K8SGovernanceServiceHTTPServer：限时 kubeconfig、巡检、终端录制回放、告警规则模板；
// 并为 server 层的集群代理与文件传输提供校验与路由。
type GovernanceService struct {
	uc *biz.GovernanceUsecase
}

// NewGovernanceService new a GovernanceService.
func NewGovernanceService(uc *biz.GovernanceUsecase) *GovernanceService {
	return &GovernanceService{uc: uc}
}

func optTS(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func kubeTokenProto(t *biz.KubeToken, now time.Time) *v1.KubeToken {
	return &v1.KubeToken{
		Id: t.ID, UserId: t.UserID, Username: t.Username, ClusterId: t.ClusterID, ClusterName: t.ClusterName, Note: t.Note,
		CreateTime: timestamppb.New(t.CreateTime), ExpireTime: timestamppb.New(t.ExpireTime),
		LastUsedTime: optTS(t.LastUsedTime), LastUsedIp: t.LastUsedIP, RevokeTime: optTS(t.RevokeTime), Active: t.Active(now),
	}
}

// IssueKubeconfig implements proto K8sGovernanceService/IssueKubeconfig.
func (s *GovernanceService) IssueKubeconfig(ctx context.Context, req *v1.IssueKubeconfigRequest) (*v1.IssueKubeconfigResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	out, err := s.uc.IssueKubeconfig(ctx, caller, req.GetClusterId(), time.Duration(req.GetTtlSeconds())*time.Second, req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.IssueKubeconfigResponse{
		Token: kubeTokenProto(out.Token, time.Now()), Secret: out.Secret, Server: out.Server, Kubeconfig: out.Kubeconfig,
	}, nil
}

// ListKubeTokens implements proto K8sGovernanceService/ListKubeTokens.
func (s *GovernanceService) ListKubeTokens(ctx context.Context, req *v1.ListKubeTokensRequest) (*v1.ListKubeTokensResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListKubeTokens(ctx, caller, req.GetAll(), req.GetUserId(),
		biz.KubeTokenFilter{ClusterID: req.GetClusterId(), ActiveOnly: req.GetActiveOnly()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListKubeTokensResponse{Total: total, Items: make([]*v1.KubeToken, 0, len(items))}
	now := time.Now()
	for _, t := range items {
		out.Items = append(out.Items, kubeTokenProto(t, now))
	}
	return out, nil
}

// RevokeKubeToken implements proto K8sGovernanceService/RevokeKubeToken.
func (s *GovernanceService) RevokeKubeToken(ctx context.Context, req *v1.RevokeKubeTokenRequest) (*v1.RevokeKubeTokenResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.RevokeKubeTokenResponse{}, s.uc.RevokeKubeToken(ctx, caller, req.GetId())
}

// RevokeUserKubeTokens implements proto K8sGovernanceService/RevokeUserKubeTokens.
func (s *GovernanceService) RevokeUserKubeTokens(ctx context.Context, req *v1.RevokeUserKubeTokensRequest) (*v1.RevokeUserKubeTokensResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	n, err := s.uc.RevokeUserKubeTokens(ctx, caller, req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &v1.RevokeUserKubeTokensResponse{Revoked: int32(n)}, nil //nolint:gosec // 条数远小于 int32 上限
}

func inspectionProto(r *biz.InspectionRun) *v1.InspectionRun {
	sum := r.Summary
	out := &v1.InspectionRun{
		Id: r.ID, ClusterId: r.ClusterID, ClusterName: r.ClusterName, Trigger: r.Trigger, Status: r.Status, Score: r.Score,
		Critical: int32(sum.Critical), Warning: int32(sum.Warning), Info: int32(sum.Info), //nolint:gosec // 计数
		Pods: int32(sum.Pods), Workloads: int32(sum.Workloads), Nodes: int32(sum.Nodes), Certs: int32(sum.Certs), //nolint:gosec // 计数
		Partial: sum.Partial, Error: r.Error, CreateTime: timestamppb.New(r.CreateTime), FinishTime: optTS(r.FinishTime),
		Checks: make([]*v1.InspectionCheckStat, 0, len(sum.Checks)),
	}
	for _, c := range sum.Checks {
		out.Checks = append(out.Checks, &v1.InspectionCheckStat{Check: c.Check, Severity: c.Severity, Count: int32(c.Count)}) //nolint:gosec // 计数
	}
	return out
}

// ListInspectionRuns implements proto K8sGovernanceService/ListInspectionRuns.
func (s *GovernanceService) ListInspectionRuns(ctx context.Context, req *v1.ListInspectionRunsRequest) (*v1.ListInspectionRunsResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListInspectionRuns(ctx, caller, req.GetClusterId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListInspectionRunsResponse{Total: total, Items: make([]*v1.InspectionRun, 0, len(items))}
	for _, r := range items {
		out.Items = append(out.Items, inspectionProto(r))
	}
	return out, nil
}

// GetInspectionRun implements proto K8sGovernanceService/GetInspectionRun.
func (s *GovernanceService) GetInspectionRun(ctx context.Context, req *v1.GetInspectionRunRequest) (*v1.GetInspectionRunResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	run, findings, scoped, err := s.uc.GetInspectionRun(ctx, caller, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetInspectionRunResponse{Run: inspectionProto(run), Scoped: scoped, Findings: make([]*v1.InspectionFinding, 0, len(findings))}
	for _, f := range findings {
		out.Findings = append(out.Findings, &v1.InspectionFinding{
			Check: f.Check, Severity: f.Severity, Namespace: f.Namespace, Kind: f.Kind, Name: f.Name, Message: f.Message,
		})
	}
	return out, nil
}

// RunInspection implements proto K8sGovernanceService/RunInspection.
func (s *GovernanceService) RunInspection(ctx context.Context, req *v1.RunInspectionRequest) (*v1.RunInspectionResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	run, err := s.uc.RunInspection(ctx, caller, req.GetClusterId())
	if err != nil {
		return nil, err
	}
	return &v1.RunInspectionResponse{Run: inspectionProto(run)}, nil
}

// GetExecRecord implements proto K8sGovernanceService/GetExecRecord.
func (s *GovernanceService) GetExecRecord(ctx context.Context, req *v1.GetExecRecordRequest) (*v1.GetExecRecordResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	events, truncated, err := s.uc.ExecRecord(ctx, caller, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetExecRecordResponse{Truncated: truncated, Events: make([]*v1.ExecRecordEvent, 0, len(events))}
	for _, e := range events {
		out.Events = append(out.Events, &v1.ExecRecordEvent{OffsetMs: e.OffsetMS, Type: e.Type, Data: e.Data, Cols: e.Cols, Rows: e.Rows})
		out.DurationMs = max(out.DurationMs, e.OffsetMS)
	}
	return out, nil
}

// GetAlertRuleTemplate implements proto K8sGovernanceService/GetAlertRuleTemplate.
func (s *GovernanceService) GetAlertRuleTemplate(context.Context, *v1.GetAlertRuleTemplateRequest) (*v1.GetAlertRuleTemplateResponse, error) {
	content, n := biz.AlertRuleTemplate()
	return &v1.GetAlertRuleTemplateResponse{Content: content, Rules: int32(n)}, nil //nolint:gosec // 规则条数
}

// AuthorizeKube 校验集群代理的一个请求（server 层调用）。
func (s *GovernanceService) AuthorizeKube(ctx context.Context, bearer, clusterID, method, kubePath, clientIP string) (*biz.KubeRoute, error) {
	return s.uc.AuthorizeKube(ctx, bearer, clusterID, method, kubePath, clientIP)
}

// InvalidateRoute 丢弃集群归属缓存。
func (s *GovernanceService) InvalidateRoute(clusterID string) { s.uc.InvalidateRoute(clusterID) }

// FileRoute 文件传输的转发目标：以鉴权后的调用者身份（server 层调用，ctx 须已过中间件链）。
func (s *GovernanceService) FileRoute(ctx context.Context, clusterID string) (string, http.Header, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return "", nil, err
	}
	return s.uc.PlaneRoute(ctx, caller, clusterID)
}

// CheckDebugContainer implements proto K8sGovernanceService/CheckDebugContainer：能调通即持有调试容器权限
// （授权中间件已判定）；打开调试终端时由 PlaneUsecase 按同一接口权限复核。
func (s *GovernanceService) CheckDebugContainer(context.Context, *v1.CheckDebugContainerRequest) (*v1.CheckDebugContainerResponse, error) {
	return &v1.CheckDebugContainerResponse{}, nil
}
