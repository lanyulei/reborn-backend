package service

import (
	"context"
	"io"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"google.golang.org/genproto/googleapis/api/httpbody"
)

// WorkloadService 实现 K8SWorkloadServiceHTTPServer（工作负载、Pod 与节点的快捷操作；终端票据由 core 签发）。
type WorkloadService struct {
	forwarder
}

// NewWorkloadService new a WorkloadService.
func NewWorkloadService(planes *biz.PlaneUsecase) *WorkloadService {
	return &WorkloadService{forwarder{planes: planes}}
}

// ScaleWorkload implements proto K8sWorkloadService/ScaleWorkload.
func (s *WorkloadService) ScaleWorkload(ctx context.Context, req *v1.ScaleWorkloadRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpScale, kc.ScaleRequest{Ref: toRef(req.GetRef()), Replicas: req.GetReplicas()})
}

// RestartWorkload implements proto K8sWorkloadService/RestartWorkload.
func (s *WorkloadService) RestartWorkload(ctx context.Context, req *v1.RestartWorkloadRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpRestart, kc.ObjectRequest{Ref: toRef(req.GetRef())})
}

// SetWorkloadImage implements proto K8sWorkloadService/SetWorkloadImage.
func (s *WorkloadService) SetWorkloadImage(ctx context.Context, req *v1.SetWorkloadImageRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpSetImage,
		kc.SetImageRequest{Ref: toRef(req.GetRef()), Container: req.GetContainer(), Image: req.GetImage()})
}

// ListWorkloadRevisions implements proto K8sWorkloadService/ListWorkloadRevisions.
func (s *WorkloadService) ListWorkloadRevisions(ctx context.Context, req *v1.ListWorkloadRevisionsRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpRevisions, kc.ObjectRequest{Ref: kc.Ref{
		Group: req.GetGroup(), Version: req.GetVersion(), Resource: req.GetResource(), Namespace: req.GetNamespace(), Name: req.GetName(),
	}})
}

// RollbackWorkload implements proto K8sWorkloadService/RollbackWorkload.
func (s *WorkloadService) RollbackWorkload(ctx context.Context, req *v1.RollbackWorkloadRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpRollback, kc.RollbackRequest{Ref: toRef(req.GetRef()), Revision: req.GetRevision()})
}

// PauseWorkload implements proto K8sWorkloadService/PauseWorkload.
func (s *WorkloadService) PauseWorkload(ctx context.Context, req *v1.PauseWorkloadRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpPause, kc.PauseRequest{Ref: toRef(req.GetRef()), Paused: req.GetPaused()})
}

// TriggerCronJob implements proto K8sWorkloadService/TriggerCronJob.
func (s *WorkloadService) TriggerCronJob(ctx context.Context, req *v1.TriggerCronJobRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpCronTrigger, kc.ObjectRequest{Ref: toRef(req.GetRef())})
}

// SuspendCronJob implements proto K8sWorkloadService/SuspendCronJob.
func (s *WorkloadService) SuspendCronJob(ctx context.Context, req *v1.SuspendCronJobRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpCronSuspend, kc.SuspendRequest{Ref: toRef(req.GetRef()), Suspend: req.GetSuspend()})
}

// EvictPod implements proto K8sWorkloadService/EvictPod.
func (s *WorkloadService) EvictPod(ctx context.Context, req *v1.EvictPodRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpPodEvict, kc.ObjectRequest{Ref: toRef(req.GetRef())})
}

// ResizePod implements proto K8sWorkloadService/ResizePod.
func (s *WorkloadService) ResizePod(ctx context.Context, req *v1.ResizePodRequest) (*httpbody.HttpBody, error) {
	containers := make([]kc.ContainerResize, 0, len(req.GetContainers()))
	for _, c := range req.GetContainers() {
		containers = append(containers, kc.ContainerResize{
			Name: c.GetName(), Requests: toResizeQuantity(c.GetRequests()), Limits: toResizeQuantity(c.GetLimits()),
		})
	}
	return s.call(ctx, req.GetClusterId(), kc.OpPodResize, kc.ResizeRequest{Ref: toRef(req.GetRef()), Containers: containers})
}

// toResizeQuantity 转为契约结构；未传的数量保持 nil（= 不改）。
func toResizeQuantity(q *v1.ResizeQuantity) *kc.ResizeQuantity {
	if q == nil {
		return nil
	}
	return &kc.ResizeQuantity{CPU: q.Cpu, Memory: q.Memory}
}

// CordonNode implements proto K8sWorkloadService/CordonNode.
func (s *WorkloadService) CordonNode(ctx context.Context, req *v1.CordonNodeRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpNodeCordon, kc.CordonRequest{Name: req.GetName(), Unschedulable: req.GetUnschedulable()})
}

// PreviewDrainNode implements proto K8sWorkloadService/PreviewDrainNode.
func (s *WorkloadService) PreviewDrainNode(ctx context.Context, req *v1.PreviewDrainNodeRequest) (*httpbody.HttpBody, error) {
	return s.call(ctx, req.GetClusterId(), kc.OpDrainPreview, kc.DrainRequest{
		Name: req.GetName(), DeleteEmptyDir: req.GetDeleteEmptyDir(), Force: req.GetForce(), GracePeriodSeconds: req.GetGracePeriodSeconds(),
	})
}

// OpenExecTicket implements proto K8sWorkloadService/OpenExecTicket。票据明文只在这里返回一次。
func (s *WorkloadService) OpenExecTicket(ctx context.Context, req *v1.OpenExecTicketRequest) (*v1.OpenExecTicketResponse, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	ticket, err := s.planes.OpenExecTicket(ctx, caller, req.GetClusterId(), kc.ExecParams{
		Namespace: req.GetNamespace(), Pod: req.GetPod(), Container: req.GetContainer(), SessionID: req.GetSessionId(),
		Kind: max(req.GetKind(), kc.ExecKindShell), DebugImage: req.GetDebugImage(),
		Cols: uint16(min(req.GetCols(), 1000)), Rows: uint16(min(req.GetRows(), 1000)),
	})
	if err != nil {
		return nil, err
	}
	return &v1.OpenExecTicketResponse{Ticket: ticket}, nil
}

// StreamService K8sStreamService 的流式接口与终端：不实现生成的 Server 接口，由 server 层的原生路由调用
// （预检走完整的中间件链之后才进到这里）。返回的流由调用方负责关闭。
type StreamService struct {
	planes *biz.PlaneUsecase
}

// NewStreamService new a StreamService.
func NewStreamService(planes *biz.PlaneUsecase) *StreamService { return &StreamService{planes: planes} }

func (s *StreamService) open(ctx context.Context, clusterID, stream string, req any) (io.ReadCloser, error) {
	caller, err := callerFrom(ctx)
	if err != nil {
		return nil, err
	}
	return s.planes.Stream(ctx, caller, clusterID, stream, req)
}

// WatchResources 订阅资源变更。
func (s *StreamService) WatchResources(ctx context.Context, req *v1.WatchResourcesRequest) (io.ReadCloser, error) {
	return s.open(ctx, req.GetClusterId(), kc.StreamWatch, kc.WatchRequest{
		Group: req.GetGroup(), Version: req.GetVersion(), Resource: req.GetResource(), Namespace: req.GetNamespace(),
	})
}

// StreamPodLog Pod 实时日志。
func (s *StreamService) StreamPodLog(ctx context.Context, req *v1.StreamPodLogRequest) (io.ReadCloser, error) {
	return s.open(ctx, req.GetClusterId(), kc.StreamPodLog, kc.PodLogRequest{
		Namespace: req.GetNamespace(), Pod: req.GetPod(), Container: req.GetContainer(), Previous: req.GetPrevious(),
		TailLines: req.GetTailLines(), SinceSeconds: req.GetSinceSeconds(), Follow: req.GetFollow(),
		Keyword: req.GetKeyword(), Cursor: req.GetCursor(),
	})
}

// StreamWorkloadLog 工作负载级日志。
func (s *StreamService) StreamWorkloadLog(ctx context.Context, req *v1.StreamWorkloadLogRequest) (io.ReadCloser, error) {
	return s.open(ctx, req.GetClusterId(), kc.StreamWorkloadLog, kc.WorkloadLogRequest{
		Ref: toRef(req.GetRef()), Container: req.GetContainer(), TailLines: req.GetTailLines(), SinceSeconds: req.GetSinceSeconds(),
		Keyword: req.GetKeyword(), Cursors: req.GetCursors(),
	})
}

// DrainNode 排空节点。
func (s *StreamService) DrainNode(ctx context.Context, req *v1.DrainNodeRequest) (io.ReadCloser, error) {
	return s.open(ctx, req.GetClusterId(), kc.StreamDrain, kc.DrainRequest{
		Name: req.GetName(), DeleteEmptyDir: req.GetDeleteEmptyDir(), Force: req.GetForce(), GracePeriodSeconds: req.GetGracePeriodSeconds(),
	})
}

// ConnectExec 核销终端票据并建立到数据面的终端连接。不依赖登录态：票据即凭据（一次性、30 秒有效）。
func (s *StreamService) ConnectExec(ctx context.Context, ticket, clientIP string) (biz.ExecConn, error) {
	return s.planes.ConnectExec(ctx, ticket, clientIP)
}
