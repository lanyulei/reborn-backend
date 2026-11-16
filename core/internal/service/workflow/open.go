package service

import (
	"context"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"

	"github.com/go-kratos/kratos/v3/transport"
)

// OpenService 实现 api/workflow/v1 生成的 WorkflowOpenServiceServer。这组接口免用户登录与
// 接口授权（见 server.authExempt / system.authzExemptOperations），身份由请求头里的接入
// 令牌确定，全部校验在用例层完成。
type OpenService struct {
	uc *biz.OpenUsecase
}

// NewOpenService new an OpenService.
func NewOpenService(uc *biz.OpenUsecase) *OpenService {
	return &OpenService{uc: uc}
}

// bearer 取请求头 Authorization: Bearer <令牌> 里的令牌；没有返回空串（由用例统一按未认证处理）。
func bearer(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	scheme, token, found := strings.Cut(tr.RequestHeader().Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func openResponse(in *biz.Instance) *v1.OpenInstanceResponse {
	return &v1.OpenInstanceResponse{Instance: &v1.OpenInstance{
		Id: in.ID, DefinitionKey: in.DefinitionKey, Title: in.Title, Status: in.Status,
		Variables: structOfMap(in.Variables), StartTime: ts(in.StartTime), EndTime: ts(in.EndTime),
	}}
}

// OpenStartInstance implements proto WorkflowOpenService/OpenStartInstance.
func (s *OpenService) OpenStartInstance(ctx context.Context, req *v1.OpenStartInstanceRequest) (*v1.OpenInstanceResponse, error) {
	in, err := s.uc.Start(ctx, bearer(ctx), biz.StartParams{
		DefinitionKey: req.GetDefinitionKey(), Title: req.GetTitle(), Variables: req.GetVariables().AsMap(), RequestID: req.GetRequestId(),
	})
	if err != nil {
		return nil, err
	}
	return openResponse(in), nil
}

// OpenGetInstance implements proto WorkflowOpenService/OpenGetInstance.
func (s *OpenService) OpenGetInstance(ctx context.Context, req *v1.OpenGetInstanceRequest) (*v1.OpenInstanceResponse, error) {
	in, err := s.uc.Get(ctx, bearer(ctx), req.GetId())
	if err != nil {
		return nil, err
	}
	return openResponse(in), nil
}

// OpenSignalInstance implements proto WorkflowOpenService/OpenSignalInstance.
func (s *OpenService) OpenSignalInstance(ctx context.Context, req *v1.OpenSignalInstanceRequest) (*v1.OpenInstanceResponse, error) {
	in, err := s.uc.Signal(ctx, bearer(ctx), req.GetId(), req.GetName(), req.GetVariables().AsMap())
	if err != nil {
		return nil, err
	}
	return openResponse(in), nil
}

// OpenCancelInstance implements proto WorkflowOpenService/OpenCancelInstance.
func (s *OpenService) OpenCancelInstance(ctx context.Context, req *v1.OpenCancelInstanceRequest) (*v1.OpenInstanceResponse, error) {
	in, err := s.uc.Cancel(ctx, bearer(ctx), req.GetId(), req.GetComment())
	if err != nil {
		return nil, err
	}
	return openResponse(in), nil
}
