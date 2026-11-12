// Package service 实现 proto 生成的回调接入接口：取令牌头、调用 biz、错误透传。
package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/cmdb/api/webhook/v1"
	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/go-kratos/kratos/v3/transport"
	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewWebhookService)

// WebhookService 实现 CmdbWebhookServiceHTTPServer。
type WebhookService struct {
	uc *biz.WebhookUsecase
}

// NewWebhookService new a WebhookService.
func NewWebhookService(uc *biz.WebhookUsecase) *WebhookService { return &WebhookService{uc: uc} }

// Trigger implements proto CmdbWebhookService/Trigger（令牌取自请求头；回调体只作记录不解析）。
func (s *WebhookService) Trigger(ctx context.Context, req *v1.TriggerRequest) (*v1.TriggerResponse, error) {
	token := ""
	if tr, ok := transport.FromServerContext(ctx); ok {
		token = tr.RequestHeader().Get(cmdbcontract.WebhookTokenHeader)
	}
	taskID, queued, err := s.uc.Trigger(ctx, req.GetSourceId(), token)
	if err != nil {
		return nil, err
	}
	return &v1.TriggerResponse{TaskId: taskID, Queued: queued}, nil
}
