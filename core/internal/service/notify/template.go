package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyTemplateService 实现 api/notify/v1 生成的 NotifyTemplateServiceServer。
type NotifyTemplateService struct {
	notifyv1.UnimplementedNotifyTemplateServiceServer

	uc    *biz.TemplateUsecase
	guard bizGuard
}

// NewNotifyTemplateService new a NotifyTemplateService.
func NewNotifyTemplateService(uc *biz.TemplateUsecase, scope biz.BizScopeResolver) *NotifyTemplateService {
	return &NotifyTemplateService{uc: uc, guard: newBizGuard(scope)}
}

// toProtoTemplate 将 DO 显式转换为 proto 实体。
func toProtoTemplate(tpl *biz.Template) *notifyv1.NotifyTemplate {
	return &notifyv1.NotifyTemplate{
		Id:          tpl.ID,
		BizId:       tpl.BizID,
		EventType:   tpl.EventType,
		ChannelType: tpl.ChannelType,
		Subject:     tpl.Subject,
		Body:        tpl.Body,
		Version:     tpl.Version,
		Enabled:     tpl.Enabled,
		CreateTime:  timestamppb.New(tpl.CreateTime),
		UpdateTime:  timestamppb.New(tpl.UpdateTime),
	}
}

// CreateTemplate implements proto NotifyTemplateService/CreateTemplate。
func (s *NotifyTemplateService) CreateTemplate(ctx context.Context, req *notifyv1.CreateTemplateRequest) (*notifyv1.CreateTemplateResponse, error) {
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	tpl, err := s.uc.Create(ctx, &biz.Template{
		BizID:       req.GetBizId(),
		EventType:   req.GetEventType(),
		ChannelType: req.GetChannelType(),
		Subject:     req.GetSubject(),
		Body:        req.GetBody(),
		Enabled:     enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.CreateTemplateResponse{Template: toProtoTemplate(tpl)}, nil
}

// UpdateTemplate implements proto NotifyTemplateService/UpdateTemplate。
func (s *NotifyTemplateService) UpdateTemplate(ctx context.Context, req *notifyv1.UpdateTemplateRequest) (*notifyv1.UpdateTemplateResponse, error) {
	upd := biz.TemplateUpdate{
		EventType:   req.EventType,
		ChannelType: req.ChannelType,
		Subject:     req.Subject,
		Body:        req.Body,
		Enabled:     req.Enabled,
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	return &notifyv1.UpdateTemplateResponse{}, nil
}

// DeleteTemplate implements proto NotifyTemplateService/DeleteTemplate。
func (s *NotifyTemplateService) DeleteTemplate(ctx context.Context, req *notifyv1.DeleteTemplateRequest) (*notifyv1.DeleteTemplateResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteTemplateResponse{}, nil
}

// GetTemplate implements proto NotifyTemplateService/GetTemplate。
func (s *NotifyTemplateService) GetTemplate(ctx context.Context, req *notifyv1.GetTemplateRequest) (*notifyv1.GetTemplateResponse, error) {
	tpl, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, tpl.BizID); err != nil {
		return nil, err
	}
	return &notifyv1.GetTemplateResponse{Template: toProtoTemplate(tpl)}, nil
}

// ListTemplates implements proto NotifyTemplateService/ListTemplates。
func (s *NotifyTemplateService) ListTemplates(ctx context.Context, req *notifyv1.ListTemplatesRequest) (*notifyv1.ListTemplatesResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.TemplateFilter{
		BizID:       bizID,
		Keyword:     req.GetKeyword(),
		EventType:   req.EventType,
		ChannelType: req.ChannelType,
		Enabled:     req.Enabled,
	}
	templates, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListTemplatesResponse{
		Items: make([]*notifyv1.NotifyTemplate, 0, len(templates)),
		Total: total,
	}
	for _, tpl := range templates {
		resp.Items = append(resp.Items, toProtoTemplate(tpl))
	}
	return resp, nil
}

// bizOf 返回「读模板取其 biz」的惰性回调（受限用户才执行）。
func (s *NotifyTemplateService) bizOf(ctx context.Context, id string) func() (string, error) {
	return func() (string, error) {
		row, err := s.uc.Get(ctx, id)
		if err != nil {
			return "", err
		}
		return row.BizID, nil
	}
}
