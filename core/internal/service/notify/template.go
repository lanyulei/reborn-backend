package service

import (
	"context"
	"time"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifyTemplateService 实现 api/notify/v1 生成的 NotifyTemplateServiceServer。
type NotifyTemplateService struct {
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
		Format:      tpl.Format,
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
		Format:      req.GetFormat(),
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
		Format:      req.Format,
		Enabled:     req.Enabled,
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	tpl, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &notifyv1.UpdateTemplateResponse{Template: toProtoTemplate(tpl)}, nil
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

// RenderTemplate implements proto NotifyTemplateService/RenderTemplate：样例缺省
// 与写入校验的试渲染上下文一致；count > 1 时注入一条 Samples 样例。
func (s *NotifyTemplateService) RenderTemplate(_ context.Context, req *notifyv1.RenderTemplateRequest) (*notifyv1.RenderTemplateResponse, error) {
	sample := biz.TrialRenderContext{
		Title: "sample", Payload: map[string]any{"host": "sample-host"}, Severity: 2, Count: 1,
		BizID: biz.DefaultBizID, EventTime: time.Now().UTC(),
	}
	if sp := req.GetSample(); sp != nil {
		if sp.Title != nil {
			sample.Title = sp.GetTitle()
		}
		if sp.GetPayload() != nil {
			sample.Payload = sp.GetPayload().AsMap()
		}
		if sp.Severity != nil {
			sample.Severity = sp.GetSeverity()
		}
		if sp.Count != nil {
			sample.Count = int(sp.GetCount())
		}
		if sp.BizId != nil {
			sample.BizID = sp.GetBizId()
		}
	}
	if sample.Count > 1 {
		sample.Samples = []map[string]any{{"event_id": "sample", "title": sample.Title, "severity": sample.Severity, "create_time": sample.EventTime}}
	}
	subject, body, err := s.uc.Render(req.GetSubject(), req.GetBody(), req.GetFormat(), sample)
	if err != nil {
		return nil, err
	}
	return &notifyv1.RenderTemplateResponse{Subject: subject, Body: body}, nil
}
