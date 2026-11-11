package service

import (
	"context"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NotifySilenceService 实现 api/notify/v1 生成的 NotifySilenceServiceServer；
// proto 与领域模型显式转换，调优项默认值在此解析（proto 三态 optional：
// severity_max=2、time_window 缺省全程、enabled=true）。
type NotifySilenceService struct {
	uc    *biz.SilenceUsecase
	guard bizGuard
}

// NewNotifySilenceService new a NotifySilenceService.
func NewNotifySilenceService(uc *biz.SilenceUsecase, scope biz.BizScopeResolver) *NotifySilenceService {
	return &NotifySilenceService{uc: uc, guard: newBizGuard(scope)}
}

// silenceSeverityDefault 严重级上限默认值（warning 及以下，与 proto 注释一致）。
const silenceSeverityDefault = 2

// silenceRuleFromProto 转换 proto 静默规则载荷并解析可选调优项默认值。
func silenceRuleFromProto(spec *notifyv1.SilenceSpec) *biz.SilenceRule {
	rule := &biz.SilenceRule{
		EventPattern: spec.GetEventPattern(),
		SeverityMax:  spec.GetSeverityMax(),
	}
	if spec.SeverityMax == nil {
		rule.SeverityMax = silenceSeverityDefault
	}
	if w := spec.GetTimeWindow(); w != nil {
		window := &biz.SilenceWindow{Start: w.GetStart(), End: w.GetEnd()}
		if w.Tz != nil {
			window.Tz = w.GetTz()
		}
		rule.Window = window
	}
	return rule
}

// toProtoWindow 时间窗 DO 转 proto（nil 返回 nil → protojson 省略字段）。
func toProtoWindow(w *biz.SilenceWindow) *notifyv1.SilenceWindow {
	if w == nil {
		return nil
	}
	window := &notifyv1.SilenceWindow{Start: w.Start, End: w.End}
	if w.Tz != "" {
		window.Tz = &w.Tz
	}
	return window
}

// toProtoSilence 领域模型转 proto 实体。
func toProtoSilence(silence *biz.Silence) *notifyv1.NotifySilence {
	return &notifyv1.NotifySilence{
		Id:           silence.ID,
		BizId:        silence.BizID,
		EventPattern: silence.EventPattern,
		SeverityMax:  silence.SeverityMax,
		TimeWindow:   toProtoWindow(silence.Window),
		Enabled:      silence.Enabled,
		CreateTime:   timestamppb.New(silence.CreateTime),
		UpdateTime:   timestamppb.New(silence.UpdateTime),
	}
}

// CreateSilence implements proto NotifySilenceService/CreateSilence.
func (s *NotifySilenceService) CreateSilence(ctx context.Context, req *notifyv1.CreateSilenceRequest) (*notifyv1.CreateSilenceResponse, error) {
	enabled := true // 与渠道/模板/路由一致：未指定默认启用
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if err := s.guard.requireBiz(ctx, req.GetBizId()); err != nil {
		return nil, err
	}
	silence, err := s.uc.Create(ctx, &biz.Silence{
		BizID:       req.GetBizId(),
		SilenceRule: *silenceRuleFromProto(req.GetSpec()),
		Enabled:     enabled,
	})
	if err != nil {
		return nil, err
	}
	return &notifyv1.CreateSilenceResponse{Silence: toProtoSilence(silence)}, nil
}

// UpdateSilence implements proto NotifySilenceService/UpdateSilence：spec 整组
// 替换或 enabled 独立启停。
func (s *NotifySilenceService) UpdateSilence(ctx context.Context, req *notifyv1.UpdateSilenceRequest) (*notifyv1.UpdateSilenceResponse, error) {
	upd := biz.SilenceUpdate{Enabled: req.Enabled}
	if req.Spec != nil {
		upd.Rule = silenceRuleFromProto(req.GetSpec())
	}
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	return &notifyv1.UpdateSilenceResponse{}, nil
}

// DeleteSilence implements proto NotifySilenceService/DeleteSilence。
func (s *NotifySilenceService) DeleteSilence(ctx context.Context, req *notifyv1.DeleteSilenceRequest) (*notifyv1.DeleteSilenceResponse, error) {
	if err := s.guard.requireRow(ctx, s.bizOf(ctx, req.GetId())); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &notifyv1.DeleteSilenceResponse{}, nil
}

// GetSilence implements proto NotifySilenceService/GetSilence。
func (s *NotifySilenceService) GetSilence(ctx context.Context, req *notifyv1.GetSilenceRequest) (*notifyv1.GetSilenceResponse, error) {
	silence, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.requireBiz(ctx, silence.BizID); err != nil {
		return nil, err
	}
	return &notifyv1.GetSilenceResponse{Silence: toProtoSilence(silence)}, nil
}

// ListSilences implements proto NotifySilenceService/ListSilences。
func (s *NotifySilenceService) ListSilences(ctx context.Context, req *notifyv1.ListSilencesRequest) (*notifyv1.ListSilencesResponse, error) {
	bizID, err := s.guard.listBiz(ctx, req.BizId)
	if err != nil {
		return nil, err
	}
	filter := biz.SilenceFilter{
		BizID:   bizID,
		Keyword: req.GetKeyword(),
		Enabled: req.Enabled,
	}
	silences, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &notifyv1.ListSilencesResponse{
		Items: make([]*notifyv1.NotifySilence, 0, len(silences)),
		Total: total,
	}
	for _, silence := range silences {
		resp.Items = append(resp.Items, toProtoSilence(silence))
	}
	return resp, nil
}

// bizOf 返回「读静默取其 biz」的惰性回调（受限用户才执行）。
func (s *NotifySilenceService) bizOf(ctx context.Context, id string) func() (string, error) {
	return func() (string, error) {
		row, err := s.uc.Get(ctx, id)
		if err != nil {
			return "", err
		}
		return row.BizID, nil
	}
}
