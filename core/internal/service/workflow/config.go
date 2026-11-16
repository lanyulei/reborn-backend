package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// ConfigService 实现 api/workflow/v1 生成的 WorkflowConfigServiceServer。
type ConfigService struct {
	calendars   *biz.CalendarUsecase
	delegations *biz.DelegationUsecase
	webhooks    *biz.WebhookUsecase
	clients     *biz.ClientUsecase
}

// NewConfigService new a ConfigService.
func NewConfigService(calendars *biz.CalendarUsecase, delegations *biz.DelegationUsecase, webhooks *biz.WebhookUsecase,
	clients *biz.ClientUsecase,
) *ConfigService {
	return &ConfigService{calendars: calendars, delegations: delegations, webhooks: webhooks, clients: clients}
}

func toProtoClient(c *biz.Client, names map[string]string) *v1.Client {
	return &v1.Client{
		Id: c.ID, Name: c.Name, TokenPrefix: c.TokenPrefix, ActingUserId: c.ActingUserID, ActingUserName: names[c.ActingUserID],
		DefinitionKeys: c.DefinitionKeys, Status: c.Status, Description: c.Description,
		LastUsedTime: ts(c.LastUsedTime), UpdateTime: ts(c.UpdateTime),
	}
}

func specToClient(id string, spec *v1.ClientSpec) *biz.Client {
	return &biz.Client{
		ID: id, Name: spec.GetName(), ActingUserID: spec.GetActingUserId(), DefinitionKeys: spec.GetDefinitionKeys(),
		Status: spec.GetStatus(), Description: spec.GetDescription(),
	}
}

// ListClients implements proto WorkflowConfigService/ListClients.
func (s *ConfigService) ListClients(ctx context.Context, _ *v1.ListClientsRequest) (*v1.ListClientsResponse, error) {
	items, names, err := s.clients.List(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListClientsResponse{Items: make([]*v1.Client, 0, len(items))}
	for _, c := range items {
		resp.Items = append(resp.Items, toProtoClient(c, names))
	}
	return resp, nil
}

// CreateClient implements proto WorkflowConfigService/CreateClient：令牌明文只在响应里出现这一次。
func (s *ConfigService) CreateClient(ctx context.Context, req *v1.CreateClientRequest) (*v1.CreateClientResponse, error) {
	c, token, err := s.clients.Create(ctx, specToClient("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateClientResponse{Client: toProtoClient(c, nil), Token: token}, nil
}

// UpdateClient implements proto WorkflowConfigService/UpdateClient.
func (s *ConfigService) UpdateClient(ctx context.Context, req *v1.UpdateClientRequest) (*v1.UpdateClientResponse, error) {
	if err := s.clients.Update(ctx, specToClient(req.GetId(), req.GetSpec())); err != nil {
		return nil, err
	}
	return &v1.UpdateClientResponse{}, nil
}

// RotateClientToken implements proto WorkflowConfigService/RotateClientToken.
func (s *ConfigService) RotateClientToken(ctx context.Context, req *v1.RotateClientTokenRequest) (*v1.RotateClientTokenResponse, error) {
	token, err := s.clients.Rotate(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RotateClientTokenResponse{Token: token}, nil
}

// DeleteClient implements proto WorkflowConfigService/DeleteClient.
func (s *ConfigService) DeleteClient(ctx context.Context, req *v1.DeleteClientRequest) (*v1.DeleteClientResponse, error) {
	if err := s.clients.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteClientResponse{}, nil
}

// GetCalendar implements proto WorkflowConfigService/GetCalendar.
func (s *ConfigService) GetCalendar(ctx context.Context, _ *v1.GetCalendarRequest) (*v1.GetCalendarResponse, error) {
	c, err := s.calendars.Get(ctx)
	if err != nil {
		return nil, err
	}
	out := &v1.Calendar{Timezone: c.Timezone, Weekdays: c.Weekdays, Holidays: c.Holidays, Workdays: c.Workdays}
	for _, seg := range c.Segments {
		out.Segments = append(out.Segments, &v1.CalendarSegment{Start: seg.Start, End: seg.End})
	}
	return &v1.GetCalendarResponse{Calendar: out}, nil
}

// UpdateCalendar implements proto WorkflowConfigService/UpdateCalendar.
func (s *ConfigService) UpdateCalendar(ctx context.Context, req *v1.UpdateCalendarRequest) (*v1.UpdateCalendarResponse, error) {
	in := req.GetCalendar()
	c := biz.CalendarConfig{
		Timezone: in.GetTimezone(), Weekdays: in.GetWeekdays(), Holidays: nonNil(in.GetHolidays()), Workdays: nonNil(in.GetWorkdays()),
	}
	for _, seg := range in.GetSegments() {
		c.Segments = append(c.Segments, biz.CalendarSegment{Start: seg.GetStart(), End: seg.GetEnd()})
	}
	if err := s.calendars.Save(ctx, c); err != nil {
		return nil, err
	}
	return &v1.UpdateCalendarResponse{}, nil
}

// nonNil 空列表落 [] 而不是 null。
func nonNil(items []string) []string {
	if items == nil {
		return []string{}
	}
	return items
}

func toProtoDelegation(d *biz.Delegation, names map[string]string) *v1.Delegation {
	return &v1.Delegation{
		Id: d.ID, DelegateId: d.DelegateID, DelegateName: names[d.DelegateID],
		StartTime: ts(d.StartTime), EndTime: ts(d.EndTime), Reason: d.Reason,
	}
}

// ListMyDelegations implements proto WorkflowConfigService/ListMyDelegations：恒按登录身份过滤。
func (s *ConfigService) ListMyDelegations(ctx context.Context, _ *v1.ListMyDelegationsRequest) (*v1.ListMyDelegationsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, names, err := s.delegations.List(ctx, uid)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListMyDelegationsResponse{Items: make([]*v1.Delegation, 0, len(items))}
	for _, d := range items {
		resp.Items = append(resp.Items, toProtoDelegation(d, names))
	}
	return resp, nil
}

// CreateDelegation implements proto WorkflowConfigService/CreateDelegation：只能给自己设置。
func (s *ConfigService) CreateDelegation(ctx context.Context, req *v1.CreateDelegationRequest) (*v1.CreateDelegationResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.delegations.Create(ctx, &biz.Delegation{
		UserID: uid, DelegateID: req.GetDelegateId(), StartTime: req.GetStartTime().AsTime(), EndTime: req.GetEndTime().AsTime(),
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateDelegationResponse{Delegation: toProtoDelegation(d, nil)}, nil
}

// DeleteDelegation implements proto WorkflowConfigService/DeleteDelegation：只能删自己的。
func (s *ConfigService) DeleteDelegation(ctx context.Context, req *v1.DeleteDelegationRequest) (*v1.DeleteDelegationResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.delegations.Delete(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteDelegationResponse{}, nil
}

func toProtoWebhook(w *biz.Webhook) *v1.Webhook {
	return &v1.Webhook{
		Id: w.ID, Name: w.Name, Url: w.URL, Description: w.Description, Status: w.Status, HasSecret: w.HasSecret,
		UpdateTime: ts(w.UpdateTime),
	}
}

// ListWebhooks implements proto WorkflowConfigService/ListWebhooks.
func (s *ConfigService) ListWebhooks(ctx context.Context, _ *v1.ListWebhooksRequest) (*v1.ListWebhooksResponse, error) {
	items, err := s.webhooks.List(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListWebhooksResponse{Items: make([]*v1.Webhook, 0, len(items))}
	for _, w := range items {
		resp.Items = append(resp.Items, toProtoWebhook(w))
	}
	return resp, nil
}

// CreateWebhook implements proto WorkflowConfigService/CreateWebhook.
func (s *ConfigService) CreateWebhook(ctx context.Context, req *v1.CreateWebhookRequest) (*v1.CreateWebhookResponse, error) {
	spec := req.GetSpec()
	w, err := s.webhooks.Create(ctx, &biz.Webhook{
		Name: spec.GetName(), URL: spec.GetUrl(), Description: spec.GetDescription(), Status: spec.GetStatus(), Secret: spec.GetSecret(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateWebhookResponse{Webhook: toProtoWebhook(w)}, nil
}

// UpdateWebhook implements proto WorkflowConfigService/UpdateWebhook.
func (s *ConfigService) UpdateWebhook(ctx context.Context, req *v1.UpdateWebhookRequest) (*v1.UpdateWebhookResponse, error) {
	spec := req.GetSpec()
	err := s.webhooks.Update(ctx, &biz.Webhook{
		ID: req.GetId(), Name: spec.GetName(), URL: spec.GetUrl(), Description: spec.GetDescription(), Status: spec.GetStatus(),
	}, spec.Secret)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateWebhookResponse{}, nil
}

// DeleteWebhook implements proto WorkflowConfigService/DeleteWebhook.
func (s *ConfigService) DeleteWebhook(ctx context.Context, req *v1.DeleteWebhookRequest) (*v1.DeleteWebhookResponse, error) {
	if err := s.webhooks.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteWebhookResponse{}, nil
}
