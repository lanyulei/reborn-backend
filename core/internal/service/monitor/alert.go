package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// AlertService 实现 MonitorAlertServiceHTTPServer。
type AlertService struct {
	uc *biz.AlertUsecase
	// 第二、三期的接口各自成一个内嵌类型（方法提升到 AlertService 上），定义在各自的文件里。
	alertActions
	shiftCounts
}

// NewAlertService new an AlertService.
func NewAlertService(uc *biz.AlertUsecase) *AlertService { return &AlertService{uc: uc} }

// ListAlerts implements proto MonitorAlertService/ListAlerts.
func (s *AlertService) ListAlerts(ctx context.Context, req *v1.ListAlertsRequest) (*v1.ListAlertsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	q := biz.AlertListQuery{
		GroupID: req.GetGroupId(), Mine: req.GetMine(),
		AlertFilter: biz.AlertFilter{
			Statuses: req.GetStatuses(), Severities: req.GetSeverities(), Muted: req.Muted, Keyword: req.GetKeyword(),
			Labels: labelFilter(req.GetLabels()), FireStart: optTime(req.GetFireTimeStart()), FireEnd: optTime(req.GetFireTimeEnd()),
			Fingerprint: req.GetFingerprint(), CIID: req.GetCiId(), Acked: req.Acked, Inhibited: req.Inhibited,
			AckUserID: req.GetAckUserId(), HandleState: req.GetHandleState(),
		},
	}
	if req.RuleId != nil {
		q.RuleIDs = []string{req.GetRuleId()}
	}
	if req.DatasourceId != nil {
		q.DatasourceIDs = []string{req.GetDatasourceId()}
	}
	alerts, total, err := s.uc.List(ctx, userID, q, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	return &v1.ListAlertsResponse{Items: toProtoAlerts(alerts), Total: total}, nil
}

// GetAlert implements proto MonitorAlertService/GetAlert.
func (s *AlertService) GetAlert(ctx context.Context, req *v1.GetAlertRequest) (*v1.GetAlertResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, extras, err := s.uc.GetDetail(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetAlertResponse{Alert: toProtoAlert(d.Alert), CanViewRule: d.CanViewRule, NotifyLogs: make([]*v1.AlertNotifyLog, 0, len(d.Logs))}
	for _, l := range d.Logs {
		resp.NotifyLogs = append(resp.NotifyLogs, toProtoNotifyLog(l))
	}
	fillAlertExtras(resp, extras)
	return resp, nil
}

// AggregateAlerts implements proto MonitorAlertService/AggregateAlerts.
func (s *AlertService) AggregateAlerts(ctx context.Context, req *v1.AggregateAlertsRequest) (*v1.AggregateAlertsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.Aggregate(ctx, userID, biz.AggregateQuery{
		By: req.GetBy(), GroupID: req.GetGroupId(), DatasourceID: req.GetDatasourceId(), Severities: req.GetSeverities(), Muted: req.Muted,
		Acked: req.Acked, Inhibited: req.Inhibited, Labels: labelFilter(req.GetLabels()),
	})
	if err != nil {
		return nil, err
	}
	resp := &v1.AggregateAlertsResponse{Items: make([]*v1.AlertAggregate, 0, len(items)), Total: int32(min(total, 1<<30))} // #nosec G115 -- 已夹紧
	for _, it := range items {
		resp.Items = append(resp.Items, &v1.AlertAggregate{
			Key: it.Key, Name: it.Name, Count: it.Count, MaxSeverity: it.MaxSeverity, SeverityCounts: it.SeverityCounts[:],
			LastFireTime: timestamppb.New(it.LastFireTime),
		})
	}
	return resp, nil
}

// MuteService 实现 MonitorMuteServiceHTTPServer。
type MuteService struct {
	uc *biz.MuteUsecase
}

// NewMuteService new a MuteService.
func NewMuteService(uc *biz.MuteUsecase) *MuteService { return &MuteService{uc: uc} }

func toMuteSpec(in *v1.MuteSpec) biz.MuteSpec {
	spec := biz.MuteSpec{
		GroupID: in.GetGroupId(), Name: in.GetName(), Reason: in.GetReason(), RuleIDs: in.GetRuleIds(), DatasourceIDs: in.GetDatasourceIds(),
		Severities: in.GetSeverities(), Matchers: toMatchers(in.GetMatchers()), Mode: in.GetMode(), TimeType: in.GetTimeType(),
		StartTime: optTime(in.GetStartTime()), EndTime: optTime(in.GetEndTime()), Enabled: in.GetEnabled(),
	}
	if in.Periodic != nil {
		periodic := toEffective(in.GetPeriodic())
		spec.Periodic = &periodic
	}
	return spec
}

func toProtoMute(m *biz.MuteView) *v1.Mute {
	s := &m.Spec
	spec := &v1.MuteSpec{
		Name: s.Name, Reason: s.Reason, RuleIds: s.RuleIDs, DatasourceIds: s.DatasourceIDs, Severities: s.Severities,
		Matchers: toProtoMatchers(s.Matchers), Mode: s.Mode, TimeType: s.TimeType,
		StartTime: protoTime(s.StartTime), EndTime: protoTime(s.EndTime), Enabled: s.Enabled,
	}
	if s.GroupID != "" {
		spec.GroupId = &s.GroupID
	}
	if s.Periodic != nil {
		spec.Periodic = toProtoEffective(*s.Periodic)
	}
	return &v1.Mute{
		Id: m.ID, Spec: spec, GroupName: m.GroupName, Rules: toProtoRefs(m.Rules), Datasources: toProtoRefs(m.Datasources),
		State: m.State, MutedCount: m.MutedCount, Version: m.Version, CanManage: m.CanManage,
		CreateTime: timestamppb.New(m.CreateTime), UpdateTime: timestamppb.New(m.UpdateTime),
		CreateUserId: m.CreateUser, CreateUserName: m.CreateUserName, UpdateUserId: m.UpdateUser, UpdateUserName: m.UpdateUserName,
	}
}

// ListMutes implements proto MonitorMuteService/ListMutes.
func (s *MuteService) ListMutes(ctx context.Context, req *v1.ListMutesRequest) (*v1.ListMutesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	mutes, total, err := s.uc.List(ctx, userID, biz.MuteListQuery{
		GroupID: req.GetGroupId(), Keyword: req.GetKeyword(), Enabled: req.Enabled, Mode: req.Mode, State: req.GetState(),
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListMutesResponse{Items: make([]*v1.Mute, 0, len(mutes)), Total: total}
	for _, m := range mutes {
		resp.Items = append(resp.Items, toProtoMute(m))
	}
	return resp, nil
}

// GetMute implements proto MonitorMuteService/GetMute.
func (s *MuteService) GetMute(ctx context.Context, req *v1.GetMuteRequest) (*v1.GetMuteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	m, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetMuteResponse{Mute: toProtoMute(m)}, nil
}

// CreateMute implements proto MonitorMuteService/CreateMute.
func (s *MuteService) CreateMute(ctx context.Context, req *v1.CreateMuteRequest) (*v1.CreateMuteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	m, err := s.uc.Create(ctx, userID, toMuteSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateMuteResponse{Mute: toProtoMute(m)}, nil
}

// UpdateMute implements proto MonitorMuteService/UpdateMute.
func (s *MuteService) UpdateMute(ctx context.Context, req *v1.UpdateMuteRequest) (*v1.UpdateMuteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	m, err := s.uc.Update(ctx, userID, req.GetId(), toMuteSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateMuteResponse{Mute: toProtoMute(m)}, nil
}

// DeleteMute implements proto MonitorMuteService/DeleteMute.
func (s *MuteService) DeleteMute(ctx context.Context, req *v1.DeleteMuteRequest) (*v1.DeleteMuteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteMuteResponse{}, nil
}

// PreviewMute implements proto MonitorMuteService/PreviewMute.
func (s *MuteService) PreviewMute(ctx context.Context, req *v1.PreviewMuteRequest) (*v1.PreviewMuteResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	alerts, total, err := s.uc.Preview(ctx, userID, toMuteSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.PreviewMuteResponse{Alerts: toProtoAlerts(alerts), Total: int32(total)}, nil // #nosec G115 -- 预览扫描有上限
}

// SubscriptionService 实现 MonitorSubscriptionServiceHTTPServer。
type SubscriptionService struct {
	uc *biz.SubscriptionUsecase
}

// NewSubscriptionService new a SubscriptionService.
func NewSubscriptionService(uc *biz.SubscriptionUsecase) *SubscriptionService {
	return &SubscriptionService{uc: uc}
}

func toSubscriptionSpec(in *v1.AlertSubscriptionSpec) biz.SubscriptionSpec {
	r := in.GetReceivers()
	return biz.SubscriptionSpec{
		Name: in.GetName(), GroupIDs: in.GetGroupIds(), RuleIDs: in.GetRuleIds(), DatasourceIDs: in.GetDatasourceIds(),
		Severities: in.GetSeverities(), OnFiring: in.GetOnFiring(), OnResolved: in.GetOnResolved(), Matchers: toMatchers(in.GetMatchers()),
		Receivers:    mc.Receivers{Self: r.GetSelf(), UserIDs: r.GetUserIds(), ScheduleIDs: r.GetScheduleIds(), Groups: r.GetGroups()},
		FollowRepeat: in.GetFollowRepeat(), Enabled: in.GetEnabled(),
	}
}

func toProtoSubscription(v *biz.SubscriptionView) *v1.AlertSubscription {
	s := &v.Spec
	return &v1.AlertSubscription{
		Id: v.ID,
		Spec: &v1.AlertSubscriptionSpec{
			Name: s.Name, GroupIds: s.GroupIDs, RuleIds: s.RuleIDs, DatasourceIds: s.DatasourceIDs, Severities: s.Severities,
			OnFiring: s.OnFiring, OnResolved: s.OnResolved, Matchers: toProtoMatchers(s.Matchers),
			Receivers: &v1.SubscriptionReceivers{
				Self: s.Receivers.Self, UserIds: s.Receivers.UserIDs, ScheduleIds: s.Receivers.ScheduleIDs, Groups: s.Receivers.Groups,
			},
			FollowRepeat: s.FollowRepeat, Enabled: s.Enabled,
		},
		OwnerId: v.OwnerID, OwnerName: v.OwnerName, Groups: toProtoRefs(v.Groups), Rules: toProtoRefs(v.Rules),
		Datasources: toProtoRefs(v.Datasources), ReceiverUsers: toProtoRefs(v.ReceiverUsers), ReceiverSchedules: toProtoRefs(v.ReceiverSchedules),
		OutOfScopeGroupIds: v.OutOfScopeGroups, LastHitTime: protoTime(v.LastHitTime), HitCount_7D: v.HitCount7d,
		Version: v.Version, CanManage: v.CanManage, CreateTime: timestamppb.New(v.CreateTime), UpdateTime: timestamppb.New(v.UpdateTime),
	}
}

// ListAlertSubscriptions implements proto MonitorSubscriptionService/ListAlertSubscriptions.
func (s *SubscriptionService) ListAlertSubscriptions(ctx context.Context, req *v1.ListAlertSubscriptionsRequest) (*v1.ListAlertSubscriptionsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	subs, total, err := s.uc.List(ctx, userID, biz.SubscriptionListQuery{
		Scope: req.GetScope(), Keyword: req.GetKeyword(), Enabled: req.Enabled, GroupID: req.GetGroupId(),
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListAlertSubscriptionsResponse{Items: make([]*v1.AlertSubscription, 0, len(subs)), Total: total}
	for _, sub := range subs {
		resp.Items = append(resp.Items, toProtoSubscription(sub))
	}
	return resp, nil
}

// GetAlertSubscription implements proto MonitorSubscriptionService/GetAlertSubscription.
func (s *SubscriptionService) GetAlertSubscription(ctx context.Context, req *v1.GetAlertSubscriptionRequest) (*v1.GetAlertSubscriptionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetAlertSubscriptionResponse{Subscription: toProtoSubscription(sub)}, nil
}

// CreateAlertSubscription implements proto MonitorSubscriptionService/CreateAlertSubscription.
func (s *SubscriptionService) CreateAlertSubscription(ctx context.Context, req *v1.CreateAlertSubscriptionRequest) (*v1.CreateAlertSubscriptionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := s.uc.Create(ctx, userID, toSubscriptionSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateAlertSubscriptionResponse{Subscription: toProtoSubscription(sub)}, nil
}

// UpdateAlertSubscription implements proto MonitorSubscriptionService/UpdateAlertSubscription.
func (s *SubscriptionService) UpdateAlertSubscription(ctx context.Context, req *v1.UpdateAlertSubscriptionRequest) (*v1.UpdateAlertSubscriptionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sub, err := s.uc.Update(ctx, userID, req.GetId(), toSubscriptionSpec(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateAlertSubscriptionResponse{Subscription: toProtoSubscription(sub)}, nil
}

// DeleteAlertSubscription implements proto MonitorSubscriptionService/DeleteAlertSubscription.
func (s *SubscriptionService) DeleteAlertSubscription(ctx context.Context, req *v1.DeleteAlertSubscriptionRequest) (*v1.DeleteAlertSubscriptionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteAlertSubscriptionResponse{}, nil
}

// PreviewAlertSubscription implements proto MonitorSubscriptionService/PreviewAlertSubscription.
func (s *SubscriptionService) PreviewAlertSubscription(ctx context.Context, req *v1.PreviewAlertSubscriptionRequest) (*v1.PreviewAlertSubscriptionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	alerts, total, err := s.uc.Preview(ctx, userID, toSubscriptionSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.PreviewAlertSubscriptionResponse{Alerts: toProtoAlerts(alerts), Total: int32(total)}, nil // #nosec G115 -- 预览扫描有上限
}
