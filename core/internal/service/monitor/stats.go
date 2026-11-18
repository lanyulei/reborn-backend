package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// StatsService 实现 MonitorStatsServiceHTTPServer。
type StatsService struct {
	uc *biz.StatsUsecase
}

// NewStatsService new a StatsService.
func NewStatsService(uc *biz.StatsUsecase) *StatsService { return &StatsService{uc: uc} }

// clampInt32 把计数夹到 int32 范围内。
func clampInt32(n int64) int32 {
	return int32(min(max(n, 0), 1<<30)) // #nosec G115 -- 已夹紧
}

// GetAlertOverview implements proto MonitorStatsService/GetAlertOverview.
func (s *StatsService) GetAlertOverview(ctx context.Context, req *v1.GetAlertOverviewRequest) (*v1.GetAlertOverviewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	o, err := s.uc.Overview(ctx, userID, req.GetGroupId(), req.GetTimezone())
	if err != nil {
		return nil, err
	}
	return &v1.GetAlertOverviewResponse{
		Firing: o.Firing, FiringBySeverity: o.FiringBySeverity[:], Unacked: o.Unacked, Muted: o.Muted, Inhibited: o.Inhibited,
		TodayFired: o.TodayFired, TodayResolved: o.TodayResolved, TodayClosed: o.TodayClosed,
	}, nil
}

// GetAlertTrend implements proto MonitorStatsService/GetAlertTrend.
func (s *StatsService) GetAlertTrend(ctx context.Context, req *v1.GetAlertTrendRequest) (*v1.GetAlertTrendResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Trend(ctx, userID, biz.StatsQuery{
		GroupID: req.GetGroupId(), Start: req.GetStartTime().AsTime(), End: req.GetEndTime().AsTime(), By: req.GetBy(),
	}, req.GetTimezone())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetAlertTrendResponse{Days: t.Days, Totals: t.Totals, Series: make([]*v1.AlertTrendSeries, 0, len(t.Series))}
	for _, sr := range t.Series {
		resp.Series = append(resp.Series, &v1.AlertTrendSeries{Key: sr.Key, Name: sr.Name, Counts: sr.Counts, Total: sr.Total})
	}
	return resp, nil
}

// GetAlertTop implements proto MonitorStatsService/GetAlertTop.
func (s *StatsService) GetAlertTop(ctx context.Context, req *v1.GetAlertTopRequest) (*v1.GetAlertTopResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.Top(ctx, userID, biz.TopQuery{
		StatsQuery: biz.StatsQuery{GroupID: req.GetGroupId(), Start: req.GetStartTime().AsTime(), End: req.GetEndTime().AsTime(), By: req.GetBy()},
		Limit:      int(req.GetLimit()),
	})
	if err != nil {
		return nil, err
	}
	resp := &v1.GetAlertTopResponse{Items: make([]*v1.AlertTopItem, 0, len(items)), Total: clampInt32(total)}
	for _, it := range items {
		resp.Items = append(resp.Items, &v1.AlertTopItem{
			Key: it.Key, Name: it.Name, Count: it.Count, Firing: it.Firing, MaxSeverity: it.MaxSeverity, GroupName: it.GroupName, Exists: it.Exists,
		})
	}
	return resp, nil
}

func toProtoEfficiency(it *biz.EfficiencyItem) *v1.AlertEfficiencyItem {
	return &v1.AlertEfficiencyItem{
		Key: it.Key, Name: it.Name, Alerts: it.Alerts, Acked: it.Acked, Resolved: it.Resolved, MttaS: it.MTTAS, MttrS: it.MTTRS,
	}
}

// GetAlertEfficiency implements proto MonitorStatsService/GetAlertEfficiency.
func (s *StatsService) GetAlertEfficiency(ctx context.Context, req *v1.GetAlertEfficiencyRequest) (*v1.GetAlertEfficiencyResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.Efficiency(ctx, userID, biz.StatsQuery{
		GroupID: req.GetGroupId(), Start: req.GetStartTime().AsTime(), End: req.GetEndTime().AsTime(), By: req.GetBy(),
	})
	if err != nil {
		return nil, err
	}
	resp := &v1.GetAlertEfficiencyResponse{Items: make([]*v1.AlertEfficiencyItem, 0, len(items)), Total: toProtoEfficiency(total)}
	for _, it := range items {
		resp.Items = append(resp.Items, toProtoEfficiency(it))
	}
	return resp, nil
}

// GetAlertNoise implements proto MonitorStatsService/GetAlertNoise.
func (s *StatsService) GetAlertNoise(ctx context.Context, req *v1.GetAlertNoiseRequest) (*v1.GetAlertNoiseResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	n, err := s.uc.Noise(ctx, userID, biz.StatsQuery{GroupID: req.GetGroupId(), Start: req.GetStartTime().AsTime(), End: req.GetEndTime().AsTime()})
	if err != nil {
		return nil, err
	}
	return &v1.GetAlertNoiseResponse{
		Alerts: n.Alerts, Muted: n.Muted, Inhibited: n.Inhibited, Notified: n.Notified, Silent: max(n.Alerts-n.Notified, 0),
		Notifications:     n.RuleNotifications + n.SubscriptionNotifications + n.EscalationNotifications,
		RuleNotifications: n.RuleNotifications, SubscriptionNotifications: n.SubscriptionNotifications,
		EscalationNotifications: n.EscalationNotifications,
	}, nil
}
