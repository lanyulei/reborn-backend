package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/oncall/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/oncall"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// OncallScheduleService 实现 OncallScheduleServiceServer。
type OncallScheduleService struct {
	uc     *biz.ScheduleUsecase
	groups *biz.GroupUsecase
	admin  AdminResolver
}

// NewOncallScheduleService new an OncallScheduleService.
func NewOncallScheduleService(uc *biz.ScheduleUsecase, groups *biz.GroupUsecase, admin AdminResolver) *OncallScheduleService {
	return &OncallScheduleService{uc: uc, groups: groups, admin: admin}
}

func fromProtoConfig(c *v1.ScheduleConfig) biz.ScheduleConfig {
	return biz.ScheduleConfig{
		Name: c.GetName(), Description: c.GetDescription(), Timezone: c.GetTimezone(),
		RotationUnit: c.GetRotationUnit(), RotationInterval: c.GetRotationInterval(), AnchorTime: c.GetAnchorTime().AsTime(),
		MemberIDs: c.GetMemberIds(), StartUserID: c.GetStartUserId(), BackupEnabled: c.GetBackupEnabled(),
		HorizonDays: c.GetHorizonDays(), RemindMinutes: c.GetRemindMinutes(), DigestTime: c.GetDigestTime(),
	}
}

func toProtoSchedule(s *biz.Schedule, canManage bool) *v1.Schedule {
	cfg := &v1.ScheduleConfig{
		Name: s.Name, Description: s.Description, Timezone: s.Timezone, RotationUnit: s.RotationUnit,
		RotationInterval: s.RotationInterval, AnchorTime: timestamppb.New(s.AnchorTime), MemberIds: s.MemberIDs,
		BackupEnabled: s.BackupEnabled, HorizonDays: s.HorizonDays, RemindMinutes: s.RemindMinutes,
	}
	if int(s.AnchorOffset) < len(s.MemberIDs) {
		cfg.StartUserId = &s.MemberIDs[s.AnchorOffset]
	}
	if s.DigestTime != "" {
		cfg.DigestTime = &s.DigestTime
	}
	members := make([]*v1.ScheduleMember, 0, len(s.Members))
	for _, u := range s.Members {
		members = append(members, &v1.ScheduleMember{UserId: u.ID, Username: u.Username, Nickname: u.Nickname, Available: u.Available})
	}
	out := &v1.Schedule{
		Id: s.ID, GroupId: s.GroupID, GroupName: s.GroupName, Config: cfg, Members: members, Status: s.Status,
		CanManage: canManage, CreateTime: timestamppb.New(s.CreateTime), UpdateTime: timestamppb.New(s.UpdateTime),
	}
	if s.GeneratedUntil != nil {
		out.GeneratedUntil = timestamppb.New(*s.GeneratedUntil)
	}
	return out
}

func toProtoShift(sh *biz.Shift) *v1.Shift {
	if sh == nil {
		return nil
	}
	return &v1.Shift{
		Id: sh.ID, ScheduleId: sh.ScheduleID, ScheduleName: sh.ScheduleName, GroupId: sh.GroupID, Role: sh.Role,
		UserId: sh.UserID, UserName: sh.UserName, UserAvailable: sh.UserAvailable,
		StartTime: timestamppb.New(sh.Start), EndTime: timestamppb.New(sh.End), Source: sh.Source,
		OriginUserId: sh.OriginUserID, OriginUserName: sh.OriginUserName, Remark: sh.Remark,
	}
}

func toProtoShifts(shifts []*biz.Shift) []*v1.Shift {
	items := make([]*v1.Shift, 0, len(shifts))
	for _, sh := range shifts {
		items = append(items, toProtoShift(sh))
	}
	return items
}

func toProtoIssues(issues []*biz.Issue) []*v1.ShiftIssue {
	items := make([]*v1.ShiftIssue, 0, len(issues))
	for _, is := range issues {
		items = append(items, &v1.ShiftIssue{
			Type: is.Type, ScheduleId: is.ScheduleID, ScheduleName: is.ScheduleName, GroupId: is.GroupID,
			UserId: is.UserID, UserName: is.UserName, StartTime: timestamppb.New(is.Start), EndTime: timestamppb.New(is.End),
			OtherScheduleName: is.OtherScheduleName, OtherScheduleCount: is.OtherScheduleCount,
		})
	}
	return items
}

// ListSchedules implements proto OncallScheduleService/ListSchedules.
func (s *OncallScheduleService) ListSchedules(ctx context.Context, req *v1.ListSchedulesRequest) (*v1.ListSchedulesResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	schedules, err := s.uc.List(ctx, req.GetGroupId())
	if err != nil {
		return nil, err
	}
	manageable, err := s.groups.Manageable(ctx, actor)
	if err != nil {
		return nil, err
	}
	items := make([]*v1.Schedule, 0, len(schedules))
	for _, sc := range schedules {
		items = append(items, toProtoSchedule(sc, manageable(sc.GroupID)))
	}
	return &v1.ListSchedulesResponse{Items: items}, nil
}

// reply 把值班表转成 proto 并标注当前用户能否管理。
func (s *OncallScheduleService) reply(ctx context.Context, actor biz.Actor, sc *biz.Schedule) (*v1.Schedule, error) {
	manageable, err := s.groups.Manageable(ctx, actor)
	if err != nil {
		return nil, err
	}
	return toProtoSchedule(sc, manageable(sc.GroupID)), nil
}

// CreateSchedule implements proto OncallScheduleService/CreateSchedule.
func (s *OncallScheduleService) CreateSchedule(ctx context.Context, req *v1.CreateScheduleRequest) (*v1.CreateScheduleResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	sc, err := s.uc.Create(ctx, actor, req.GetGroupId(), fromProtoConfig(req.GetConfig()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateScheduleResponse{Schedule: toProtoSchedule(sc, true)}, nil
}

// GetSchedule implements proto OncallScheduleService/GetSchedule.
func (s *OncallScheduleService) GetSchedule(ctx context.Context, req *v1.GetScheduleRequest) (*v1.GetScheduleResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	sc, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	out, err := s.reply(ctx, actor, sc)
	if err != nil {
		return nil, err
	}
	return &v1.GetScheduleResponse{Schedule: out}, nil
}

// UpdateSchedule implements proto OncallScheduleService/UpdateSchedule.
func (s *OncallScheduleService) UpdateSchedule(ctx context.Context, req *v1.UpdateScheduleRequest) (*v1.UpdateScheduleResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	sc, err := s.uc.Update(ctx, actor, req.GetId(), fromProtoConfig(req.GetConfig()))
	if err != nil {
		return nil, err
	}
	return &v1.UpdateScheduleResponse{Schedule: toProtoSchedule(sc, true)}, nil
}

// UpdateScheduleStatus implements proto OncallScheduleService/UpdateScheduleStatus.
func (s *OncallScheduleService) UpdateScheduleStatus(ctx context.Context, req *v1.UpdateScheduleStatusRequest) (*v1.UpdateScheduleStatusResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.SetStatus(ctx, actor, req.GetId(), req.GetStatus()); err != nil {
		return nil, err
	}
	return &v1.UpdateScheduleStatusResponse{}, nil
}

// DeleteSchedule implements proto OncallScheduleService/DeleteSchedule.
func (s *OncallScheduleService) DeleteSchedule(ctx context.Context, req *v1.DeleteScheduleRequest) (*v1.DeleteScheduleResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, actor, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteScheduleResponse{}, nil
}

// PreviewSchedule implements proto OncallScheduleService/PreviewSchedule.
func (s *OncallScheduleService) PreviewSchedule(ctx context.Context, req *v1.PreviewScheduleRequest) (*v1.PreviewScheduleResponse, error) {
	shifts, issues, err := s.uc.Preview(ctx, req.GetGroupId(), req.GetScheduleId(), fromProtoConfig(req.GetConfig()), req.GetDays())
	if err != nil {
		return nil, err
	}
	return &v1.PreviewScheduleResponse{Items: toProtoShifts(shifts), Issues: toProtoIssues(issues)}, nil
}

// ListShifts implements proto OncallScheduleService/ListShifts.
func (s *OncallScheduleService) ListShifts(ctx context.Context, req *v1.ListShiftsRequest) (*v1.ListShiftsResponse, error) {
	q := biz.ShiftQuery{
		From: req.GetStartTime().AsTime(), To: req.GetEndTime().AsTime(),
		GroupID: req.GetGroupId(), ScheduleID: req.GetScheduleId(), UserID: req.GetUserId(),
	}
	if req.GetMine() {
		actor, err := actorFromContext(ctx, s.admin)
		if err != nil {
			return nil, err
		}
		q.UserID = actor.UserID
	}
	shifts, issues, err := s.uc.ListShifts(ctx, q)
	if err != nil {
		return nil, err
	}
	return &v1.ListShiftsResponse{Items: toProtoShifts(shifts), Issues: toProtoIssues(issues)}, nil
}

// CreateOverride implements proto OncallScheduleService/CreateOverride.
func (s *OncallScheduleService) CreateOverride(ctx context.Context, req *v1.CreateOverrideRequest) (*v1.CreateOverrideResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	shifts, err := s.uc.CreateOverride(ctx, actor, biz.OverrideInput{
		ScheduleID: req.GetScheduleId(), Role: req.GetRole(), UserID: req.GetUserId(),
		Start: req.GetStartTime().AsTime(), End: req.GetEndTime().AsTime(), Remark: req.GetRemark(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateOverrideResponse{Items: toProtoShifts(shifts)}, nil
}

// RevokeOverride implements proto OncallScheduleService/RevokeOverride.
func (s *OncallScheduleService) RevokeOverride(ctx context.Context, req *v1.RevokeOverrideRequest) (*v1.RevokeOverrideResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RevokeOverride(ctx, actor, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.RevokeOverrideResponse{}, nil
}

// GetCurrentDuty implements proto OncallScheduleService/GetCurrentDuty.
func (s *OncallScheduleService) GetCurrentDuty(ctx context.Context, req *v1.GetCurrentDutyRequest) (*v1.GetCurrentDutyResponse, error) {
	duties, err := s.uc.Current(ctx, req.GetGroupId(), req.GetScheduleId())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.CurrentDuty, 0, len(duties))
	for _, d := range duties {
		item := &v1.CurrentDuty{
			ScheduleId: d.Schedule.ID, ScheduleName: d.Schedule.Name, GroupId: d.Schedule.GroupID, GroupName: d.Schedule.GroupName,
			Primary: toProtoShift(d.Primary), Backup: toProtoShift(d.Backup),
		}
		if d.Primary != nil {
			item.NextHandoverTime = timestamppb.New(d.Primary.End)
		}
		items = append(items, item)
	}
	return &v1.GetCurrentDutyResponse{Items: items}, nil
}

// SendScheduleNotification implements proto OncallScheduleService/SendScheduleNotification.
func (s *OncallScheduleService) SendScheduleNotification(ctx context.Context, req *v1.SendScheduleNotificationRequest) (*v1.SendScheduleNotificationResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	res, err := s.uc.SendNotification(ctx, actor, biz.ManualNotifyInput{ScheduleID: req.GetId(), Kind: req.GetKind(), Target: req.GetTarget()})
	if err != nil {
		return nil, err
	}
	recipients := make([]*v1.NotificationRecipient, 0, len(res.Recipients))
	for _, sh := range res.Recipients {
		recipients = append(recipients, &v1.NotificationRecipient{
			Role: sh.Role, UserId: sh.UserID, UserName: sh.UserName,
			StartTime: timestamppb.New(sh.Start), EndTime: timestamppb.New(sh.End),
		})
	}
	return &v1.SendScheduleNotificationResponse{EventIds: res.EventIDs, Recipients: recipients}, nil
}

// ListScheduleNotificationResults implements proto OncallScheduleService/ListScheduleNotificationResults.
func (s *OncallScheduleService) ListScheduleNotificationResults(ctx context.Context, req *v1.ListScheduleNotificationResultsRequest) (*v1.ListScheduleNotificationResultsResponse, error) {
	actor, err := actorFromContext(ctx, s.admin)
	if err != nil {
		return nil, err
	}
	results, err := s.uc.NotificationResults(ctx, actor, req.GetId(), req.GetEventIds())
	if err != nil {
		return nil, err
	}
	items := make([]*v1.NotificationResult, 0, len(results))
	for _, res := range results {
		deliveries := make([]*v1.NotificationDelivery, 0, len(res.Deliveries))
		for _, d := range res.Deliveries {
			deliveries = append(deliveries, &v1.NotificationDelivery{
				ChannelName: d.ChannelName, ChannelType: d.ChannelType, Recipient: d.Recipient, Status: d.Status, Error: d.Error,
			})
		}
		items = append(items, &v1.NotificationResult{EventId: res.EventID, EventType: res.EventType, Status: res.Status, Deliveries: deliveries})
	}
	return &v1.ListScheduleNotificationResultsResponse{Items: items}, nil
}

// GetNotificationStatus implements proto OncallScheduleService/GetNotificationStatus.
func (s *OncallScheduleService) GetNotificationStatus(ctx context.Context, _ *v1.GetNotificationStatusRequest) (*v1.GetNotificationStatusResponse, error) {
	statuses, err := s.uc.NotificationStatus(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]*v1.NotificationStatus, 0, len(statuses))
	for _, st := range statuses {
		items = append(items, &v1.NotificationStatus{Kind: st.Class, EventType: st.EventType, RouteName: st.RouteName, ChannelCount: st.ChannelCount})
	}
	return &v1.GetNotificationStatusResponse{Items: items}, nil
}
