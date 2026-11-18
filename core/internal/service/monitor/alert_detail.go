package service

import (
	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func toProtoNotifyLog(l *biz.NotifyLog) *v1.AlertNotifyLog {
	item := &v1.AlertNotifyLog{
		Id: l.ID, Kind: l.Kind, Seq: l.Seq, Source: l.Source, SubscriptionId: l.SubscriptionID, SubscriptionName: l.SubscriptionName,
		NotifyEventIds: l.NotifyEventIDs, Note: l.Note, CreateTime: timestamppb.New(l.CreateTime), Level: l.Level,
		Recipients: make([]*v1.AlertRecipient, 0, len(l.RecipientRefs)),
	}
	for _, r := range l.RecipientRefs {
		item.Recipients = append(item.Recipients, &v1.AlertRecipient{Type: r.Type, Id: r.ID, Name: r.Name})
	}
	return item
}

func toProtoRef(r *biz.NamedRef) *v1.NamedRef {
	if r == nil {
		return nil
	}
	return &v1.NamedRef{Id: r.ID, Name: r.Name, Missing: r.Missing}
}

// toProtoTimelineItem 时间线条目 → proto。detail 恒为对象（通知类条目各字段为零值）。
func toProtoTimelineItem(it *biz.TimelineItem) *v1.AlertTimelineItem {
	d := it.Detail
	out := &v1.AlertTimelineItem{
		Id: it.ID, Type: it.Type, Time: timestamppb.New(it.Time), Operator: toProtoRef(it.Operator),
		Detail: &v1.AlertEventDetail{
			Text: d.Text, Cause: d.Cause, FromUser: toProtoRef(it.FromUser), ToUser: toProtoRef(it.ToUser),
			MuteId: d.MuteID, MuteName: it.MuteName, Maintenance: it.Maintenance,
			InhibitKind: d.InhibitKind, InhibitId: d.InhibitID, InhibitName: it.InhibitName, InhibitAlertId: d.InhibitAlertID,
			Level: d.Level, Severity: d.Severity, TicketId: d.TicketID, TicketNumber: d.TicketNumber, Auto: d.Auto,
			WorkflowInstanceId: d.WorkflowInstanceID, ActionKind: d.ActionKind, Error: d.Error,
		},
	}
	if it.Log != nil {
		out.NotifyLog = toProtoNotifyLog(it.Log)
	}
	return out
}

func toProtoAlertTicket(t *biz.AlertTicketView) *v1.AlertTicket {
	if t == nil {
		return nil
	}
	return &v1.AlertTicket{Id: t.ID, Number: t.Number, Title: t.Title, Status: t.Status, Auto: t.Auto, CanView: t.CanView}
}

// fillAlertExtras 把时间线、资源卡片、影响面与工单卡片写进详情响应。
func fillAlertExtras(resp *v1.GetAlertResponse, x *biz.AlertExtras) {
	resp.Timeline = make([]*v1.AlertTimelineItem, 0, len(x.Timeline))
	for _, it := range x.Timeline {
		resp.Timeline = append(resp.Timeline, toProtoTimelineItem(it))
	}
	if ci := x.CI; ci != nil {
		resp.Ci = &v1.AlertCI{
			Id: ci.ID, Name: ci.Name, ModelCode: ci.ModelCode, ModelName: ci.ModelName, Env: ci.Env, Status: ci.Status,
			Owner: ci.Owner, OwnerId: ci.OwnerID, Tags: ci.Tags, Exists: ci.Exists, CanView: ci.CanView, FiringCount: ci.FiringCount,
		}
	}
	resp.Impact = make([]*v1.AlertImpactCI, 0, len(x.Impact))
	for _, n := range x.Impact {
		resp.Impact = append(resp.Impact, &v1.AlertImpactCI{
			Id: n.ID, Name: n.Name, ModelCode: n.ModelCode, ModelName: n.ModelName, Depth: n.Depth, FiringCount: n.FiringCount, CanView: n.CanView,
		})
	}
	resp.ImpactTruncated, resp.Ticket = x.ImpactTruncated, toProtoAlertTicket(x.Ticket)
}
