// Package service 故障管理域 proto 接口实现：只做身份提取、参数编排与 proto ↔ 领域模型转换；业务规则、角色判定
// 全部收敛在 biz。
package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is 故障管理域 service providers。
var ProviderSet = wire.NewSet(
	NewIncidentService, NewConfigService, NewProposalService, NewUpdateService, NewPostmortemService, NewStatsService, NewOpenService,
	wire.Struct(new(Services), "*"),
)

// Services 本域全部服务：合成一组交给 server.NewHTTPServer 注册。
type Services struct {
	Incident   *IncidentService
	Config     *ConfigService
	Proposal   *ProposalService
	Update     *UpdateService
	Postmortem *PostmortemService
	Stats      *StatsService
	Open       *OpenService
}

func userID(ctx context.Context) (string, error) { return systemservice.UserIDFromContext(ctx) }

func protoTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func optTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

func named(id, name string) *v1.NamedRef {
	if id == "" {
		return nil
	}
	return &v1.NamedRef{Id: id, Name: name, Missing: name == ""}
}

func namedList(items []biz.Named) []*v1.NamedRef {
	out := make([]*v1.NamedRef, 0, len(items))
	for _, it := range items {
		out = append(out, &v1.NamedRef{Id: it.ID, Name: it.Name, Missing: it.Name == ""})
	}
	return out
}

func toMatchers(in []*v1.LabelMatcher) []biz.Matcher {
	out := make([]biz.Matcher, 0, len(in))
	for _, m := range in {
		out = append(out, biz.Matcher{Label: m.GetLabel(), Op: m.GetOp(), Value: m.GetValue(), Values: m.GetValues()})
	}
	return out
}

func toProtoMatchers(in []biz.Matcher) []*v1.LabelMatcher {
	out := make([]*v1.LabelMatcher, 0, len(in))
	for _, m := range in {
		out = append(out, &v1.LabelMatcher{Label: m.Label, Op: m.Op, Value: m.Value, Values: m.Values})
	}
	return out
}

// toProtoIncident 故障视图 → proto。
func toProtoIncident(v *biz.View) *v1.Incident {
	in := v.Incident
	out := &v1.Incident{
		Id: in.ID, Number: in.Number, Title: in.Title, Severity: in.Severity, Status: in.Status, ReviewStatus: in.ReviewStatus, Source: in.Source,
		Summary: in.Summary, AiSummary: in.AISummary, RootCauseCode: in.RootCauseCode, RootCause: in.RootCause, ResolveNote: in.ResolveNote,
		Commander: named(in.CommanderID, v.CommanderName), Responders: namedList(v.Responders), Watchers: namedList(v.Watchers),
		AffectedCis: namedList(v.AffectedCIs), Groups: in.Groups, IsDrill: in.IsDrill,
		OccurTime: timestamppb.New(in.OccurTime), OpenTime: timestamppb.New(in.OpenTime), RespondTime: protoTime(in.RespondTime),
		MitigateTime: protoTime(in.MitigateTime), ResolveTime: protoTime(in.ResolveTime), FirstResolveTime: protoTime(in.FirstResolve),
		CloseTime: protoTime(in.CloseTime), ReopenCount: in.ReopenCount, MergedInto: named(in.MergedInto, v.MergedIntoNumber),
		EscalationLevel: in.EscalationLevel, AlertCount: in.AlertCount, ActiveAlertCount: v.ActiveAlerts, UpdateCount: in.UpdateCount,
		ProposalPending: in.PendingProposals, AutoApprove: in.AutoApprove, AgentOwner: named(in.AgentOwner, ""), AgentLeaseUntil: protoTime(in.AgentLeaseUntil),
		ReviewDue: protoTime(in.ReviewDue), LastActivityTime: timestamppb.New(in.LastActivity), CanManage: v.CanManage, CanHandle: v.CanHandle,
		Version: in.Version, CreateTime: timestamppb.New(in.CreateTime), UpdateTime: timestamppb.New(in.UpdateTime),
		Business: named(in.BusinessID, v.BusinessName), Rule: named(in.RuleID, v.RuleName),
	}
	if in.SourceRef != "" {
		out.SourceRef = &v1.NamedRef{Id: in.SourceRef, Name: v.SourceRefName}
	}
	return out
}

func toProtoIncidents(views []*biz.View) []*v1.Incident {
	out := make([]*v1.Incident, 0, len(views))
	for _, v := range views {
		out = append(out, toProtoIncident(v))
	}
	return out
}

func toProtoChange(c *biz.ChangeRef, names map[string]string) *v1.ChangeRef {
	out := &v1.ChangeRef{Domain: c.Domain, Kind: c.Kind, RefId: c.RefID, Summary: c.Summary, RefTime: protoTime(c.RefTime), Relation: c.Relation, Url: c.URL}
	if c.OperatorID != "" {
		out.Operator = &v1.NamedRef{Id: c.OperatorID, Name: names["user:"+c.OperatorID]}
	}
	if c.CIID != "" {
		out.Ci = &v1.NamedRef{Id: c.CIID, Name: names["ci:"+c.CIID]}
	}
	if c.CreateUser != "" {
		out.MarkedBy = &v1.NamedRef{Id: c.CreateUser, Name: names["user:"+c.CreateUser]}
	}
	return out
}

func toProtoFinding(f *biz.Finding) *v1.Finding {
	if f == nil {
		return nil
	}
	return &v1.Finding{Title: f.Title, Conclusion: f.Conclusion, Confidence: f.Confidence, Evidence: f.Evidence, NextStep: f.NextStep, RunId: f.RunID}
}

func toProtoEvent(it *biz.TimelineItem) *v1.TimelineItem {
	d := it.Detail
	detail := &v1.EventDetail{
		Text: d.Text, From: d.From, To: d.To, AlertIds: d.AlertIDs, AlertCount: d.AlertCount, Level: d.Level, Version: d.Version,
		FileIds: d.FileIDs, ExecKind: d.ExecKind, ExecRef: d.ExecRef, Error: d.Error, Finding: toProtoFinding(d.Finding),
	}
	if d.UserID != "" {
		detail.User = &v1.NamedRef{Id: d.UserID, Name: it.Names["user:"+d.UserID]}
	}
	if d.IncidentID != "" {
		detail.Incident = &v1.NamedRef{Id: d.IncidentID, Name: d.IncidentNumber}
		if detail.Incident.Name == "" {
			detail.Incident.Name = it.Names["incident:"+d.IncidentID]
		}
	}
	if d.ProposalID != "" {
		detail.Proposal = &v1.NamedRef{Id: d.ProposalID, Name: d.ProposalTitle}
	}
	if d.TicketID != "" {
		detail.Ticket = &v1.NamedRef{Id: d.TicketID, Name: d.TicketNumber}
	}
	if d.UpdateID != "" {
		detail.Update = &v1.NamedRef{Id: d.UpdateID, Name: biz.UpdateKindLabel(d.UpdateKind)}
	}
	if d.Change != nil {
		detail.Change = toProtoChange(d.Change, it.Names)
	}
	out := &v1.TimelineItem{Id: it.ID, Type: it.Type, ActorType: it.ActorType, Detail: detail, Pinned: it.Pinned, CreateTime: timestamppb.New(it.CreateTime)}
	if it.ActorID != "" {
		out.Actor = &v1.NamedRef{Id: it.ActorID, Name: it.ActorName}
	}
	return out
}

func toProtoAlertItem(it *biz.AlertItem) *v1.IncidentAlert {
	out := &v1.IncidentAlert{
		AlertId: it.Link.AlertID, RuleName: it.Link.RuleName, Severity: it.Link.Severity, Method: it.Link.Method,
		AttachTime: timestamppb.New(it.Link.AttachTime), FirstFireTime: protoTime(it.Link.FirstFireTime), CanView: it.CanView,
	}
	if it.Link.CIID != "" {
		out.Ci = &v1.NamedRef{Id: it.Link.CIID, Name: it.CIName}
	}
	if it.Link.AttachUser != "" {
		out.AttachUser = &v1.NamedRef{Id: it.Link.AttachUser}
	}
	if a := it.Alert; a != nil {
		out.RuleName, out.Severity, out.Status, out.Labels, out.ResolveTime, out.Value = a.RuleName, a.Severity, a.Status, a.Labels, protoTime(a.ResolveTime), a.Value
		if !a.FirstFireTime.IsZero() {
			out.FirstFireTime = timestamppb.New(a.FirstFireTime)
		}
	}
	return out
}

func toProtoAlertItems(items []*biz.AlertItem) []*v1.IncidentAlert {
	out := make([]*v1.IncidentAlert, 0, len(items))
	for _, it := range items {
		out = append(out, toProtoAlertItem(it))
	}
	return out
}
