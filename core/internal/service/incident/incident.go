package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// IncidentService 实现 IncidentServiceHTTPServer。
type IncidentService struct {
	uc *biz.IncidentUsecase
}

// NewIncidentService new an IncidentService.
func NewIncidentService(uc *biz.IncidentUsecase) *IncidentService { return &IncidentService{uc: uc} }

// ListIncidents implements proto IncidentService/ListIncidents.
func (s *IncidentService) ListIncidents(ctx context.Context, req *v1.ListIncidentsRequest) (*v1.ListIncidentsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	f := biz.IncidentFilter{
		Keyword: req.GetKeyword(), Statuses: req.GetStatuses(), Severities: req.GetSeverities(), BusinessID: req.GetBusinessId(),
		CommanderID: req.GetCommanderId(), IsDrill: req.IsDrill, ReviewStatus: req.ReviewStatus, ReviewOverdue: req.GetReviewOverdue(),
		OpenStart: optTime(req.OpenTimeStart), OpenEnd: optTime(req.OpenTimeEnd), CIID: req.GetCiId(),
	}
	if req.GetActive() && len(f.Statuses) == 0 {
		f.Statuses = []int32{biz.StatusOpen, biz.StatusResponding, biz.StatusMitigated}
	}
	if req.GetMine() {
		f.MemberUserID = uid
	}
	items, total, err := s.uc.List(ctx, uid, f, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	return &v1.ListIncidentsResponse{Items: toProtoIncidents(items), Total: total}, nil
}

// GetIncident implements proto IncidentService/GetIncident.
func (s *IncidentService) GetIncident(ctx context.Context, req *v1.GetIncidentRequest) (*v1.GetIncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Get(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetIncidentResponse{Incident: toProtoIncident(v)}, nil
}

func (s *IncidentService) resp(v *biz.View) *v1.IncidentResponse {
	return &v1.IncidentResponse{Incident: toProtoIncident(v)}
}

// CreateIncident implements proto IncidentService/CreateIncident.
func (s *IncidentService) CreateIncident(ctx context.Context, req *v1.CreateIncidentRequest) (*v1.CreateIncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Create(ctx, uid, biz.CreateInput{
		Title: req.GetTitle(), Severity: req.GetSeverity(), BusinessID: req.GetBusinessId(), Summary: req.GetSummary(), AlertIDs: req.GetAlertIds(),
		AffectedCIIDs: req.GetAffectedCiIds(), CommanderID: req.GetCommanderId(), ResponderIDs: req.GetResponderIds(), Groups: req.GetGroups(),
		OccurTime: optTime(req.OccurTime), IsDrill: req.GetIsDrill(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateIncidentResponse{Incident: toProtoIncident(v)}, nil
}

// UpdateIncident implements proto IncidentService/UpdateIncident.
func (s *IncidentService) UpdateIncident(ctx context.Context, req *v1.UpdateIncidentRequest) (*v1.UpdateIncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Update(ctx, uid, req.GetId(), biz.UpdateInput{
		Title: req.GetTitle(), BusinessID: req.GetBusinessId(), Summary: req.GetSummary(), AffectedCIIDs: req.GetAffectedCiIds(), Groups: req.GetGroups(),
		OccurTime: optTime(req.OccurTime), IsDrill: req.GetIsDrill(), Version: req.GetVersion(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateIncidentResponse{Incident: toProtoIncident(v)}, nil
}

// ChangeSeverity implements proto IncidentService/ChangeSeverity.
func (s *IncidentService) ChangeSeverity(ctx context.Context, req *v1.ChangeSeverityRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.ChangeSeverity(ctx, uid, req.GetId(), req.GetSeverity(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// TransitionIncident implements proto IncidentService/TransitionIncident.
func (s *IncidentService) TransitionIncident(ctx context.Context, req *v1.TransitionIncidentRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Transition(ctx, uid, req.GetId(), biz.TransitionInput{Action: req.GetAction(), Note: req.GetNote(), RootCauseCode: req.GetRootCauseCode(), Force: req.GetForce()})
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// MergeIncident implements proto IncidentService/MergeIncident.
func (s *IncidentService) MergeIncident(ctx context.Context, req *v1.MergeIncidentRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Merge(ctx, uid, req.GetId(), req.GetTargetId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// AssignCommander implements proto IncidentService/AssignCommander.
func (s *IncidentService) AssignCommander(ctx context.Context, req *v1.AssignCommanderRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.AssignCommander(ctx, uid, req.GetId(), req.GetUserId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// UpdateMembers implements proto IncidentService/UpdateMembers.
func (s *IncidentService) UpdateMembers(ctx context.Context, req *v1.UpdateMembersRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.UpdateMembers(ctx, uid, req.GetId(), req.GetRole(), req.GetAddUserIds(), req.GetRemoveUserIds())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// AddComment implements proto IncidentService/AddComment.
func (s *IncidentService) AddComment(ctx context.Context, req *v1.AddCommentRequest) (*v1.TimelineItemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	ev, err := s.uc.AddComment(ctx, uid, req.GetId(), req.GetText(), req.GetFileIds())
	if err != nil {
		return nil, err
	}
	return &v1.TimelineItemResponse{Item: toProtoEvent(&biz.TimelineItem{Event: ev})}, nil
}

// PinEvent implements proto IncidentService/PinEvent.
func (s *IncidentService) PinEvent(ctx context.Context, req *v1.PinEventRequest) (*v1.TimelineItemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	ev, err := s.uc.PinEvent(ctx, uid, req.GetId(), req.GetEventId(), req.GetPinned())
	if err != nil {
		return nil, err
	}
	return &v1.TimelineItemResponse{Item: toProtoEvent(&biz.TimelineItem{Event: ev})}, nil
}

// ListTimeline implements proto IncidentService/ListTimeline.
func (s *IncidentService) ListTimeline(ctx context.Context, req *v1.ListTimelineRequest) (*v1.ListTimelineResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListTimeline(ctx, req.GetId(), biz.TimelineFilter{Types: req.GetTypes(), PinnedOnly: req.GetPinnedOnly()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListTimelineResponse{Items: make([]*v1.TimelineItem, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoEvent(it))
	}
	return out, nil
}

// AttachAlerts implements proto IncidentService/AttachAlerts.
func (s *IncidentService) AttachAlerts(ctx context.Context, req *v1.AttachAlertsRequest) (*v1.AttachAlertsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	res, err := s.uc.Attach(ctx, uid, req.GetId(), req.GetAlertIds(), req.GetMove())
	if err != nil {
		return nil, err
	}
	return &v1.AttachAlertsResponse{Incident: toProtoIncident(res.View), Attached: res.Attached, Skipped: res.Skipped}, nil
}

// DetachAlert implements proto IncidentService/DetachAlert.
func (s *IncidentService) DetachAlert(ctx context.Context, req *v1.DetachAlertRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Detach(ctx, uid, req.GetId(), req.GetAlertId())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// ListIncidentAlerts implements proto IncidentService/ListIncidentAlerts.
func (s *IncidentService) ListIncidentAlerts(ctx context.Context, req *v1.ListIncidentAlertsRequest) (*v1.ListIncidentAlertsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListAlerts(ctx, uid, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	return &v1.ListIncidentAlertsResponse{Items: toProtoAlertItems(items), Total: total}, nil
}

// ListIncidentsByAlert implements proto IncidentService/ListIncidentsByAlert.
func (s *IncidentService) ListIncidentsByAlert(ctx context.Context, req *v1.ListIncidentsByAlertRequest) (*v1.ListIncidentsByAlertResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.ByAlert(ctx, uid, req.GetAlertId())
	if err != nil {
		return nil, err
	}
	return &v1.ListIncidentsByAlertResponse{Items: toProtoIncidents(items)}, nil
}

func toProtoImpact(nodes []*biz.ImpactNode) []*v1.ImpactNode {
	out := make([]*v1.ImpactNode, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &v1.ImpactNode{Id: n.ID, Name: n.Name, ModelCode: n.ModelCode, ModelName: n.ModelName, Depth: n.Depth, CanView: n.CanView, Via: n.Via})
	}
	return out
}

// GetImpact implements proto IncidentService/GetImpact.
func (s *IncidentService) GetImpact(ctx context.Context, req *v1.GetImpactRequest) (*v1.GetImpactResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	nodes, truncated, counts, err := s.uc.Impact(ctx, uid, req.GetId(), int(req.GetDepth()))
	if err != nil {
		return nil, err
	}
	return &v1.GetImpactResponse{Nodes: toProtoImpact(nodes), Truncated: truncated, ModelCounts: counts}, nil
}

// ListChanges implements proto IncidentService/ListChanges.
func (s *IncidentService) ListChanges(ctx context.Context, req *v1.ListChangesRequest) (*v1.ListChangesResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, notes, err := s.uc.Changes(ctx, uid, req.GetId(), int(req.GetLookbackH()))
	if err != nil {
		return nil, err
	}
	names, err := s.uc.ChangeNames(ctx, items)
	if err != nil {
		return nil, err
	}
	out := &v1.ListChangesResponse{Items: make([]*v1.ChangeRef, 0, len(items)), Notes: notes}
	for _, c := range items {
		out.Items = append(out.Items, toProtoChange(c, names))
	}
	return out, nil
}

// MarkChange implements proto IncidentService/MarkChange.
func (s *IncidentService) MarkChange(ctx context.Context, req *v1.MarkChangeRequest) (*v1.MarkChangeResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.MarkChange(ctx, uid, req.GetId(), biz.ChangeRef{Domain: req.GetDomain(), Kind: req.GetKind(), RefID: req.GetRefId(), Summary: req.GetSummary(), RefTime: optTime(req.RefTime), Relation: req.GetRelation()})
	if err != nil {
		return nil, err
	}
	return &v1.MarkChangeResponse{Change: toProtoChange(c, nil)}, nil
}

// CreateIncidentTicket implements proto IncidentService/CreateIncidentTicket.
func (s *IncidentService) CreateIncidentTicket(ctx context.Context, req *v1.CreateIncidentTicketRequest) (*v1.CreateIncidentTicketResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.CreateTicket(ctx, uid, req.GetId(), req.GetTemplateId(), req.GetTitle(), req.GetDescription(), req.GetPriority(), req.GetRequestId())
	if err != nil {
		return nil, err
	}
	return &v1.CreateIncidentTicketResponse{Ticket: &v1.NamedRef{Id: t.ID, Name: t.Title}, TicketNumber: t.Number}, nil
}

func toProtoSimilar(items []*biz.Similar) []*v1.SimilarIncident {
	out := make([]*v1.SimilarIncident, 0, len(items))
	for _, s := range items {
		out = append(out, &v1.SimilarIncident{Id: s.ID, Number: s.Number, Title: s.Title, Severity: s.Severity, RootCauseCode: s.RootCauseCode, RootCause: s.RootCause, OpenTime: timestamppb.New(s.OpenTime), Score: s.Score})
	}
	return out
}

// GetIncidentContext implements proto IncidentService/GetIncidentContext.
func (s *IncidentService) GetIncidentContext(ctx context.Context, req *v1.GetIncidentContextRequest) (*v1.GetIncidentContextResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.uc.GetContext(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	names, err := s.uc.ChangeNames(ctx, c.Changes)
	if err != nil {
		return nil, err
	}
	out := &v1.GetIncidentContextResponse{
		Incident: toProtoIncident(c.View), Alerts: toProtoAlertItems(c.Alerts), Impact: toProtoImpact(c.Impact), Similar: toProtoSimilar(c.Similar),
		PendingProposals: c.Pending, Notes: c.Notes,
	}
	for _, ch := range c.Changes {
		out.Changes = append(out.Changes, toProtoChange(ch, names))
	}
	for _, it := range c.Timeline {
		out.Timeline = append(out.Timeline, toProtoEvent(it))
	}
	for _, u := range c.Updates {
		out.Updates = append(out.Updates, &v1.IncidentUpdateBrief{Id: u.ID, Kind: u.Kind, Content: u.Content, Author: named(u.AuthorID, u.AuthorName), CreateTime: timestamppb.New(u.CreateTime)})
	}
	for _, p := range c.Playbooks {
		out.Playbooks = append(out.Playbooks, &v1.PlaybookBrief{Id: p.ID, Name: p.Spec.Name, Description: p.Spec.Description, StepCount: int32(len(p.Spec.Steps))}) // #nosec G115 -- 步骤至多 20
	}
	return out, nil
}

// SearchIncidents implements proto IncidentService/SearchIncidents.
func (s *IncidentService) SearchIncidents(ctx context.Context, req *v1.SearchIncidentsRequest) (*v1.SearchIncidentsResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, err := s.uc.Search(ctx, req.GetQ(), req.GetExcludeId(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	return &v1.SearchIncidentsResponse{Items: toProtoSimilar(items)}, nil
}

// RegenerateSummary implements proto IncidentService/RegenerateSummary.
func (s *IncidentService) RegenerateSummary(ctx context.Context, req *v1.RegenerateSummaryRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.RegenerateSummary(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// ListCandidates implements proto IncidentService/ListCandidates.
func (s *IncidentService) ListCandidates(ctx context.Context, req *v1.ListCandidatesRequest) (*v1.ListCandidatesResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListCandidates(ctx, uid, req.GetStatus(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListCandidatesResponse{Items: make([]*v1.Candidate, 0, len(items)), Total: total}
	for _, c := range items {
		out.Items = append(out.Items, &v1.Candidate{
			Id: c.ID, Rule: &v1.NamedRef{Id: c.RuleID, Name: c.RuleName, Missing: c.RuleName == ""}, CorrelationKey: c.CorrelationKey,
			AlertCount: int32(len(c.AlertIDs)), CiCount: int32(len(c.CIIDs)), Alerts: toProtoAlertItems(c.Alerts), Status: c.Status, // #nosec G115 -- 上限 1000
			Incident: named(c.IncidentID, c.IncidentNumber), FirstTime: timestamppb.New(c.FirstTime), UpdateTime: timestamppb.New(c.UpdateTime),
		})
	}
	return out, nil
}

// PromoteCandidate implements proto IncidentService/PromoteCandidate.
func (s *IncidentService) PromoteCandidate(ctx context.Context, req *v1.PromoteCandidateRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.PromoteCandidate(ctx, uid, req.GetId(), req.GetTitle(), req.GetSeverity())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// DismissCandidate implements proto IncidentService/DismissCandidate.
func (s *IncidentService) DismissCandidate(ctx context.Context, req *v1.DismissCandidateRequest) (*v1.DismissCandidateResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.DismissCandidate(ctx, uid, req.GetId(), req.GetNote()); err != nil {
		return nil, err
	}
	return &v1.DismissCandidateResponse{}, nil
}

// AcquireAgentLease implements proto IncidentService/AcquireAgentLease.
func (s *IncidentService) AcquireAgentLease(ctx context.Context, req *v1.AgentLeaseRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.AcquireAgentLease(ctx, uid, req.GetId(), time.Duration(req.GetTtlS())*time.Second)
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// ReleaseAgentLease implements proto IncidentService/ReleaseAgentLease.
func (s *IncidentService) ReleaseAgentLease(ctx context.Context, req *v1.AgentLeaseRequest) (*v1.IncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.ReleaseAgentLease(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return s.resp(v), nil
}

// AddFinding implements proto IncidentService/AddFinding.
func (s *IncidentService) AddFinding(ctx context.Context, req *v1.AddFindingRequest) (*v1.TimelineItemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	f := req.GetFinding()
	ev, err := s.uc.AddFinding(ctx, uid, req.GetId(), biz.Finding{Title: f.GetTitle(), Conclusion: f.GetConclusion(), Confidence: f.GetConfidence(), Evidence: f.GetEvidence(), NextStep: f.GetNextStep(), RunID: f.GetRunId()})
	if err != nil {
		return nil, err
	}
	return &v1.TimelineItemResponse{Item: toProtoEvent(&biz.TimelineItem{Event: ev})}, nil
}

// ListIncidentOptions implements proto IncidentService/ListIncidentOptions.
func (s *IncidentService) ListIncidentOptions(ctx context.Context, req *v1.ListIncidentOptionsRequest) (*v1.ListIncidentOptionsResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	o, err := s.uc.Options(ctx, req.GetKinds())
	if err != nil {
		return nil, err
	}
	out := &v1.ListIncidentOptionsResponse{Schedules: namedList(o.Schedules), Businesses: namedList(o.Businesses), TicketTemplates: namedList(o.TicketTemplates)}
	for _, rc := range o.RootCauses {
		out.RootCauses = append(out.RootCauses, &v1.RootCause{Code: rc.Code, Name: rc.Name})
	}
	for _, p := range o.Playbooks {
		out.Playbooks = append(out.Playbooks, &v1.PlaybookBrief{Id: p.ID, Name: p.Spec.Name, Description: p.Spec.Description, StepCount: int32(len(p.Spec.Steps))}) // #nosec G115 -- 步骤至多 20
	}
	return out, nil
}
