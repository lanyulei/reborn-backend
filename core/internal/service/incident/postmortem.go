package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// PostmortemService 实现 IncidentPostmortemServiceHTTPServer。
type PostmortemService struct {
	uc *biz.PostmortemUsecase
}

// NewPostmortemService new a PostmortemService.
func NewPostmortemService(uc *biz.PostmortemUsecase) *PostmortemService {
	return &PostmortemService{uc: uc}
}

func toDoc(in *v1.PostmortemDoc) biz.PostmortemDoc {
	return biz.PostmortemDoc{
		Summary: in.GetSummary(), Impact: in.GetImpact(), Timeline: in.GetTimeline(), RootCauseCode: in.GetRootCauseCode(), RootCause: in.GetRootCause(),
		Trigger: in.GetTrigger(), Detection: in.GetDetection(), Handling: in.GetHandling(), WentWell: in.GetWentWell(), WentWrong: in.GetWentWrong(),
	}
}

func toProtoDoc(d *biz.PostmortemDoc) *v1.PostmortemDoc {
	return &v1.PostmortemDoc{
		Summary: d.Summary, Impact: d.Impact, Timeline: d.Timeline, RootCauseCode: d.RootCauseCode, RootCause: d.RootCause,
		Trigger: d.Trigger, Detection: d.Detection, Handling: d.Handling, WentWell: d.WentWell, WentWrong: d.WentWrong,
	}
}

func toProtoItem(v *biz.ActionItemView) *v1.ActionItem {
	it := v.ActionItem
	out := &v1.ActionItem{
		Id: it.ID, IncidentId: it.IncidentID, IncidentNumber: it.IncidentNumber, Title: it.Title, Kind: it.Kind, Owner: named(it.OwnerID, v.OwnerName),
		Status: it.Status, Ticket: named(it.TicketID, it.TicketNumber), TicketNumber: it.TicketNumber, CreateTime: timestamppb.New(it.CreateTime),
		UpdateTime: timestamppb.New(it.UpdateTime), Overdue: v.Overdue,
	}
	if it.DueDate != nil {
		out.DueDate = it.DueDate.Format(time.DateOnly)
	}
	return out
}

func toProtoPostmortem(v *biz.PostmortemView) *v1.Postmortem {
	out := &v1.Postmortem{
		IncidentId: v.IncidentID, Status: v.Status, Doc: toProtoDoc(&v.Doc), CurrentVersion: v.CurrentVersion, ReviewDue: protoTime(v.ReviewDue), Overdue: v.Overdue,
		UpdateUser: named(v.UpdateUser, v.UpdateUserName), CanEdit: v.CanEdit,
	}
	if !v.UpdateTime.IsZero() {
		out.UpdateTime = timestamppb.New(v.UpdateTime)
	}
	for _, it := range v.Items {
		out.ActionItems = append(out.ActionItems, toProtoItem(it))
	}
	return out
}

// GetPostmortem implements proto IncidentPostmortemService/GetPostmortem.
func (s *PostmortemService) GetPostmortem(ctx context.Context, req *v1.GetPostmortemRequest) (*v1.PostmortemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Get(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.PostmortemResponse{Postmortem: toProtoPostmortem(v)}, nil
}

// SavePostmortem implements proto IncidentPostmortemService/SavePostmortem.
func (s *PostmortemService) SavePostmortem(ctx context.Context, req *v1.SavePostmortemRequest) (*v1.PostmortemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Save(ctx, uid, req.GetId(), toDoc(req.GetDoc()))
	if err != nil {
		return nil, err
	}
	return &v1.PostmortemResponse{Postmortem: toProtoPostmortem(v)}, nil
}

// CompletePostmortem implements proto IncidentPostmortemService/CompletePostmortem.
func (s *PostmortemService) CompletePostmortem(ctx context.Context, req *v1.CompletePostmortemRequest) (*v1.PostmortemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Complete(ctx, uid, req.GetId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.PostmortemResponse{Postmortem: toProtoPostmortem(v)}, nil
}

// RevisePostmortem implements proto IncidentPostmortemService/RevisePostmortem.
func (s *PostmortemService) RevisePostmortem(ctx context.Context, req *v1.RevisePostmortemRequest) (*v1.PostmortemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Revise(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.PostmortemResponse{Postmortem: toProtoPostmortem(v)}, nil
}

// ListPostmortemVersions implements proto IncidentPostmortemService/ListPostmortemVersions.
func (s *PostmortemService) ListPostmortemVersions(ctx context.Context, req *v1.ListPostmortemVersionsRequest) (*v1.ListPostmortemVersionsResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, names, err := s.uc.Versions(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.ListPostmortemVersionsResponse{Items: make([]*v1.PostmortemVersion, 0, len(items))}
	for _, v := range items {
		out.Items = append(out.Items, &v1.PostmortemVersion{Id: v.ID, Version: v.Version, Doc: toProtoDoc(&v.Doc), Note: v.Note, Creator: named(v.CreateUser, names[v.CreateUser]), CreateTime: timestamppb.New(v.CreateTime)})
	}
	return out, nil
}

// DraftPostmortem implements proto IncidentPostmortemService/DraftPostmortem.
func (s *PostmortemService) DraftPostmortem(ctx context.Context, req *v1.DraftPostmortemRequest) (*v1.PostmortemDocResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	doc, err := s.uc.Draft(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.PostmortemDocResponse{Doc: toProtoDoc(doc)}, nil
}

// GenerateTimeline implements proto IncidentPostmortemService/GenerateTimeline.
func (s *PostmortemService) GenerateTimeline(ctx context.Context, req *v1.GenerateTimelineRequest) (*v1.GenerateTimelineResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	text, err := s.uc.GenerateTimeline(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GenerateTimelineResponse{Text: text}, nil
}

// ListActionItems implements proto IncidentPostmortemService/ListActionItems.
func (s *PostmortemService) ListActionItems(ctx context.Context, req *v1.ListActionItemsRequest) (*v1.ListActionItemsResponse, error) {
	if _, err := userID(ctx); err != nil {
		return nil, err
	}
	items, total, err := s.uc.ListItems(ctx, biz.ActionItemFilter{IncidentID: req.GetIncidentId(), Status: req.Status, OwnerID: req.GetOwnerId(), Overdue: req.GetOverdue()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListActionItemsResponse{Items: make([]*v1.ActionItem, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoItem(it))
	}
	return out, nil
}

// SaveActionItem implements proto IncidentPostmortemService/SaveActionItem.
func (s *PostmortemService) SaveActionItem(ctx context.Context, req *v1.SaveActionItemRequest) (*v1.ActionItemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	in := biz.ItemInput{ID: req.GetItemId(), Title: req.GetTitle(), Kind: req.GetKind(), OwnerID: req.GetOwnerId(), Status: req.GetStatus()}
	if req.GetDueDate() != "" {
		d, err := time.Parse(time.DateOnly, req.GetDueDate())
		if err != nil {
			return nil, biz.ErrPostmortemInvalid
		}
		in.DueDate = &d
	}
	v, err := s.uc.SaveItem(ctx, uid, req.GetId(), in)
	if err != nil {
		return nil, err
	}
	return &v1.ActionItemResponse{Item: toProtoItem(v)}, nil
}

// DeleteActionItem implements proto IncidentPostmortemService/DeleteActionItem.
func (s *PostmortemService) DeleteActionItem(ctx context.Context, req *v1.DeleteActionItemRequest) (*v1.EmptyPostmortemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.DeleteItem(ctx, uid, req.GetId(), req.GetItemId()); err != nil {
		return nil, err
	}
	return &v1.EmptyPostmortemResponse{}, nil
}

// CreateActionItemTicket implements proto IncidentPostmortemService/CreateActionItemTicket.
func (s *PostmortemService) CreateActionItemTicket(ctx context.Context, req *v1.CreateActionItemTicketRequest) (*v1.ActionItemResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.CreateItemTicket(ctx, uid, req.GetId(), req.GetItemId(), req.GetTemplateId(), req.GetPriority())
	if err != nil {
		return nil, err
	}
	return &v1.ActionItemResponse{Item: toProtoItem(v)}, nil
}
