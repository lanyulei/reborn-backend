package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProposalService 实现 IncidentProposalServiceHTTPServer。
type ProposalService struct {
	uc *biz.ProposalUsecase
}

// NewProposalService new a ProposalService.
func NewProposalService(uc *biz.ProposalUsecase) *ProposalService { return &ProposalService{uc: uc} }

func toProtoProposal(v *biz.ProposalView) *v1.Proposal {
	p := v.Proposal
	out := &v1.Proposal{
		Id: p.ID, IncidentId: p.IncidentID, Title: p.Title, Reason: p.Reason, Evidence: p.Evidence,
		Action:   &v1.ProposalAction{Kind: p.Action.Kind, Ref: p.Action.Ref, RefName: p.Action.RefName, Params: p.Action.Params, CiIds: p.Action.CIIDs, CiRefs: namedList(v.CIRefs)},
		Playbook: named(p.PlaybookID, v.PlaybookName), StepIndex: p.StepIndex, Risk: p.Risk, Status: p.Status, Auto: p.Auto, ProposerType: p.ProposerType,
		Proposer: named(p.ProposerID, v.ProposerName), DecidedBy: named(p.DecidedBy, v.DecidedByName), DecidedTime: protoTime(p.DecidedTime), DecisionNote: p.DecisionNote,
		ExpireTime: timestamppb.New(p.ExpireTime), ExecKind: p.ExecKind, ExecRef: p.ExecRef, ExecUser: named(p.ExecUser, v.ExecUserName), Result: p.Result,
		VerifyUntil: protoTime(p.VerifyUntil), CreateTime: timestamppb.New(p.CreateTime), UpdateTime: timestamppb.New(p.UpdateTime), CanDecide: v.CanDecide, ExecUrl: v.ExecURL,
		AutoEligible: p.AutoEligible,
	}
	if vr := p.VerifyResult; vr != nil {
		out.VerifyResult = &v1.VerifyResult{AlertsBefore: vr.AlertsBefore, AlertsAfter: vr.AlertsAfter, Resolved: vr.Resolved, NewAlerts: vr.NewAlerts, Worse: vr.Worse, CheckedTime: timestamppb.New(vr.CheckedTime)}
	}
	return out
}

// ListProposals implements proto IncidentProposalService/ListProposals.
func (s *ProposalService) ListProposals(ctx context.Context, req *v1.ListProposalsRequest) (*v1.ListProposalsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.List(ctx, uid, req.GetId(), req.GetStatus())
	if err != nil {
		return nil, err
	}
	out := &v1.ListProposalsResponse{Items: make([]*v1.Proposal, 0, len(items))}
	for _, it := range items {
		out.Items = append(out.Items, toProtoProposal(it))
	}
	return out, nil
}

// CreateProposal implements proto IncidentProposalService/CreateProposal.
func (s *ProposalService) CreateProposal(ctx context.Context, req *v1.CreateProposalRequest) (*v1.ProposalResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	in := biz.ProposalInput{PlaybookID: req.GetPlaybookId(), StepIndex: req.GetStepIndex(), Title: req.GetTitle(), Reason: req.GetReason(), Evidence: req.GetEvidence(), Approve: req.GetApprove(), AutoEligible: req.GetAutoEligible(), AutoMaxTargets: req.GetAutoMaxTargets()}
	if a := req.GetAction(); a != nil && a.GetRef() != "" {
		in.Action = &biz.ProposalAction{Kind: a.GetKind(), Ref: a.GetRef(), Params: a.GetParams(), CIIDs: a.GetCiIds()}
	}
	v, err := s.uc.Create(ctx, uid, req.GetId(), in)
	if err != nil {
		return nil, err
	}
	return &v1.ProposalResponse{Proposal: toProtoProposal(v)}, nil
}

// DecideProposal implements proto IncidentProposalService/DecideProposal.
func (s *ProposalService) DecideProposal(ctx context.Context, req *v1.DecideProposalRequest) (*v1.ProposalResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Decide(ctx, uid, req.GetId(), req.GetProposalId(), req.GetApproved(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.ProposalResponse{Proposal: toProtoProposal(v)}, nil
}

// CancelProposal implements proto IncidentProposalService/CancelProposal.
func (s *ProposalService) CancelProposal(ctx context.Context, req *v1.CancelProposalRequest) (*v1.ProposalResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.Cancel(ctx, uid, req.GetId(), req.GetProposalId())
	if err != nil {
		return nil, err
	}
	return &v1.ProposalResponse{Proposal: toProtoProposal(v)}, nil
}

// ResetAutoApprove implements proto IncidentProposalService/ResetAutoApprove.
func (s *ProposalService) ResetAutoApprove(ctx context.Context, req *v1.ResetAutoApproveRequest) (*v1.ResetAutoApproveResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.ResetAutoApprove(ctx, uid, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.ResetAutoApproveResponse{}, nil
}
