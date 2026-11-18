package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// PolicyService 实现 JobPolicyServiceHTTPServer。
type PolicyService struct {
	uc *biz.PolicyUsecase
}

// NewPolicyService new a PolicyService.
func NewPolicyService(uc *biz.PolicyUsecase) *PolicyService { return &PolicyService{uc: uc} }

func toRiskRule(id string, spec *v1.JobRiskRuleSpec) *biz.RiskRule {
	return &biz.RiskRule{
		ID: id, Name: spec.GetName(), Pattern: spec.GetPattern(), Action: spec.GetAction(),
		Enabled: spec.GetEnabled(), Description: spec.GetDescription(),
	}
}

func toProtoRiskRule(r *biz.RiskRule) *v1.JobRiskRule {
	return &v1.JobRiskRule{
		Id:      r.ID,
		Spec:    &v1.JobRiskRuleSpec{Name: r.Name, Pattern: r.Pattern, Action: r.Action, Enabled: r.Enabled, Description: r.Description},
		Version: r.Version, UpdateTime: timestamppb.New(r.UpdateTime), UpdateUserName: r.UpdateUserName,
	}
}

// ListJobRiskRules implements proto JobPolicyService/ListJobRiskRules.
func (s *PolicyService) ListJobRiskRules(ctx context.Context, _ *v1.ListJobRiskRulesRequest) (*v1.ListJobRiskRulesResponse, error) {
	items, err := s.uc.ListRules(ctx)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobRiskRulesResponse{Items: make([]*v1.JobRiskRule, 0, len(items))}
	for _, r := range items {
		resp.Items = append(resp.Items, toProtoRiskRule(r))
	}
	return resp, nil
}

// CreateJobRiskRule implements proto JobPolicyService/CreateJobRiskRule.
func (s *PolicyService) CreateJobRiskRule(ctx context.Context, req *v1.CreateJobRiskRuleRequest) (*v1.CreateJobRiskRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.CreateRule(ctx, userID, toRiskRule("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobRiskRuleResponse{Rule: toProtoRiskRule(r)}, nil
}

// UpdateJobRiskRule implements proto JobPolicyService/UpdateJobRiskRule.
func (s *PolicyService) UpdateJobRiskRule(ctx context.Context, req *v1.UpdateJobRiskRuleRequest) (*v1.UpdateJobRiskRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.UpdateRule(ctx, userID, toRiskRule(req.GetId(), req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobRiskRuleResponse{Rule: toProtoRiskRule(r)}, nil
}

// DeleteJobRiskRule implements proto JobPolicyService/DeleteJobRiskRule.
func (s *PolicyService) DeleteJobRiskRule(ctx context.Context, req *v1.DeleteJobRiskRuleRequest) (*v1.DeleteJobRiskRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.DeleteRule(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobRiskRuleResponse{}, nil
}

// TestJobRiskRules implements proto JobPolicyService/TestJobRiskRules.
func (s *PolicyService) TestJobRiskRules(ctx context.Context, req *v1.TestJobRiskRulesRequest) (*v1.TestJobRiskRulesResponse, error) {
	var draft *biz.RiskRule
	if req.GetDraft() != nil {
		draft = toRiskRule("", req.GetDraft())
	}
	hits, err := s.uc.TestRules(ctx, req.GetContent(), draft)
	if err != nil {
		return nil, err
	}
	return &v1.TestJobRiskRulesResponse{Hits: toProtoRiskHits(hits)}, nil
}

func toProtoPolicy(p *biz.Policy) *v1.JobPolicy {
	return &v1.JobPolicy{
		ProtectedEnvs: p.ProtectedEnvs, ApprovalWorkflowKey: p.ApprovalWorkflowKey, AdhocApproval: p.AdhocApproval, MaxHosts: p.MaxHosts,
		Version: p.Version, UpdateTime: timestamppb.New(p.UpdateTime), UpdateUserName: p.UpdateUserName,
		ApprovalWorkflowName: p.ApprovalWorkflowName, ApprovalWorkflowReady: p.ApprovalWorkflowReady,
	}
}

// GetJobPolicy implements proto JobPolicyService/GetJobPolicy.
func (s *PolicyService) GetJobPolicy(ctx context.Context, _ *v1.GetJobPolicyRequest) (*v1.GetJobPolicyResponse, error) {
	p, err := s.uc.GetPolicy(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetJobPolicyResponse{Policy: toProtoPolicy(p)}, nil
}

// UpdateJobPolicy implements proto JobPolicyService/UpdateJobPolicy.
func (s *PolicyService) UpdateJobPolicy(ctx context.Context, req *v1.UpdateJobPolicyRequest) (*v1.UpdateJobPolicyResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	in := req.GetPolicy()
	p, err := s.uc.UpdatePolicy(ctx, userID, &biz.Policy{
		ProtectedEnvs: in.GetProtectedEnvs(), ApprovalWorkflowKey: in.GetApprovalWorkflowKey(),
		AdhocApproval: in.GetAdhocApproval(), MaxHosts: in.GetMaxHosts(), Version: in.GetVersion(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobPolicyResponse{Policy: toProtoPolicy(p)}, nil
}
