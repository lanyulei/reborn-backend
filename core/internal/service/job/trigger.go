package service

import (
	"context"
	"encoding/json"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TriggerService 实现 JobTriggerServiceHTTPServer，并承载开放触发接口（原生路由，见 server/job.go）。
type TriggerService struct {
	uc *biz.TriggerUsecase
}

// NewTriggerService new a TriggerService.
func NewTriggerService(uc *biz.TriggerUsecase) *TriggerService { return &TriggerService{uc: uc} }

func toTrigger(id string, spec *v1.JobTriggerSpec) *biz.Trigger {
	a := spec.GetAlert()
	return &biz.Trigger{
		ID: id, Name: spec.GetName(), Type: spec.GetType(), TemplateID: spec.GetTemplateId(), Params: spec.GetParams().AsMap(),
		PassParams: spec.GetPassParams(), Signal: spec.GetSignal(),
		Alert:     jc.AlertMatch{RuleIDs: a.GetRuleIds(), Severities: a.GetSeverities(), Labels: a.GetLabels(), HostLabel: a.GetHostLabel()},
		CooldownS: spec.GetCooldownS(), Enabled: spec.GetEnabled(), Description: spec.GetDescription(),
		TemplateVersion: spec.GetTemplateVersion(),
	}
}

func toProtoTrigger(t *biz.Trigger) (*v1.JobTrigger, error) {
	params, err := structpb.NewStruct(t.Params)
	if err != nil {
		return nil, err
	}
	spec := &v1.JobTriggerSpec{
		Name: t.Name, Type: t.Type, TemplateId: t.TemplateID, Params: params, PassParams: t.PassParams, Signal: t.Signal,
		CooldownS: t.CooldownS, Enabled: t.Enabled, Description: t.Description, TemplateVersion: t.TemplateVersion,
	}
	if t.Type == jc.TriggerTypeAlert {
		spec.Alert = &v1.JobAlertMatch{RuleIds: t.Alert.RuleIDs, Severities: t.Alert.Severities, Labels: t.Alert.Labels, HostLabel: t.Alert.HostLabel}
	}
	return &v1.JobTrigger{
		Id: t.ID, Spec: spec, TemplateName: t.TemplateName, OwnerId: t.Owner, OwnerName: t.OwnerName,
		LastFireTime: protoTime(t.LastFireTime), Version: t.Version,
		CreateTime: timestamppb.New(t.CreateTime), UpdateTime: timestamppb.New(t.UpdateTime), RuleNames: t.RuleNames,
	}, nil
}

// ListJobTriggers implements proto JobTriggerService/ListJobTriggers.
func (s *TriggerService) ListJobTriggers(ctx context.Context, req *v1.ListJobTriggersRequest) (*v1.ListJobTriggersResponse, error) {
	filter := biz.TriggerFilter{Keyword: req.GetKeyword(), Type: req.Type, TemplateID: req.GetTemplateId()}
	items, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobTriggersResponse{Items: make([]*v1.JobTrigger, 0, len(items)), Total: total}
	for _, it := range items {
		out, err := toProtoTrigger(it)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, out)
	}
	return resp, nil
}

// CreateJobTrigger implements proto JobTriggerService/CreateJobTrigger。令牌明文只在这里返回一次。
func (s *TriggerService) CreateJobTrigger(ctx context.Context, req *v1.CreateJobTriggerRequest) (*v1.CreateJobTriggerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, token, err := s.uc.Create(ctx, userID, toTrigger("", req.GetSpec()))
	if err != nil {
		return nil, err
	}
	out, err := toProtoTrigger(it)
	if err != nil {
		return nil, err
	}
	return &v1.CreateJobTriggerResponse{Trigger: out, Token: token}, nil
}

// UpdateJobTrigger implements proto JobTriggerService/UpdateJobTrigger.
func (s *TriggerService) UpdateJobTrigger(ctx context.Context, req *v1.UpdateJobTriggerRequest) (*v1.UpdateJobTriggerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	it, err := s.uc.Update(ctx, userID, toTrigger(req.GetId(), req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	out, err := toProtoTrigger(it)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateJobTriggerResponse{Trigger: out}, nil
}

// ResetJobTriggerToken implements proto JobTriggerService/ResetJobTriggerToken.
func (s *TriggerService) ResetJobTriggerToken(ctx context.Context, req *v1.ResetJobTriggerTokenRequest) (*v1.ResetJobTriggerTokenResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	token, err := s.uc.ResetToken(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.ResetJobTriggerTokenResponse{Token: token}, nil
}

// DeleteJobTrigger implements proto JobTriggerService/DeleteJobTrigger.
func (s *TriggerService) DeleteJobTrigger(ctx context.Context, req *v1.DeleteJobTriggerRequest) (*v1.DeleteJobTriggerResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteJobTriggerResponse{}, nil
}

// ListJobTriggerFires implements proto JobTriggerService/ListJobTriggerFires.
func (s *TriggerService) ListJobTriggerFires(ctx context.Context, req *v1.ListJobTriggerFiresRequest) (*v1.ListJobTriggerFiresResponse, error) {
	items, total, err := s.uc.ListFires(ctx, biz.FireFilter{TriggerID: req.GetTriggerId(), Result: req.Result}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobTriggerFiresResponse{Items: make([]*v1.JobTriggerFire, 0, len(items)), Total: total}
	for _, f := range items {
		resp.Items = append(resp.Items, &v1.JobTriggerFire{
			Id: f.ID, TriggerId: f.TriggerID, TriggerName: f.TriggerName, Result: f.Result, Reason: f.Reason,
			ExecutionId: f.ExecutionID, Summary: f.Summary, WorkflowInstanceId: f.WorkflowInstanceID,
			SignalState: f.SignalState, CreateTime: timestamppb.New(f.CreateTime),
		})
	}
	return resp, nil
}

// OpenFireHeaders 开放触发接口用到的请求头。
type OpenFireHeaders struct {
	Authorization string
	Timestamp     string
	Signature     string
	// WorkflowJobID 流程服务节点的作业 ID（X-Workflow-Job-Id）：没有显式幂等键时用它去重。
	WorkflowJobID string
}

// OpenFireResult 开放触发接口的应答。variables 供流程服务节点合并进流程变量。
type OpenFireResult struct {
	ExecutionID string         `json:"executionId"`
	Result      int32          `json:"result"`
	Reason      string         `json:"reason,omitempty"`
	Variables   map[string]any `json:"variables"`
}

// openFireBody 开放触发接口的请求体：显式字段，加上流程 Webhook 请求体里用得到的字段。
type openFireBody struct {
	RequestID  string         `json:"requestId"`
	Params     map[string]any `json:"params"`
	InstanceID string         `json:"instanceId"`
	// 以下来自流程服务节点的请求体。
	JobID     string         `json:"jobId"`
	Title     string         `json:"title"`
	Variables map[string]any `json:"variables"`
}

// OpenFire 处理一次开放触发调用。body 为原始请求体（签名对它计算）。
func (s *TriggerService) OpenFire(ctx context.Context, triggerID string, h OpenFireHeaders, body []byte) (*OpenFireResult, error) {
	var in openFireBody
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, biz.ErrTriggerInvalid
		}
	}
	auth := biz.FireAuth{Timestamp: h.Timestamp, Signature: h.Signature, Body: body}
	if scheme, token, ok := strings.Cut(h.Authorization, " "); ok && strings.EqualFold(scheme, "Bearer") {
		auth.Bearer = strings.TrimSpace(token)
	}
	ref := in.RequestID
	if ref == "" {
		ref = in.JobID
	}
	// 请求头里的作业 ID 不在签名范围内：只有令牌认证时才用它去重（签名认证时以请求体里的为准）。
	if ref == "" && auth.Bearer != "" {
		ref = h.WorkflowJobID
	}
	fire, err := s.uc.Fire(ctx, triggerID, auth, biz.FireRequest{
		Ref: ref, Params: in.Params, Variables: in.Variables, InstanceID: in.InstanceID, Summary: in.Title,
	})
	if err != nil {
		return nil, err
	}
	out := &OpenFireResult{ExecutionID: fire.ExecutionID, Result: fire.Result, Reason: fire.Reason, Variables: map[string]any{}}
	if fire.ExecutionID != "" {
		out.Variables["jobExecutionId"] = fire.ExecutionID
	}
	return out, nil
}
