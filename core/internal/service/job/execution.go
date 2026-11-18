package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecutionService 实现 JobExecutionServiceHTTPServer。
type ExecutionService struct {
	uc *biz.ExecutionUsecase
}

// NewExecutionService new an ExecutionService.
func NewExecutionService(uc *biz.ExecutionUsecase) *ExecutionService {
	return &ExecutionService{uc: uc}
}

// toProtoExecution withDetail 为真时带上提交时固化的执行内容（仅详情接口）。
func toProtoExecution(e *biz.Execution, withDetail bool) (*v1.JobExecution, error) {
	out := &v1.JobExecution{
		Id: e.ID, TemplateId: e.TemplateID, TemplateName: e.TemplateName, TemplateVersion: e.TemplateVersion,
		Trigger: e.Trigger, CreatorId: e.Creator, CreatorName: e.CreatorName, Status: e.Status, Error: e.Error,
		HostTotal: e.HostTotal, HostOk: e.HostOK, HostFailed: e.HostFailed, HostUnreachable: e.HostUnreachable,
		CancelRequested: e.CancelRequested, CancelUserId: e.CancelUser, CancelUserName: e.CancelUserName,
		RunnerId: e.RunnerID, RunnerName: e.RunnerName, CommitId: e.CommitID,
		CreateTime: timestamppb.New(e.CreateTime), StartTime: protoTime(e.StartTime), FinishTime: protoTime(e.FinishTime),
		Check: e.Spec.Options.Check, Kind: e.Kind, TriggerRef: e.TriggerRef, TriggerName: e.TriggerName,
		GroupId: e.GroupID, SliceNo: e.SliceNo, SliceTotal: e.SliceTotal,
		ApprovalReason: e.ApprovalReason, ApprovalInstanceId: e.ApprovalInstanceID, RiskHits: toProtoRiskHits(e.RiskHits),
	}
	if !withDetail {
		return out, nil
	}
	params, err := structpb.NewStruct(e.Spec.ExtraVars)
	if err != nil {
		return nil, err
	}
	out.Detail = &v1.JobExecutionDetail{
		ProjectName: e.Spec.ProjectName, RepoUrl: e.Spec.RepoURL, Ref: e.Spec.Ref, Playbook: e.Spec.Playbook,
		CredentialName: e.Spec.CredentialName, Params: params, Options: toProtoOptions(e.Spec.Options),
		RunnerZone: e.RunnerZone, LogTruncated: e.LogTruncated, CredentialId: e.Spec.CredentialID,
	}
	if a := e.Spec.Adhoc; a != nil {
		out.Detail.Adhoc = &v1.JobAdhoc{
			Command: a.Command, Script: a.Script, Interpreter: a.Interpreter, ScriptArgs: a.ScriptArgs,
			ScriptId: a.ScriptID, ScriptName: a.ScriptName, ScriptVersion: a.ScriptVersion,
			FileName: a.FileName, Dest: a.Dest, Mode: a.Mode, FileSize: a.FileSize, FileSha256: a.FileSHA256,
		}
	}
	if e.Spec.Target != nil {
		out.Detail.Target = toProtoTarget(*e.Spec.Target)
	}
	return out, nil
}

// SubmitJobExecution implements proto JobExecutionService/SubmitJobExecution.
func (s *ExecutionService) SubmitJobExecution(ctx context.Context, req *v1.SubmitJobExecutionRequest) (*v1.SubmitJobExecutionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	e, err := s.uc.Submit(ctx, userID, biz.SubmitInput{
		TemplateID: req.GetTemplateId(), Params: req.GetParams().AsMap(), CIIDs: req.GetCiIds(),
		Check: req.Check, RequestID: req.GetRequestId(), TemplateVersion: req.GetTemplateVersion(),
	})
	if err != nil {
		return nil, err
	}
	out, err := toProtoExecution(e, true)
	if err != nil {
		return nil, err
	}
	return &v1.SubmitJobExecutionResponse{Execution: out}, nil
}

// ListJobExecutions implements proto JobExecutionService/ListJobExecutions.
func (s *ExecutionService) ListJobExecutions(ctx context.Context, req *v1.ListJobExecutionsRequest) (*v1.ListJobExecutionsResponse, error) {
	filter := biz.ExecutionFilter{
		TemplateID: req.GetTemplateId(), Statuses: req.GetStatuses(), Creator: req.GetCreatorId(), Keyword: req.GetKeyword(),
		StartTime: optTime(req.GetStartTime()), EndTime: optTime(req.GetEndTime()), CIID: req.GetCiId(),
		Kinds: req.GetKinds(), Triggers: req.GetTriggers(), GroupID: req.GetGroupId(), TriggerRef: req.GetTriggerRef(),
	}
	if req.GetMine() {
		userID, err := systemservice.UserIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		filter.Creator = userID
	}
	items, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobExecutionsResponse{Items: make([]*v1.JobExecution, 0, len(items)), Total: total}
	for _, e := range items {
		out, err := toProtoExecution(e, false)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, out)
	}
	return resp, nil
}

// GetJobExecution implements proto JobExecutionService/GetJobExecution.
func (s *ExecutionService) GetJobExecution(ctx context.Context, req *v1.GetJobExecutionRequest) (*v1.GetJobExecutionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RequireVisible(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	e, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	out, err := toProtoExecution(e, true)
	if err != nil {
		return nil, err
	}
	return &v1.GetJobExecutionResponse{Execution: out}, nil
}

// ListJobExecutionHosts implements proto JobExecutionService/ListJobExecutionHosts.
func (s *ExecutionService) ListJobExecutionHosts(ctx context.Context, req *v1.ListJobExecutionHostsRequest) (*v1.ListJobExecutionHostsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RequireVisible(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	filter := biz.ExecutionHostFilter{Statuses: req.GetStatuses(), Keyword: req.GetKeyword()}
	items, total, err := s.uc.ListHosts(ctx, req.GetId(), filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListJobExecutionHostsResponse{Items: make([]*v1.JobExecutionHost, 0, len(items)), Total: total}
	for _, h := range items {
		resp.Items = append(resp.Items, &v1.JobExecutionHost{
			Seq: h.Seq, CiId: h.CIID, Name: h.Name, Env: h.Env, Address: h.Address, Port: h.Port, Status: h.Status,
			Ok: h.OK, Changed: h.Changed, Failed: h.Failed, Unreachable: h.Unreachable, Skipped: h.Skipped,
		})
	}
	return resp, nil
}

// GetJobExecutionLog implements proto JobExecutionService/GetJobExecutionLog.
func (s *ExecutionService) GetJobExecutionLog(ctx context.Context, req *v1.GetJobExecutionLogRequest) (*v1.GetJobExecutionLogResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.RequireVisible(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	page, err := s.uc.Log(ctx, req.GetId(), req.GetAfterSeq())
	if err != nil {
		return nil, err
	}
	return &v1.GetJobExecutionLogResponse{
		Content: page.Content, LastSeq: page.LastSeq, More: page.More, Status: page.Status, Truncated: page.Truncated,
	}, nil
}

// CancelJobExecution implements proto JobExecutionService/CancelJobExecution.
func (s *ExecutionService) CancelJobExecution(ctx context.Context, req *v1.CancelJobExecutionRequest) (*v1.CancelJobExecutionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	e, err := s.uc.Cancel(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	out, err := toProtoExecution(e, true)
	if err != nil {
		return nil, err
	}
	return &v1.CancelJobExecutionResponse{Execution: out}, nil
}

func toProtoRiskHits(hits []jc.RiskHit) []*v1.JobRiskHit {
	out := make([]*v1.JobRiskHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, &v1.JobRiskHit{RuleId: h.RuleID, Name: h.Name, Action: h.Action, Description: h.Description, Match: h.Match})
	}
	return out
}

func toAdhocInput(kind int32, a *v1.JobAdhoc, target *v1.JobTarget) biz.AdhocInput {
	return biz.AdhocInput{
		Kind: kind,
		Adhoc: jc.Adhoc{
			Command: a.GetCommand(), Script: a.GetScript(), Interpreter: a.GetInterpreter(), ScriptArgs: a.GetScriptArgs(),
			ScriptID: a.GetScriptId(), FileName: a.GetFileName(), Dest: a.GetDest(), Mode: a.GetMode(),
		},
		FileID: a.GetFileId(), Target: toTarget(target),
	}
}

// SubmitJobAdhoc implements proto JobExecutionService/SubmitJobAdhoc.
func (s *ExecutionService) SubmitJobAdhoc(ctx context.Context, req *v1.SubmitJobAdhocRequest) (*v1.SubmitJobAdhocResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	in := toAdhocInput(req.GetKind(), req.GetAdhoc(), req.GetTarget())
	in.CredentialID, in.Options, in.RunnerZone, in.RequestID = req.GetCredentialId(), toOptions(req.GetOptions()), req.GetRunnerZone(), req.GetRequestId()
	e, err := s.uc.SubmitAdhoc(ctx, userID, in)
	if err != nil {
		return nil, err
	}
	out, err := toProtoExecution(e, true)
	if err != nil {
		return nil, err
	}
	return &v1.SubmitJobAdhocResponse{Execution: out}, nil
}

// CheckJobAdhoc implements proto JobExecutionService/CheckJobAdhoc.
func (s *ExecutionService) CheckJobAdhoc(ctx context.Context, req *v1.CheckJobAdhocRequest) (*v1.CheckJobAdhocResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pf, err := s.uc.CheckAdhoc(ctx, userID, toAdhocInput(req.GetKind(), req.GetAdhoc(), req.GetTarget()))
	if err != nil {
		return nil, err
	}
	return &v1.CheckJobAdhocResponse{
		Hosts: toProtoTargetHosts(pf.Hosts), Truncated: pf.Truncated, RiskHits: toProtoRiskHits(pf.RiskHits), Blocked: pf.Blocked,
		ApprovalReasons: pf.ApprovalReasons, ApprovalReady: pf.ApprovalReady,
		MaxHosts: int32(pf.MaxHosts), //nolint:gosec // 至多 jobcontract.MaxHosts
	}, nil
}

func toProtoStatCounts(items []biz.StatCount) []*v1.JobStatCount {
	out := make([]*v1.JobStatCount, 0, len(items))
	for _, c := range items {
		out = append(out, &v1.JobStatCount{Key: c.Key, Name: c.Name, Count: c.Count, Failed: c.Failed})
	}
	return out
}

// GetJobStats implements proto JobExecutionService/GetJobStats.
func (s *ExecutionService) GetJobStats(ctx context.Context, req *v1.GetJobStatsRequest) (*v1.GetJobStatsResponse, error) {
	st, err := s.uc.Stats(ctx, req.GetDays(), req.GetTimezone())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetJobStatsResponse{
		Total: st.Total, Finished: st.Finished, Success: st.Success, AvgDurationSeconds: st.AvgDurationSeconds, HostRuns: st.HostRuns,
		ByStatus: toProtoStatCounts(st.ByStatus), ByKind: toProtoStatCounts(st.ByKind), ByTrigger: toProtoStatCounts(st.ByTrigger),
		TopTemplates: toProtoStatCounts(st.TopTemplates), Timezone: st.Timezone,
		Active: st.Active, RunnersOnline: st.RunnersOnline, RunnersTotal: st.RunnersTotal,
		Trend: make([]*v1.JobStatTrendPoint, 0, len(st.Trend)),
	}
	for _, p := range st.Trend {
		resp.Trend = append(resp.Trend, &v1.JobStatTrendPoint{Day: p.Day, Total: p.Total, Success: p.Success, Failed: p.Failed})
	}
	return resp, nil
}
