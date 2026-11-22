// Package service 实现运维 Agent 域的 proto 服务接口：参数编排、模型转换与错误透传，业务规则全部在 biz。
package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is 运维 Agent 域 service providers。
var ProviderSet = wire.NewSet(
	NewProfileService, NewTaskService, NewStatsService,
	wire.Struct(new(Services), "*"),
)

// Services 本域全部服务：合成一组交给 server.NewHTTPServer 注册。
type Services struct {
	Profile *ProfileService
	Task    *TaskService
	Stats   *StatsService
}

func userID(ctx context.Context) (string, error) { return systemservice.UserIDFromContext(ctx) }

func protoTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil || t.IsZero() {
		return nil
	}
	return timestamppb.New(*t)
}

func optTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func named(id, name string) *v1.AgentNamedRef {
	if id == "" {
		return nil
	}
	return &v1.AgentNamedRef{Id: id, Name: name, Missing: name == ""}
}

func toProtoSpec(s *biz.ProfileSpec) *v1.ProfileSpec {
	out := &v1.ProfileSpec{
		Name: s.Name, Description: s.Description, Kind: s.Kind, AgentUserId: s.AgentUserID, Priority: s.Priority, Enabled: s.Enabled,
		Severities: s.Severities, BusinessIds: s.BusinessIDs, HourlyLimit: s.HourlyLimit, KnowledgeSpaceId: s.KnowledgeSpaceID,
		Budget: &v1.BudgetSpec{MaxSteps: s.Budget.MaxSteps, TimeoutS: s.Budget.TimeoutS, PromptTokens: s.Budget.PromptTokens, CompletionTokens: s.Budget.CompletionTokens},
		Auto:   &v1.AutoSpec{Enabled: s.Auto.Enabled, MinConfidence: s.Auto.MinConfidence, RequireMetricEvidence: s.Auto.RequireMetricEvidence, MaxTargets: s.Auto.MaxTargets, Envs: s.Auto.Envs},
	}
	for _, st := range s.Stages {
		out.Stages = append(out.Stages, &v1.StageSpec{Key: st.Key, MaxSteps: st.MaxSteps, Tools: st.Tools})
	}
	return out
}

func toSpec(in *v1.ProfileSpec) biz.ProfileSpec {
	s := biz.ProfileSpec{
		Name: in.GetName(), Description: in.GetDescription(), Kind: in.GetKind(), AgentUserID: in.GetAgentUserId(), Priority: in.GetPriority(), Enabled: in.GetEnabled(),
		Severities: in.GetSeverities(), BusinessIDs: in.GetBusinessIds(), HourlyLimit: in.GetHourlyLimit(), KnowledgeSpaceID: in.GetKnowledgeSpaceId(),
	}
	if b := in.GetBudget(); b != nil {
		s.Budget = biz.BudgetSpec{MaxSteps: b.GetMaxSteps(), TimeoutS: b.GetTimeoutS(), PromptTokens: b.GetPromptTokens(), CompletionTokens: b.GetCompletionTokens()}
	}
	if a := in.GetAuto(); a != nil {
		s.Auto = biz.AutoSpec{Enabled: a.GetEnabled(), MinConfidence: a.GetMinConfidence(), RequireMetricEvidence: a.GetRequireMetricEvidence(), MaxTargets: a.GetMaxTargets(), Envs: a.GetEnvs()}
	}
	for _, st := range in.GetStages() {
		s.Stages = append(s.Stages, biz.StageSpec{Key: st.GetKey(), MaxSteps: st.GetMaxSteps(), Tools: st.GetTools()})
	}
	return s
}

func toProtoProfile(v *biz.ProfileView) *v1.Profile {
	p := v.Profile
	out := &v1.Profile{
		Id: p.ID, Spec: toProtoSpec(&p.Spec), BreakerOpen: p.BreakerOpen, BreakerReason: p.BreakerReason, Version: p.Version,
		CreateTime: timestamppb.New(p.CreateTime), UpdateTime: timestamppb.New(p.UpdateTime), UpdateUser: named(p.UpdateUser, v.UpdateUserName),
	}
	out.AgentUser = &v1.AgentNamedRef{Id: p.Spec.AgentUserID, Name: v.AgentUserName, Missing: v.AgentUserGone}
	return out
}

func toProtoTask(v *biz.TaskView) *v1.Task {
	t := v.Task
	return &v1.Task{
		Id: t.ID, Profile: named(t.ProfileID, v.ProfileName), Kind: t.Kind, Source: t.Source, DryRun: t.DryRun,
		Incident: &v1.AgentNamedRef{Id: t.IncidentID, Name: t.IncidentNumber}, IncidentTitle: t.IncidentTitle, Severity: t.Severity, Status: t.Status,
		Attempts: t.Attempts, Reason: t.Result, RunId: t.RunID, Requester: named(t.RequesterID, v.RequesterName), Input: t.Input,
		QueuedTime: timestamppb.New(t.QueuedTime), StartTime: protoTime(t.StartTime), EndTime: protoTime(t.EndTime), NextTime: queuedNext(t), UpdateTime: timestamppb.New(t.UpdateTime),
	}
}

// queuedNext 排队中的任务才返回下次执行时间。
func queuedNext(t *biz.Task) *timestamppb.Timestamp {
	if t.Status != biz.TaskQueued {
		return nil
	}
	return timestamppb.New(t.NextTime)
}

func toProtoRun(v *biz.RunView) *v1.Run {
	r := v.Run
	out := &v1.Run{
		Id: r.ID, TaskId: r.TaskID, Profile: named(r.ProfileID, v.ProfileName), Incident: &v1.AgentNamedRef{Id: r.IncidentID, Name: v.IncidentNumber},
		AgentUser: named(r.AgentUserID, v.AgentUserName), ConversationId: r.ConversationID, Status: r.Status, DryRun: r.DryRun,
		PromptTokens: r.PromptTokens, CompletionTokens: r.CompletionTokens, TokensEstimated: r.TokensEstimated, FindingEventId: r.FindingEventID, EndReason: r.EndReason,
		Feedback: r.Feedback, FeedbackUser: named(r.FeedbackUser, v.FeedbackUserName), FeedbackNote: r.FeedbackNote, FeedbackTime: protoTime(r.FeedbackTime),
		StartTime: timestamppb.New(r.StartTime), EndTime: protoTime(r.EndTime), CanRate: v.CanRate,
	}
	for _, st := range r.Stages {
		out.Stages = append(out.Stages, &v1.RunStage{
			Key: st.Key, Status: st.Status, Steps: st.Steps, PromptTokens: st.PromptTokens, CompletionTokens: st.CompletionTokens,
			DurationMs: st.DurationMS, Summary: st.Summary, EndReason: st.EndReason, LastMessageId: st.LastMessageID,
		})
	}
	if f := r.Finding; f != nil {
		out.Finding = &v1.RunFinding{Title: f.Title, Conclusion: f.Conclusion, Confidence: f.Confidence, Evidence: f.Evidence, NextStep: f.NextStep}
	}
	for _, p := range r.Proposals {
		out.Proposals = append(out.Proposals, &v1.AgentNamedRef{Id: p.ID, Name: p.Name})
	}
	for _, d := range r.Denied {
		out.Denied = append(out.Denied, &v1.DeniedCall{Tool: d.Tool, Reason: d.Reason, Message: d.Message})
	}
	for _, c := range r.Captured {
		out.Captured = append(out.Captured, &v1.CapturedCall{Tool: c.Tool, Input: c.Input})
	}
	return out
}
