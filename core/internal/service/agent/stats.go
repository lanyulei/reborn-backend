package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
)

// StatsService 实现 AgentStatsServiceHTTPServer。
type StatsService struct {
	uc *biz.StatsUsecase
}

// NewStatsService new a StatsService.
func NewStatsService(uc *biz.StatsUsecase) *StatsService { return &StatsService{uc: uc} }

// GetAgentOverview implements proto AgentStatsService/GetAgentOverview.
func (s *StatsService) GetAgentOverview(ctx context.Context, req *v1.GetAgentOverviewRequest) (*v1.GetAgentOverviewResponse, error) {
	o, err := s.uc.Overview(ctx, biz.StatsRange{Start: optTime(req.StartTime), End: optTime(req.EndTime), ProfileID: req.GetProfileId()})
	if err != nil {
		return nil, err
	}
	out := &v1.GetAgentOverviewResponse{
		Runs: o.Runs, Succeeded: o.Succeeded, NoFinding: o.NoFinding, Failed: o.Failed, AvgDurationS: o.AvgDurationS, P95DurationS: o.P95DurationS,
		PromptTokens: o.PromptTokens, CompletionTokens: o.CompletionTokens, Proposals: o.Proposals, AutoEligible: o.AutoEligible, Helpful: o.Helpful, Unhelpful: o.Unhelpful,
		TodayTasks: o.TodayTasks, TodayPromptTokens: o.TodayPromptTokens, DailyPromptBudget: o.DailyPromptBudget,
	}
	for _, status := range []string{biz.TaskQueued, biz.TaskRunning, biz.TaskSucceeded, biz.TaskNoFinding, biz.TaskFailed, biz.TaskCancelled, biz.TaskSkipped, biz.TaskExpired} {
		out.TasksByStatus = append(out.TasksByStatus, &v1.StatusCount{Status: status, Count: o.TasksByStatus[status]})
	}
	for _, p := range o.ByProfile {
		out.ByProfile = append(out.ByProfile, &v1.ProfileStat{
			Profile: &v1.AgentNamedRef{Id: p.ProfileID, Name: p.ProfileName, Missing: p.ProfileName == ""}, Tasks: p.Tasks, Succeeded: p.Succeeded, NoFinding: p.NoFinding, Failed: p.Failed,
			Helpful: p.Helpful, Unhelpful: p.Unhelpful, PromptTokens: p.PromptTokens, CompletionTokens: p.CompletionTokens, AvgDurationS: p.AvgDurationS,
		})
	}
	return out, nil
}
