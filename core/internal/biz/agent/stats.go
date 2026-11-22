package biz

import (
	"context"
	"time"
)

// maxStatsDays 统计时间范围上限（天）。
const maxStatsDays = 90

// StatsUsecase 统计：任务按终态、运行耗时、token 用量、申请自动的建议数、有用率、按 Profile 分布、今日预算。
type StatsUsecase struct {
	d   *Deps
	now func() time.Time
}

// NewStatsUsecase new a StatsUsecase.
func NewStatsUsecase(d *Deps) *StatsUsecase { return &StatsUsecase{d: d, now: time.Now} }

// Overview 概览；start / end 零值取近 7 天 / 现在。
func (uc *StatsUsecase) Overview(ctx context.Context, r StatsRange) (*Overview, error) {
	now := uc.now()
	if r.End.IsZero() {
		r.End = now
	}
	if r.Start.IsZero() {
		r.Start = r.End.AddDate(0, 0, -7)
	}
	if !r.End.After(r.Start) || r.End.Sub(r.Start) > maxStatsDays*24*time.Hour {
		return nil, ErrTimeRangeInvalid
	}
	byStatus, err := uc.d.Tasks.CountByStatus(ctx, r)
	if err != nil {
		return nil, err
	}
	out, byProfile, err := uc.d.Runs.Stats(ctx, r)
	if err != nil {
		return nil, err
	}
	out.TasksByStatus = byStatus
	out.ByProfile = byProfile
	today, err := uc.d.Budgets.Total(ctx, dayKey(now))
	if err != nil {
		return nil, err
	}
	out.TodayTasks, out.TodayPromptTokens, out.DailyPromptBudget = today.Tasks, today.PromptTokens, uc.d.Config.DailyPromptBudget
	return out, nil
}
