package biz

import (
	"context"
	"time"
)

// 统计（功能设计 §12.1）：演练与已合并的不计；范围 ≤ 366 天。

const statsMaxRange = 366 * 24 * time.Hour

// StatsUsecase 统计。
type StatsUsecase struct {
	d        *Deps
	settings *SettingsUsecase
	now      func() time.Time
}

// NewStatsUsecase new a StatsUsecase.
func NewStatsUsecase(d *Deps, settings *SettingsUsecase) *StatsUsecase {
	return &StatsUsecase{d: d, settings: settings, now: time.Now}
}

// normalizeRange 缺省近 30 天；校验范围。
func (uc *StatsUsecase) normalizeRange(start, end *time.Time, businessID string) (StatsRange, error) {
	now := uc.now()
	r := StatsRange{Start: now.Add(-30 * 24 * time.Hour), End: now, BusinessID: businessID}
	if start != nil {
		r.Start = *start
	}
	if end != nil {
		r.End = *end
	}
	if !r.Start.Before(r.End) || r.End.Sub(r.Start) > statsMaxRange {
		return r, detail(ErrTimeRangeInvalid, "time range must be positive and within 366 days")
	}
	return r, nil
}

// Overview 概览。
func (uc *StatsUsecase) Overview(ctx context.Context, businessID string) (*OverviewStats, error) {
	return uc.d.Stats.Overview(ctx, businessID, uc.now())
}

// Trend 趋势。
func (uc *StatsUsecase) Trend(ctx context.Context, start, end *time.Time, businessID, bucket string) ([]*TrendPoint, error) {
	r, err := uc.normalizeRange(start, end, businessID)
	if err != nil {
		return nil, err
	}
	if bucket == "" {
		bucket = "day"
	}
	return uc.d.Stats.Trend(ctx, r, bucket)
}

// Efficiency 效率指标。
type Efficiency struct {
	Rows          []*EfficiencyRow
	ReopenRate    float32
	ReviewOverdue float32
	ItemDoneRate  float32
	ItemTotal     int32
	ItemDone      int32
}

// Efficiency MTTD / MTTA / MTTM / MTTR、重开率、复盘超期率、改进项完成率。
func (uc *StatsUsecase) Efficiency(ctx context.Context, start, end *time.Time, businessID string) (*Efficiency, error) {
	r, err := uc.normalizeRange(start, end, businessID)
	if err != nil {
		return nil, err
	}
	rows, err := uc.d.Stats.Efficiency(ctx, r)
	if err != nil {
		return nil, err
	}
	out := &Efficiency{Rows: rows}
	for _, row := range rows {
		if row.Severity == 0 && row.Count > 0 {
			out.ReopenRate = float32(row.Reopened) / float32(row.Count)
		}
	}
	required, overdue, err := uc.d.Stats.ReviewOverdue(ctx, r, uc.now())
	if err != nil {
		return nil, err
	}
	if required > 0 {
		out.ReviewOverdue = float32(overdue) / float32(required)
	}
	total, done, err := uc.d.Items.Counts(ctx, r)
	if err != nil {
		return nil, err
	}
	out.ItemTotal, out.ItemDone = i32(total), i32(done)
	if total > 0 {
		out.ItemDoneRate = float32(done) / float32(total)
	}
	return out, nil
}

// Distribution 分布与转化率。
type Distribution struct {
	*DistributionStats
	CandidateTotal    int32
	CandidatePromoted int32
}

// Distribution 根因 / 来源 / 业务 / 规则分布、候选转化率、关联告警数。
func (uc *StatsUsecase) Distribution(ctx context.Context, start, end *time.Time, businessID string) (*Distribution, error) {
	r, err := uc.normalizeRange(start, end, businessID)
	if err != nil {
		return nil, err
	}
	d, err := uc.d.Stats.Distribution(ctx, r)
	if err != nil {
		return nil, err
	}
	settings, err := uc.settings.Get(ctx)
	if err != nil {
		return nil, err
	}
	for i := range d.RootCauses {
		if d.RootCauses[i].Name == "" {
			d.RootCauses[i].Name = settings.RootCauseName(d.RootCauses[i].Key)
			if d.RootCauses[i].Key == "" {
				d.RootCauses[i].Name = "未填写"
			}
		}
	}
	for i := range d.Sources {
		d.Sources[i].Name = sourceLabel(d.Sources[i].Key)
	}
	total, promoted, err := uc.d.Candidates.Counts(ctx, r)
	if err != nil {
		return nil, err
	}
	return &Distribution{DistributionStats: d, CandidateTotal: i32(total), CandidatePromoted: i32(promoted)}, nil
}

func sourceLabel(key string) string {
	switch key {
	case "1":
		return "手动"
	case "2":
		return "触发规则"
	case "3":
		return "开放接口"
	case "4":
		return "Agent"
	}
	return key
}
