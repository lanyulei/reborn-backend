package service

import (
	"context"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StatsService 实现 IncidentStatsServiceHTTPServer。
type StatsService struct {
	uc *biz.StatsUsecase
}

// NewStatsService new a StatsService.
func NewStatsService(uc *biz.StatsUsecase) *StatsService { return &StatsService{uc: uc} }

// GetOverview implements proto IncidentStatsService/GetOverview.
func (s *StatsService) GetOverview(ctx context.Context, req *v1.StatsRangeRequest) (*v1.GetOverviewResponse, error) {
	o, err := s.uc.Overview(ctx, req.GetBusinessId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetOverviewResponse{
		Active: o.Active, Unresponded: o.Unresponded, ReviewPending: o.ReviewPending, ReviewOverdue: o.ReviewOverdue, ProposalPending: o.ProposalPending,
		TodayOpened: o.TodayOpened, TodayResolved: o.TodayResolved, Candidates: o.Candidates,
	}
	for sev := int32(1); sev <= 4; sev++ {
		out.ActiveBySeverity = append(out.ActiveBySeverity, &v1.SeverityCount{Severity: sev, Count: o.ActiveBySev[sev]})
	}
	return out, nil
}

// GetTrend implements proto IncidentStatsService/GetTrend.
func (s *StatsService) GetTrend(ctx context.Context, req *v1.StatsRangeRequest) (*v1.GetTrendResponse, error) {
	points, err := s.uc.Trend(ctx, optTime(req.StartTime), optTime(req.EndTime), req.GetBusinessId(), req.GetBucket())
	if err != nil {
		return nil, err
	}
	out := &v1.GetTrendResponse{Points: make([]*v1.TrendPoint, 0, len(points))}
	for _, p := range points {
		tp := &v1.TrendPoint{Bucket: p.Bucket.UTC().Format(time.RFC3339), Total: p.Total, DurationS: p.Duration}
		for sev := int32(1); sev <= 4; sev++ {
			tp.BySeverity = append(tp.BySeverity, &v1.SeverityCount{Severity: sev, Count: p.BySev[sev]})
		}
		out.Points = append(out.Points, tp)
	}
	return out, nil
}

// GetEfficiency implements proto IncidentStatsService/GetEfficiency.
func (s *StatsService) GetEfficiency(ctx context.Context, req *v1.StatsRangeRequest) (*v1.GetEfficiencyResponse, error) {
	e, err := s.uc.Efficiency(ctx, optTime(req.StartTime), optTime(req.EndTime), req.GetBusinessId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetEfficiencyResponse{ReopenRate: e.ReopenRate, ReviewOverdueRate: e.ReviewOverdue, ActionItemDoneRate: e.ItemDoneRate, ActionItemTotal: e.ItemTotal, ActionItemDone: e.ItemDone}
	for _, r := range e.Rows {
		out.Rows = append(out.Rows, &v1.EfficiencyRow{Severity: r.Severity, Count: r.Count, MttdS: r.MTTD, MttaS: r.MTTA, MttmS: r.MTTM, MttrS: r.MTTR})
	}
	return out, nil
}

func toProtoCounts(items []biz.NamedCount) []*v1.NamedCount {
	out := make([]*v1.NamedCount, 0, len(items))
	for _, it := range items {
		out = append(out, &v1.NamedCount{Key: it.Key, Name: it.Name, Count: it.Count, DurationS: it.Duration})
	}
	return out
}

// GetDistribution implements proto IncidentStatsService/GetDistribution.
func (s *StatsService) GetDistribution(ctx context.Context, req *v1.StatsRangeRequest) (*v1.GetDistributionResponse, error) {
	d, err := s.uc.Distribution(ctx, optTime(req.StartTime), optTime(req.EndTime), req.GetBusinessId())
	if err != nil {
		return nil, err
	}
	return &v1.GetDistributionResponse{
		RootCauses: toProtoCounts(d.RootCauses), Sources: toProtoCounts(d.Sources), Businesses: toProtoCounts(d.Businesses), Rules: toProtoCounts(d.Rules),
		CandidateTotal: d.CandidateTotal, CandidatePromoted: d.CandidatePromoted, AlertLinked: d.AlertLinked,
	}, nil
}

// ---------------------------------------------------------------------------
// 开放接口
// ---------------------------------------------------------------------------

// OpenService 实现 IncidentOpenServiceHTTPServer（Bearer 令牌即凭据）。
type OpenService struct {
	uc   *biz.OpenUsecase
	site biz.SiteURL
}

// NewOpenService new an OpenService.
func NewOpenService(uc *biz.OpenUsecase, site biz.SiteURL) *OpenService {
	return &OpenService{uc: uc, site: site}
}

// bearer 取请求头 Authorization: Bearer <令牌> 里的令牌；没有返回空串（由用例统一按未认证处理）。
func bearer(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	scheme, token, found := strings.Cut(tr.RequestHeader().Get("Authorization"), " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func (s *OpenService) resp(r *biz.OpenResult) *v1.OpenIncidentResponse {
	in := r.Incident
	return &v1.OpenIncidentResponse{
		Id: in.ID, Number: in.Number, Title: in.Title, Severity: in.Severity, Status: in.Status, OpenTime: timestamppb.New(in.OpenTime),
		ResolveTime: protoTime(in.ResolveTime), Created: r.Created, Url: s.site.IncidentURL(in.ID),
	}
}

// OpenCreateIncident implements proto IncidentOpenService/OpenCreateIncident.
func (s *OpenService) OpenCreateIncident(ctx context.Context, req *v1.OpenCreateIncidentRequest) (*v1.OpenIncidentResponse, error) {
	r, err := s.uc.Create(ctx, bearer(ctx), systemservice.ClientIP(ctx), biz.OpenCreateInput{
		ExternalKey: req.GetExternalKey(), Title: req.GetTitle(), Severity: req.GetSeverity(), Summary: req.GetSummary(), BusinessCode: req.GetBusinessCode(),
		OccurTime: optTime(req.OccurTime), CINames: req.GetCiNames(),
	})
	if err != nil {
		return nil, err
	}
	return s.resp(r), nil
}

// OpenGetIncident implements proto IncidentOpenService/OpenGetIncident.
func (s *OpenService) OpenGetIncident(ctx context.Context, req *v1.OpenGetIncidentRequest) (*v1.OpenIncidentResponse, error) {
	r, err := s.uc.Get(ctx, bearer(ctx), systemservice.ClientIP(ctx), req.GetId())
	if err != nil {
		return nil, err
	}
	return s.resp(r), nil
}

// OpenAppendNote implements proto IncidentOpenService/OpenAppendNote.
func (s *OpenService) OpenAppendNote(ctx context.Context, req *v1.OpenAppendNoteRequest) (*v1.OpenIncidentResponse, error) {
	r, err := s.uc.Note(ctx, bearer(ctx), systemservice.ClientIP(ctx), req.GetId(), req.GetText())
	if err != nil {
		return nil, err
	}
	return s.resp(r), nil
}

// OpenResolveIncident implements proto IncidentOpenService/OpenResolveIncident.
func (s *OpenService) OpenResolveIncident(ctx context.Context, req *v1.OpenResolveIncidentRequest) (*v1.OpenIncidentResponse, error) {
	r, err := s.uc.Resolve(ctx, bearer(ctx), systemservice.ClientIP(ctx), req.GetId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return s.resp(r), nil
}
