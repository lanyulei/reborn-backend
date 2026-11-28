package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// DatasourceService 实现 MonitorDatasourceServiceHTTPServer。
type DatasourceService struct {
	uc *biz.DatasourceUsecase
}

// NewDatasourceService new a DatasourceService.
func NewDatasourceService(uc *biz.DatasourceUsecase) *DatasourceService {
	return &DatasourceService{uc: uc}
}

func toConnection(c *v1.DatasourceConnection) biz.Connection {
	return biz.Connection{
		URL: c.GetUrl(), AuthType: c.GetAuthType(), Username: c.GetUsername(), Headers: c.GetHeaders(),
		TimeoutMS: c.GetTimeoutMs(), TLSSkipVerify: c.GetTlsSkipVerify(),
	}
}

func toDatasourceInput(spec *v1.DatasourceSpec) biz.DatasourceInput {
	return biz.DatasourceInput{
		Name: spec.GetName(), Conn: toConnection(spec.GetConnection()), Secret: spec.GetConnection().Secret,
		RemoteWriteURL: spec.GetRemoteWriteUrl(), Description: spec.GetDescription(), IsDefault: spec.GetIsDefault(), Enabled: spec.GetEnabled(),
	}
}

// toProtoDatasource 凭据只写不读：响应里只有 has_secret；请求头的值已由用例换成掩码。
func toProtoDatasource(d *biz.Datasource) *v1.Datasource {
	return &v1.Datasource{
		Id: d.ID,
		Spec: &v1.DatasourceSpec{
			Name: d.Name,
			Connection: &v1.DatasourceConnection{
				Url: d.URL, AuthType: d.AuthType, Username: d.Username, Headers: d.Headers, TimeoutMs: d.TimeoutMS, TlsSkipVerify: d.TLSSkipVerify,
			},
			RemoteWriteUrl: d.RemoteWriteURL, Description: d.Description, IsDefault: d.IsDefault, Enabled: d.Enabled,
		},
		Type: d.Type, HasSecret: d.HasSecret, HealthStatus: d.HealthStatus, HealthError: d.HealthError, HealthTime: protoTime(d.HealthTime),
		RuleCount: d.RuleCount, Version: d.Version, CreateTime: timestamppb.New(d.CreateTime), UpdateTime: timestamppb.New(d.UpdateTime),
		UpdateUserId: d.UpdateUser, UpdateUserName: d.UpdateUserName,
	}
}

// ListDatasources implements proto MonitorDatasourceService/ListDatasources.
func (s *DatasourceService) ListDatasources(ctx context.Context, req *v1.ListDatasourcesRequest) (*v1.ListDatasourcesResponse, error) {
	items, total, err := s.uc.List(ctx, biz.DatasourceFilter{Keyword: req.GetKeyword(), Enabled: req.Enabled, HealthStatus: req.HealthStatus},
		req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDatasourcesResponse{Items: make([]*v1.Datasource, 0, len(items)), Total: total}
	for _, d := range items {
		resp.Items = append(resp.Items, toProtoDatasource(d))
	}
	return resp, nil
}

// GetDatasource implements proto MonitorDatasourceService/GetDatasource.
func (s *DatasourceService) GetDatasource(ctx context.Context, req *v1.GetDatasourceRequest) (*v1.GetDatasourceResponse, error) {
	d, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetDatasourceResponse{Datasource: toProtoDatasource(d)}, nil
}

// CreateDatasource implements proto MonitorDatasourceService/CreateDatasource.
func (s *DatasourceService) CreateDatasource(ctx context.Context, req *v1.CreateDatasourceRequest) (*v1.CreateDatasourceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.uc.Create(ctx, userID, toDatasourceInput(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateDatasourceResponse{Datasource: toProtoDatasource(d)}, nil
}

// UpdateDatasource implements proto MonitorDatasourceService/UpdateDatasource.
func (s *DatasourceService) UpdateDatasource(ctx context.Context, req *v1.UpdateDatasourceRequest) (*v1.UpdateDatasourceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.uc.Update(ctx, userID, req.GetId(), toDatasourceInput(req.GetSpec()), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.UpdateDatasourceResponse{Datasource: toProtoDatasource(d)}, nil
}

// DeleteDatasource implements proto MonitorDatasourceService/DeleteDatasource.
func (s *DatasourceService) DeleteDatasource(ctx context.Context, req *v1.DeleteDatasourceRequest) (*v1.DeleteDatasourceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteDatasourceResponse{}, nil
}

// TestDatasourceConfig implements proto MonitorDatasourceService/TestDatasourceConfig.
func (s *DatasourceService) TestDatasourceConfig(ctx context.Context, req *v1.TestDatasourceConfigRequest) (*v1.TestDatasourceConfigResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	res, err := s.uc.TestConfig(ctx, userID, toConnection(req.GetConnection()), req.GetConnection().Secret, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.TestDatasourceConfigResponse{Ok: res.OK, LatencyMs: millis(res.Latency), Error: res.Error}, nil
}

// TestDatasource implements proto MonitorDatasourceService/TestDatasource.
func (s *DatasourceService) TestDatasource(ctx context.Context, req *v1.TestDatasourceRequest) (*v1.TestDatasourceResponse, error) {
	res, err := s.uc.Test(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.TestDatasourceResponse{Ok: res.OK, LatencyMs: millis(res.Latency), Error: res.Error}, nil
}

// ListDatasourceOptions implements proto MonitorDatasourceService/ListDatasourceOptions.
func (s *DatasourceService) ListDatasourceOptions(ctx context.Context, req *v1.ListDatasourceOptionsRequest) (*v1.ListDatasourceOptionsResponse, error) {
	items, err := s.uc.ListOptions(ctx, req.GetEnabledOnly())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDatasourceOptionsResponse{Items: make([]*v1.DatasourceOption, 0, len(items))}
	for _, d := range items {
		resp.Items = append(resp.Items, &v1.DatasourceOption{Id: d.ID, Name: d.Name, IsDefault: d.IsDefault, Enabled: d.Enabled, HealthStatus: d.HealthStatus})
	}
	return resp, nil
}

// millis 时长 → 毫秒（上限由查询超时保证远小于 int32）。
func millis(d time.Duration) int32 {
	return int32(min(d.Milliseconds(), 1<<30)) // #nosec G115 -- 已夹紧
}

func toProtoSeries(series []biz.QuerySeries) []*v1.QuerySeries {
	out := make([]*v1.QuerySeries, 0, len(series))
	for _, s := range series {
		points := make([]*v1.QueryPoint, 0, len(s.Points))
		for _, p := range s.Points {
			points = append(points, &v1.QueryPoint{TimestampMs: p.Time.UnixMilli(), Value: p.Value})
		}
		out = append(out, &v1.QuerySeries{Labels: s.Labels, Points: points})
	}
	return out
}

// QueryDatasource implements proto MonitorDatasourceService/QueryDatasource.
func (s *DatasourceService) QueryDatasource(ctx context.Context, req *v1.QueryDatasourceRequest) (*v1.QueryDatasourceResponse, error) {
	var ts time.Time
	if req.GetTime() != nil {
		ts = req.GetTime().AsTime()
	}
	res, err := s.uc.Query(ctx, req.GetDatasourceId(), req.GetExpr(), ts)
	if err != nil {
		return nil, err
	}
	return &v1.QueryDatasourceResponse{
		ResultType: res.Type, Series: toProtoSeries(res.Series), Truncated: res.Truncated, Warnings: res.Warnings, DurationMs: millis(res.Duration),
	}, nil
}

// QueryDatasourceRange implements proto MonitorDatasourceService/QueryDatasourceRange.
func (s *DatasourceService) QueryDatasourceRange(ctx context.Context, req *v1.QueryDatasourceRangeRequest) (*v1.QueryDatasourceRangeResponse, error) {
	res, err := s.uc.QueryRange(ctx, req.GetDatasourceId(), req.GetExpr(), req.GetStart().AsTime(), req.GetEnd().AsTime(),
		time.Duration(req.GetStepS())*time.Second)
	if err != nil {
		return nil, err
	}
	return &v1.QueryDatasourceRangeResponse{
		ResultType: res.Type, Series: toProtoSeries(res.Series), Truncated: res.Truncated, Warnings: res.Warnings, DurationMs: millis(res.Duration),
	}, nil
}

// ListMetricNames implements proto MonitorDatasourceService/ListMetricNames.
func (s *DatasourceService) ListMetricNames(ctx context.Context, req *v1.ListMetricNamesRequest) (*v1.ListMetricNamesResponse, error) {
	names, err := s.uc.ListMetricNames(ctx, req.GetDatasourceId(), int(req.GetLimit())) // #nosec G115 -- proto 校验 ≤1000
	if err != nil {
		return nil, err
	}
	return &v1.ListMetricNamesResponse{Names: names}, nil
}

// ListLabelNames implements proto MonitorDatasourceService/ListLabelNames.
func (s *DatasourceService) ListLabelNames(ctx context.Context, req *v1.ListLabelNamesRequest) (*v1.ListLabelNamesResponse, error) {
	names, err := s.uc.ListLabelNames(ctx, req.GetDatasourceId(), int(req.GetLimit())) // #nosec G115 -- proto 校验 ≤1000
	if err != nil {
		return nil, err
	}
	return &v1.ListLabelNamesResponse{Names: names}, nil
}

// ListLabelValues implements proto MonitorDatasourceService/ListLabelValues.
func (s *DatasourceService) ListLabelValues(ctx context.Context, req *v1.ListLabelValuesRequest) (*v1.ListLabelValuesResponse, error) {
	values, err := s.uc.ListLabelValues(ctx, req.GetDatasourceId(), req.GetLabel(), int(req.GetLimit())) // #nosec G115 -- proto 校验 ≤1000
	if err != nil {
		return nil, err
	}
	return &v1.ListLabelValuesResponse{Values: values}, nil
}
