package service

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"

	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 导出 CSV 固定列（在动态字段之前）。
var exportFixedColumns = []string{"name", "status", "env", "owner", "tags", "externalId", "id", "version", "updateTime"}

// ImportCIs implements proto CmdbCIService/ImportCIs.
func (s *CmdbCIService) ImportCIs(ctx context.Context, req *v1.ImportCIsRequest) (*v1.ImportCIsResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// 受限用户：类型须可见，且每一行的环境都须在范围内（整批拒绝，避免部分写入）。
	scope, err := s.guard.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if !scope.AllowsModel(req.GetModelCode()) {
		return nil, biz.ErrScopeForbidden
	}
	for _, r := range req.GetRows() {
		if r.Env != nil && !scope.AllowsEnv(r.GetEnv()) {
			return nil, biz.ErrScopeForbidden
		}
	}
	rows := make([]biz.ImportRow, 0, len(req.GetRows()))
	for _, r := range req.GetRows() {
		row := biz.ImportRow{Name: r.GetName(), Status: r.GetStatus(), Env: r.Env, Owner: r.Owner, ExternalID: r.ExternalId}
		if r.Tags != nil {
			row.Tags = r.GetTags()
		}
		if r.Attrs != nil {
			row.Attrs = r.GetAttrs().AsMap()
		}
		for _, rel := range r.GetRelations() {
			row.Relations = append(row.Relations, biz.ImportRelation{Type: rel.GetRelationType(), Direction: rel.GetDirection(), TargetModel: rel.GetTargetModel(), TargetName: rel.GetTargetName()})
		}
		rows = append(rows, row)
	}
	result, err := s.uc.Import(ctx, actor, req.GetModelCode(), rows, req.GetDryRun())
	if err != nil {
		return nil, err
	}
	resp := &v1.ImportCIsResponse{
		DryRun: result.DryRun, Created: result.Created, Updated: result.Updated, Skipped: result.Skipped, Failed: result.Failed,
		Results: make([]*v1.ImportRowResult, 0, len(result.Results)),
	}
	for _, r := range result.Results {
		resp.Results = append(resp.Results, &v1.ImportRowResult{Row: r.Row, Name: r.Name, Action: r.Action, Message: r.Message, CiId: r.CIID})
	}
	return resp, nil
}

// ExportCIs implements proto CmdbCIService/ExportCIs：CSV（UTF-8 BOM，Excel 可直接打开）。
func (s *CmdbCIService) ExportCIs(ctx context.Context, req *v1.ExportCIsRequest) (*httpbody.HttpBody, error) {
	filter := biz.CIFilter{ModelCode: req.GetModelCode(), Keyword: req.GetKeyword(), Status: req.GetStatus(), Env: req.GetEnv()}
	visible, err := s.guard.narrowFilter(ctx, &filter)
	if err != nil {
		return nil, err
	}
	if !visible {
		return nil, biz.ErrScopeForbidden
	}
	items, fields, err := s.uc.Export(ctx, filter)
	if err != nil {
		return nil, err
	}
	format := req.GetFormat()
	if format == "" {
		format = "csv"
	}
	codec, ok := exportCodecs[format]
	if !ok {
		return nil, biz.ErrCIInvalid
	}
	active := make([]*biz.Field, 0, len(fields))
	for _, f := range fields {
		if !f.Deprecated {
			active = append(active, f)
		}
	}
	data, contentType, err := codec(items, active)
	if err != nil {
		return nil, err
	}
	return &httpbody.HttpBody{ContentType: contentType, Data: data}, nil
}

// exportCodec 导出编码器：资源 + 字段 → 字节流与 Content-Type。
type exportCodec func(items []*biz.CI, fields []*biz.Field) ([]byte, string, error)

// exportCodecs 导出格式注册表：新增格式只需追加一项。
var exportCodecs = map[string]exportCodec{
	"csv":  exportCSV,
	"json": exportJSON,
}

func exportCSV(items []*biz.CI, fields []*biz.Field) ([]byte, string, error) {
	var buf bytes.Buffer
	buf.WriteString("\uFEFF")
	w := csv.NewWriter(&buf)
	header := append([]string{}, exportFixedColumns...)
	for _, f := range fields {
		header = append(header, f.Code)
	}
	if err := w.Write(header); err != nil {
		return nil, "", err
	}
	for _, ci := range items {
		record := []string{
			ci.Name, ci.Status, ci.Env, ci.Owner, strings.Join(ci.Tags, ";"), ci.ExternalID, ci.ID,
			fmt.Sprint(ci.Version), ci.UpdateTime.UTC().Format("2006-01-02T15:04:05Z07:00"),
		}
		for _, f := range fields {
			record = append(record, exportCell(ci.Attrs[f.Code]))
		}
		if err := w.Write(record); err != nil {
			return nil, "", err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), "text/csv; charset=utf-8", nil
}

// exportJSON 与导入行同构的 JSON 数组（attrs 只含已定义字段）。
func exportJSON(items []*biz.CI, fields []*biz.Field) ([]byte, string, error) {
	type row struct {
		Name       string         `json:"name"`
		Status     string         `json:"status"`
		Env        string         `json:"env"`
		Owner      string         `json:"owner"`
		Tags       []string       `json:"tags"`
		ExternalID string         `json:"externalId"`
		ID         string         `json:"id"`
		Version    int64          `json:"version"`
		UpdateTime string         `json:"updateTime"`
		Attrs      map[string]any `json:"attrs"`
	}
	out := make([]row, 0, len(items))
	for _, ci := range items {
		attrs := make(map[string]any, len(fields))
		for _, f := range fields {
			if v, ok := ci.Attrs[f.Code]; ok {
				attrs[f.Code] = v
			}
		}
		tags := ci.Tags
		if tags == nil {
			tags = []string{}
		}
		out = append(out, row{
			Name: ci.Name, Status: ci.Status, Env: ci.Env, Owner: ci.Owner, Tags: tags, ExternalID: ci.ExternalID, ID: ci.ID,
			Version: ci.Version, UpdateTime: ci.UpdateTime.UTC().Format("2006-01-02T15:04:05Z07:00"), Attrs: attrs,
		})
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, "", err
	}
	return data, "application/json; charset=utf-8", nil
}

// exportCell 属性值 → CSV 单元格：标量直出，对象/数组 JSON 文本。
func exportCell(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64, int64, int, bool:
		return fmt.Sprint(t)
	default:
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(raw)
	}
}

// CmdbCIService 实现 CmdbCIServiceServer。
type CmdbCIService struct {
	uc    *biz.CIUsecase
	guard scopeGuard
}

// NewCmdbCIService new a CmdbCIService.
func NewCmdbCIService(uc *biz.CIUsecase, scopes *biz.ScopeUsecase) *CmdbCIService {
	return &CmdbCIService{uc: uc, guard: newScopeGuard(scopes)}
}

// SearchCIs implements proto CmdbCIService/SearchCIs（数据范围叠加为类型/环境集合）。
func (s *CmdbCIService) SearchCIs(ctx context.Context, req *v1.SearchCIsRequest) (*v1.SearchCIsResponse, error) {
	narrow := biz.CIFilter{}
	visible, err := s.guard.narrowFilter(ctx, &narrow)
	if err != nil {
		return nil, err
	}
	resp := &v1.SearchCIsResponse{Items: []*v1.SearchHit{}}
	if !visible {
		return resp, nil
	}
	hits, err := s.uc.Search(ctx, biz.SearchFilter{Query: req.GetQ(), ModelCodes: narrow.ModelCodes, Envs: narrow.Envs, Limit: req.GetLimit()})
	if err != nil {
		return nil, err
	}
	for _, h := range hits {
		p, err := toProtoCI(h.CI)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, &v1.SearchHit{Ci: p, MatchedField: h.Field, MatchedValue: h.Value})
	}
	return resp, nil
}

// ListCIs implements proto CmdbCIService/ListCIs.
func (s *CmdbCIService) ListCIs(ctx context.Context, req *v1.ListCIsRequest) (*v1.ListCIsResponse, error) {
	filter := biz.CIFilter{
		ModelCode: req.GetModelCode(), Keyword: req.GetKeyword(), Status: req.GetStatus(),
		Env: req.GetEnv(), Tag: req.GetTag(), Attrs: req.GetAttrs().AsMap(), Cursor: req.GetCursor(),
		RelType: req.GetRelType(), RelDirection: req.GetRelDirection(), RelTargetID: req.GetRelTargetId(), RelNone: req.GetRelNone(),
	}
	if filter.RelType != "" && filter.RelDirection == "" {
		filter.RelDirection = biz.DirectionOutgoing
	}
	visible, err := s.guard.narrowFilter(ctx, &filter)
	if err != nil {
		return nil, err
	}
	if !visible {
		return &v1.ListCIsResponse{Items: []*v1.CmdbCI{}}, nil
	}
	items, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListCIsResponse{Items: make([]*v1.CmdbCI, 0, len(items)), Total: total}
	if len(items) > 0 && len(items) == int(req.GetPageSize()) {
		resp.NextCursor = biz.EncodeCursor(items[len(items)-1])
	}
	for _, ci := range items {
		p, err := toProtoCI(ci)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, p)
	}
	return resp, nil
}

// GetCI implements proto CmdbCIService/GetCI.
func (s *CmdbCIService) GetCI(ctx context.Context, req *v1.GetCIRequest) (*v1.GetCIResponse, error) {
	ci, summaries, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.guard.require(ctx, ci.ModelCode, ci.Env); err != nil {
		return nil, err
	}
	p, err := toProtoCI(ci)
	if err != nil {
		return nil, err
	}
	resp := &v1.GetCIResponse{Ci: p}
	for _, sm := range summaries {
		resp.Relations = append(resp.Relations, &v1.RelationSummary{
			RelationType: sm.RelationType, Direction: sm.Direction, Name: sm.Name, TargetModel: sm.TargetModel, Count: sm.Count,
		})
	}
	return resp, nil
}

// CreateCI implements proto CmdbCIService/CreateCI.
func (s *CmdbCIService) CreateCI(ctx context.Context, req *v1.CreateCIRequest) (*v1.CreateCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.guard.require(ctx, req.GetModelCode(), req.GetEnv()); err != nil {
		return nil, err
	}
	ci, err := s.uc.Create(ctx, actor, &biz.CI{
		ModelCode: req.GetModelCode(), Name: req.GetName(), Status: req.GetStatus(), Env: req.GetEnv(),
		Owner: req.GetOwner(), Tags: req.GetTags(), ExternalID: req.GetExternalId(), Attrs: req.GetAttrs().AsMap(),
	})
	if err != nil {
		return nil, err
	}
	p, err := toProtoCI(ci)
	if err != nil {
		return nil, err
	}
	return &v1.CreateCIResponse{Ci: p}, nil
}

// UpdateCI implements proto CmdbCIService/UpdateCI.
func (s *CmdbCIService) UpdateCI(ctx context.Context, req *v1.UpdateCIRequest) (*v1.UpdateCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cur, err := s.guard.requireCI(ctx, s.uc.GetByID, req.GetId())
	if err != nil {
		return nil, err
	}
	if req.Env != nil && req.GetEnv() != cur.Env {
		// 环境变更后也须在范围内（防止把资源移出自己的可见范围之外）。
		if err := s.guard.require(ctx, cur.ModelCode, req.GetEnv()); err != nil {
			return nil, err
		}
	}
	upd := biz.CIUpdate{Name: req.Name, Env: req.Env, Owner: req.Owner, ExternalID: req.ExternalId}
	if req.Tags != nil {
		tags := req.GetTags().GetItems()
		if tags == nil {
			tags = []string{}
		}
		upd.Tags = &tags
	}
	if req.Attrs != nil {
		upd.Attrs = req.GetAttrs().AsMap()
	}
	ci, err := s.uc.Update(ctx, actor, req.GetId(), req.GetVersion(), upd)
	if err != nil {
		return nil, err
	}
	p, err := toProtoCI(ci)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateCIResponse{Ci: p}, nil
}

// TransitionCI implements proto CmdbCIService/TransitionCI.
func (s *CmdbCIService) TransitionCI(ctx context.Context, req *v1.TransitionCIRequest) (*v1.TransitionCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.guard.requireCI(ctx, s.uc.GetByID, req.GetId()); err != nil {
		return nil, err
	}
	ci, err := s.uc.Transition(ctx, actor, req.GetId(), req.GetVersion(), req.GetStatus())
	if err != nil {
		return nil, err
	}
	p, err := toProtoCI(ci)
	if err != nil {
		return nil, err
	}
	return &v1.TransitionCIResponse{Ci: p}, nil
}

// DeleteCI implements proto CmdbCIService/DeleteCI.
func (s *CmdbCIService) DeleteCI(ctx context.Context, req *v1.DeleteCIRequest) (*v1.DeleteCIResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.guard.requireCI(ctx, s.uc.GetByID, req.GetId()); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, actor, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteCIResponse{}, nil
}

// ListCIHistory implements proto CmdbCIService/ListCIHistory.
func (s *CmdbCIService) ListCIHistory(ctx context.Context, req *v1.ListCIHistoryRequest) (*v1.ListCIHistoryResponse, error) {
	if _, err := s.guard.requireCI(ctx, s.uc.GetByID, req.GetId()); err != nil {
		return nil, err
	}
	items, total, err := s.uc.History(ctx, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListCIHistoryResponse{Items: make([]*v1.CmdbHistory, 0, len(items)), Total: total}
	for _, h := range items {
		diff, err := jsonStruct(h.Diff)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, toProtoHistory(h, diff))
	}
	return resp, nil
}

func toProtoHistory(h *biz.History, diff *structpb.Struct) *v1.CmdbHistory {
	return &v1.CmdbHistory{
		Id: h.ID, CiId: h.CIID, Action: h.Action, Diff: diff, Operator: h.Operator, Source: h.Source,
		RequestId: h.RequestID, CreateTime: timestamppb.New(h.CreateTime), CiName: h.CIName, ModelCode: h.ModelCode,
	}
}

// ListHistory implements proto CmdbCIService/ListHistory（数据范围叠加为类型/环境集合）。
func (s *CmdbCIService) ListHistory(ctx context.Context, req *v1.ListHistoryRequest) (*v1.ListHistoryResponse, error) {
	narrow := biz.CIFilter{}
	visible, err := s.guard.narrowFilter(ctx, &narrow)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListHistoryResponse{Items: []*v1.CmdbHistory{}}
	if !visible {
		return resp, nil
	}
	filter := biz.HistoryFilter{
		ModelCodes: narrow.ModelCodes, Envs: narrow.Envs, ModelCode: req.GetModelCode(), Action: req.GetAction(),
		Operator: req.GetOperator(), Keyword: req.GetKeyword(),
	}
	if req.From != nil {
		from := req.GetFrom().AsTime()
		filter.From = &from
	}
	if req.To != nil {
		to := req.GetTo().AsTime()
		filter.To = &to
	}
	items, total, err := s.uc.ListHistory(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp.Total = total
	for _, h := range items {
		diff, err := jsonStruct(h.Diff)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, toProtoHistory(h, diff))
	}
	return resp, nil
}

// toProtoCI 领域模型 → proto（attrs 经 structpb；int64 以 number 表达）。
func toProtoCI(ci *biz.CI) (*v1.CmdbCI, error) {
	attrs, err := jsonStruct(ci.Attrs)
	if err != nil {
		return nil, err
	}
	p := &v1.CmdbCI{
		Id: ci.ID, ModelCode: ci.ModelCode, ModelName: ci.ModelName, Name: ci.Name, Status: ci.Status, Env: ci.Env,
		Owner: ci.Owner, Tags: ci.Tags, Source: ci.Source, ExternalId: ci.ExternalID, Attrs: attrs, Stale: ci.Stale,
		Version: ci.Version, CreateTime: timestamppb.New(ci.CreateTime), UpdateTime: timestamppb.New(ci.UpdateTime),
		SourceName: ci.SourceName, SourceDeleted: ci.SourceDeleted,
	}
	if ci.LastSyncedAt != nil {
		p.LastSyncedAt = timestamppb.New(*ci.LastSyncedAt)
	}
	return p, nil
}

// jsonStruct 任意 map（含 int64、[]string、嵌套）经 JSON 往返转为 structpb.Struct。
func jsonStruct(m map[string]any) (*structpb.Struct, error) {
	if m == nil {
		m = map[string]any{}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var st structpb.Struct
	if err := st.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	return &st, nil
}
