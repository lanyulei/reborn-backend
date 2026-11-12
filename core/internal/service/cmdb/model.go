// Package service 实现 api/cmdb/v1 生成的服务接口：参数编排、proto ↔ DO 转换、
// 操作者提取；业务规则全部在 biz。
package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/transport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// actorFromContext 取操作者（JWT 用户 ID）与请求 ID（X-Request-Id 头）。
func actorFromContext(ctx context.Context) (biz.Actor, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return biz.Actor{}, err
	}
	actor := biz.Actor{UserID: userID}
	if tr, ok := transport.FromServerContext(ctx); ok {
		actor.RequestID = tr.RequestHeader().Get("X-Request-Id")
	}
	return actor, nil
}

// CmdbModelService 实现 CmdbModelServiceServer。
type CmdbModelService struct {
	uc *biz.ModelUsecase
}

// NewCmdbModelService new a CmdbModelService.
func NewCmdbModelService(uc *biz.ModelUsecase) *CmdbModelService { return &CmdbModelService{uc: uc} }

// ListModels implements proto CmdbModelService/ListModels.
func (s *CmdbModelService) ListModels(ctx context.Context, req *v1.ListModelsRequest) (*v1.ListModelsResponse, error) {
	items, total, err := s.uc.ListModels(ctx, biz.ModelFilter{
		Keyword: req.GetKeyword(), Category: req.GetCategory(), Status: req.Status,
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListModelsResponse{Items: make([]*v1.CmdbModel, 0, len(items)), Total: total}
	for _, m := range items {
		resp.Items = append(resp.Items, toProtoModel(m))
	}
	return resp, nil
}

// GetModel implements proto CmdbModelService/GetModel.
func (s *CmdbModelService) GetModel(ctx context.Context, req *v1.GetModelRequest) (*v1.GetModelResponse, error) {
	m, fields, types, err := s.uc.GetModel(ctx, req.GetCode())
	if err != nil {
		return nil, err
	}
	resp := &v1.GetModelResponse{Model: toProtoModel(m)}
	for _, f := range fields {
		resp.Fields = append(resp.Fields, toProtoField(f))
	}
	for _, t := range types {
		resp.RelationTypes = append(resp.RelationTypes, toProtoRelationType(t))
	}
	return resp, nil
}

// CreateModel implements proto CmdbModelService/CreateModel.
func (s *CmdbModelService) CreateModel(ctx context.Context, req *v1.CreateModelRequest) (*v1.CreateModelResponse, error) {
	m, err := s.uc.CreateModel(withActor(ctx), &biz.Model{
		Code: req.GetCode(), Name: req.GetName(), Category: req.GetCategory(),
		Icon: req.GetIcon(), Description: req.GetDescription(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateModelResponse{Model: toProtoModel(m)}, nil
}

// UpdateModel implements proto CmdbModelService/UpdateModel.
func (s *CmdbModelService) UpdateModel(ctx context.Context, req *v1.UpdateModelRequest) (*v1.UpdateModelResponse, error) {
	m, err := s.uc.UpdateModel(withActor(ctx), req.GetCode(), biz.ModelUpdate{
		Name: req.Name, Category: req.Category, Icon: req.Icon, Description: req.Description, Status: req.Status,
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateModelResponse{Model: toProtoModel(m)}, nil
}

// DeleteModel implements proto CmdbModelService/DeleteModel.
func (s *CmdbModelService) DeleteModel(ctx context.Context, req *v1.DeleteModelRequest) (*v1.DeleteModelResponse, error) {
	if err := s.uc.DeleteModel(withActor(ctx), req.GetCode()); err != nil {
		return nil, err
	}
	return &v1.DeleteModelResponse{}, nil
}

// CreateField implements proto CmdbModelService/CreateField.
func (s *CmdbModelService) CreateField(ctx context.Context, req *v1.CreateFieldRequest) (*v1.CreateFieldResponse, error) {
	f, err := s.uc.CreateField(withActor(ctx), &biz.Field{
		ModelCode: req.GetModelCode(), Code: req.GetCode(), Name: req.GetName(), Type: req.GetType(),
		Required: req.GetRequired(), Unique: req.GetUnique(), EnumValues: req.GetEnumValues(), RefModel: req.GetRefModel(),
		Rule: ruleFromProto(req.GetRule()), Owner: req.GetOwner(), ListVisible: req.GetListVisible(), Sort: req.GetSort(),
		Description: req.GetDescription(), Indexed: req.GetIndexed(), Sensitive: req.GetSensitive(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateFieldResponse{Field: toProtoField(f)}, nil
}

// UpdateField implements proto CmdbModelService/UpdateField.
func (s *CmdbModelService) UpdateField(ctx context.Context, req *v1.UpdateFieldRequest) (*v1.UpdateFieldResponse, error) {
	upd := biz.FieldUpdate{
		Name: req.Name, Required: req.Required, Unique: req.Unique, Owner: req.Owner,
		ListVisible: req.ListVisible, Sort: req.Sort, Description: req.Description, Indexed: req.Indexed, Sensitive: req.Sensitive,
	}
	if len(req.GetEnumValues()) > 0 {
		values := req.GetEnumValues()
		upd.EnumValues = &values
	}
	if req.Rule != nil {
		rule := ruleFromProto(req.GetRule())
		upd.Rule = &rule
	}
	f, err := s.uc.UpdateField(withActor(ctx), req.GetModelCode(), req.GetId(), upd)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateFieldResponse{Field: toProtoField(f)}, nil
}

// DeleteField implements proto CmdbModelService/DeleteField.
func (s *CmdbModelService) DeleteField(ctx context.Context, req *v1.DeleteFieldRequest) (*v1.DeleteFieldResponse, error) {
	if err := s.uc.DeprecateField(withActor(ctx), req.GetModelCode(), req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteFieldResponse{}, nil
}

// ListRelationTypes implements proto CmdbModelService/ListRelationTypes.
func (s *CmdbModelService) ListRelationTypes(ctx context.Context, req *v1.ListRelationTypesRequest) (*v1.ListRelationTypesResponse, error) {
	items, err := s.uc.ListRelationTypes(ctx, req.GetModelCode())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRelationTypesResponse{Items: make([]*v1.CmdbRelationType, 0, len(items))}
	for _, t := range items {
		resp.Items = append(resp.Items, toProtoRelationType(t))
	}
	return resp, nil
}

// CreateRelationType implements proto CmdbModelService/CreateRelationType.
func (s *CmdbModelService) CreateRelationType(ctx context.Context, req *v1.CreateRelationTypeRequest) (*v1.CreateRelationTypeResponse, error) {
	t, err := s.uc.CreateRelationType(withActor(ctx), &biz.RelationType{
		Code: req.GetCode(), Name: req.GetName(), ForwardName: req.GetForwardName(), BackwardName: req.GetBackwardName(),
		SrcModel: req.GetSrcModel(), DstModel: req.GetDstModel(), Cardinality: req.GetCardinality(),
		OnDelete: req.GetOnDelete(), Description: req.GetDescription(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateRelationTypeResponse{RelationType: toProtoRelationType(t)}, nil
}

// UpdateRelationType implements proto CmdbModelService/UpdateRelationType.
func (s *CmdbModelService) UpdateRelationType(ctx context.Context, req *v1.UpdateRelationTypeRequest) (*v1.UpdateRelationTypeResponse, error) {
	t, err := s.uc.UpdateRelationType(withActor(ctx), req.GetCode(), biz.RelationTypeUpdate{
		Name: req.Name, ForwardName: req.ForwardName, BackwardName: req.BackwardName, OnDelete: req.OnDelete, Description: req.Description,
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRelationTypeResponse{RelationType: toProtoRelationType(t)}, nil
}

// DeleteRelationType implements proto CmdbModelService/DeleteRelationType.
func (s *CmdbModelService) DeleteRelationType(ctx context.Context, req *v1.DeleteRelationTypeRequest) (*v1.DeleteRelationTypeResponse, error) {
	if err := s.uc.DeleteRelationType(withActor(ctx), req.GetCode()); err != nil {
		return nil, err
	}
	return &v1.DeleteRelationTypeResponse{}, nil
}

// GetModelView implements proto CmdbModelService/GetModelView.
func (s *CmdbModelService) GetModelView(ctx context.Context, req *v1.GetModelViewRequest) (*v1.GetModelViewResponse, error) {
	sections, configured, err := s.uc.GetView(ctx, req.GetModelCode())
	if err != nil {
		return nil, err
	}
	return &v1.GetModelViewResponse{ModelCode: req.GetModelCode(), Configured: configured, Sections: toProtoSections(sections)}, nil
}

// UpdateModelView implements proto CmdbModelService/UpdateModelView.
func (s *CmdbModelService) UpdateModelView(ctx context.Context, req *v1.UpdateModelViewRequest) (*v1.UpdateModelViewResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sections := make([]biz.ViewSection, 0, len(req.GetSections()))
	for _, sec := range req.GetSections() {
		sections = append(sections, biz.ViewSection{
			Type: sec.GetType(), Title: sec.GetTitle(), Fields: sec.GetFields(), RelationType: sec.GetRelationType(),
			Direction: sec.GetDirection(), Columns: sec.GetColumns(), Collapsed: sec.GetCollapsed(),
		})
	}
	saved, err := s.uc.UpdateView(ctx, actor, req.GetModelCode(), sections)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateModelViewResponse{Sections: toProtoSections(saved)}, nil
}

func toProtoSections(sections []biz.ViewSection) []*v1.ViewSection {
	out := make([]*v1.ViewSection, 0, len(sections))
	for _, sec := range sections {
		p := &v1.ViewSection{Type: sec.Type, Fields: sec.Fields, Columns: sec.Columns, Collapsed: sec.Collapsed}
		if sec.Title != "" {
			p.Title = &sec.Title
		}
		if sec.RelationType != "" {
			rt := sec.RelationType
			p.RelationType = &rt
		}
		if sec.Direction != "" {
			d := sec.Direction
			p.Direction = &d
		}
		out = append(out, p)
	}
	return out
}

func toProtoModel(m *biz.Model) *v1.CmdbModel {
	return &v1.CmdbModel{
		Code: m.Code, Name: m.Name, Category: m.Category, Icon: m.Icon, Description: m.Description,
		Status: m.Status, Builtin: m.Builtin, CiCount: m.CICount,
		CreateTime: timestamppb.New(m.CreateTime), UpdateTime: timestamppb.New(m.UpdateTime),
	}
}

func toProtoField(f *biz.Field) *v1.CmdbField {
	return &v1.CmdbField{
		Id: f.ID, ModelCode: f.ModelCode, Code: f.Code, Name: f.Name, Type: f.Type, Required: f.Required, Unique: f.Unique,
		EnumValues: f.EnumValues, RefModel: f.RefModel, Rule: ruleToProto(f.Rule), Owner: f.Owner, ListVisible: f.ListVisible,
		Sort: f.Sort, Description: f.Description, Deprecated: f.Deprecated, Indexed: f.Indexed, Sensitive: f.Sensitive,
	}
}

func toProtoRelationType(t *biz.RelationType) *v1.CmdbRelationType {
	return &v1.CmdbRelationType{
		Code: t.Code, Name: t.Name, ForwardName: t.ForwardName, BackwardName: t.BackwardName,
		SrcModel: t.SrcModel, DstModel: t.DstModel, Cardinality: t.Cardinality, OnDelete: t.OnDelete,
		Description: t.Description, RelationCount: t.RelationCount,
	}
}

func ruleFromProto(r *v1.FieldRule) biz.FieldRule {
	if r == nil {
		return biz.FieldRule{}
	}
	return biz.FieldRule{Pattern: r.Pattern, Min: r.Min, Max: r.Max, MinLen: r.MinLen, MaxLen: r.MaxLen}
}

func ruleToProto(r biz.FieldRule) *v1.FieldRule {
	if r == (biz.FieldRule{}) {
		return nil
	}
	return &v1.FieldRule{Pattern: r.Pattern, Min: r.Min, Max: r.Max, MinLen: r.MinLen, MaxLen: r.MaxLen}
}
