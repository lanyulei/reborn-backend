package service

import (
	"context"
	"strings"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefinitionService 实现 api/workflow/v1 生成的 WorkflowDefinitionServiceServer。
type DefinitionService struct {
	uc     *biz.DefinitionUsecase
	dir    biz.Directory
	duty   biz.Duty
	admins biz.Admins
	users  biz.Users
}

// NewDefinitionService new a DefinitionService。dir / users / duty / admins 供模拟运行解析处理人与显示名。
func NewDefinitionService(uc *biz.DefinitionUsecase, dir biz.Directory, users biz.Users, duty biz.Duty, admins biz.Admins) *DefinitionService {
	return &DefinitionService{uc: uc, dir: dir, users: users, duty: duty, admins: admins}
}

// SimulateDefinition implements proto WorkflowDefinitionService/SimulateDefinition。
func (s *DefinitionService) SimulateDefinition(ctx context.Context, req *v1.SimulateDefinitionRequest) (*v1.SimulateDefinitionResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := req.GetGraph().MarshalJSON()
	if err != nil {
		return nil, biz.ErrDefinitionInvalid
	}
	initiator := strings.ToLower(req.GetInitiator())
	if initiator == "" {
		initiator = uid
	}
	res, err := s.uc.Simulate(ctx, req.GetId(), raw, biz.Instance{
		Initiator: initiator, InitiatorDept: strings.ToLower(req.GetInitiatorDept()), Variables: req.GetVariables().AsMap(),
	}, s.dir, s.users, s.duty, s.admins)
	if err != nil {
		return nil, err
	}
	resp := &v1.SimulateDefinitionResponse{Status: res.Status, Steps: make([]*v1.SimulatedStep, 0, len(res.Steps))}
	for _, st := range res.Steps {
		step := &v1.SimulatedStep{
			NodeId: st.NodeID, NodeName: st.NodeName, Type: st.Type, Mode: st.Mode, Skipped: st.Skipped, Blocked: st.Blocked,
		}
		for _, id := range st.Assignees {
			step.Assignees = append(step.Assignees, &v1.SimulatedUser{Id: id, Name: res.Names[id]})
		}
		resp.Steps = append(resp.Steps, step)
	}
	return resp, nil
}

// CreateDefinition implements proto WorkflowDefinitionService/CreateDefinition.
func (s *DefinitionService) CreateDefinition(ctx context.Context, req *v1.CreateDefinitionRequest) (*v1.CreateDefinitionResponse, error) {
	d, err := s.uc.Create(ctx, &biz.Definition{
		Key: req.GetKey(), Name: req.GetName(), Category: req.GetCategory(), Description: req.GetDescription(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateDefinitionResponse{Definition: toProtoDefinition(d)}, nil
}

// UpdateDefinition implements proto WorkflowDefinitionService/UpdateDefinition.
func (s *DefinitionService) UpdateDefinition(ctx context.Context, req *v1.UpdateDefinitionRequest) (*v1.UpdateDefinitionResponse, error) {
	d, err := s.uc.Update(ctx, req.GetId(), biz.DefinitionUpdate{
		Name: req.Name, Category: req.Category, Description: req.Description, Status: req.Status,
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateDefinitionResponse{Definition: toProtoDefinition(d)}, nil
}

// SaveDefinitionDraft implements proto WorkflowDefinitionService/SaveDefinitionDraft.
func (s *DefinitionService) SaveDefinitionDraft(ctx context.Context, req *v1.SaveDefinitionDraftRequest) (*v1.SaveDefinitionDraftResponse, error) {
	raw, err := req.GetGraph().MarshalJSON()
	if err != nil {
		return nil, biz.ErrDefinitionInvalid
	}
	revision, issues, err := s.uc.SaveDraft(ctx, req.GetId(), raw, req.GetRevision())
	if err != nil {
		return nil, err
	}
	return &v1.SaveDefinitionDraftResponse{Revision: revision, Issues: toProtoIssues(issues)}, nil
}

// PublishDefinition implements proto WorkflowDefinitionService/PublishDefinition.
func (s *DefinitionService) PublishDefinition(ctx context.Context, req *v1.PublishDefinitionRequest) (*v1.PublishDefinitionResponse, error) {
	d, err := s.uc.Publish(ctx, req.GetId(), req.GetRevision())
	if err != nil {
		return nil, err
	}
	return &v1.PublishDefinitionResponse{Definition: toProtoDefinition(d)}, nil
}

// DeleteDefinition implements proto WorkflowDefinitionService/DeleteDefinition.
func (s *DefinitionService) DeleteDefinition(ctx context.Context, req *v1.DeleteDefinitionRequest) (*v1.DeleteDefinitionResponse, error) {
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteDefinitionResponse{}, nil
}

// GetDefinition implements proto WorkflowDefinitionService/GetDefinition.
func (s *DefinitionService) GetDefinition(ctx context.Context, req *v1.GetDefinitionRequest) (*v1.GetDefinitionResponse, error) {
	d, issues, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetDefinitionResponse{Definition: toProtoDefinition(d), Draft: structOf(d.Draft), Issues: toProtoIssues(issues)}, nil
}

// ListDefinitions implements proto WorkflowDefinitionService/ListDefinitions.
func (s *DefinitionService) ListDefinitions(ctx context.Context, req *v1.ListDefinitionsRequest) (*v1.ListDefinitionsResponse, error) {
	defs, total, err := s.uc.List(ctx, biz.DefinitionFilter{Keyword: req.GetKeyword(), Status: req.Status, Published: req.Published},
		req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDefinitionsResponse{Items: make([]*v1.Definition, 0, len(defs)), Total: total}
	for _, d := range defs {
		resp.Items = append(resp.Items, toProtoDefinition(d))
	}
	return resp, nil
}

// ListDefinitionVersions implements proto WorkflowDefinitionService/ListDefinitionVersions.
func (s *DefinitionService) ListDefinitionVersions(ctx context.Context, req *v1.ListDefinitionVersionsRequest) (*v1.ListDefinitionVersionsResponse, error) {
	versions, err := s.uc.ListVersions(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListDefinitionVersionsResponse{Items: make([]*v1.DefinitionVersion, 0, len(versions))}
	for _, v := range versions {
		resp.Items = append(resp.Items, toProtoVersion(v))
	}
	return resp, nil
}

// GetDefinitionVersion implements proto WorkflowDefinitionService/GetDefinitionVersion.
func (s *DefinitionService) GetDefinitionVersion(ctx context.Context, req *v1.GetDefinitionVersionRequest) (*v1.GetDefinitionVersionResponse, error) {
	v, err := s.uc.GetVersion(ctx, req.GetId(), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.GetDefinitionVersionResponse{Version: toProtoVersion(v), Graph: structOf(v.Graph)}, nil
}

func toProtoDefinition(d *biz.Definition) *v1.Definition {
	return &v1.Definition{
		Id: d.ID, Key: d.Key, Name: d.Name, Category: d.Category, Description: d.Description,
		Status: d.Status, Revision: d.Revision, PublishedVersion: d.PublishedVersion, Builtin: d.Builtin,
		CreateTime: timestamppb.New(d.CreateTime), UpdateTime: timestamppb.New(d.UpdateTime),
	}
}

func toProtoVersion(v *biz.DefinitionVersion) *v1.DefinitionVersion {
	return &v1.DefinitionVersion{Id: v.ID, Version: v.Version, CreateUser: v.CreateUser, CreateTime: timestamppb.New(v.CreateTime)}
}

func toProtoIssues(issues []biz.Issue) []*v1.GraphIssue {
	out := make([]*v1.GraphIssue, 0, len(issues))
	for _, is := range issues {
		out = append(out, &v1.GraphIssue{NodeId: is.NodeID, EdgeId: is.EdgeID, Code: is.Code, Message: is.Message})
	}
	return out
}
