package service

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CmdbSyncService 实现 CmdbSyncServiceServer。
type CmdbSyncService struct {
	uc *biz.SyncUsecase
}

// NewCmdbSyncService new a CmdbSyncService.
func NewCmdbSyncService(uc *biz.SyncUsecase) *CmdbSyncService { return &CmdbSyncService{uc: uc} }

func optionalTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toProtoSource(src *biz.SyncSource) (*v1.SyncSource, error) {
	cfg := &structpb.Struct{}
	if len(src.Config) > 0 {
		if err := protojson.Unmarshal(src.Config, cfg); err != nil {
			return nil, fmt.Errorf("decode sync source config: %w", err)
		}
	}
	return &v1.SyncSource{
		Id: src.ID, Name: src.Name, Provider: src.Provider, Config: cfg, HasCredential: src.HasCredential, Env: src.Env,
		IntervalSeconds: src.IntervalSeconds, GracePeriodSeconds: src.GracePeriodSeconds, Enabled: src.Enabled,
		NextRunTime: optionalTime(src.NextRunTime), LastRunTime: optionalTime(src.LastRunTime), LastStatus: src.LastStatus,
		ActiveTask: src.ActiveTask, Version: src.Version, WebhookEnabled: src.WebhookEnabled, WebhookPath: cmdbcontract.WebhookPath(src.ID),
		CreateTime: timestamppb.New(src.CreateTime), UpdateTime: timestamppb.New(src.UpdateTime),
	}, nil
}

func configBytes(s *structpb.Struct) ([]byte, error) {
	if s == nil {
		return []byte("{}"), nil
	}
	return protojson.Marshal(s)
}

// ListSources implements proto CmdbSyncService/ListSources.
func (s *CmdbSyncService) ListSources(ctx context.Context, req *v1.ListSourcesRequest) (*v1.ListSourcesResponse, error) {
	items, total, err := s.uc.List(ctx, biz.SyncSourceFilter{Keyword: req.GetKeyword(), Provider: req.GetProvider()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListSourcesResponse{Items: make([]*v1.SyncSource, 0, len(items)), Total: total}
	for _, src := range items {
		p, err := toProtoSource(src)
		if err != nil {
			return nil, err
		}
		resp.Items = append(resp.Items, p)
	}
	return resp, nil
}

// GetSource implements proto CmdbSyncService/GetSource.
func (s *CmdbSyncService) GetSource(ctx context.Context, req *v1.GetSourceRequest) (*v1.GetSourceResponse, error) {
	src, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	p, err := toProtoSource(src)
	if err != nil {
		return nil, err
	}
	return &v1.GetSourceResponse{Source: p}, nil
}

// CreateSource implements proto CmdbSyncService/CreateSource.
func (s *CmdbSyncService) CreateSource(ctx context.Context, req *v1.CreateSourceRequest) (*v1.CreateSourceResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cfg, err := configBytes(req.GetConfig())
	if err != nil {
		return nil, err
	}
	src := &biz.SyncSource{
		Name: req.GetName(), Provider: req.GetProvider(), Config: cfg, Env: req.GetEnv(),
		IntervalSeconds: req.GetIntervalSeconds(), GracePeriodSeconds: biz.DefaultSyncGraceSeconds, Enabled: true,
	}
	if req.GracePeriodSeconds != nil {
		src.GracePeriodSeconds = req.GetGracePeriodSeconds()
	}
	if req.Enabled != nil {
		src.Enabled = req.GetEnabled()
	}
	created, err := s.uc.Create(ctx, actor, src, req.GetCredential())
	if err != nil {
		return nil, err
	}
	p, err := toProtoSource(created)
	if err != nil {
		return nil, err
	}
	return &v1.CreateSourceResponse{Source: p}, nil
}

// UpdateSource implements proto CmdbSyncService/UpdateSource.
func (s *CmdbSyncService) UpdateSource(ctx context.Context, req *v1.UpdateSourceRequest) (*v1.UpdateSourceResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	cfg, err := configBytes(req.GetConfig())
	if err != nil {
		return nil, err
	}
	upd := biz.SyncSourceUpdate{
		Name: req.GetName(), Config: cfg, Env: req.GetEnv(), IntervalSeconds: req.GetIntervalSeconds(),
		GracePeriodSeconds: req.GetGracePeriodSeconds(), Enabled: req.GetEnabled(),
	}
	updated, err := s.uc.Update(ctx, actor, req.GetId(), req.GetVersion(), upd, req.Credential)
	if err != nil {
		return nil, err
	}
	p, err := toProtoSource(updated)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateSourceResponse{Source: p}, nil
}

// DeleteSource implements proto CmdbSyncService/DeleteSource.
func (s *CmdbSyncService) DeleteSource(ctx context.Context, req *v1.DeleteSourceRequest) (*v1.DeleteSourceResponse, error) {
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteSourceResponse{}, nil
}

// TriggerSync implements proto CmdbSyncService/TriggerSync.
func (s *CmdbSyncService) TriggerSync(ctx context.Context, req *v1.TriggerSyncRequest) (*v1.TriggerSyncResponse, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return nil, err
	}
	task, err := s.uc.Trigger(ctx, actor, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.TriggerSyncResponse{TaskId: task.ID}, nil
}

// RotateWebhook implements proto CmdbSyncService/RotateWebhook.
func (s *CmdbSyncService) RotateWebhook(ctx context.Context, req *v1.RotateWebhookRequest) (*v1.RotateWebhookResponse, error) {
	token, path, header, err := s.uc.RotateWebhook(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RotateWebhookResponse{Token: token, Path: path, Header: header}, nil
}

// DisableWebhook implements proto CmdbSyncService/DisableWebhook.
func (s *CmdbSyncService) DisableWebhook(ctx context.Context, req *v1.DisableWebhookRequest) (*v1.DisableWebhookResponse, error) {
	if err := s.uc.DisableWebhook(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DisableWebhookResponse{}, nil
}

// ListRuns implements proto CmdbSyncService/ListRuns.
func (s *CmdbSyncService) ListRuns(ctx context.Context, req *v1.ListRunsRequest) (*v1.ListRunsResponse, error) {
	items, total, err := s.uc.ListRuns(ctx, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRunsResponse{Items: make([]*v1.SyncRun, 0, len(items)), Total: total}
	for _, run := range items {
		p := &v1.SyncRun{
			Id: run.ID, SourceId: run.SourceID, TaskId: run.TaskID, Status: run.Status,
			Discovered: run.Discovered, Created: run.Created, Updated: run.Updated, Unchanged: run.Unchanged,
			Disappeared: run.Disappeared, Retired: run.Retired, Failed: run.Failed, Error: run.Error,
			StartTime: timestamppb.New(run.StartTime), FinishTime: optionalTime(run.FinishTime),
			Errors: make([]*structpb.Struct, 0, len(run.Errors)),
		}
		for _, e := range run.Errors {
			st, err := jsonStruct(map[string]any{"modelCode": e.ModelCode, "externalId": e.ExternalID, "name": e.Name, "reason": e.Reason})
			if err != nil {
				return nil, err
			}
			p.Errors = append(p.Errors, st)
		}
		resp.Items = append(resp.Items, p)
	}
	return resp, nil
}

// ListConflicts implements proto CmdbSyncService/ListConflicts。
func (s *CmdbSyncService) ListConflicts(ctx context.Context, req *v1.ListConflictsRequest) (*v1.ListConflictsResponse, error) {
	items, total, err := s.uc.ListConflicts(ctx, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListConflictsResponse{Items: make([]*v1.SyncConflict, 0, len(items)), Total: total}
	for _, c := range items {
		resp.Items = append(resp.Items, &v1.SyncConflict{
			ModelCode: c.ModelCode, ModelName: c.ModelName, ExternalId: c.ExternalID, Name: c.Name, Reason: c.Reason,
			FirstSeen: timestamppb.New(c.FirstSeen), LastSeen: timestamppb.New(c.LastSeen),
		})
	}
	return resp, nil
}
