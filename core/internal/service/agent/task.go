package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/agent/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/agent"
	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// TaskService 实现 AgentTaskServiceHTTPServer。
type TaskService struct {
	uc *biz.TaskUsecase
}

// NewTaskService new a TaskService.
func NewTaskService(uc *biz.TaskUsecase) *TaskService { return &TaskService{uc: uc} }

// ListTasks implements proto AgentTaskService/ListTasks.
func (s *TaskService) ListTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksResponse, error) {
	items, total, err := s.uc.List(ctx, biz.TaskFilter{ProfileID: req.GetProfileId(), IncidentID: req.GetIncidentId(), Status: req.GetStatus(), Source: req.GetSource()}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListTasksResponse{Items: make([]*v1.Task, 0, len(items)), Total: total}
	for _, it := range items {
		out.Items = append(out.Items, toProtoTask(it))
	}
	return out, nil
}

// GetTask implements proto AgentTaskService/GetTask.
func (s *TaskService) GetTask(ctx context.Context, req *v1.GetTaskRequest) (*v1.GetTaskResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	t, run, err := s.uc.Get(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetTaskResponse{Task: toProtoTask(t)}
	if run != nil {
		out.Run = toProtoRun(run)
	}
	return out, nil
}

// RunForIncident implements proto AgentTaskService/RunForIncident.
func (s *TaskService) RunForIncident(ctx context.Context, req *v1.RunForIncidentRequest) (*v1.RunForIncidentResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	t, existing, err := s.uc.RunForIncident(ctx, uid, req.GetIncidentId(), req.GetProfileId(), req.GetInput())
	if err != nil {
		return nil, err
	}
	return &v1.RunForIncidentResponse{Task: toProtoTask(t), Existing: existing}, nil
}

// CancelTask implements proto AgentTaskService/CancelTask.
func (s *TaskService) CancelTask(ctx context.Context, req *v1.CancelTaskRequest) (*v1.CancelTaskResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.uc.Cancel(ctx, uid, req.GetId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.CancelTaskResponse{Task: toProtoTask(t)}, nil
}

// GetRun implements proto AgentTaskService/GetRun.
func (s *TaskService) GetRun(ctx context.Context, req *v1.GetRunRequest) (*v1.GetRunResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	run, transcript, visible, err := s.uc.RunDetail(ctx, uid, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.GetRunResponse{Run: toProtoRun(run), TranscriptVisible: visible}
	if transcript != nil {
		out.Transcript = toProtoTranscript(transcript)
	}
	return out, nil
}

// ListIncidentRuns implements proto AgentTaskService/ListIncidentRuns.
func (s *TaskService) ListIncidentRuns(ctx context.Context, req *v1.ListIncidentRunsRequest) (*v1.ListIncidentRunsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	runs, active, err := s.uc.ListIncidentRuns(ctx, uid, req.GetIncidentId())
	if err != nil {
		return nil, err
	}
	out := &v1.ListIncidentRunsResponse{Items: make([]*v1.Run, 0, len(runs))}
	for _, r := range runs {
		out.Items = append(out.Items, toProtoRun(r))
	}
	if active != nil {
		out.ActiveTask = toProtoTask(active)
	}
	return out, nil
}

// RateRun implements proto AgentTaskService/RateRun.
func (s *TaskService) RateRun(ctx context.Context, req *v1.RateRunRequest) (*v1.RateRunResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	run, err := s.uc.Rate(ctx, uid, req.GetId(), req.GetFeedback(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.RateRunResponse{Run: toProtoRun(run)}, nil
}

// toProtoTranscript 把模型协议消息聚合为展示轮次：assistant 消息带上它发起的工具调用（含审计状态与出参）；
// tool 消息不单独展示（内容已在工具调用的 output 里）。
func toProtoTranscript(t *biz.Transcript) []*v1.TranscriptMessage {
	callsByMessage := make(map[string][]*bizassistant.ToolCall, len(t.Calls))
	for _, c := range t.Calls {
		callsByMessage[c.MessageID] = append(callsByMessage[c.MessageID], c)
	}
	out := make([]*v1.TranscriptMessage, 0, len(t.Messages))
	for _, m := range t.Messages {
		if m.Role == bizassistant.RoleTool {
			continue
		}
		item := &v1.TranscriptMessage{Id: m.ID, Role: m.Role, Content: m.Content, Reasoning: m.Reasoning, CreateTime: timestamppb.New(m.CreateTime)}
		if m.Role == bizassistant.RoleAssistant {
			byCallID := make(map[string]*bizassistant.ToolCall, len(callsByMessage[m.ID]))
			for _, c := range callsByMessage[m.ID] {
				byCallID[c.CallID] = c
			}
			for _, ref := range m.ToolCalls {
				tc := byCallID[ref.ID]
				call := &v1.TranscriptToolCall{Tool: ref.Name, Input: ref.Arguments}
				if tc != nil {
					call.Id, call.Title, call.Write, call.Status, call.Error, call.DurationMs = tc.ID, tc.Title, tc.Write, tc.Status, tc.Error, tc.DurationMS
					if len(tc.Input) > 0 {
						call.Input = string(tc.Input)
					}
					if len(tc.Output) > 0 {
						call.Output = string(tc.Output)
					}
				}
				item.ToolCalls = append(item.ToolCalls, call)
			}
		}
		out = append(out, item)
	}
	return out
}
