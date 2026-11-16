package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
)

// InstanceService 实现 api/workflow/v1 生成的 WorkflowInstanceServiceServer。
type InstanceService struct {
	uc    *biz.RuntimeUsecase
	jobs  biz.JobTx
	authz PermissionChecker
}

// NewInstanceService new an InstanceService.
func NewInstanceService(uc *biz.RuntimeUsecase, jobs biz.JobTx, authz PermissionChecker) *InstanceService {
	return &InstanceService{uc: uc, jobs: jobs, authz: authz}
}

// StartInstance implements proto WorkflowInstanceService/StartInstance：发起人取自登录身份。
func (s *InstanceService) StartInstance(ctx context.Context, req *v1.StartInstanceRequest) (*v1.StartInstanceResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	in, err := s.uc.Start(ctx, biz.StartParams{
		DefinitionKey: req.GetDefinitionKey(), Title: req.GetTitle(), Initiator: uid,
		InitiatorDept: req.GetInitiatorDept(), Variables: req.GetVariables().AsMap(), RequestID: req.GetRequestId(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.StartInstanceResponse{Instance: toProtoInstance(in, nil, nil)}, nil
}

// GetInstance implements proto WorkflowInstanceService/GetInstance：参与者可看；
// 持有「查询全部流程实例」权限者可看任意实例。
func (s *InstanceService) GetInstance(ctx context.Context, req *v1.GetInstanceRequest) (*v1.GetInstanceResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	viewAll, err := s.authz.Allowed(ctx, uid, opListInstances)
	if err != nil {
		return nil, err
	}
	d, err := s.uc.Get(ctx, req.GetId(), uid, viewAll)
	if err != nil {
		return nil, err
	}
	st := d.State
	running := st.Instance.Status == biz.InstanceRunning
	// 管理动作只对进行中的流程有意义：已结束的不必再查权限。
	var canManage, canReassign bool
	if running {
		if canManage, err = s.authz.Allowed(ctx, uid, opTerminateInstance); err != nil {
			return nil, err
		}
		if canReassign, err = s.authz.Allowed(ctx, uid, opReassignTask); err != nil {
			return nil, err
		}
	}
	resp := &v1.GetInstanceResponse{
		Instance:  toProtoInstance(&st.Instance, d.Names, currentNodes(st)),
		Graph:     structOf(d.Graph),
		Variables: structOfMap(st.Instance.Variables),
		Tasks:     make([]*v1.Task, 0, len(st.Tasks)),
		Histories: make([]*v1.HistoryEntry, 0, len(st.Histories)),
		MyTaskId:  d.MyTaskID,
		// 子流程不能单独撤回（要撤回就撤回父流程）。
		CanCancel: running && st.Instance.Initiator == uid && st.Instance.ParentInstanceID == "",
		CanManage: canManage, CanReassign: canReassign,
		Waits:    make([]*v1.InstanceWait, 0, len(st.Jobs)),
		Children: make([]*v1.ChildInstance, 0, len(d.Children)),
	}
	for _, j := range st.Jobs {
		resp.Waits = append(resp.Waits, &v1.InstanceWait{
			JobId: j.ID, NodeId: j.NodeID, Kind: j.Kind, Status: j.Status, DueTime: ts(j.DueTime), Attempts: j.Attempts, LastError: j.LastError,
		})
	}
	for _, sg := range d.Signals {
		resp.Waits = append(resp.Waits, &v1.InstanceWait{NodeId: sg.NodeID, Kind: "signal", Status: biz.JobPending, Signal: sg.Signal})
	}
	for _, c := range d.Children {
		resp.Children = append(resp.Children, &v1.ChildInstance{Id: c.ID, NodeId: c.NodeID, DefinitionName: c.DefinitionName, Status: c.Status})
	}
	for _, t := range st.Tasks {
		pt := &v1.Task{
			Id: t.ID, NodeId: t.NodeID, NodeName: t.NodeName, Kind: t.Kind, Mode: t.Mode, Status: t.Status,
			CreateTime: ts(t.CreateTime), EndTime: ts(t.EndTime), DueTime: ts(t.DueTime),
			Assignees: make([]*v1.TaskAssignee, 0, len(t.Assignees)),
		}
		for _, a := range t.Assignees {
			pt.Assignees = append(pt.Assignees, &v1.TaskAssignee{
				UserId: a.UserID, UserName: d.Names[a.UserID], Status: a.Status, Comment: a.Comment, ActionTime: ts(a.ActionTime),
				Origin: a.Origin, OnBehalf: a.OnBehalf, OnBehalfName: d.Names[a.OnBehalf],
			})
			if t.ID == d.MyTaskID && a.UserID == uid && a.Status == biz.AssigneePending {
				resp.MySeatOrigin = a.Origin
			}
		}
		resp.Tasks = append(resp.Tasks, pt)
	}
	for _, h := range st.Histories {
		resp.Histories = append(resp.Histories, &v1.HistoryEntry{
			Id: h.ID, NodeId: h.NodeID, NodeName: h.NodeName, Action: h.Action, Operator: h.Operator,
			OperatorName: d.Names[h.Operator], Comment: h.Comment, Detail: structOfMap(h.Detail), CreateTime: ts(h.CreateTime),
		})
	}
	return resp, nil
}

// currentNodes 当前待处理的节点名称。
func currentNodes(s *biz.State) []string {
	var out []string
	for _, t := range s.Tasks {
		if t.Status == biz.TaskPending {
			out = append(out, t.NodeName)
		}
	}
	return out
}

func (s *InstanceService) list(ctx context.Context, filter biz.InstanceFilter, page int64, pageSize int32) (*v1.ListInstancesResponse, error) {
	items, names, total, err := s.uc.ListInstances(ctx, filter, page, int64(pageSize))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListInstancesResponse{Items: make([]*v1.Instance, 0, len(items)), Total: total}
	for _, it := range items {
		resp.Items = append(resp.Items, toProtoInstance(&it.Instance, names, it.CurrentNodes))
	}
	return resp, nil
}

// ListMyInstances implements proto WorkflowInstanceService/ListMyInstances：恒按登录身份过滤。
func (s *InstanceService) ListMyInstances(ctx context.Context, req *v1.ListMyInstancesRequest) (*v1.ListInstancesResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	return s.list(ctx, biz.InstanceFilter{Keyword: req.GetKeyword(), Status: req.Status, Initiator: uid}, req.GetPage(), req.GetPageSize())
}

// ListInstances implements proto WorkflowInstanceService/ListInstances（管理视图，由接口授权把关）。
func (s *InstanceService) ListInstances(ctx context.Context, req *v1.ListInstancesRequest) (*v1.ListInstancesResponse, error) {
	return s.list(ctx, biz.InstanceFilter{
		Keyword: req.GetKeyword(), Status: req.Status, DefinitionID: req.GetDefinitionId(), Initiator: req.GetInitiator(),
		BusinessType: req.GetBusinessType(),
	}, req.GetPage(), req.GetPageSize())
}

// CancelInstance implements proto WorkflowInstanceService/CancelInstance。
func (s *InstanceService) CancelInstance(ctx context.Context, req *v1.CancelInstanceRequest) (*v1.CancelInstanceResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Cancel(ctx, req.GetId(), uid, req.GetComment()); err != nil {
		return nil, err
	}
	return &v1.CancelInstanceResponse{}, nil
}

// TerminateInstance implements proto WorkflowInstanceService/TerminateInstance。
func (s *InstanceService) TerminateInstance(ctx context.Context, req *v1.TerminateInstanceRequest) (*v1.TerminateInstanceResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Terminate(ctx, req.GetId(), uid, req.GetReason()); err != nil {
		return nil, err
	}
	return &v1.TerminateInstanceResponse{}, nil
}

// SignalInstance implements proto WorkflowInstanceService/SignalInstance（管理动作，由接口授权把关）。
func (s *InstanceService) SignalInstance(ctx context.Context, req *v1.SignalInstanceRequest) (*v1.TaskActionResponse, error) {
	status, err := s.uc.Signal(ctx, req.GetId(), req.GetName(), req.GetVariables().AsMap())
	if err != nil {
		return nil, err
	}
	return &v1.TaskActionResponse{InstanceStatus: status}, nil
}

// RetryInstanceJob implements proto WorkflowInstanceService/RetryInstanceJob（管理动作，由接口授权把关）。
func (s *InstanceService) RetryInstanceJob(ctx context.Context, req *v1.RetryInstanceJobRequest) (*v1.RetryInstanceJobResponse, error) {
	if err := s.uc.RetryJob(ctx, s.jobs, req.GetId(), req.GetJobId()); err != nil {
		return nil, err
	}
	return &v1.RetryInstanceJobResponse{}, nil
}

// SkipInstanceJob implements proto WorkflowInstanceService/SkipInstanceJob（管理动作，由接口授权把关）。
func (s *InstanceService) SkipInstanceJob(ctx context.Context, req *v1.SkipInstanceJobRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.SkipJob(ctx, s.jobs, req.GetId(), req.GetJobId(), uid, req.GetReason())
	})
}

// HandoverTasks implements proto WorkflowInstanceService/HandoverTasks（管理动作，由接口授权把关）。
func (s *InstanceService) HandoverTasks(ctx context.Context, req *v1.HandoverTasksRequest) (*v1.HandoverTasksResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	moved, failed, next, err := s.uc.Handover(ctx, req.GetFromUserId(), req.GetToUserId(), uid, req.GetReason(), req.GetCursor())
	if err != nil {
		return nil, err
	}
	return &v1.HandoverTasksResponse{
		Moved: int32(min(moved, 1<<30)), Failed: int32(min(failed, 1<<30)), More: next != "", NextCursor: next, //nolint:gosec // 已封顶，不会溢出
	}, nil
}

func toProtoInstance(in *biz.Instance, names map[string]string, current []string) *v1.Instance {
	return &v1.Instance{
		Id: in.ID, DefinitionId: in.DefinitionID, DefinitionKey: in.DefinitionKey, DefinitionName: in.DefinitionName,
		Version: in.Version, BusinessType: in.BusinessType, BusinessId: in.BusinessID, Title: in.Title,
		Initiator: in.Initiator, InitiatorName: names[in.Initiator], InitiatorDept: in.InitiatorDept, Status: in.Status,
		StartTime: ts(in.StartTime), EndTime: ts(in.EndTime), CurrentNodes: current, ParentInstanceId: in.ParentInstanceID,
		RootId: in.RootID, RootBusinessType: in.RootBusinessType, RootBusinessId: in.RootBusinessID,
	}
}

// TaskService 实现 api/workflow/v1 生成的 WorkflowTaskServiceServer。
type TaskService struct {
	uc *biz.RuntimeUsecase
}

// NewTaskService new a TaskService.
func NewTaskService(uc *biz.RuntimeUsecase) *TaskService {
	return &TaskService{uc: uc}
}

func (s *TaskService) list(ctx context.Context, view string, req *v1.ListTasksRequest) (*v1.ListTasksResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	items, names, total, err := s.uc.ListTasks(ctx, uid, view, req.GetKeyword(), req.GetBusinessType(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListTasksResponse{Items: make([]*v1.TaskItem, 0, len(items)), Total: total}
	for _, it := range items {
		resp.Items = append(resp.Items, &v1.TaskItem{
			TaskId: it.TaskID, InstanceId: it.InstanceID, Title: it.Title, DefinitionName: it.DefinitionName,
			NodeName: it.NodeName, Initiator: it.Initiator, InitiatorName: names[it.Initiator],
			InstanceStatus: it.InstanceStatus, Status: it.Status, CreateTime: ts(it.CreateTime), ActionTime: ts(it.ActionTime),
			BusinessType: it.BusinessType, BusinessId: it.BusinessID,
		})
	}
	return resp, nil
}

// ListTodoTasks implements proto WorkflowTaskService/ListTodoTasks。
func (s *TaskService) ListTodoTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksResponse, error) {
	return s.list(ctx, biz.TaskViewTodo, req)
}

// ListDoneTasks implements proto WorkflowTaskService/ListDoneTasks。
func (s *TaskService) ListDoneTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksResponse, error) {
	return s.list(ctx, biz.TaskViewDone, req)
}

// ListCcTasks implements proto WorkflowTaskService/ListCcTasks。
func (s *TaskService) ListCcTasks(ctx context.Context, req *v1.ListTasksRequest) (*v1.ListTasksResponse, error) {
	return s.list(ctx, biz.TaskViewCC, req)
}

// act 任务处理动作的公共骨架：取登录身份 → 执行 → 回传实例状态。
func act(ctx context.Context, fn func(uid string) (int32, error)) (*v1.TaskActionResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	status, err := fn(uid)
	if err != nil {
		return nil, err
	}
	return &v1.TaskActionResponse{InstanceStatus: status}, nil
}

// ApproveTask implements proto WorkflowTaskService/ApproveTask。
func (s *TaskService) ApproveTask(ctx context.Context, req *v1.ApproveTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) { return s.uc.Approve(ctx, req.GetId(), uid, req.GetComment()) })
}

// RejectTask implements proto WorkflowTaskService/RejectTask。
func (s *TaskService) RejectTask(ctx context.Context, req *v1.RejectTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) { return s.uc.Reject(ctx, req.GetId(), uid, req.GetComment()) })
}

// ReturnTask implements proto WorkflowTaskService/ReturnTask。
func (s *TaskService) ReturnTask(ctx context.Context, req *v1.ReturnTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.Return(ctx, req.GetId(), uid, req.GetToNodeId(), req.GetComment())
	})
}

// ListReturnTargets implements proto WorkflowTaskService/ListReturnTargets。
func (s *TaskService) ListReturnTargets(ctx context.Context, req *v1.ListReturnTargetsRequest) (*v1.ListReturnTargetsResponse, error) {
	uid, err := userID(ctx)
	if err != nil {
		return nil, err
	}
	nodes, err := s.uc.ReturnTargets(ctx, req.GetId(), uid)
	if err != nil {
		return nil, err
	}
	resp := &v1.ListReturnTargetsResponse{Items: make([]*v1.ReturnTarget, 0, len(nodes))}
	for _, n := range nodes {
		resp.Items = append(resp.Items, &v1.ReturnTarget{NodeId: n.ID, NodeName: n.Name})
	}
	return resp, nil
}

// TransferTask implements proto WorkflowTaskService/TransferTask。
func (s *TaskService) TransferTask(ctx context.Context, req *v1.TransferTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.Transfer(ctx, req.GetId(), uid, req.GetToUserId(), req.GetComment())
	})
}

// ReassignTask implements proto WorkflowTaskService/ReassignTask（管理动作，由接口授权把关）。
func (s *TaskService) ReassignTask(ctx context.Context, req *v1.ReassignTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.Reassign(ctx, req.GetId(), uid, req.GetUserIds(), req.GetReason())
	})
}

// AddSignTask implements proto WorkflowTaskService/AddSignTask。
func (s *TaskService) AddSignTask(ctx context.Context, req *v1.AddSignTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.AddSign(ctx, req.GetId(), uid, req.GetUserIds(), req.GetPosition(), req.GetComment())
	})
}

// DelegateTask implements proto WorkflowTaskService/DelegateTask。
func (s *TaskService) DelegateTask(ctx context.Context, req *v1.DelegateTaskRequest) (*v1.TaskActionResponse, error) {
	return act(ctx, func(uid string) (int32, error) {
		return s.uc.Delegate(ctx, req.GetId(), uid, req.GetToUserId(), req.GetComment())
	})
}

// ListUserCandidates implements proto WorkflowTaskService/ListUserCandidates。
func (s *TaskService) ListUserCandidates(ctx context.Context, req *v1.ListWorkflowUserCandidatesRequest) (*v1.ListWorkflowUserCandidatesResponse, error) {
	users, err := s.uc.Candidates(ctx, req.GetKeyword())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListWorkflowUserCandidatesResponse{Items: make([]*v1.WorkflowUserCandidate, 0, len(users))}
	for _, u := range users {
		resp.Items = append(resp.Items, &v1.WorkflowUserCandidate{Id: u.ID, Username: u.Username, Nickname: u.Nickname})
	}
	return resp, nil
}
