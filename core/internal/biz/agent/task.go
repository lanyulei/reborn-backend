package biz

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 菜单码（与 system 权限目录一致）：持有「Agent 任务」菜单者可见完整转录。
const menuAgentTask = "agent.task"

// maxIncidentRuns 故障详情「AI 诊断」页签一次返回的运行数。
const maxIncidentRuns = 50

// TaskUsecase 任务：入队（incident 同事务）、人工召唤、干跑、取消、查询、运行详情与评价。
type TaskUsecase struct {
	d   *Deps
	now func() time.Time
}

// NewTaskUsecase new a TaskUsecase.
func NewTaskUsecase(d *Deps) *TaskUsecase { return &TaskUsecase{d: d, now: time.Now} }

// EnqueueRef 故障域交来的任务引用（与 bizincident.AgentTaskRef 同形，经组合根转换）。
type EnqueueRef struct {
	IncidentID string
	Number     string
	Title      string
	Severity   int32
	BusinessID string
	RuleID     string
	Reason     string
	// Kind 任务类型（空 = diagnose）；Version 复盘版本（postmortem 时每个版本只入队一次）。
	Kind    string
	Version int32
}

// Enqueue 在故障域的事务内插一行最小任务（Profile 未定；诊断任务命中按故障唯一时只更新等级，经验沉淀按复盘版本去重）。
// 只可能返回数据库错误。
func (uc *TaskUsecase) Enqueue(ctx context.Context, tx Tx, ref EnqueueRef) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	now := uc.now()
	kind, source, period := KindDiagnose, SourceIncident, ""
	if ref.Kind == KindPostmortem {
		kind, source, period = KindPostmortem, SourcePostmortem, fmt.Sprintf("pm:v%d", ref.Version)
	}
	return uc.d.Tasks.EnqueueTx(ctx, tx, &Task{
		ID: id, Kind: kind, Source: source, IncidentID: ref.IncidentID, IncidentNumber: ref.Number, IncidentTitle: clip(ref.Title, 255),
		Severity: ref.Severity, BusinessID: ref.BusinessID, RuleID: ref.RuleID, Reason: ref.Reason, PeriodKey: period, Status: TaskQueued, NextTime: now, QueuedTime: now, UpdateTime: now,
	})
}

// RunForIncident 人工召唤：对一条活跃故障立即入队一次诊断；已有排队 / 进行中任务时返回它。
// 调用者须持有「查询故障详情」接口权限（故障记录对持有者全局可见）。
func (uc *TaskUsecase) RunForIncident(ctx context.Context, userID, incidentID, profileID, input string) (*TaskView, bool, error) {
	if !uc.d.Config.Enabled {
		return nil, false, ErrDisabled
	}
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, false, err
	}
	inc, err := uc.d.Incidents.Snapshot(ctx, incidentID)
	if err != nil {
		return nil, false, err
	}
	kind := KindDiagnose
	if profileID != "" {
		p, err := uc.d.Profiles.Get(ctx, profileID)
		if err != nil {
			return nil, false, err
		}
		if !p.Spec.Enabled {
			return nil, false, detail(ErrProfileInvalid, "profile is disabled")
		}
		// 任务类型跟随指定的 Profile（经验沉淀 Profile 也可以被人工召唤：重新起草）。
		kind = p.Spec.Kind
	}
	// 诊断只对活跃故障；经验沉淀面向已结束（复盘完成）的故障。
	if kind == KindDiagnose && !inc.Active {
		return nil, false, detail(ErrTaskStateInvalid, "incident is not active")
	}
	if existing, err := uc.d.Tasks.ActiveByIncident(ctx, incidentID, kind); err != nil {
		return nil, false, err
	} else if existing != nil {
		v, err := uc.view(ctx, existing)
		return v, true, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, false, err
	}
	now := uc.now()
	t := &Task{
		ID: id, ProfileID: profileID, Kind: kind, Source: SourceManual, IncidentID: inc.ID, IncidentNumber: inc.Number, IncidentTitle: clip(inc.Title, 255),
		Severity: inc.Severity, BusinessID: inc.BusinessID, Reason: ReasonManual, Status: TaskQueued, NextTime: now, RequesterID: userID,
		Input: strings.TrimSpace(input), QueuedTime: now, UpdateTime: now,
	}
	if err := uc.d.Tasks.Create(ctx, t); err != nil {
		if errors.Is(err, ErrTaskExists) {
			if existing, gerr := uc.d.Tasks.ActiveByIncident(ctx, incidentID, kind); gerr == nil && existing != nil {
				v, verr := uc.view(ctx, existing)
				return v, true, verr
			}
		}
		return nil, false, err
	}
	v, err := uc.view(ctx, t)
	return v, false, err
}

// DryRun 对指定历史故障发起一次干跑（阶段 4 只捕获不写入）；不受按故障唯一限制，不计配额。
func (uc *TaskUsecase) DryRun(ctx context.Context, userID, profileID, incidentID string) (*TaskView, error) {
	if !uc.d.Config.Enabled {
		return nil, ErrDisabled
	}
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, err
	}
	p, err := uc.d.Profiles.Get(ctx, profileID)
	if err != nil {
		return nil, err
	}
	inc, err := uc.d.Incidents.Snapshot(ctx, incidentID)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := uc.now()
	t := &Task{
		ID: id, ProfileID: p.ID, Kind: p.Spec.Kind, Source: SourceDryRun, DryRun: true, IncidentID: inc.ID, IncidentNumber: inc.Number, IncidentTitle: clip(inc.Title, 255),
		Severity: inc.Severity, BusinessID: inc.BusinessID, Reason: ReasonDryRun, Status: TaskQueued, NextTime: now, RequesterID: userID, QueuedTime: now, UpdateTime: now,
	}
	if err := uc.d.Tasks.Create(ctx, t); err != nil {
		return nil, err
	}
	return uc.view(ctx, t)
}

// Cancel 取消排队 / 进行中的任务：发起人、故障指挥、管理员或持有「Agent 任务」菜单者。
func (uc *TaskUsecase) Cancel(ctx context.Context, userID, id, note string) (*TaskView, error) {
	t, err := uc.d.Tasks.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !t.Active() {
		return nil, ErrTaskStateInvalid
	}
	allowed := t.RequesterID == userID
	if !allowed {
		admin, commander, _, err := uc.d.Incidents.MemberAccess(ctx, userID, t.IncidentID)
		if err != nil {
			return nil, err
		}
		allowed = admin || commander
	}
	if !allowed {
		ok, err := uc.d.Accounts.HasMenu(ctx, userID, menuAgentTask)
		if err != nil {
			return nil, err
		}
		allowed = ok
	}
	if !allowed {
		return nil, ErrForbidden
	}
	result := "已取消"
	if note = strings.TrimSpace(note); note != "" {
		result = clip("已取消："+note, 255)
	}
	ok, err := uc.d.Tasks.Cancel(ctx, id, result)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, detail(ErrTaskStateInvalid, "task already finished")
	}
	if t.RunID != "" {
		// 只收口仍为 running 的运行；运行器之后的写回会因围栏落空而放弃。
		if _, err := uc.d.Runs.Close(ctx, t.RunID, RunCancelled, "cancelled", uc.now()); err != nil {
			return nil, err
		}
	}
	t, err = uc.d.Tasks.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, t)
}

// List 分页查询（持有「Agent 任务」菜单者的列表页；接口授权在中间件）。
func (uc *TaskUsecase) List(ctx context.Context, filter TaskFilter, page, pageSize int64) ([]*TaskView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Tasks.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, items)
	return views, total, err
}

// Get 任务详情与其运行摘要（与运行详情同一可见性：须持有「查询故障详情」接口权限）。
func (uc *TaskUsecase) Get(ctx context.Context, userID, id string) (*TaskView, *RunView, error) {
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, nil, err
	}
	t, err := uc.d.Tasks.Get(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	v, err := uc.view(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	if t.RunID == "" {
		return v, nil, nil
	}
	run, err := uc.d.Runs.Get(ctx, t.RunID)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			return v, nil, nil
		}
		return nil, nil, err
	}
	rv, err := uc.runView(ctx, userID, run)
	return v, rv, err
}

// RunDetail 运行详情：阶段摘要与产出对持有「查询故障详情」权限者开放；完整转录只对持有「Agent 任务」菜单者返回。
func (uc *TaskUsecase) RunDetail(ctx context.Context, userID, id string) (*RunView, *Transcript, bool, error) {
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, nil, false, err
	}
	run, err := uc.d.Runs.Get(ctx, id)
	if err != nil {
		return nil, nil, false, err
	}
	v, err := uc.runView(ctx, userID, run)
	if err != nil {
		return nil, nil, false, err
	}
	full, err := uc.d.Accounts.HasMenu(ctx, userID, menuAgentTask)
	if err != nil {
		return nil, nil, false, err
	}
	if !full {
		return v, nil, false, nil
	}
	msgs, calls, err := uc.d.Conversations.Transcript(ctx, run.ConversationID)
	if err != nil {
		return nil, nil, false, err
	}
	return v, &Transcript{Messages: msgs, Calls: calls}, true, nil
}

// ListIncidentRuns 一条故障的全部运行（最新在前）与当前活跃任务。
func (uc *TaskUsecase) ListIncidentRuns(ctx context.Context, userID, incidentID string) ([]*RunView, *TaskView, error) {
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, nil, err
	}
	runs, err := uc.d.Runs.ListByIncident(ctx, incidentID, maxIncidentRuns)
	if err != nil {
		return nil, nil, err
	}
	views := make([]*RunView, 0, len(runs))
	for _, r := range runs {
		v, err := uc.runView(ctx, userID, r)
		if err != nil {
			return nil, nil, err
		}
		views = append(views, v)
	}
	active, err := uc.d.Tasks.ActiveByIncident(ctx, incidentID, KindDiagnose)
	if err != nil {
		return nil, nil, err
	}
	var tv *TaskView
	if active != nil {
		if tv, err = uc.view(ctx, active); err != nil {
			return nil, nil, err
		}
	}
	return views, tv, nil
}

// Rate 指挥 / 处理人 / 管理员对一次运行的发现标记有用 / 无用（只保留最后一次）。
func (uc *TaskUsecase) Rate(ctx context.Context, userID, id string, feedback int32, note string) (*RunView, error) {
	if feedback != FeedbackHelpful && feedback != FeedbackUnhelpful {
		return nil, detail(ErrTaskStateInvalid, "feedback must be 1 or 2")
	}
	run, err := uc.d.Runs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if run.Status == RunRunning {
		return nil, detail(ErrTaskStateInvalid, "run is still in progress")
	}
	admin, commander, responder, err := uc.d.Incidents.MemberAccess(ctx, userID, run.IncidentID)
	if err != nil {
		return nil, err
	}
	if !admin && !commander && !responder {
		return nil, ErrForbidden
	}
	if err := uc.d.Runs.SetFeedback(ctx, id, feedback, userID, clip(strings.TrimSpace(note), 500), uc.now()); err != nil {
		return nil, err
	}
	run, err = uc.d.Runs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.runView(ctx, userID, run)
}

// Transcript 一次运行的完整转录（助手会话的消息与工具调用记录）。
type Transcript struct {
	Messages []*bizassistant.Message
	Calls    []*bizassistant.ToolCall
}

// requireReader 调用者须持有「查询故障详情」接口权限。
func (uc *TaskUsecase) requireReader(ctx context.Context, userID string) error {
	ok, err := uc.d.Incidents.CanRead(ctx, userID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrForbidden
	}
	return nil
}

// view / views 补显示名。
func (uc *TaskUsecase) view(ctx context.Context, t *Task) (*TaskView, error) {
	views, err := uc.views(ctx, []*Task{t})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

func (uc *TaskUsecase) views(ctx context.Context, items []*Task) ([]*TaskView, error) {
	profileIDs := make([]string, 0, len(items))
	userIDs := make([]string, 0, len(items))
	for _, t := range items {
		profileIDs = append(profileIDs, t.ProfileID)
		userIDs = append(userIDs, t.RequesterID)
	}
	profiles, err := uc.profileNames(ctx, profileIDs)
	if err != nil {
		return nil, err
	}
	names, err := uc.d.Accounts.Names(ctx, uniq(userIDs))
	if err != nil {
		return nil, err
	}
	out := make([]*TaskView, 0, len(items))
	for _, t := range items {
		out = append(out, &TaskView{Task: t, ProfileName: profiles[t.ProfileID], RequesterName: names[t.RequesterID]})
	}
	return out, nil
}

// profileNames 批量取 Profile 名称。
func (uc *TaskUsecase) profileNames(ctx context.Context, ids []string) (map[string]string, error) {
	ids = uniq(ids)
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	profiles, err := uc.d.Profiles.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, p := range profiles {
		out[p.ID] = p.Spec.Name
	}
	return out, nil
}

// runView 补显示名与评价权。
func (uc *TaskUsecase) runView(ctx context.Context, userID string, run *Run) (*RunView, error) {
	profiles, err := uc.profileNames(ctx, []string{run.ProfileID})
	if err != nil {
		return nil, err
	}
	names, err := uc.d.Accounts.Names(ctx, uniq([]string{run.AgentUserID, run.FeedbackUser}))
	if err != nil {
		return nil, err
	}
	v := &RunView{Run: run, ProfileName: profiles[run.ProfileID], AgentUserName: names[run.AgentUserID], FeedbackUserName: names[run.FeedbackUser]}
	if inc, err := uc.d.Incidents.Snapshot(ctx, run.IncidentID); err == nil {
		v.IncidentNumber = inc.Number
	}
	if userID != "" && run.Status != RunRunning {
		admin, commander, responder, err := uc.d.Incidents.MemberAccess(ctx, userID, run.IncidentID)
		if err == nil {
			v.CanRate = admin || commander || responder
		}
	}
	return v, nil
}

// ptr 取地址。
func ptr[T any](v T) *T { return &v }
