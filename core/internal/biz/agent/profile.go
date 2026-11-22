package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 阶段 / 预算的界（与 proto 校验一致，biz 再守一次：Profile 也可能来自内置缺省）。
const (
	maxStageSteps    = 15
	maxStageTools    = 30
	minBudgetTimeout = 60
	maxBudgetTimeout = 1800
	defaultHourly    = 30
	defaultMinConf   = 80
	defaultMaxTarget = 3
	// prodEnv 生产环境编码：缺省不允许自动执行，须管理员在 Profile 里显式勾选。
	prodEnv = "prod"
)

// allowedWriteTools 可进白名单的写工具（工具名）：Agent 想「做事」只有这些出口；其余写工具即使账号有权限也不下发。
var allowedWriteTools = map[string][]string{
	"IncidentService_AddFinding":             {StageConclude},
	"IncidentService_MarkChange":             {StageConclude},
	"IncidentService_AttachAlerts":           {StageConclude},
	"IncidentService_AddComment":             {StageConclude},
	"IncidentProposalService_CreateProposal": {StageConclude},
	"KnowledgeService_CreateDocument":        {StageConclude},
}

// 运行器自己用（不经模型）的工具名。
const (
	toolAcquireLease   = "IncidentService_AcquireAgentLease"
	toolReleaseLease   = "IncidentService_ReleaseAgentLease"
	toolGetContext     = "IncidentService_GetIncidentContext"
	toolAddFinding     = "IncidentService_AddFinding"
	toolCreateProposal = "IncidentProposalService_CreateProposal"
	toolAttachAlerts   = "IncidentService_AttachAlerts"
	toolMarkChange     = "IncidentService_MarkChange"
	toolQueryRange     = "MonitorDatasourceService_QueryDatasourceRange"
	toolQueryInstant   = "MonitorDatasourceService_QueryDatasource"
	toolK8sMetrics     = "K8sInsightService_QueryMetrics"
	toolCreateDocument = "KnowledgeService_CreateDocument"
)

// writeToolsByKind 各类型 Profile 允许的写工具：诊断不建知识文档；经验沉淀只建知识草稿、可留评论。
var writeToolsByKind = map[string][]string{
	KindDiagnose:   {toolAddFinding, toolMarkChange, toolAttachAlerts, "IncidentService_AddComment", toolCreateProposal},
	KindPostmortem: {toolCreateDocument, "IncidentService_AddComment"},
}

// DefaultStagesFor 某类型的缺省阶段：诊断四阶段；经验沉淀两阶段（读复盘与上下文 → 起草知识文档）。
func DefaultStagesFor(kind string) []StageSpec {
	if kind != KindPostmortem {
		return DefaultStages()
	}
	return []StageSpec{
		{Key: StageCollect, MaxSteps: 6, Tools: []string{
			toolGetContext, "IncidentPostmortemService_GetPostmortem", "IncidentPostmortemService_ListPostmortemVersions",
			"IncidentService_ListTimeline", "IncidentService_ListChanges", "KnowledgeService_Search",
		}},
		{Key: StageConclude, MaxSteps: 3, Tools: []string{toolCreateDocument}},
	}
}

// DefaultStages 内置缺省的四阶段配置（设计 §5.1）。
func DefaultStages() []StageSpec {
	return []StageSpec{
		{Key: StageCollect, MaxSteps: 6, Tools: []string{
			toolGetContext, "MonitorAlertService_GetAlert", "CmdbCIService_GetCI", "CmdbRelationService_ListRelated", "IncidentService_ListChanges",
		}},
		{Key: StageHypothesize, MaxSteps: 1},
		{Key: StageVerify, MaxSteps: 9, Tools: []string{
			"MonitorDatasourceService_ListDatasources", "MonitorDatasourceService_QueryDatasource", toolQueryRange,
			"K8sInsightService_DiagnosePod", "K8sInsightService_DiagnoseWorkload", "K8sResourceService_ListObjectEvents", "K8sInsightService_QueryMetrics",
			"JobExecutionService_GetJobExecution", "JobExecutionService_GetJobExecutionLog", "WorkflowInstanceService_GetInstance",
			"KnowledgeService_Search", "IncidentService_SearchIncidents", "IncidentPostmortemService_ListPostmortemVersions",
		}},
		{Key: StageConclude, MaxSteps: 4, Tools: []string{
			toolAddFinding, toolMarkChange, toolCreateProposal, toolAttachAlerts, "IncidentService_AddComment",
		}},
	}
}

// DefaultBudget 内置缺省预算。
func DefaultBudget() BudgetSpec {
	return BudgetSpec{MaxSteps: 20, TimeoutS: 480, PromptTokens: 150_000, CompletionTokens: 20_000}
}

// DefaultAuto 内置缺省的自动通过条件（缺省关闭）。
func DefaultAuto() AutoSpec {
	return AutoSpec{Enabled: false, MinConfidence: defaultMinConf, RequireMetricEvidence: true, MaxTargets: defaultMaxTarget, Envs: []string{"dev", "test", "staging"}}
}

// ProfileUsecase Profile 的增删改查、干跑与变更历史。
type ProfileUsecase struct {
	d   *Deps
	now func() time.Time
}

// NewProfileUsecase new a ProfileUsecase.
func NewProfileUsecase(d *Deps) *ProfileUsecase { return &ProfileUsecase{d: d, now: time.Now} }

// List 分页查询。
func (uc *ProfileUsecase) List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*ProfileView, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Profiles.List(ctx, strings.TrimSpace(keyword), enabled, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, items)
	return views, total, err
}

// Get 详情。
func (uc *ProfileUsecase) Get(ctx context.Context, id string) (*ProfileView, error) {
	p, err := uc.d.Profiles.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.views(ctx, []*Profile{p})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// views 补显示名。
func (uc *ProfileUsecase) views(ctx context.Context, items []*Profile) ([]*ProfileView, error) {
	ids := make([]string, 0, len(items)*2)
	for _, p := range items {
		ids = append(ids, p.Spec.AgentUserID, p.UpdateUser)
	}
	names, err := uc.d.Accounts.Names(ctx, uniq(ids))
	if err != nil {
		return nil, err
	}
	out := make([]*ProfileView, 0, len(items))
	for _, p := range items {
		name, ok := names[p.Spec.AgentUserID]
		out = append(out, &ProfileView{Profile: p, AgentUserName: name, AgentUserGone: !ok, UpdateUserName: names[p.UpdateUser]})
	}
	return out, nil
}

// Create 新建：校验内容、机器账号；写变更历史（before 为空）。
func (uc *ProfileUsecase) Create(ctx context.Context, userID string, spec ProfileSpec) (*ProfileView, error) {
	if err := uc.normalize(ctx, &spec); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := uc.now()
	p := &Profile{ID: id, Spec: spec, Version: 1, CreateTime: now, UpdateTime: now, CreateUser: userID, UpdateUser: userID}
	if err := uc.d.Profiles.Create(ctx, p); err != nil {
		return nil, err
	}
	if err := uc.history(ctx, p, nil, userID, "新建"); err != nil {
		return nil, err
	}
	return uc.Get(ctx, p.ID)
}

// Update 编辑（乐观锁）：每次变更写历史；生产环境自动执行开关的变化在历史里可见，并通知管理员角色。
func (uc *ProfileUsecase) Update(ctx context.Context, userID, id string, spec ProfileSpec, version int32, resetBreaker bool) (*ProfileView, error) {
	cur, err := uc.d.Profiles.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := uc.normalize(ctx, &spec); err != nil {
		return nil, err
	}
	before := cur.Spec
	next := &Profile{ID: id, Spec: spec, Version: version, UpdateUser: userID}
	if err := uc.d.Profiles.Update(ctx, next, version); err != nil {
		return nil, err
	}
	note := "编辑"
	if resetBreaker && cur.BreakerOpen {
		if err := uc.d.Profiles.SetBreaker(ctx, id, false, ""); err != nil {
			return nil, err
		}
		note = "编辑并恢复熔断"
	}
	if err := uc.history(ctx, next, &before, userID, note); err != nil {
		return nil, err
	}
	if prodAllowed(spec.Auto) && !prodAllowed(before.Auto) && uc.d.Notifier != nil {
		_ = uc.d.Notifier.Notify(ctx, "agent.profile.prod", "Agent Profile 开放了生产环境自动执行",
			"Profile「"+spec.Name+"」允许对生产环境资源自动执行建议，请确认是否符合预期。", "profile:"+id+":prod")
	}
	return uc.Get(ctx, id)
}

// prodAllowed 自动通过条件是否包含生产环境。
func prodAllowed(a AutoSpec) bool {
	return a.Enabled && slices.ContainsFunc(a.Envs, func(e string) bool { return strings.EqualFold(e, prodEnv) || strings.EqualFold(e, "production") })
}

// Delete 删除：仍有排队 / 进行中任务时拒绝。
func (uc *ProfileUsecase) Delete(ctx context.Context, userID, id string, version int32) error {
	n, err := uc.d.Tasks.CountActiveByProfile(ctx, id)
	if err != nil {
		return err
	}
	if n > 0 {
		return detail(ErrProfileInUse, "%d task(s) queued or running", n)
	}
	return uc.d.Profiles.Delete(ctx, id, version, userID)
}

// History 变更历史。
func (uc *ProfileUsecase) History(ctx context.Context, id string, page, pageSize int64) ([]*ProfileHistoryView, int64, error) {
	if _, err := uc.d.Profiles.Get(ctx, id); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.d.Profiles.ListHistory(ctx, id, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, h := range items {
		ids = append(ids, h.ActorID)
	}
	names, err := uc.d.Accounts.Names(ctx, uniq(ids))
	if err != nil {
		return nil, 0, err
	}
	out := make([]*ProfileHistoryView, 0, len(items))
	for _, h := range items {
		out = append(out, &ProfileHistoryView{ProfileHistory: h, ActorName: names[h.ActorID]})
	}
	return out, total, nil
}

// ToolOption 可配置的工具。
type ToolOption struct {
	Name   string
	Title  string
	Module string
	Write  bool
	Stages []string
}

// ToolOptions 可写进白名单的工具目录：助手目录里全部只读工具 + 允许的写工具，按模块与名称排序。
func (uc *ProfileUsecase) ToolOptions() []ToolOption {
	all := uc.d.Tools.All()
	out := make([]ToolOption, 0, len(all))
	for _, t := range all {
		stages, writeOK := allowedWriteTools[t.Name]
		if t.Write && !writeOK {
			continue
		}
		if !t.Write {
			stages = []string{StageCollect, StageVerify}
		}
		out = append(out, ToolOption{Name: t.Name, Title: t.Title, Module: t.Module, Write: t.Write, Stages: stages})
	}
	slices.SortFunc(out, func(a, b ToolOption) int {
		if a.Module != b.Module {
			return strings.Compare(a.Module, b.Module)
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

// history 写一条变更记录。
func (uc *ProfileUsecase) history(ctx context.Context, p *Profile, before *ProfileSpec, actor, note string) error {
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	return uc.d.Profiles.AppendHistory(ctx, &ProfileHistory{ID: id, ProfileID: p.ID, Version: p.Version, ActorID: actor, Before: before, After: p.Spec, Note: note, CreateTime: uc.now()})
}

// normalize 规整并校验 Profile 内容：名称 / 类型 / 机器账号 / 阶段（缺省补齐、工具名须在目录内且在允许范围）/ 预算 / 自动条件。
func (uc *ProfileUsecase) normalize(ctx context.Context, s *ProfileSpec) error {
	s.Name = strings.TrimSpace(s.Name)
	s.Description = strings.TrimSpace(s.Description)
	if s.Name == "" {
		return detail(ErrProfileInvalid, "name is required")
	}
	if s.Kind == "" {
		s.Kind = KindDiagnose
	}
	if s.Kind != KindDiagnose && s.Kind != KindPostmortem {
		return detail(ErrProfileInvalid, "kind must be diagnose or postmortem")
	}
	s.KnowledgeSpaceID = strings.TrimSpace(s.KnowledgeSpaceID)
	if s.Kind == KindPostmortem && s.KnowledgeSpaceID == "" {
		return detail(ErrProfileInvalid, "knowledge space is required for postmortem profiles")
	}
	if s.Kind != KindPostmortem {
		s.KnowledgeSpaceID = ""
		// 经验沉淀才有的能力在诊断 Profile 上无意义：自动通过只属于诊断。
	} else {
		s.Auto = AutoSpec{}
	}
	order := StageOrderFor(s.Kind)
	if s.AgentUserID == "" {
		return detail(ErrProfileInvalid, "agent user is required")
	}
	machine, err := uc.d.Accounts.IsMachine(ctx, s.AgentUserID)
	if err != nil {
		return err
	}
	if !machine {
		return ErrAccountInvalid
	}
	if len(s.Severities) == 0 {
		s.Severities = []int32{1, 2, 3}
	}
	slices.Sort(s.Severities)
	s.Severities = slices.Compact(s.Severities)
	for _, sev := range s.Severities {
		if sev < 1 || sev > 4 {
			return detail(ErrProfileInvalid, "severity must be 1-4")
		}
	}
	s.BusinessIDs = uniq(s.BusinessIDs)
	if s.HourlyLimit <= 0 {
		s.HourlyLimit = defaultHourly
	}
	if len(s.Stages) == 0 {
		s.Stages = DefaultStagesFor(s.Kind)
	}
	seen := map[string]bool{}
	for i := range s.Stages {
		st := &s.Stages[i]
		if !slices.Contains(order, st.Key) {
			return detail(ErrProfileInvalid, "unknown stage %q", st.Key)
		}
		if seen[st.Key] {
			return detail(ErrProfileInvalid, "duplicate stage %q", st.Key)
		}
		seen[st.Key] = true
		if st.MaxSteps <= 0 || st.MaxSteps > maxStageSteps {
			return detail(ErrProfileInvalid, "stage %s max_steps must be within 1-%d", st.Key, maxStageSteps)
		}
		st.Tools = uniq(st.Tools)
		if len(st.Tools) > maxStageTools {
			return detail(ErrProfileInvalid, "stage %s has more than %d tools", st.Key, maxStageTools)
		}
		for _, name := range st.Tools {
			spec := uc.d.Tools.Find(name)
			if spec == nil {
				return detail(ErrProfileInvalid, "unknown tool %q", name)
			}
			if spec.Write {
				stages, ok := allowedWriteTools[name]
				if !ok || !slices.Contains(stages, st.Key) {
					return detail(ErrProfileInvalid, "write tool %q is not allowed in stage %s", name, st.Key)
				}
				if !slices.Contains(writeToolsByKind[s.Kind], name) {
					return detail(ErrProfileInvalid, "write tool %q is not allowed for %s profiles", name, s.Kind)
				}
			}
		}
	}
	for _, key := range order {
		if !seen[key] {
			return detail(ErrProfileInvalid, "stage %s is missing", key)
		}
	}
	slices.SortFunc(s.Stages, func(a, b StageSpec) int { return slices.Index(order, a.Key) - slices.Index(order, b.Key) })
	if s.Budget == (BudgetSpec{}) {
		s.Budget = DefaultBudget()
	}
	b := &s.Budget
	if b.MaxSteps <= 0 {
		b.MaxSteps = DefaultBudget().MaxSteps
	}
	if b.TimeoutS < minBudgetTimeout || b.TimeoutS > maxBudgetTimeout {
		return detail(ErrProfileInvalid, "budget timeout_s must be within %d-%d", minBudgetTimeout, maxBudgetTimeout)
	}
	if b.PromptTokens <= 0 {
		b.PromptTokens = DefaultBudget().PromptTokens
	}
	if b.CompletionTokens <= 0 {
		b.CompletionTokens = DefaultBudget().CompletionTokens
	}
	if s.Auto.MinConfidence <= 0 {
		s.Auto.MinConfidence = defaultMinConf
	}
	if s.Auto.MaxTargets <= 0 {
		s.Auto.MaxTargets = defaultMaxTarget
	}
	if s.Auto.Envs == nil {
		s.Auto.Envs = DefaultAuto().Envs
	}
	s.Auto.Envs = uniq(s.Auto.Envs)
	return nil
}

// uniq 去重保序（空串剔除）。
func uniq(items []string) []string {
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		if _, ok := seen[it]; ok {
			continue
		}
		seen[it] = struct{}{}
		out = append(out, it)
	}
	return out
}
