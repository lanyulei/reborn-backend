package service

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/monitor/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RuleService 实现 MonitorRuleServiceHTTPServer。
type RuleService struct {
	uc      *biz.RuleUsecase
	targets *biz.TargetUsecase
	// ruleP2 第二、三期的接口（内置模板、模拟触发、配置候选项），定义在 rule_p2.go。
	ruleP2
}

// NewRuleService new a RuleService.
func NewRuleService(uc *biz.RuleUsecase, targets *biz.TargetUsecase) *RuleService {
	return &RuleService{uc: uc, targets: targets, ruleP2: ruleP2{packs: uc}}
}

func toNotifyTargets(n *v1.RuleNotify) mc.NotifyTargets {
	return mc.NotifyTargets{UserIDs: n.GetUserIds(), ScheduleIDs: n.GetScheduleIds(), DeptIDs: n.GetDeptIds(), CIOwner: n.GetCiOwner()}
}

// toRelabel / toProtoRelabel 标签改写步骤的 proto ↔ 契约结构转换。
func toRelabel(in []*v1.RuleRelabelStep) []mc.RelabelStep {
	out := make([]mc.RelabelStep, 0, len(in))
	for _, st := range in {
		out = append(out, mc.RelabelStep{
			Action: st.GetAction(), SourceLabels: st.GetSourceLabels(), Separator: st.GetSeparator(), Regex: st.GetRegex(),
			TargetLabel: st.GetTargetLabel(), Replacement: st.GetReplacement(),
		})
	}
	return out
}

func toProtoRelabel(in []mc.RelabelStep) []*v1.RuleRelabelStep {
	out := make([]*v1.RuleRelabelStep, 0, len(in))
	for _, st := range in {
		out = append(out, &v1.RuleRelabelStep{
			Action: st.Action, SourceLabels: st.SourceLabels, Separator: st.Separator, Regex: st.Regex,
			TargetLabel: st.TargetLabel, Replacement: st.Replacement,
		})
	}
	return out
}

// toRuleP2 把 proto 里第二、三期的配置项填进规则内容。
func toRuleP2(in *v1.RuleSpec, spec *biz.RuleSpec) {
	m, sc, a := in.GetCiMatch(), in.GetResourceScope(), in.GetActions()
	spec.CIMatch = mc.CIMatch{
		Disabled: m.GetDisabled(), Label: m.GetLabel(), By: m.GetBy(), Attr: m.GetAttr(), ModelCodes: m.GetModelCodes(), EnrichTags: m.GetEnrichTags(),
	}
	spec.ResourceScope = mc.ResourceScope{Enabled: sc.GetEnabled(), ModelCodes: sc.GetModelCodes(), Envs: sc.GetEnvs(), Tags: sc.GetTags()}
	spec.Relabel, spec.DropExpr = toRelabel(in.GetRelabel()), in.GetDropExpr()
	spec.TopologyInhibit, spec.EscalationID = in.GetTopologyInhibit(), in.GetEscalationId()
	spec.Actions = mc.RuleActions{
		Ticket:    mc.TicketAction{Enabled: a.GetTicket().GetEnabled(), TemplateID: a.GetTicket().GetTemplateId(), AgentUserID: a.GetTicket().GetAgentUserId()},
		Workflow:  mc.WorkflowAction{Enabled: a.GetWorkflow().GetEnabled(), DefinitionKey: a.GetWorkflow().GetDefinitionKey(), AgentUserID: a.GetWorkflow().GetAgentUserId()},
		AISummary: a.GetAiSummary(),
	}
}

// toProtoRuleP2 把第二、三期的配置项写进 proto（各子消息恒非空：前端不必判空）。
func toProtoRuleP2(s *biz.RuleSpec, out *v1.RuleSpec) {
	out.CiMatch = &v1.RuleCIMatch{
		Disabled: s.CIMatch.Disabled, Label: s.CIMatch.Label, By: s.CIMatch.By, Attr: s.CIMatch.Attr,
		ModelCodes: s.CIMatch.ModelCodes, EnrichTags: s.CIMatch.EnrichTags,
	}
	out.ResourceScope = &v1.RuleResourceScope{
		Enabled: s.ResourceScope.Enabled, ModelCodes: s.ResourceScope.ModelCodes, Envs: s.ResourceScope.Envs, Tags: s.ResourceScope.Tags,
	}
	out.Relabel, out.DropExpr = toProtoRelabel(s.Relabel), s.DropExpr
	out.TopologyInhibit, out.EscalationId = s.TopologyInhibit, s.EscalationID
	out.Actions = &v1.RuleActions{
		Ticket:    &v1.RuleTicketAction{Enabled: s.Actions.Ticket.Enabled, TemplateId: s.Actions.Ticket.TemplateID, AgentUserId: s.Actions.Ticket.AgentUserID},
		Workflow:  &v1.RuleWorkflowAction{Enabled: s.Actions.Workflow.Enabled, DefinitionKey: s.Actions.Workflow.DefinitionKey, AgentUserId: s.Actions.Workflow.AgentUserID},
		AiSummary: s.Actions.AISummary,
	}
}

func toRuleSpec(in *v1.RuleSpec) biz.RuleSpec {
	queries := make([]mc.Query, 0, len(in.GetQueries()))
	for _, q := range in.GetQueries() {
		queries = append(queries, mc.Query{Expr: q.GetExpr(), Severity: q.GetSeverity()})
	}
	spec := biz.RuleSpec{
		GroupID: in.GetGroupId(), Name: in.GetName(), Description: in.GetDescription(), Enabled: in.GetEnabled(), Labels: in.GetLabels(),
		DatasourceScope: in.GetDatasourceScope(), DatasourceIDs: in.GetDatasourceIds(), Queries: queries, LevelInhibit: in.GetLevelInhibit(),
		EvalIntervalS: in.GetEvalIntervalS(), ForS: in.GetForS(), RecoverS: in.GetRecoverS(), NodataKeepS: in.GetNodataKeepS(),
		Effective: toEffective(in.GetEffective()), Annotations: in.GetAnnotations(), RecoveryExpr: in.GetRecoveryExpr(),
		Notify: toNotifyTargets(in.GetNotify()), NotifyRecovery: in.GetNotifyRecovery(), RepeatIntervalM: in.GetRepeatIntervalM(),
		MaxNotify: in.GetMaxNotify(), GroupKeyLabels: in.GetGroupKeyLabels(),
	}
	toRuleP2(in, &spec)
	return spec
}

func toProtoRuleSpec(s *biz.RuleSpec) *v1.RuleSpec {
	queries := make([]*v1.RuleQuery, 0, len(s.Queries))
	for _, q := range s.Queries {
		queries = append(queries, &v1.RuleQuery{Expr: q.Expr, Severity: q.Severity})
	}
	out := &v1.RuleSpec{
		GroupId: s.GroupID, Name: s.Name, Description: s.Description, Enabled: s.Enabled, Labels: s.Labels,
		DatasourceScope: s.DatasourceScope, DatasourceIds: s.DatasourceIDs, Queries: queries, LevelInhibit: s.LevelInhibit,
		EvalIntervalS: s.EvalIntervalS, ForS: s.ForS, RecoverS: s.RecoverS, NodataKeepS: s.NodataKeepS,
		Effective: toProtoEffective(s.Effective), Annotations: s.Annotations, RecoveryExpr: s.RecoveryExpr,
		Notify:         &v1.RuleNotify{UserIds: s.Notify.UserIDs, ScheduleIds: s.Notify.ScheduleIDs, DeptIds: s.Notify.DeptIDs, CiOwner: s.Notify.CIOwner},
		NotifyRecovery: s.NotifyRecovery, RepeatIntervalM: s.RepeatIntervalM, MaxNotify: s.MaxNotify, GroupKeyLabels: s.GroupKeyLabels,
	}
	toProtoRuleP2(s, out)
	return out
}

func toProtoRule(r *biz.RuleView) *v1.Rule {
	out := &v1.Rule{
		Id: r.ID, Spec: toProtoRuleSpec(&r.Spec), GroupName: r.GroupName, Datasources: toProtoRefs(r.Datasources),
		NotifyUsers: toProtoRefs(r.NotifyUsers), NotifySchedules: toProtoRefs(r.NotifySchedules), NotifyDepts: toProtoRefs(r.NotifyDepts),
		MaxSeverity: r.Spec.MaxSeverity(), Version: r.Version, Status: r.Status, FiringCount: r.FiringCount,
		LastEvalTime: protoTime(r.LastEvalTime), LastError: r.LastError, CanManage: r.CanManage,
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime),
		UpdateUserId: r.UpdateUser, UpdateUserName: r.UpdateUserName, EvalTasks: make([]*v1.RuleEvalTask, 0, len(r.Tasks)),
	}
	for _, t := range r.Tasks {
		out.EvalTasks = append(out.EvalTasks, &v1.RuleEvalTask{
			DatasourceId: t.DatasourceID, DatasourceName: t.DatasourceName, LastStatus: t.LastStatus, LastEvalTime: protoTime(t.LastEvalTime),
			LastError: t.LastError, LastDurationMs: t.LastDurationMS, FiringCount: t.FiringCount, NextEvalTime: timestamppb.New(t.NextEvalTime),
		})
	}
	return out
}

// protoRules 转换一批规则并补上第二、三期引用对象的展示名（升级策略、工单模板、流程、代理用户；
// 各类名称整批各查一次）。
func (s *RuleService) protoRules(ctx context.Context, rules []*biz.RuleView) ([]*v1.Rule, error) {
	refs, err := s.uc.Refs(ctx, rules)
	if err != nil {
		return nil, err
	}
	ref := func(r *biz.NamedRef) *v1.NamedRef {
		if r == nil {
			return nil
		}
		return &v1.NamedRef{Id: r.ID, Name: r.Name, Missing: r.Missing}
	}
	out := make([]*v1.Rule, 0, len(rules))
	for _, r := range rules {
		item, rr := toProtoRule(r), refs[r.ID]
		item.Escalation, item.TicketTemplate, item.TicketAgent = ref(rr.Escalation), ref(rr.TicketTemplate), ref(rr.TicketAgent)
		item.WorkflowDefinition, item.WorkflowAgent = ref(rr.WorkflowDefinition), ref(rr.WorkflowAgent)
		out = append(out, item)
	}
	return out, nil
}

// protoRule 转换一条规则（同 protoRules）。
func (s *RuleService) protoRule(ctx context.Context, r *biz.RuleView) (*v1.Rule, error) {
	out, err := s.protoRules(ctx, []*biz.RuleView{r})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// ListRules implements proto MonitorRuleService/ListRules.
func (s *RuleService) ListRules(ctx context.Context, req *v1.ListRulesRequest) (*v1.ListRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	rules, total, err := s.uc.List(ctx, userID, biz.RuleListQuery{
		GroupID: req.GetGroupId(),
		RuleFilter: biz.RuleFilter{
			Keyword: req.GetKeyword(), Severity: req.Severity, DatasourceID: req.GetDatasourceId(), Enabled: req.Enabled,
			Firing: req.Firing, LastStatus: req.LastStatus, Labels: labelFilter(req.GetLabels()), EscalationID: req.GetEscalationId(),
		},
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	items, err := s.protoRules(ctx, rules)
	if err != nil {
		return nil, err
	}
	return &v1.ListRulesResponse{Items: items, Total: total}, nil
}

// GetRule implements proto MonitorRuleService/GetRule.
func (s *RuleService) GetRule(ctx context.Context, req *v1.GetRuleRequest) (*v1.GetRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.GetRuleResponse{Rule: rule}, nil
}

// CreateRule implements proto MonitorRuleService/CreateRule.
func (s *RuleService) CreateRule(ctx context.Context, req *v1.CreateRuleRequest) (*v1.CreateRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Create(ctx, userID, toRuleSpec(req.GetSpec()))
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.CreateRuleResponse{Rule: rule}, nil
}

// UpdateRule implements proto MonitorRuleService/UpdateRule.
func (s *RuleService) UpdateRule(ctx context.Context, req *v1.UpdateRuleRequest) (*v1.UpdateRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Update(ctx, userID, req.GetId(), toRuleSpec(req.GetSpec()), req.GetVersion(), req.GetSummary())
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRuleResponse{Rule: rule}, nil
}

// DeleteRule implements proto MonitorRuleService/DeleteRule.
func (s *RuleService) DeleteRule(ctx context.Context, req *v1.DeleteRuleRequest) (*v1.DeleteRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteRuleResponse{}, nil
}

// ToggleRule implements proto MonitorRuleService/ToggleRule.
func (s *RuleService) ToggleRule(ctx context.Context, req *v1.ToggleRuleRequest) (*v1.ToggleRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Toggle(ctx, userID, req.GetId(), req.GetEnabled())
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.ToggleRuleResponse{Rule: rule}, nil
}

// CloneRule implements proto MonitorRuleService/CloneRule.
func (s *RuleService) CloneRule(ctx context.Context, req *v1.CloneRuleRequest) (*v1.CloneRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Clone(ctx, userID, req.GetId(), req.GetName(), req.GetGroupId())
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.CloneRuleResponse{Rule: rule}, nil
}

// BatchRules implements proto MonitorRuleService/BatchRules.
func (s *RuleService) BatchRules(ctx context.Context, req *v1.BatchRulesRequest) (*v1.BatchRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	in := biz.BatchInput{
		IDs: req.GetIds(), Action: req.GetAction(), GroupID: req.GroupId, Severity: req.Severity,
		EvalIntervalS: req.EvalIntervalS, ForS: req.ForS, RecoverS: req.RecoverS, Labels: req.GetLabels(), LabelNames: req.GetLabelNames(),
		NotifyRecovery: req.NotifyRecovery, RepeatIntervalM: req.RepeatIntervalM, MaxNotify: req.MaxNotify,
		EscalationID: req.EscalationId,
	}
	if req.Notify != nil {
		targets := toNotifyTargets(req.GetNotify())
		in.Notify = &targets
	}
	results, err := s.uc.Batch(ctx, userID, in)
	if err != nil {
		return nil, err
	}
	resp := &v1.BatchRulesResponse{Results: make([]*v1.BatchRuleResult, 0, len(results))}
	for _, r := range results {
		resp.Results = append(resp.Results, &v1.BatchRuleResult{Id: r.ID, Name: r.Name, Ok: r.OK, Reason: r.Reason, Message: r.Message})
		if r.OK {
			resp.Succeeded++
		} else {
			resp.Failed++
		}
	}
	return resp, nil
}

func toProtoRuleVersion(v *biz.RuleVersion) *v1.RuleVersion {
	out := &v1.RuleVersion{
		RuleId: v.RuleID, Version: v.Version, Action: v.Action, Summary: v.Summary, OperatorId: v.OperatorID, OperatorName: v.OperatorName,
		CreateTime: timestamppb.New(v.CreateTime), GroupName: v.GroupName,
	}
	if v.Spec != nil {
		out.Spec = toProtoRuleSpec(v.Spec)
	}
	return out
}

// ListRuleVersions implements proto MonitorRuleService/ListRuleVersions.
func (s *RuleService) ListRuleVersions(ctx context.Context, req *v1.ListRuleVersionsRequest) (*v1.ListRuleVersionsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	versions, total, err := s.uc.ListVersions(ctx, userID, req.GetId(), req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRuleVersionsResponse{Items: make([]*v1.RuleVersion, 0, len(versions)), Total: total}
	for _, v := range versions {
		resp.Items = append(resp.Items, toProtoRuleVersion(v))
	}
	return resp, nil
}

// GetRuleVersion implements proto MonitorRuleService/GetRuleVersion.
func (s *RuleService) GetRuleVersion(ctx context.Context, req *v1.GetRuleVersionRequest) (*v1.GetRuleVersionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.uc.GetVersion(ctx, userID, req.GetId(), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.GetRuleVersionResponse{Version: toProtoRuleVersion(v)}, nil
}

// RollbackRule implements proto MonitorRuleService/RollbackRule.
func (s *RuleService) RollbackRule(ctx context.Context, req *v1.RollbackRuleRequest) (*v1.RollbackRuleResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.uc.Rollback(ctx, userID, req.GetId(), req.GetVersion(), req.GetCurrentVersion())
	if err != nil {
		return nil, err
	}
	rule, err := s.protoRule(ctx, r)
	if err != nil {
		return nil, err
	}
	return &v1.RollbackRuleResponse{Rule: rule}, nil
}

// ValidateRuleExprs implements proto MonitorRuleService/ValidateRuleExprs。只做语法校验，不访问数据源
// （请求里的 datasource_id 不参与）。
func (s *RuleService) ValidateRuleExprs(_ context.Context, req *v1.ValidateRuleExprsRequest) (*v1.ValidateRuleExprsResponse, error) {
	results := s.uc.ValidateExprs(req.GetExprs())
	resp := &v1.ValidateRuleExprsResponse{Results: make([]*v1.RuleExprResult, 0, len(results))}
	for _, r := range results {
		resp.Results = append(resp.Results, &v1.RuleExprResult{Expr: r.Expr, Valid: r.Valid, Error: r.Error})
	}
	return resp, nil
}

func toImportOptions(o *v1.ImportRulesOptions) biz.ImportOptions {
	return biz.ImportOptions{
		Content: o.GetContent(), GroupID: o.GetGroupId(), DatasourceScope: o.GetDatasourceScope(), DatasourceIDs: o.GetDatasourceIds(),
		DefaultSeverity: o.GetDefaultSeverity(), Notify: toNotifyTargets(o.GetNotify()), SeverityMap: o.GetSeverityMap(),
	}
}

// PreviewImportRules implements proto MonitorRuleService/PreviewImportRules.
func (s *RuleService) PreviewImportRules(ctx context.Context, req *v1.PreviewImportRulesRequest) (*v1.PreviewImportRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.PreviewImport(ctx, userID, toImportOptions(req.GetOptions()))
	if err != nil {
		return nil, err
	}
	resp := &v1.PreviewImportRulesResponse{Items: make([]*v1.ImportRuleItem, 0, len(items)), Total: int32(len(items))} // #nosec G115 -- 至多 biz.MaxImportRules
	for _, it := range items {
		item := &v1.ImportRuleItem{
			Index: it.Index, SourceGroup: it.SourceGroup, Name: it.Name, Status: it.Status,
			ExistingRuleId: it.ExistingRuleID, Error: it.Error, Warnings: it.Warnings,
		}
		if it.Spec != nil {
			item.Spec = toProtoRuleSpec(it.Spec)
		}
		switch it.Status {
		case biz.ImportNew:
			resp.NewCount++
		case biz.ImportConflict:
			resp.ConflictCount++
		case biz.ImportInvalid:
			resp.InvalidCount++
		case biz.ImportSkipped:
			resp.SkippedCount++
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}

// ImportRules implements proto MonitorRuleService/ImportRules.
func (s *RuleService) ImportRules(ctx context.Context, req *v1.ImportRulesRequest) (*v1.ImportRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	results, err := s.uc.Import(ctx, userID, toImportOptions(req.GetOptions()), req.GetOnConflict(), req.GetEnabled())
	if err != nil {
		return nil, err
	}
	resp := &v1.ImportRulesResponse{Results: make([]*v1.ImportRuleResult, 0, len(results))}
	for _, r := range results {
		resp.Results = append(resp.Results, &v1.ImportRuleResult{Index: r.Index, Name: r.Name, Action: r.Action, RuleId: r.RuleID, Error: r.Error})
		switch r.Action {
		case biz.ImportCreated:
			resp.Created++
		case biz.ImportUpdated:
			resp.Updated++
		case biz.ImportSkippedRule:
			resp.Skipped++
		case biz.ImportFailed:
			resp.Failed++
		}
	}
	return resp, nil
}

// ExportRules implements proto MonitorRuleService/ExportRules.
func (s *RuleService) ExportRules(ctx context.Context, req *v1.ExportRulesRequest) (*v1.ExportRulesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	content, count, err := s.uc.Export(ctx, userID, req.GetIds(), req.GetGroupId(), req.GetFormat())
	if err != nil {
		return nil, err
	}
	ext := ".yaml"
	if req.GetFormat() == biz.ExportJSON {
		ext = ".json"
	}
	return &v1.ExportRulesResponse{
		Content: content, Count: int32(count), // #nosec G115 -- 至多 biz.MaxExportRules
		Filename: "alert-rules-" + time.Now().UTC().Format("20060102-150405") + ext,
	}, nil
}

// ListMonitorTargetOptions implements proto MonitorRuleService/ListMonitorTargetOptions.
func (s *RuleService) ListMonitorTargetOptions(ctx context.Context, req *v1.ListMonitorTargetOptionsRequest) (*v1.ListMonitorTargetOptionsResponse, error) {
	opts, err := s.targets.Options(ctx, req.GetKeyword(), req.GetKinds())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListMonitorTargetOptionsResponse{
		Schedules: make([]*v1.MonitorTargetSchedule, 0, len(opts.Schedules)),
		Depts:     make([]*v1.MonitorTargetDept, 0, len(opts.Depts)),
		Users:     make([]*v1.MonitorTargetUser, 0, len(opts.Users)),
	}
	for _, it := range opts.Schedules {
		resp.Schedules = append(resp.Schedules, &v1.MonitorTargetSchedule{Id: it.ID, Name: it.Name, GroupName: it.GroupName})
	}
	for _, it := range opts.Depts {
		resp.Depts = append(resp.Depts, &v1.MonitorTargetDept{Id: it.ID, Name: it.Name, ParentId: it.ParentID})
	}
	for _, it := range opts.Users {
		resp.Users = append(resp.Users, &v1.MonitorTargetUser{Id: it.ID, Username: it.Username, Nickname: it.Nickname})
	}
	return resp, nil
}

// ListRuleEvalRecords implements proto MonitorRuleService/ListRuleEvalRecords.
func (s *RuleService) ListRuleEvalRecords(ctx context.Context, req *v1.ListRuleEvalRecordsRequest) (*v1.ListRuleEvalRecordsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	records, total, err := s.uc.ListEvalRecords(ctx, userID, biz.EvalRecordFilter{
		RuleID: req.GetId(), DatasourceID: req.GetDatasourceId(), Status: req.Status,
		Start: optTime(req.GetEvalTimeStart()), End: optTime(req.GetEvalTimeEnd()),
	}, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRuleEvalRecordsResponse{Items: make([]*v1.RuleEvalRecord, 0, len(records)), Total: total}
	for _, r := range records {
		item := &v1.RuleEvalRecord{
			Id: r.ID, RuleId: r.RuleID, DatasourceId: r.DatasourceID, DatasourceName: r.DatasourceName, EvalTime: timestamppb.New(r.EvalTime),
			DurationMs: r.DurationMS, Status: r.Status, Error: r.Error, Series: r.Series, Anomalies: r.Anomalies, Recovered: r.Recovered,
			PointsTruncated: r.Detail.Truncated,
			Queries:         make([]*v1.EvalQueryDetail, 0, len(r.Detail.Queries)), Points: make([]*v1.EvalPoint, 0, len(r.Detail.Points)),
		}
		for i := range r.Detail.Queries {
			q := &r.Detail.Queries[i]
			item.Queries = append(item.Queries, &v1.EvalQueryDetail{
				Index: q.Index, Severity: q.Severity, Series: q.Series, Truncated: q.Truncated, Error: biz.QueryNote(q),
			})
		}
		for _, p := range r.Detail.Points {
			item.Points = append(item.Points, &v1.EvalPoint{
				Fingerprint: p.Fingerprint, Labels: p.Labels, Severity: p.Severity, Value: p.Value, Step: p.Step, AlertId: p.AlertID, MuteId: p.MuteID,
			})
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}
