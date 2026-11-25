package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	"github.com/google/uuid"
)

// RuleExportFormat 本系统规则导出文件的格式标识（带版本）。字段有不兼容变化时换新的版本号，
// 导入方按它决定怎么读；不认识的版本一律拒绝。
const RuleExportFormat = "reborn.monitor.rules/v1"

// ruleExport 本系统的规则导出文件（JSON）：规则的全部可编辑内容；引用的分组、数据源、用户、值班表、
// 部门以 ID + 名称表示（名称只供阅读，导入按 ID 识别）。json 标签即文件格式，改动须换版本号。
type ruleExport struct {
	Format     string         `json:"format"`
	ExportTime string         `json:"export_time"`
	Rules      []exportedRule `json:"rules"`
}

// exportedRef 被引用对象的 ID 与名称。
type exportedRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type exportedNotify struct {
	Users     []exportedRef `json:"users"`
	Schedules []exportedRef `json:"schedules"`
	Depts     []exportedRef `json:"depts"`
	// CIOwner 通知事件关联资源的负责人（第二期增加；旧文件里没有 = false）。
	CIOwner bool `json:"ci_owner"`
}

// exportedTicketAction / exportedWorkflowAction / exportedActions 事件动作：引用的模板、流程、代理用户
// 以 ID + 名称表示（流程的 ID 是流程标识）。
type exportedTicketAction struct {
	Enabled  bool        `json:"enabled"`
	Template exportedRef `json:"template"`
	Agent    exportedRef `json:"agent"`
}

type exportedWorkflowAction struct {
	Enabled    bool        `json:"enabled"`
	Definition exportedRef `json:"definition"`
	Agent      exportedRef `json:"agent"`
}

type exportedActions struct {
	Ticket    exportedTicketAction   `json:"ticket"`
	Workflow  exportedWorkflowAction `json:"workflow"`
	AISummary bool                   `json:"ai_summary"`
}

// exportedRule 导出文件里的一条规则。
type exportedRule struct {
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	Enabled         bool              `json:"enabled"`
	Group           exportedRef       `json:"group"`
	Labels          map[string]string `json:"labels"`
	DatasourceScope int32             `json:"datasource_scope"`
	Datasources     []exportedRef     `json:"datasources"`
	Queries         []mc.Query        `json:"queries"`
	LevelInhibit    bool              `json:"level_inhibit"`
	EvalIntervalS   int32             `json:"eval_interval_s"`
	ForS            int32             `json:"for_s"`
	RecoverS        int32             `json:"recover_s"`
	NodataKeepS     int32             `json:"nodata_keep_s"`
	Effective       mc.Effective      `json:"effective"`
	Annotations     map[string]string `json:"annotations"`
	RecoveryExpr    string            `json:"recovery_expr"`
	Notify          exportedNotify    `json:"notify"`
	NotifyRecovery  bool              `json:"notify_recovery"`
	RepeatIntervalM int32             `json:"repeat_interval_m"`
	MaxNotify       int32             `json:"max_notify"`
	GroupKeyLabels  []string          `json:"group_key_labels"`
	// 第二、三期增加的字段（格式标识不变：只增不改，旧文件里没有这些键 = 未配置）。
	CIMatch         mc.CIMatch       `json:"ci_match"`
	ResourceScope   mc.ResourceScope `json:"resource_scope"`
	Relabel         []mc.RelabelStep `json:"relabel"`
	DropExpr        string           `json:"drop_expr"`
	TopologyInhibit bool             `json:"topology_inhibit"`
	// Escalation 引用的升级策略；没有为 null。
	Escalation *exportedRef    `json:"escalation"`
	Actions    exportedActions `json:"actions"`
}

// exportedRefs 按 ids 的顺序生成引用列表（查不到名称的留空）。
func exportedRefs(ids []string, lookup map[string]string) []exportedRef {
	out := make([]exportedRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, exportedRef{ID: id, Name: lookup[id]})
	}
	return out
}

func refIDs(refs []exportedRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.ID)
	}
	return out
}

// exportJSON 把规则编码为本系统的导出文件；名称各查一次。
func (uc *RuleUsecase) exportJSON(ctx context.Context, scope Scope, rules []*Rule) ([]byte, error) {
	set := &refSet{}
	for _, r := range rules {
		set.users = append(set.users, r.Spec.Notify.UserIDs...)
		set.schedules = append(set.schedules, r.Spec.Notify.ScheduleIDs...)
		set.depts = append(set.depts, r.Spec.Notify.DeptIDs...)
		set.datasources = append(set.datasources, r.Spec.DatasourceIDs...)
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	views := make([]*RuleView, 0, len(rules))
	for _, r := range rules {
		views = append(views, &RuleView{Rule: r})
	}
	p2, err := uc.Refs(ctx, views)
	if err != nil {
		return nil, err
	}
	datasourceNames := make(map[string]string, len(set.datasourceBriefs))
	for id, b := range set.datasourceBriefs {
		datasourceNames[id] = b.Name
	}
	file := ruleExport{Format: RuleExportFormat, ExportTime: uc.now().UTC().Format(time.RFC3339), Rules: make([]exportedRule, 0, len(rules))}
	for _, r := range rules {
		s := r.Spec
		normalizeSpec(&s) // 文件里不出现 null
		pr := p2[r.ID]
		toRef := func(n *NamedRef) exportedRef {
			if n == nil {
				return exportedRef{}
			}
			return exportedRef{ID: n.ID, Name: n.Name}
		}
		var escalation *exportedRef
		if pr.Escalation != nil {
			ref := toRef(pr.Escalation)
			escalation = &ref
		}
		file.Rules = append(file.Rules, exportedRule{
			CIMatch: s.CIMatch, ResourceScope: s.ResourceScope, Relabel: s.Relabel, DropExpr: s.DropExpr,
			TopologyInhibit: s.TopologyInhibit, Escalation: escalation,
			Actions: exportedActions{
				Ticket:    exportedTicketAction{Enabled: s.Actions.Ticket.Enabled, Template: toRef(pr.TicketTemplate), Agent: toRef(pr.TicketAgent)},
				Workflow:  exportedWorkflowAction{Enabled: s.Actions.Workflow.Enabled, Definition: toRef(pr.WorkflowDefinition), Agent: toRef(pr.WorkflowAgent)},
				AISummary: s.Actions.AISummary,
			},
			Name: s.Name, Description: s.Description, Enabled: s.Enabled,
			Group: exportedRef{ID: s.GroupID, Name: scope.GroupName(s.GroupID)}, Labels: s.Labels,
			DatasourceScope: s.DatasourceScope, Datasources: exportedRefs(s.DatasourceIDs, datasourceNames),
			Queries: s.Queries, LevelInhibit: s.LevelInhibit, EvalIntervalS: s.EvalIntervalS, ForS: s.ForS,
			RecoverS: s.RecoverS, NodataKeepS: s.NodataKeepS, Effective: s.Effective, Annotations: s.Annotations,
			RecoveryExpr: s.RecoveryExpr,
			Notify: exportedNotify{
				Users:     exportedRefs(s.Notify.UserIDs, set.userNames),
				Schedules: exportedRefs(s.Notify.ScheduleIDs, set.scheduleNames),
				Depts:     exportedRefs(s.Notify.DeptIDs, set.deptNames),
				CIOwner:   s.Notify.CIOwner,
			},
			NotifyRecovery: s.NotifyRecovery, RepeatIntervalM: s.RepeatIntervalM, MaxNotify: s.MaxNotify,
			GroupKeyLabels: s.GroupKeyLabels,
		})
	}
	return json.MarshalIndent(file, "", "  ")
}

// isRuleExport 报告导入内容是否本系统的导出文件：一个带 format 键的 JSON 对象。其余内容（含用 JSON
// 写的 Prometheus 规则文件——JSON 是合法的 YAML）都交给 YAML 解析。
func isRuleExport(content string) bool {
	if !strings.HasPrefix(strings.TrimSpace(content), "{") {
		return false
	}
	var probe struct {
		Format *string `json:"format"`
	}
	return json.Unmarshal([]byte(content), &probe) == nil && probe.Format != nil
}

// parseRuleExport 解析导出文件。大小已由调用方限制（maxImportBytes）；encoding/json 自带嵌套深度
// 上限，未知字段忽略。
func parseRuleExport(content string) ([]exportedRule, error) {
	var file ruleExport
	dec := json.NewDecoder(bytes.NewReader([]byte(content)))
	if err := dec.Decode(&file); err != nil {
		return nil, importInvalid("not a valid rule export: %s", truncate(err.Error(), 200))
	}
	if dec.More() {
		return nil, importInvalid("unexpected content after the JSON document")
	}
	if file.Format != RuleExportFormat {
		return nil, importInvalid("unsupported format %q (expecting %q)", truncate(file.Format, 64), RuleExportFormat)
	}
	if len(file.Rules) == 0 {
		return nil, importInvalid("no rules found (expecting a top-level `rules` list)")
	}
	if len(file.Rules) > MaxImportRules {
		return nil, importInvalid("more than %d rules in one file, please split it", MaxImportRules)
	}
	exprBytes := 0
	for i := range file.Rules {
		exprBytes += len(file.Rules[i].RecoveryExpr)
		for _, q := range file.Rules[i].Queries {
			exprBytes += len(q.Expr)
		}
		if exprBytes > maxImportExprBytes {
			return nil, errImportExprBytes()
		}
		lowerExportRefs(&file.Rules[i])
	}
	return file.Rules, nil
}

// lowerExportRefs 把文件里以 UUID 识别的引用（数据源、通知用户 / 值班表 / 部门、升级策略、工单模板、代理
// 用户）统一成小写：UUID 不区分大小写，而目录与仓储按库里的小写 ID 比对，大写写法否则会被当成「已不存在
// 或不可用」而从规则里去掉。流程标识不是 UUID，保持原样。
func lowerExportRefs(r *exportedRule) {
	for i := range r.Datasources {
		r.Datasources[i].ID = strings.ToLower(r.Datasources[i].ID)
	}
	for _, refs := range [][]exportedRef{r.Notify.Users, r.Notify.Schedules, r.Notify.Depts} {
		for i := range refs {
			refs[i].ID = strings.ToLower(refs[i].ID)
		}
	}
	if r.Escalation != nil {
		r.Escalation.ID = strings.ToLower(r.Escalation.ID)
	}
	r.Actions.Ticket.Template.ID = strings.ToLower(r.Actions.Ticket.Template.ID)
	r.Actions.Ticket.Agent.ID = strings.ToLower(r.Actions.Ticket.Agent.ID)
	r.Actions.Workflow.Agent.ID = strings.ToLower(r.Actions.Workflow.Agent.ID)
}

// validIDs 去掉不是 UUID 的 ID（文件内容不可信：非法 ID 交给数据层会变成 500）并去重。
func validIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup || uuid.Validate(id) != nil {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// tooManyRefs 引用个数超过规则本身允许的上限（与 validateStatic 同一口径）。文件内容不可信：先按个数
// 拦下，免得为一条注定非法的规则去查成千上万个 ID。
func (r *exportedRule) tooManyRefs() bool {
	return len(r.Datasources) > MaxRuleDatasources || len(r.Notify.Users) > maxNotifyUsers ||
		len(r.Notify.Schedules) > maxNotifySchedules || len(r.Notify.Depts) > maxNotifyDepts
}

// convertExport 把导出文件转换为预览项。引用按 ID 判断是否还可用（各查一次），口径与新建规则时的
// 校验相同：数据源存在且已启用、用户可用、值班表启用、部门存在则保留，否则丢弃并在预览里说明——
// 这样转换出来的规则一定能通过新建规则的引用校验。目标分组、是否启用由导入选项决定；导入选项给了
// 通知对象时覆盖文件里的。每条规则再走与新建相同的静态校验。
func (uc *RuleUsecase) convertExport(ctx context.Context, scope Scope, opts *ImportOptions) ([]*ImportItem, error) {
	rules, err := parseRuleExport(opts.Content)
	if err != nil {
		return nil, err
	}
	overrideNotify := len(opts.Notify.UserIDs)+len(opts.Notify.ScheduleIDs)+len(opts.Notify.DeptIDs) > 0
	set := &refSet{}
	var users []string
	for i := range rules {
		if rules[i].tooManyRefs() {
			continue
		}
		set.datasources = append(set.datasources, refIDs(rules[i].Datasources)...)
		if !overrideNotify {
			users = append(users, refIDs(rules[i].Notify.Users)...)
			set.schedules = append(set.schedules, refIDs(rules[i].Notify.Schedules)...)
			set.depts = append(set.depts, refIDs(rules[i].Notify.Depts)...)
		}
	}
	set.datasources, set.schedules, set.depts = validIDs(set.datasources), validIDs(set.schedules), validIDs(set.depts)
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	// 可用用户放进集合：每条规则至多 50 个通知用户、一次至多导入 MaxImportRules 条，逐个在切片里线性查找是
	// 「引用数 × 可用用户数」。
	activeUsers := map[string]struct{}{}
	if users = validIDs(users); len(users) > 0 {
		active, err := uc.dir.ActiveUserIDs(ctx, users)
		if err != nil {
			return nil, err
		}
		for _, id := range active {
			activeUsers[id] = struct{}{}
		}
	}
	p2, err := uc.newImportRefs(ctx, scope, opts.GroupID, rules)
	if err != nil {
		return nil, err
	}
	items := make([]*ImportItem, 0, len(rules))
	for i := range rules {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := convertExported(i, &rules[i], opts, set, activeUsers, overrideNotify)
		if item.Spec != nil {
			if err := uc.importP2(ctx, item, &rules[i], p2); err != nil {
				return nil, err
			}
		}
		items = append(items, item)
	}
	return items, nil
}

// keepRefs 留下 ok 认可的引用，其余记一条提示。
func keepRefs(item *ImportItem, kind string, refs []exportedRef, ok func(id string) bool) []string {
	kept := make([]string, 0, len(refs))
	var dropped []string
	for _, r := range refs {
		switch {
		case slices.Contains(kept, r.ID):
		case ok(r.ID):
			kept = append(kept, r.ID)
		default:
			dropped = append(dropped, refLabel(r))
		}
	}
	if len(dropped) > 0 {
		item.Warnings = append(item.Warnings, fmt.Sprintf("%s已不存在或不可用（已删除 / 已停用），已从规则中去掉：%s", kind, strings.Join(dropped, "、")))
	}
	return kept
}

// convertExported 把导出文件里的一条规则转换为预览项。
func convertExported(index int, r *exportedRule, opts *ImportOptions, set *refSet, activeUsers map[string]struct{}, overrideNotify bool) *ImportItem {
	item := &ImportItem{Index: int32(index), SourceGroup: truncate(r.Group.Name, 512), Name: truncate(strings.TrimSpace(r.Name), 128), Status: ImportNew} // #nosec G115 -- 至多 MaxImportRules
	if r.tooManyRefs() {
		item.Status, item.Error = ImportInvalid, "too many datasources or notify targets"
		return item
	}
	spec := &RuleSpec{
		GroupID: opts.GroupID, Name: r.Name, Description: r.Description, Labels: r.Labels,
		DatasourceScope: r.DatasourceScope, Queries: r.Queries, LevelInhibit: r.LevelInhibit,
		EvalIntervalS: r.EvalIntervalS, ForS: r.ForS, RecoverS: r.RecoverS, NodataKeepS: r.NodataKeepS,
		Effective: r.Effective, Annotations: r.Annotations, RecoveryExpr: r.RecoveryExpr,
		NotifyRecovery: r.NotifyRecovery, RepeatIntervalM: r.RepeatIntervalM, MaxNotify: r.MaxNotify,
		GroupKeyLabels: r.GroupKeyLabels,
		CIMatch:        r.CIMatch, ResourceScope: r.ResourceScope, Relabel: r.Relabel, DropExpr: r.DropExpr,
		TopologyInhibit: r.TopologyInhibit,
		Actions: mc.RuleActions{
			Ticket:    mc.TicketAction{Enabled: r.Actions.Ticket.Enabled, TemplateID: r.Actions.Ticket.Template.ID, AgentUserID: r.Actions.Ticket.Agent.ID},
			Workflow:  mc.WorkflowAction{Enabled: r.Actions.Workflow.Enabled, DefinitionKey: r.Actions.Workflow.Definition.ID, AgentUserID: r.Actions.Workflow.Agent.ID},
			AISummary: r.Actions.AISummary,
		},
	}
	if r.Escalation != nil {
		spec.EscalationID = r.Escalation.ID
	}
	// 文件内容不可信：引用的 ID 不合法时按「没有引用」处理（importP2 会给出提示）。
	if !validID(spec.EscalationID) {
		spec.EscalationID = ""
	}
	if a := &spec.Actions; !validID(a.Ticket.TemplateID) || !validID(a.Ticket.AgentUserID) {
		a.Ticket = mc.TicketAction{}
	}
	if a := &spec.Actions; !validID(a.Workflow.AgentUserID) || len(a.Workflow.DefinitionKey) > 64 {
		a.Workflow = mc.WorkflowAction{}
	}
	spec.DatasourceIDs = keepRefs(item, "数据源", r.Datasources, func(id string) bool { return set.datasourceBriefs[id].Enabled })
	if spec.DatasourceScope == mc.DatasourceScopeSpecified && len(spec.DatasourceIDs) == 0 {
		spec.DatasourceScope, spec.DatasourceIDs = opts.DatasourceScope, slices.Clone(opts.DatasourceIDs)
		item.Warnings = append(item.Warnings, "规则指定的数据源都已不可用，已改用导入选项里的数据源范围")
	}
	if overrideNotify {
		spec.Notify = mc.NotifyTargets{
			UserIDs: slices.Clone(opts.Notify.UserIDs), ScheduleIDs: slices.Clone(opts.Notify.ScheduleIDs), DeptIDs: slices.Clone(opts.Notify.DeptIDs),
			CIOwner: opts.Notify.CIOwner,
		}
	} else {
		spec.Notify = mc.NotifyTargets{
			UserIDs: keepRefs(item, "通知用户", r.Notify.Users, func(id string) bool {
				_, ok := activeUsers[id]
				return ok
			}),
			ScheduleIDs: keepRefs(item, "值班表", r.Notify.Schedules, func(id string) bool {
				_, ok := set.scheduleNames[id]
				return ok
			}),
			DeptIDs: keepRefs(item, "部门", r.Notify.Depts, func(id string) bool {
				_, ok := set.deptNames[id]
				return ok
			}),
			CIOwner: r.Notify.CIOwner,
		}
	}
	if err := validateStatic(spec); err != nil {
		item.Status, item.Error = ImportInvalid, errorMessage(err)
		return item
	}
	item.Name, item.Spec = spec.Name, spec
	return item
}

// importRefs 导入时第二、三期引用对象的可用性：升级策略能否被目标分组引用、代理用户是否可用（整批各查
// 一次）；工单模板对代理提单人是否可用、流程能否发起按需查询并缓存。
type importRefs struct {
	escalations map[string]bool
	activeUsers []string
	// templates 键为「模板 ID / 代理提单人」；flows 键为流程标识。
	templates map[string]bool
	flows     map[string]bool
}

// newImportRefs 收齐导出文件里引用的升级策略与代理用户并判断可用性。
func (uc *RuleUsecase) newImportRefs(ctx context.Context, scope Scope, groupID string, rules []exportedRule) (*importRefs, error) {
	refs := &importRefs{escalations: map[string]bool{}, templates: map[string]bool{}, flows: map[string]bool{}}
	var ids, users []string
	for i := range rules {
		if e := rules[i].Escalation; e != nil {
			ids = append(ids, e.ID)
		}
		users = append(users, rules[i].Actions.Ticket.Agent.ID, rules[i].Actions.Workflow.Agent.ID)
	}
	if ids = validIDs(ids); len(ids) > 0 && uc.ext.Escalations != nil {
		list, err := uc.ext.Escalations.ListByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		ancestors := scope.Ancestors(groupID)
		for _, e := range list {
			refs.escalations[e.ID] = escalationUsable(e, ancestors)
		}
	}
	if users = validIDs(users); len(users) > 0 {
		var err error
		if refs.activeUsers, err = uc.dir.ActiveUserIDs(ctx, users); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

// importP2 核对一条导入规则里第二、三期的引用：不可用的去掉（升级策略 → 不升级，自动建单 / 触发流程 →
// 关闭），并在预览里说明——与数据源、通知对象同一做法，转换出来的规则一定能通过新建规则的引用校验。
// 关联属性不在部署配置里登记时该条规则不合法。
func (uc *RuleUsecase) importP2(ctx context.Context, item *ImportItem, r *exportedRule, refs *importRefs) error {
	spec := item.Spec
	if err := uc.checkMatchAttr(spec); err != nil {
		item.Status, item.Spec, item.Error = ImportInvalid, nil, errorMessage(err)
		return nil
	}
	if r.Escalation != nil && r.Escalation.ID != "" && !refs.escalations[spec.EscalationID] {
		spec.EscalationID = ""
		item.Warnings = append(item.Warnings, "升级策略已不存在、已停用或不能被目标分组引用，已从规则中去掉："+refLabel(*r.Escalation))
	}
	if t := r.Actions.Ticket; t.Enabled {
		a := spec.Actions.Ticket
		ok := a.Enabled && slices.Contains(refs.activeUsers, a.AgentUserID)
		if ok {
			key := a.TemplateID + "/" + a.AgentUserID
			usable, known := refs.templates[key]
			if !known && uc.ext.Tickets != nil {
				tpl, err := uc.ext.Tickets.Template(ctx, a.TemplateID, a.AgentUserID)
				if err != nil {
					return err
				}
				usable = tpl != nil
			}
			refs.templates[key], ok = usable, usable
		}
		if !ok {
			spec.Actions.Ticket = mc.TicketAction{}
			item.Warnings = append(item.Warnings, "自动建单的工单模板或代理提单人已不可用，已关闭自动建单："+refLabel(t.Template)+" / "+refLabel(t.Agent))
		}
	}
	if w := r.Actions.Workflow; w.Enabled {
		a := spec.Actions.Workflow
		ok := a.Enabled && slices.Contains(refs.activeUsers, a.AgentUserID)
		if ok {
			usable, known := refs.flows[a.DefinitionKey]
			if !known && uc.ext.Workflows != nil {
				def, err := uc.ext.Workflows.Definition(ctx, a.DefinitionKey)
				if err != nil {
					return err
				}
				usable = def != nil
			}
			refs.flows[a.DefinitionKey], ok = usable, usable
		}
		if !ok {
			spec.Actions.Workflow = mc.WorkflowAction{}
			item.Warnings = append(item.Warnings, "触发流程的流程或发起人已不可用，已关闭触发流程："+refLabel(w.Definition)+" / "+refLabel(w.Agent))
		}
	}
	return nil
}

// refLabel 提示里对一个引用的写法：名称 (ID)。
func refLabel(r exportedRef) string {
	return strings.TrimSpace(truncate(r.Name, 64) + " (" + truncate(r.ID, 64) + ")")
}
