package biz

import (
	"context"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/google/uuid"
)

// 规则第二、三期配置项（关联资源、资源范围、标签改写、条件丢弃、拓扑抑制、升级策略、事件动作）的
// 校验与展示名。流水线配置的合法性只问 monitorpipeline.Compile——评估引擎加载规则时调的是同一个函数，
// 能保存的配置引擎一定能执行。

// RuleExt 规则用例第二、三期的协作者。零值可用：对应的引用一律按「不可用」拒绝。
type RuleExt struct {
	Escalations EscalationReader
	Tickets     TicketCatalog
	Workflows   Workflows
	// MatchAttrs 部署配置里登记的可关联属性（monitor.ci_match_attrs；空 = 只有 ip）。
	MatchAttrs []string
}

// WithP2 挂上第二、三期的协作者，返回用例自身（装配期调用一次）。
func (uc *RuleUsecase) WithP2(ext RuleExt) *RuleUsecase {
	ext.MatchAttrs = ruleMatchAttrs(ext.MatchAttrs)
	uc.ext = ext
	return uc
}

// ProvideRuleUsecase 装配带第二、三期协作者的规则用例（wire 用它代替 NewRuleUsecase）。
func ProvideRuleUsecase(scopes *ScopeUsecase, reader RuleReader, lookup RuleLookup, writer RuleWriter, versions RuleVersionReader,
	evals EvalReader, datasources DatasourceLookup, dir Directory, schedules Schedules,
	escalations EscalationReader, tickets TicketCatalog, workflows Workflows, c *conf.Monitor,
) *RuleUsecase {
	return NewRuleUsecase(scopes, reader, lookup, writer, versions, evals, datasources, dir, schedules).
		WithP2(RuleExt{Escalations: escalations, Tickets: tickets, Workflows: workflows, MatchAttrs: c.GetCiMatchAttrs()})
}

// ruleMatchAttrs 规整可关联属性清单：去空白、去重；为空时取缺省（ip）。
func ruleMatchAttrs(configured []string) []string {
	out := make([]string, 0, len(configured))
	for _, a := range configured {
		if a = strings.TrimSpace(a); a != "" && !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	if len(out) == 0 {
		return []string{mc.DefaultCIMatchAttr}
	}
	return out
}

// pipelineSpec 规则里与事件流水线有关的配置。
func pipelineSpec(s *RuleSpec) monitorpipeline.Spec {
	return monitorpipeline.Spec{Relabel: s.Relabel, DropExpr: s.DropExpr, CIMatch: s.CIMatch, Scope: s.ResourceScope}
}

// validID 非空时须是 UUID（落库列是 uuid，或会被拿去查 uuid 列）。
func validID(id string) bool { return id == "" || uuid.Validate(id) == nil }

// validateP2Static 第二、三期配置项里不依赖外部数据的校验。
func validateP2Static(spec *RuleSpec) error {
	if len(spec.Relabel) > mc.MaxRelabelSteps {
		return ruleInvalid("at most %d relabel steps", mc.MaxRelabelSteps)
	}
	if len(spec.DropExpr) > mc.MaxDropExprBytes {
		return ruleInvalid("drop_expr must be at most %d bytes", mc.MaxDropExprBytes)
	}
	if _, err := monitorpipeline.Compile(pipelineSpec(spec)); err != nil {
		return ruleInvalid("%s", truncate(strings.TrimPrefix(err.Error(), mc.ErrInvalid.Error()+": "), 300))
	}
	if !validID(spec.EscalationID) {
		return ruleInvalid("escalation_id is not a valid id")
	}
	t, w := spec.Actions.Ticket, spec.Actions.Workflow
	if !validID(t.TemplateID) || !validID(t.AgentUserID) || !validID(w.AgentUserID) || len(w.DefinitionKey) > 64 {
		return ruleInvalid("actions contain an invalid id")
	}
	if t.Enabled && (t.TemplateID == "" || t.AgentUserID == "") {
		return ruleInvalid("auto ticket needs a ticket template and a proxy user")
	}
	if w.Enabled && (w.DefinitionKey == "" || w.AgentUserID == "") {
		return ruleInvalid("workflow trigger needs a workflow and an initiator")
	}
	return nil
}

// checkMatchAttr 关联用的属性须是部署配置里登记的（只有登记的属性会被物化给评估引擎）。
func (uc *RuleUsecase) checkMatchAttr(spec *RuleSpec) error {
	m := spec.CIMatch
	if m.Label == "" || m.By != mc.CIMatchByAttr {
		return nil
	}
	attr := m.Attr
	if attr == "" {
		attr = mc.DefaultCIMatchAttr
	}
	if !slices.Contains(ruleMatchAttrs(uc.ext.MatchAttrs), attr) {
		return ruleInvalid("ci_match attr %q is not one of the configured match attributes", attr)
	}
	return nil
}

// errEscalationUnusable 升级策略不可用。不存在、已停用、不能被该分组引用（操作者可能无权看到它）是
// 同一句话，不泄露别的分组里有没有这条策略。
var errEscalationUnusable = ruleInvalid("the escalation policy does not exist, is disabled or cannot be used by rules in this group")

// escalationUsable 升级策略能否被 ancestors（规则所在分组自根到自身的 ID 链）里的规则新引用。
func escalationUsable(e *Escalation, ancestors []string) bool {
	return e != nil && e.Spec.Enabled && e.UsableIn(ancestors)
}

// checkEscalation 规则引用的升级策略须存在、已启用，且是全局策略或归属于规则所在分组 / 其上级分组。
// 保存前就已引用的策略（old）不再要求存在与启用——策略被删除 / 停用后规则上留着的 ID 按「没有策略」
// 处理，不该让规则从此无法保存；但规则换了分组时仍要求它能被新分组引用。
func (uc *RuleUsecase) checkEscalation(ctx context.Context, scope Scope, spec, old *RuleSpec) error {
	if spec.EscalationID == "" {
		return nil
	}
	kept := old != nil && old.EscalationID == spec.EscalationID
	if kept && old.GroupID == spec.GroupID {
		return nil
	}
	if uc.ext.Escalations == nil {
		return errEscalationUnusable
	}
	e, err := uc.ext.Escalations.Get(ctx, spec.EscalationID)
	switch {
	case kerrors.Is(err, ErrEscalationNotFound):
		if kept {
			return nil
		}
		return errEscalationUnusable
	case err != nil:
		return err
	case !e.UsableIn(scope.Ancestors(spec.GroupID)), !kept && !e.Spec.Enabled:
		return errEscalationUnusable
	}
	return nil
}

// checkActions 事件动作引用的对象须可用：代理提单人 / 发起人是可用用户，工单模板对代理提单人可用，
// 流程当前可以发起。与保存前完全相同的动作配置不再校验（人离职、模板停用后不该让规则无法保存；
// 执行时失败会记入事件时间线）。
func (uc *RuleUsecase) checkActions(ctx context.Context, spec, old *RuleSpec) error {
	t, w := spec.Actions.Ticket, spec.Actions.Workflow
	if t.Enabled && (old == nil || old.Actions.Ticket != t) {
		if err := uc.requireAgent(ctx, t.AgentUserID, "the proxy user of the auto ticket"); err != nil {
			return err
		}
		var tpl *TicketTemplate
		if uc.ext.Tickets != nil {
			var err error
			if tpl, err = uc.ext.Tickets.Template(ctx, t.TemplateID, t.AgentUserID); err != nil {
				return err
			}
		}
		if tpl == nil {
			return ruleInvalid("the ticket template does not exist, is disabled or cannot be used by the proxy user")
		}
	}
	if w.Enabled && (old == nil || old.Actions.Workflow != w) {
		if err := uc.requireAgent(ctx, w.AgentUserID, "the initiator of the workflow"); err != nil {
			return err
		}
		var def *WorkflowDef
		if uc.ext.Workflows != nil {
			var err error
			if def, err = uc.ext.Workflows.Definition(ctx, w.DefinitionKey); err != nil {
				return err
			}
		}
		if def == nil {
			return ruleInvalid("workflow %q does not exist, is disabled or has no published version", truncate(w.DefinitionKey, 64))
		}
	}
	return nil
}

func (uc *RuleUsecase) requireAgent(ctx context.Context, userID, what string) error {
	active, err := uc.dir.ActiveUserIDs(ctx, []string{userID})
	if err != nil {
		return err
	}
	if len(active) != 1 {
		return ruleInvalid("%s does not exist or is not active", what)
	}
	return nil
}

// validateP2 第二、三期配置项里依赖外部数据的校验（静态部分在 validateStatic 里已做）。
func (uc *RuleUsecase) validateP2(ctx context.Context, scope Scope, spec, old *RuleSpec) error {
	if err := uc.checkMatchAttr(spec); err != nil {
		return err
	}
	if err := uc.checkEscalation(ctx, scope, spec, old); err != nil {
		return err
	}
	return uc.checkActions(ctx, spec, old)
}

// pipelineChanged 流水线配置（改写 / 丢弃 / 关联 / 资源范围）是否变化：它们决定事件的标签、指纹与去留，
// 变化即推进 spec_time。
func pipelineChanged(a, b *RuleSpec) bool {
	if a.DropExpr != b.DropExpr || !slices.EqualFunc(a.Relabel, b.Relabel, func(x, y mc.RelabelStep) bool {
		return x.Action == y.Action && slices.Equal(x.SourceLabels, y.SourceLabels) && x.Separator == y.Separator &&
			x.Regex == y.Regex && x.TargetLabel == y.TargetLabel && x.Replacement == y.Replacement
	}) {
		return true
	}
	ma, mb := a.CIMatch, b.CIMatch
	if ma.Disabled != mb.Disabled || ma.Label != mb.Label || ma.By != mb.By || ma.Attr != mb.Attr ||
		!slices.Equal(ma.ModelCodes, mb.ModelCodes) || !slices.Equal(ma.EnrichTags, mb.EnrichTags) {
		return true
	}
	sa, sb := a.ResourceScope, b.ResourceScope
	return sa.Enabled != sb.Enabled || !slices.Equal(sa.ModelCodes, sb.ModelCodes) || !slices.Equal(sa.Envs, sb.Envs) || !slices.Equal(sa.Tags, sb.Tags)
}

// RuleRefs 规则第二、三期引用对象的展示名；未引用的为 nil。
type RuleRefs struct {
	Escalation         *NamedRef
	TicketTemplate     *NamedRef
	TicketAgent        *NamedRef
	WorkflowDefinition *NamedRef
	WorkflowAgent      *NamedRef
}

// Refs 批量取这些规则引用的升级策略、工单模板、流程与代理用户的名称（规则 ID → 展示名）。各类名称各查
// 一次，不逐条查询；查不到的（已删除 / 已停用）标记 Missing。
func (uc *RuleUsecase) Refs(ctx context.Context, rules []*RuleView) (map[string]RuleRefs, error) {
	var escalationIDs, users []string
	needTpl, needFlow := false, false
	for _, r := range rules {
		a := r.Spec.Actions
		escalationIDs = append(escalationIDs, r.Spec.EscalationID)
		users = append(users, a.Ticket.AgentUserID, a.Workflow.AgentUserID)
		needTpl = needTpl || a.Ticket.TemplateID != ""
		needFlow = needFlow || a.Workflow.DefinitionKey != ""
	}
	escNames, tplNames, flowNames, userNames := map[string]string{}, map[string]string{}, map[string]string{}, map[string]string{}
	var err error
	if escalationIDs = uniq(escalationIDs); len(escalationIDs) > 0 && uc.ext.Escalations != nil {
		list, err := uc.ext.Escalations.ListByIDs(ctx, escalationIDs)
		if err != nil {
			return nil, err
		}
		for _, e := range list {
			escNames[e.ID] = e.Spec.Name
		}
	}
	if needTpl && uc.ext.Tickets != nil {
		list, err := uc.ext.Tickets.Templates(ctx)
		if err != nil {
			return nil, err
		}
		for _, t := range list {
			tplNames[t.ID] = t.Name
		}
	}
	if needFlow && uc.ext.Workflows != nil {
		list, err := uc.ext.Workflows.Definitions(ctx)
		if err != nil {
			return nil, err
		}
		for _, d := range list {
			flowNames[d.Key] = d.Name
		}
	}
	if users = uniq(users); len(users) > 0 {
		if userNames, err = uc.dir.UserNames(ctx, users); err != nil {
			return nil, err
		}
	}
	ref := func(id string, lookup map[string]string) *NamedRef {
		if id == "" {
			return nil
		}
		name, ok := lookup[id]
		return &NamedRef{ID: id, Name: name, Missing: !ok}
	}
	out := make(map[string]RuleRefs, len(rules))
	for _, r := range rules {
		a := r.Spec.Actions
		out[r.ID] = RuleRefs{
			Escalation:     ref(r.Spec.EscalationID, escNames),
			TicketTemplate: ref(a.Ticket.TemplateID, tplNames), TicketAgent: ref(a.Ticket.AgentUserID, userNames),
			WorkflowDefinition: ref(a.Workflow.DefinitionKey, flowNames), WorkflowAgent: ref(a.Workflow.AgentUserID, userNames),
		}
	}
	return out, nil
}
