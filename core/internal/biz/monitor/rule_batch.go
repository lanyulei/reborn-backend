package biz

import (
	"context"
	"maps"
	"slices"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 批量操作。
const (
	BatchEnable       = "enable"
	BatchDisable      = "disable"
	BatchMove         = "move"
	BatchSetSeverity  = "set_severity"
	BatchSetTiming    = "set_timing"
	BatchAddLabels    = "add_labels"
	BatchRemoveLabels = "remove_labels"
	BatchSetNotify    = "set_notify"
	BatchDelete       = "delete"
	// BatchSetEscalation 把升级策略改为 EscalationID（不给 = 取消升级策略）。
	BatchSetEscalation = "set_escalation"
)

// BatchInput 批量操作入参：Action 决定使用哪些参数，其余须留空。
type BatchInput struct {
	IDs             []string
	Action          string
	GroupID         *string
	Severity        *int32
	EvalIntervalS   *int32
	ForS            *int32
	RecoverS        *int32
	Labels          map[string]string
	LabelNames      []string
	Notify          *mc.NotifyTargets
	NotifyRecovery  *bool
	RepeatIntervalM *int32
	MaxNotify       *int32
	// EscalationID 仅 set_escalation 使用：nil = 取消升级策略。
	EscalationID *string
}

// BatchResult 批量操作中一条规则的结果。
type BatchResult struct {
	ID      string
	Name    string
	OK      bool
	Reason  string
	Message string
}

// checkParams 校验操作与参数匹配：该操作需要的参数必须给出，不属于它的参数必须留空。
func (in *BatchInput) checkParams() error {
	used := map[string]bool{
		"group_id": in.GroupID != nil, "severity": in.Severity != nil,
		"eval_interval_s": in.EvalIntervalS != nil, "for_s": in.ForS != nil, "recover_s": in.RecoverS != nil,
		"labels": len(in.Labels) > 0, "label_names": len(in.LabelNames) > 0,
		"notify": in.Notify != nil, "notify_recovery": in.NotifyRecovery != nil,
		"repeat_interval_m": in.RepeatIntervalM != nil, "max_notify": in.MaxNotify != nil,
		"escalation_id": in.EscalationID != nil,
	}
	var allowed []string
	required := true
	switch in.Action {
	case BatchEnable, BatchDisable, BatchDelete:
	case BatchSetEscalation:
		allowed = []string{"escalation_id"}
	case BatchMove:
		allowed, required = []string{"group_id"}, used["group_id"]
	case BatchSetSeverity:
		allowed, required = []string{"severity"}, used["severity"]
	case BatchSetTiming:
		allowed = []string{"eval_interval_s", "for_s", "recover_s"}
		required = used["eval_interval_s"] || used["for_s"] || used["recover_s"]
	case BatchAddLabels:
		allowed, required = []string{"labels"}, used["labels"]
	case BatchRemoveLabels:
		allowed, required = []string{"label_names"}, used["label_names"]
	case BatchSetNotify:
		allowed = []string{"notify", "notify_recovery", "repeat_interval_m", "max_notify"}
		required = used["notify"] || used["notify_recovery"] || used["repeat_interval_m"] || used["max_notify"]
	default:
		return detail(ErrRuleBatchInvalid, "unknown action %q", in.Action)
	}
	if !required {
		return detail(ErrRuleBatchInvalid, "action %q is missing its parameters", in.Action)
	}
	for name, set := range used {
		if set && !slices.Contains(allowed, name) {
			return detail(ErrRuleBatchInvalid, "parameter %s does not belong to action %q", name, in.Action)
		}
	}
	return nil
}

// Batch 批量操作规则（至多 MaxBatchRules 条）：逐条独立事务，部分失败不影响其余，结果与入参同序。
// 每条规则各自判定是否可管理；与单条操作走同一套校验与留痕。
func (uc *RuleUsecase) Batch(ctx context.Context, userID string, in BatchInput) ([]BatchResult, error) {
	if len(in.IDs) == 0 || len(in.IDs) > MaxBatchRules || hasDuplicate(in.IDs) {
		return nil, detail(ErrRuleBatchInvalid, "ids must contain 1-%d distinct rules", MaxBatchRules)
	}
	if err := in.checkParams(); err != nil {
		return nil, err
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	// 对整批都相同的前置条件只判一次：目标分组、通知对象。
	if in.Action == BatchMove {
		if err := requireTargetGroup(scope, *in.GroupID); err != nil {
			return nil, err
		}
	}
	if in.Notify != nil {
		if err := uc.validateNotify(ctx, *in.Notify, nil); err != nil {
			return nil, err
		}
	}
	rules, err := uc.reader.ListByIDs(ctx, in.IDs)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*Rule, len(rules))
	for _, r := range rules {
		byID[r.ID] = r
	}
	results := make([]BatchResult, 0, len(in.IDs))
	for _, id := range in.IDs {
		res := BatchResult{ID: id}
		rule, ok := byID[id]
		err := error(ErrRuleNotFound)
		if ok {
			// 名称只在对规则所在分组可读时回显（与导出同一口径）：对调用方完全不可读的规则只回报无权限。
			if scope.CanRead(rule.Spec.GroupID) {
				res.Name = rule.Spec.Name
			}
			if err = scope.requireManage(rule.Spec.GroupID); err == nil {
				err = uc.batchOne(ctx, userID, scope, rule, &in)
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err() // 请求已取消：不再把剩余规则逐条记成失败
			}
			e := kerrors.FromError(err)
			res.Reason, res.Message = e.Reason, e.Message
			if e.Code >= 500 {
				// 内部错误不向调用方透出细节。
				res.Reason, res.Message = "INTERNAL", "internal error"
			}
		}
		res.OK = err == nil
		results = append(results, res)
	}
	return results, nil
}

// batchOne 对一条规则执行批量操作（一个事务）。
func (uc *RuleUsecase) batchOne(ctx context.Context, userID string, scope Scope, rule *Rule, in *BatchInput) error {
	old := rule.Spec
	change := RuleChange{Action: mc.RuleActionBatch, Operator: userID}
	switch in.Action {
	case BatchEnable:
		return uc.toggle(ctx, rule, true, userID)
	case BatchDisable:
		return uc.toggle(ctx, rule, false, userID)
	case BatchDelete:
		return uc.writer.Delete(ctx, rule, RuleChange{Action: mc.RuleActionDelete, Summary: "批量删除", Operator: userID})
	case BatchMove:
		if rule.Spec.GroupID == *in.GroupID {
			return nil
		}
		rule.Spec.GroupID = *in.GroupID
		change.Action, change.Summary = mc.RuleActionMove, "批量移动分组"
	case BatchSetSeverity:
		if len(rule.Spec.Queries) != 1 {
			return ruleInvalid("set_severity only applies to rules with a single query")
		}
		rule.Spec.Queries = []mc.Query{{Expr: rule.Spec.Queries[0].Expr, Severity: *in.Severity}}
		change.Summary = "批量修改级别"
	case BatchSetTiming:
		setIf(&rule.Spec.EvalIntervalS, in.EvalIntervalS)
		setIf(&rule.Spec.ForS, in.ForS)
		setIf(&rule.Spec.RecoverS, in.RecoverS)
		change.Summary = "批量修改时间参数"
	case BatchAddLabels:
		rule.Spec.Labels = maps.Clone(rule.Spec.Labels)
		if rule.Spec.Labels == nil {
			rule.Spec.Labels = map[string]string{}
		}
		maps.Copy(rule.Spec.Labels, in.Labels)
		change.Summary = "批量添加附加标签"
	case BatchRemoveLabels:
		rule.Spec.Labels = maps.Clone(rule.Spec.Labels)
		for _, name := range in.LabelNames {
			delete(rule.Spec.Labels, name)
		}
		change.Summary = "批量移除附加标签"
	case BatchSetNotify:
		if in.Notify != nil {
			rule.Spec.Notify = *in.Notify
		}
		if in.NotifyRecovery != nil {
			rule.Spec.NotifyRecovery = *in.NotifyRecovery
		}
		setIf(&rule.Spec.RepeatIntervalM, in.RepeatIntervalM)
		setIf(&rule.Spec.MaxNotify, in.MaxNotify)
		change.Summary = "批量修改通知配置"
	case BatchSetEscalation:
		rule.Spec.EscalationID = ""
		if in.EscalationID != nil {
			rule.Spec.EscalationID = *in.EscalationID
		}
		if rule.Spec.EscalationID == old.EscalationID {
			return nil
		}
		change.Summary = "批量设置升级策略"
	}
	// 只改了一部分字段，其余内容保存时已校验过；这里重跑不依赖外部数据的校验即可覆盖
	// 标签上限、时间参数范围等（通知对象已在批量入口统一校验）。
	if err := validateStatic(&rule.Spec); err != nil {
		return err
	}
	// 升级策略须能被规则（移动后）所在的分组引用：换策略、换分组都要重新判定。
	if err := uc.checkEscalation(ctx, scope, &rule.Spec, &old); err != nil {
		return err
	}
	return uc.save(ctx, rule, old, rule.Version, change)
}

// setIf 入参给出时才覆盖（批量操作的可选参数）。
func setIf(dst, src *int32) {
	if src != nil {
		*dst = *src
	}
}
