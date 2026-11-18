package biz

import (
	"context"
	"errors"
	"slices"
	"strings"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// errorMessage 取错误的说明文字（Kratos 错误取 message，其余取 Error()）。
func errorMessage(err error) string { return kerrors.FromError(err).Message }

func ruleInvalid(format string, args ...any) error { return detail(ErrRuleInvalid, format, args...) }

// 规则通知对象的个数上限（与接口校验同一口径）。
const (
	maxNotifyUsers     = 50
	maxNotifySchedules = 20
	maxNotifyDepts     = 20
)

// normalizeSpec 规整规则内容：去首尾空白、nil 容器补空值（落库与快照里不出现 null）。
func normalizeSpec(spec *RuleSpec) {
	spec.Name = strings.TrimSpace(spec.Name)
	spec.Description = strings.TrimSpace(spec.Description)
	spec.RecoveryExpr = strings.TrimSpace(spec.RecoveryExpr)
	for i := range spec.Queries {
		spec.Queries[i].Expr = strings.TrimSpace(spec.Queries[i].Expr)
	}
	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}
	if spec.Annotations == nil {
		spec.Annotations = map[string]string{}
	}
	if spec.DatasourceIDs == nil {
		spec.DatasourceIDs = []string{}
	}
	if spec.GroupKeyLabels == nil {
		spec.GroupKeyLabels = []string{}
	}
	if spec.Effective.Windows == nil {
		spec.Effective.Windows = []mc.TimeWindow{}
	}
	if spec.Notify.UserIDs == nil {
		spec.Notify.UserIDs = []string{}
	}
	if spec.Notify.ScheduleIDs == nil {
		spec.Notify.ScheduleIDs = []string{}
	}
	if spec.Notify.DeptIDs == nil {
		spec.Notify.DeptIDs = []string{}
	}
	spec.DropExpr = strings.TrimSpace(spec.DropExpr)
	if spec.Relabel == nil {
		spec.Relabel = []mc.RelabelStep{}
	}
}

// validateStatic 不依赖外部数据的校验：名称、查询与级别、PromQL 语法、标签、附加信息模板、
// 恢复取值、时间参数、生效时间、重复通知、聚合键。
func validateStatic(spec *RuleSpec) error {
	normalizeSpec(spec)
	if spec.Name == "" || len([]rune(spec.Name)) > 128 {
		return ruleInvalid("name must be 1-128 characters")
	}
	if len([]rune(spec.Description)) > 512 {
		return ruleInvalid("description must be at most 512 characters")
	}
	if n := len(spec.Queries); n < 1 || n > mc.MaxRuleQueries {
		return ruleInvalid("a rule needs 1-%d queries", mc.MaxRuleQueries)
	}
	var seen [mc.SeverityFatal + 1]bool
	for i, q := range spec.Queries {
		if !mc.ValidSeverity(q.Severity) {
			return ruleInvalid("query %d: severity must be 1-4", i+1)
		}
		if seen[q.Severity] {
			return ruleInvalid("query %d: each query must have a distinct severity", i+1)
		}
		seen[q.Severity] = true
		if len(q.Expr) > maxExprBytes {
			return ruleInvalid("query %d: expression is too long", i+1)
		}
		if err := checkExpr(q.Expr); err != nil {
			return ruleInvalid("query %d: %s", i+1, truncate(errorMessage(err), 300))
		}
	}
	if err := mc.ValidateLabels(spec.Labels); err != nil {
		return ruleInvalid("labels: %v", err)
	}
	if len(spec.Annotations) > mc.MaxRuleAnnotations {
		return ruleInvalid("at most %d annotations", mc.MaxRuleAnnotations)
	}
	for name, text := range spec.Annotations {
		if !mc.ValidLabelName(name) {
			return ruleInvalid("annotation name %q is invalid", name)
		}
		if text == "" || len([]rune(text)) > MaxAnnotationLen {
			return ruleInvalid("annotation %q must be 1-%d characters", name, MaxAnnotationLen)
		}
		if err := checkTemplate(text); err != nil {
			return ruleInvalid("annotation %q: %s", name, truncate(err.Error(), 300))
		}
	}
	if spec.RecoveryExpr != "" {
		if len(spec.RecoveryExpr) > maxExprBytes {
			return ruleInvalid("recovery expression is too long")
		}
		if err := checkRecoveryExpr(spec.RecoveryExpr); err != nil {
			return ruleInvalid("recovery expression: %s", truncate(errorMessage(err), 300))
		}
	}
	if spec.EvalIntervalS < 15 || spec.EvalIntervalS > 3600 {
		return ruleInvalid("eval_interval_s must be between 15 and 3600")
	}
	for name, v := range map[string]int32{"for_s": spec.ForS, "recover_s": spec.RecoverS, "nodata_keep_s": spec.NodataKeepS} {
		if v < 0 || v > 86400 {
			return ruleInvalid("%s must be between 0 and 86400", name)
		}
	}
	if err := spec.Effective.Validate(); err != nil {
		return ruleInvalid("effective time: %v", err)
	}
	if spec.RepeatIntervalM != 0 && (spec.RepeatIntervalM < 5 || spec.RepeatIntervalM > 10080) {
		return ruleInvalid("repeat_interval_m must be 0 or between 5 and 10080")
	}
	if spec.MaxNotify < 0 || spec.MaxNotify > 1000 {
		return ruleInvalid("max_notify must be between 0 and 1000")
	}
	if len(spec.GroupKeyLabels) > mc.MaxGroupKeyLabels {
		return ruleInvalid("at most %d group key labels", mc.MaxGroupKeyLabels)
	}
	for _, name := range spec.GroupKeyLabels {
		if !mc.ValidLabelName(name) {
			return ruleInvalid("group key label %q is invalid", name)
		}
	}
	switch spec.DatasourceScope {
	case mc.DatasourceScopeSpecified:
		if n := len(spec.DatasourceIDs); n < 1 || n > MaxRuleDatasources {
			return ruleInvalid("select 1-%d datasources", MaxRuleDatasources)
		}
		if hasDuplicate(spec.DatasourceIDs) {
			return ruleInvalid("datasources must not repeat")
		}
	case mc.DatasourceScopeAll:
		if len(spec.DatasourceIDs) != 0 {
			return ruleInvalid("datasource_ids must be empty when the rule applies to all datasources")
		}
	default:
		return ruleInvalid("datasource_scope must be 1 or 2")
	}
	if len(spec.Notify.UserIDs) > maxNotifyUsers || len(spec.Notify.ScheduleIDs) > maxNotifySchedules || len(spec.Notify.DeptIDs) > maxNotifyDepts {
		return ruleInvalid("too many notify targets")
	}
	if hasDuplicate(spec.Notify.UserIDs) || hasDuplicate(spec.Notify.ScheduleIDs) || hasDuplicate(spec.Notify.DeptIDs) {
		return ruleInvalid("notify targets must not repeat")
	}
	return validateP2Static(spec)
}

// hasDuplicate 报告切片里是否有重复元素（入参都很短，直接两两比较）。
func hasDuplicate[T comparable](items []T) bool {
	for i, it := range items {
		if slices.Contains(items[:i], it) {
			return true
		}
	}
	return false
}

// validate 规则保存前的完整校验。old 为保存前的内容（新建时为 nil）：新加入的数据源须已启用，
// 原本就在规则里的只要求仍然存在——数据源被停用不应让规则从此无法保存。scope 为操作者的数据范围
// （升级策略能否被规则所在分组引用要看分组的上级链）。
func (uc *RuleUsecase) validate(ctx context.Context, scope Scope, spec *RuleSpec, old *RuleSpec) error {
	if err := validateStatic(spec); err != nil {
		return err
	}
	var kept, keptUsers []string
	if old != nil {
		kept, keptUsers = old.DatasourceIDs, old.Notify.UserIDs
	}
	if err := uc.validateDatasources(ctx, spec.DatasourceIDs, kept); err != nil {
		return err
	}
	if err := uc.validateNotify(ctx, spec.Notify, keptUsers); err != nil {
		return err
	}
	return uc.validateP2(ctx, scope, spec, old)
}

// validateDatasources 指定的数据源须存在；不在 kept 里的（新加入的）还须已启用。
func (uc *RuleUsecase) validateDatasources(ctx context.Context, ids, kept []string) error {
	if len(ids) == 0 {
		return nil
	}
	briefs, err := uc.names.datasources.Briefs(ctx, ids)
	if err != nil {
		return err
	}
	for _, id := range ids {
		b, ok := briefs[id]
		if !ok {
			return ruleInvalid("datasource %s does not exist", id)
		}
		if !b.Enabled && !slices.Contains(kept, id) {
			return ruleInvalid("datasource %q is disabled", b.Name)
		}
	}
	return nil
}

// validateNotify 通知对象须存在：用户可用（keptUsers 里保存前就在规则里的除外，见
// requireActiveUsers）、值班表启用、部门存在。
func (uc *RuleUsecase) validateNotify(ctx context.Context, n mc.NotifyTargets, keptUsers []string) error {
	set := &refSet{schedules: n.ScheduleIDs, depts: n.DeptIDs}
	if err := uc.names.resolve(ctx, set); err != nil {
		return err
	}
	if err := requireActiveUsers(ctx, uc.dir, n.UserIDs, keptUsers); err != nil {
		if errors.Is(err, errInactiveUser) {
			return ruleInvalid("some of the notify users do not exist or are not active")
		}
		return err
	}
	for _, id := range n.ScheduleIDs {
		if _, ok := set.scheduleNames[id]; !ok {
			return ruleInvalid("notify schedule %s does not exist or is disabled", id)
		}
	}
	for _, id := range n.DeptIDs {
		if _, ok := set.deptNames[id]; !ok {
			return ruleInvalid("notify department %s does not exist", id)
		}
	}
	return nil
}
