package biz

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// RuleUsecase 告警规则用例：增删改查、启停、克隆、批量、版本与回滚、表达式校验、导入导出、判定记录。
// 查看类操作要求对规则所在分组只读以上，变更类要求可管理；范围判定全部经 ScopeUsecase。
type RuleUsecase struct {
	scopes   *ScopeUsecase
	reader   RuleReader
	lookup   RuleLookup
	writer   RuleWriter
	versions RuleVersionReader
	evals    EvalReader
	names    names
	dir      Directory
	now      func() time.Time
	// ext 第二、三期的协作者（WithP2 挂上；零值时相应的引用一律拒绝）。
	ext RuleExt
}

// NewRuleUsecase new a RuleUsecase.
func NewRuleUsecase(scopes *ScopeUsecase, reader RuleReader, lookup RuleLookup, writer RuleWriter, versions RuleVersionReader,
	evals EvalReader, datasources DatasourceLookup, dir Directory, schedules Schedules,
) *RuleUsecase {
	return &RuleUsecase{
		scopes: scopes, reader: reader, lookup: lookup, writer: writer, versions: versions, evals: evals, dir: dir,
		names: names{dir: dir, schedules: schedules, datasources: datasources, rules: lookup}, now: time.Now,
	}
}

// RuleListQuery 规则列表查询入参。
type RuleListQuery struct {
	GroupID string
	RuleFilter
}

// List 分页查询规则（只返回数据范围内的），带各规则的评估状态汇总。
func (uc *RuleUsecase) List(ctx context.Context, userID string, q RuleListQuery, page, pageSize int64) ([]*RuleView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	filter := q.RuleFilter
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	if filter.Groups = scope.Filter(q.GroupID); filter.Groups.Empty() {
		return []*RuleView{}, 0, nil
	}
	rules, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, scope, rules, false)
	return views, total, err
}

// Get 查询规则详情（含在各数据源上的评估状态）。
func (uc *RuleUsecase) Get(ctx context.Context, userID, id string) (*RuleView, error) {
	scope, rule, err := uc.readable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

// readable 读取规则并要求对其所在分组只读以上。
func (uc *RuleUsecase) readable(ctx context.Context, userID, id string) (Scope, *Rule, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	rule, err := uc.reader.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	return scope, rule, scope.requireRead(rule.Spec.GroupID)
}

// manageable 读取规则并要求对其所在分组可管理。
func (uc *RuleUsecase) manageable(ctx context.Context, userID, id string) (Scope, *Rule, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	rule, err := uc.reader.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	return scope, rule, scope.requireManage(rule.Spec.GroupID)
}

func (uc *RuleUsecase) view(ctx context.Context, scope Scope, rule *Rule) (*RuleView, error) {
	views, err := uc.views(ctx, scope, []*Rule{rule}, true)
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// views 批量补齐展示信息与评估状态（名称各查一次，评估任务一次）。
func (uc *RuleUsecase) views(ctx context.Context, scope Scope, rules []*Rule, withTasks bool) ([]*RuleView, error) {
	set := &refSet{}
	ids := make([]string, 0, len(rules))
	for _, r := range rules {
		ids = append(ids, r.ID)
		set.users = append(append(set.users, r.UpdateUser), r.Spec.Notify.UserIDs...)
		set.depts = append(set.depts, r.Spec.Notify.DeptIDs...)
		set.schedules = append(set.schedules, r.Spec.Notify.ScheduleIDs...)
		set.datasources = append(set.datasources, r.Spec.DatasourceIDs...)
	}
	tasks := map[string][]*EvalTask{}
	if len(ids) > 0 {
		var err error
		if tasks, err = uc.evals.ListTasks(ctx, ids); err != nil {
			return nil, err
		}
	}
	if withTasks {
		for _, ts := range tasks {
			for _, t := range ts {
				set.datasources = append(set.datasources, t.DatasourceID)
			}
		}
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	out := make([]*RuleView, 0, len(rules))
	for _, r := range rules {
		v := &RuleView{
			Rule: r, GroupName: scope.GroupName(r.Spec.GroupID), CanManage: scope.CanManage(r.Spec.GroupID),
			Datasources: set.datasourceRefs(r.Spec.DatasourceIDs), NotifyUsers: refs(r.Spec.Notify.UserIDs, set.userNames),
			NotifySchedules: refs(r.Spec.Notify.ScheduleIDs, set.scheduleNames), NotifyDepts: refs(r.Spec.Notify.DeptIDs, set.deptNames),
			UpdateUserName: set.userNames[r.UpdateUser],
		}
		summarize(v, tasks[r.ID])
		if withTasks {
			v.Tasks = tasks[r.ID]
			for _, t := range v.Tasks {
				t.DatasourceName = set.datasourceBriefs[t.DatasourceID].Name
			}
		}
		out = append(out, v)
	}
	return out, nil
}

// statusRank 评估状态的关注优先级：告警中 > 查询错误 > 无数据 > 正常 > 不在生效时间 > 未评估。
func statusRank(status int32) int {
	switch status {
	case mc.EvalFiring:
		return 5
	case mc.EvalError:
		return 4
	case mc.EvalNoData:
		return 3
	case mc.EvalOK:
		return 2
	case mc.EvalInactive:
		return 1
	}
	return 0
}

// IncompletePrefix 「结果不完整」说明的固定前缀。评估引擎在查询成功但结果不完整（序列数被截断、
// 上游只返回了部分结果）时，把以它开头的说明写进 monitor_eval_task.last_error 与
// monitor_eval_record.error，而状态仍是评估结果（不是查询错误）。字面量与引擎
// （monitor/internal/biz 的 incompletePrefix）一致；前端按这个前缀识别并本地化。
const IncompletePrefix = "incomplete result: "

// QueryPartialNote 判定记录里一条查询的结果被上游声明为不完整时，该查询 error 字段的取值
// （接口没有单独的 partial 字段）。
const QueryPartialNote = IncompletePrefix + "partial response from the datasource"

// QueryNote 判定记录里一条查询的 error 字段：查询出错时是错误原文；查询成功但上游只给了部分结果时
// 是 QueryPartialNote；其余为空。
func QueryNote(q *mc.EvalQueryDetail) string {
	if q.Error == "" && q.Partial {
		return QueryPartialNote
	}
	return q.Error
}

// summarize 把各数据源上的评估状态汇总到规则上；规则停用时状态恒为未评估。LastError 优先取查询
// 出错的数据源的错误；都没出错时取「结果不完整」的说明（IncompletePrefix 开头，状态不是查询错误）。
func summarize(v *RuleView, tasks []*EvalTask) {
	if !v.Spec.Enabled {
		return
	}
	incomplete := ""
	for _, t := range tasks {
		if t.LastStatus != mc.EvalError && incomplete == "" && strings.HasPrefix(t.LastError, IncompletePrefix) {
			incomplete = t.LastError
		}
		if statusRank(t.LastStatus) > statusRank(v.Status) {
			v.Status = t.LastStatus
		}
		v.FiringCount += t.FiringCount
		if t.LastEvalTime != nil && (v.LastEvalTime == nil || t.LastEvalTime.After(*v.LastEvalTime)) {
			v.LastEvalTime = t.LastEvalTime
		}
		if t.LastStatus == mc.EvalError && v.LastError == "" {
			v.LastError = t.LastError
		}
	}
	if v.LastError == "" {
		v.LastError = incomplete
	}
}

// Create 新建规则：要求对所属分组可管理。
func (uc *RuleUsecase) Create(ctx context.Context, userID string, spec RuleSpec) (*RuleView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := requireTargetGroup(scope, spec.GroupID); err != nil {
		return nil, err
	}
	if err := uc.validate(ctx, scope, &spec, nil); err != nil {
		return nil, err
	}
	rule, err := uc.insert(ctx, spec, RuleChange{Action: mc.RuleActionCreate, Summary: "新建规则", Operator: userID})
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

// requireTargetGroup 目标分组须存在且可管理。分组不可读时一律按无权限处理，不泄露其是否存在。
// 告警规则、记录规则、模拟触发共用。
func requireTargetGroup(scope Scope, groupID string) error {
	if err := scope.requireManage(groupID); err != nil {
		return err
	}
	if !scope.GroupExists(groupID) {
		return ErrGroupNotFound
	}
	return nil
}

func (uc *RuleUsecase) insert(ctx context.Context, spec RuleSpec, change RuleChange) (*Rule, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	rule := &Rule{ID: id, Spec: spec, UpdateUser: change.Operator}
	change.Summary = truncate(change.Summary, 255)
	if err := uc.writer.Create(ctx, rule, change); err != nil {
		return nil, err
	}
	return rule, nil
}

// save 以乐观锁保存修改后的规则；spec_time 是否推进由新旧内容比较决定。
func (uc *RuleUsecase) save(ctx context.Context, rule *Rule, old RuleSpec, expectVersion int32, change RuleChange) error {
	change.BumpSpecTime = evalRelevantChanged(&old, &rule.Spec)
	change.Summary = truncate(change.Summary, 255)
	rule.UpdateUser = change.Operator
	return uc.writer.Update(ctx, rule, expectVersion, change)
}

// evalRelevantChanged 判定相关的配置是否变化：查询、级别抑制、时间参数、数据源范围、附加标签，以及事件
// 流水线（标签改写 / 条件丢弃 / 关联资源 / 资源范围）。只有这些变化才推进 spec_time（引擎据此重置判定）；
// 改名、说明、通知配置、拓扑抑制、升级策略、事件动作等不影响判定。
func evalRelevantChanged(a, b *RuleSpec) bool {
	return pipelineChanged(a, b) || !slices.Equal(a.Queries, b.Queries) || a.LevelInhibit != b.LevelInhibit ||
		a.EvalIntervalS != b.EvalIntervalS || a.ForS != b.ForS || a.RecoverS != b.RecoverS || a.NodataKeepS != b.NodataKeepS ||
		a.DatasourceScope != b.DatasourceScope || !sameSet(a.DatasourceIDs, b.DatasourceIDs) || !sameLabels(a.Labels, b.Labels)
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !slices.Contains(b, x) {
			return false
		}
	}
	return true
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// Update 编辑规则（整体替换）。更换分组要求对原分组与目标分组都可管理。
func (uc *RuleUsecase) Update(ctx context.Context, userID, id string, spec RuleSpec, version int32, summary string) (*RuleView, error) {
	scope, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	old := rule.Spec
	action := mc.RuleActionUpdate
	if spec.GroupID != old.GroupID {
		if err := requireTargetGroup(scope, spec.GroupID); err != nil {
			return nil, err
		}
		if specEqualExceptGroup(&old, &spec) {
			action = mc.RuleActionMove
		}
	}
	if err := uc.validate(ctx, scope, &spec, &old); err != nil {
		return nil, err
	}
	if summary = strings.TrimSpace(summary); summary == "" {
		summary = "编辑规则"
	}
	rule.Spec = spec
	if err := uc.save(ctx, rule, old, version, RuleChange{Action: action, Summary: summary, Operator: userID}); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

func specEqualExceptGroup(a, b *RuleSpec) bool {
	x, y := *a, *b
	x.GroupID, y.GroupID = "", ""
	return !evalRelevantChanged(&x, &y) && x.Name == y.Name && x.Enabled == y.Enabled && x.Description == y.Description
}

// Delete 删除规则；其活跃告警由评估引擎随后关闭。
func (uc *RuleUsecase) Delete(ctx context.Context, userID, id string) error {
	_, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return err
	}
	return uc.writer.Delete(ctx, rule, RuleChange{Action: mc.RuleActionDelete, Summary: "删除规则", Operator: userID})
}

// Toggle 启用或停用规则；已是目标状态时不产生新版本。
func (uc *RuleUsecase) Toggle(ctx context.Context, userID, id string, enabled bool) (*RuleView, error) {
	scope, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if err := uc.toggle(ctx, rule, enabled, userID); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

func (uc *RuleUsecase) toggle(ctx context.Context, rule *Rule, enabled bool, userID string) error {
	if rule.Spec.Enabled == enabled {
		return nil
	}
	old := rule.Spec
	rule.Spec.Enabled = enabled
	change := RuleChange{Action: mc.RuleActionDisable, Summary: "停用规则", Operator: userID}
	if enabled {
		change.Action, change.Summary = mc.RuleActionEnable, "启用规则"
	}
	return uc.save(ctx, rule, old, rule.Version, change)
}

// cloneNameTries 克隆 / 改名导入时自动加序号的尝试次数上限。
const cloneNameTries = 50

// Clone 克隆规则为同分组或其他可管理分组下的新规则（停用状态）：要求对原规则只读、对目标分组可管理。
func (uc *RuleUsecase) Clone(ctx context.Context, userID, id, name, groupID string) (*RuleView, error) {
	scope, src, err := uc.readable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	spec := src.Spec
	if groupID != "" {
		spec.GroupID = groupID
	}
	if err := requireTargetGroup(scope, spec.GroupID); err != nil {
		return nil, err
	}
	spec.Enabled = false
	// 克隆到别的分组：原规则的升级策略若不能被目标分组引用，新规则不带升级策略。
	if spec.GroupID != src.Spec.GroupID && uc.checkEscalation(ctx, scope, &spec, &src.Spec) != nil {
		spec.EscalationID = ""
	}
	change := RuleChange{Action: mc.RuleActionCreate, Summary: "克隆自「" + src.Spec.Name + "」", Operator: userID}
	if name = strings.TrimSpace(name); name != "" {
		spec.Name = name
		rule, err := uc.insert(ctx, spec, change)
		if err != nil {
			return nil, err
		}
		return uc.view(ctx, scope, rule)
	}
	rule, err := uc.insertNumbered(ctx, spec, src.Spec.Name+" - 副本", change)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

// insertNumbered 以 base 为名新建规则，重名时依次尝试「base (2)」「base (3)」…。名称是否可用以
// 插入时的唯一索引为准（先查后写不可靠），冲突即换下一个。
func (uc *RuleUsecase) insertNumbered(ctx context.Context, spec RuleSpec, base string, change RuleChange) (*Rule, error) {
	for i := 1; i <= cloneNameTries; i++ {
		spec.Name = numberedName(base, i)
		rule, err := uc.insert(ctx, spec, change)
		if !kerrors.Is(err, ErrRuleNameExists) {
			return rule, err
		}
	}
	return nil, ErrRuleNameExists
}

// numberedName 第 n 个候选名称（n=1 为 base 本身），整体不超过 128 个字符。
func numberedName(base string, n int) string {
	suffix := ""
	if n > 1 {
		suffix = fmt.Sprintf(" (%d)", n)
	}
	return truncate(base, 128-len(suffix)) + suffix
}

// ListVersions 分页查询规则的变更历史。已删除规则的历史只有超级管理员可查。
func (uc *RuleUsecase) ListVersions(ctx context.Context, userID, id string, page, pageSize int64) ([]*RuleVersion, int64, error) {
	if _, err := uc.versionScope(ctx, userID, id); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.versions.List(ctx, id, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillOperators(ctx, items)
}

// GetVersion 查询规则某个历史版本的完整内容。
func (uc *RuleUsecase) GetVersion(ctx context.Context, userID, id string, version int32) (*RuleVersion, error) {
	scope, err := uc.versionScope(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	v, err := uc.versions.Get(ctx, id, version)
	if err != nil {
		return nil, err
	}
	if v.Spec != nil {
		v.GroupName = scope.GroupName(v.Spec.GroupID)
	}
	return v, uc.fillOperators(ctx, []*RuleVersion{v})
}

// versionScope 变更历史的查看权限：规则存在时要求对其分组只读；规则已删除时仅超级管理员。
func (uc *RuleUsecase) versionScope(ctx context.Context, userID, id string) (Scope, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, err
	}
	rule, err := uc.reader.Get(ctx, id)
	if kerrors.Is(err, ErrRuleNotFound) && scope.IsAdmin() {
		return scope, nil
	}
	if err != nil {
		return Scope{}, err
	}
	return scope, scope.requireRead(rule.Spec.GroupID)
}

func (uc *RuleUsecase) fillOperators(ctx context.Context, items []*RuleVersion) error {
	ids := make([]string, 0, len(items))
	for _, v := range items {
		ids = append(ids, v.OperatorID)
	}
	if ids = uniq(ids); len(ids) == 0 {
		return nil
	}
	userNames, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, v := range items {
		v.OperatorName = userNames[v.OperatorID]
	}
	return nil
}

// Rollback 把规则回滚到某个历史版本的内容（生成一个新版本）。快照里的分组与当前不同时，
// 要求对两个分组都可管理。
func (uc *RuleUsecase) Rollback(ctx context.Context, userID, id string, version, currentVersion int32) (*RuleView, error) {
	scope, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	snap, err := uc.versions.Get(ctx, id, version)
	if err != nil {
		return nil, err
	}
	if snap.Spec == nil {
		return nil, ErrRuleVersionNotFound
	}
	old, spec := rule.Spec, *snap.Spec
	if spec.GroupID != old.GroupID {
		if err := requireTargetGroup(scope, spec.GroupID); err != nil {
			return nil, err
		}
	}
	if err := uc.validate(ctx, scope, &spec, &old); err != nil {
		return nil, err
	}
	rule.Spec = spec
	change := RuleChange{Action: mc.RuleActionRollback, Summary: fmt.Sprintf("回滚到版本 %d", version), Operator: userID}
	if err := uc.save(ctx, rule, old, currentVersion, change); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

// ExprResult 一条表达式的校验结果。
type ExprResult struct {
	Expr  string
	Valid bool
	Error string
}

// ValidateExprs 校验 PromQL 语法（不保存、不访问数据源）。
func (uc *RuleUsecase) ValidateExprs(exprs []string) []ExprResult {
	out := make([]ExprResult, 0, len(exprs))
	for _, expr := range exprs {
		res := ExprResult{Expr: expr, Valid: true}
		if err := checkExpr(expr); err != nil {
			res.Valid, res.Error = false, truncate(errorMessage(err), 512)
		}
		out = append(out, res)
	}
	return out
}

// evalRecordRetention 判定记录的保留期（引擎按天分区清理）；查询下界不早于它。
const evalRecordRetention = 8 * 24 * time.Hour

// ListEvalRecords 分页查询规则的判定记录。
func (uc *RuleUsecase) ListEvalRecords(ctx context.Context, userID string, filter EvalRecordFilter, page, pageSize int64) ([]*EvalRecord, int64, error) {
	if _, _, err := uc.readable(ctx, userID, filter.RuleID); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	// 下界收紧到保留期内：分区表按 eval_time 裁剪分区，不做全表统计。
	floor := uc.now().Add(-evalRecordRetention)
	if filter.Start == nil || filter.Start.Before(floor) {
		filter.Start = &floor
	}
	items, total, err := uc.evals.ListRecords(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, r := range items {
		ids = append(ids, r.DatasourceID)
	}
	if ids = uniq(ids); len(ids) > 0 {
		briefs, err := uc.names.datasources.Briefs(ctx, ids)
		if err != nil {
			return nil, 0, err
		}
		for _, r := range items {
			r.DatasourceName = briefs[r.DatasourceID].Name
		}
	}
	return items, total, nil
}
