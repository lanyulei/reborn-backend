package biz

import (
	"context"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// RecordRuleSpec 记录规则的可编辑内容。Name 即写回的指标名。
type RecordRuleSpec struct {
	GroupID      string
	Name         string
	Description  string
	DatasourceID string
	Expr         string
	IntervalS    int32
	Labels       map[string]string
	Enabled      bool
}

// RecordTask 记录规则的执行状态（monitor_record_task，引擎写、core 只读）。
type RecordTask struct {
	LastStatus     int32
	LastEvalTime   *time.Time
	LastError      string
	LastDurationMS int32
	LastSeries     int32
	NextEvalTime   *time.Time
}

// RecordRule 记录规则。Task 为最近一次执行的状态；引擎尚未为它物化任务时为 nil。
type RecordRule struct {
	ID         string
	Spec       RecordRuleSpec
	Version    int32
	CreateTime time.Time
	UpdateTime time.Time
	UpdateUser string
	Task       *RecordTask
}

// RecordRuleFilter 记录规则列表过滤条件。LastStatus 按展示口径过滤（停用的规则恒为 0）。
type RecordRuleFilter struct {
	Groups       GroupFilter
	DatasourceID string
	Keyword      string
	Enabled      *bool
	LastStatus   *int32
}

// RecordRuleReader 记录规则读仓储。
type RecordRuleReader interface {
	// List 按更新时间倒序，带执行状态。
	List(ctx context.Context, filter RecordRuleFilter, page, pageSize int64) ([]*RecordRule, int64, error)
	// Get 不存在（含已删除）返回 ErrRecordRuleNotFound。
	Get(ctx context.Context, id string) (*RecordRule, error)
	// FindByName 同一分组、同一数据源下叫这个名字的未删除记录规则的 ID；没有返回空串。
	FindByName(ctx context.Context, groupID, datasourceID, name string) (string, error)
}

// RecordRuleWriter 记录规则写仓储。每个方法一个事务：事务内确认分组与数据源仍然存在（与它们的删除互斥），
// 启用的规则还要求数据源此刻配置了 Remote Write 地址（与数据源清除该地址互斥）。成功后回填 Version。
type RecordRuleWriter interface {
	// Create 重名返回 ErrRecordRuleNameExists。
	Create(ctx context.Context, r *RecordRule, actor string) error
	Update(ctx context.Context, r *RecordRule, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// DatasourceWriteTarget 数据源作为记录规则写入目标的状态。
type DatasourceWriteTarget struct {
	Name    string
	Enabled bool
	// HasWriteURL 是否配置了 Remote Write 地址。
	HasWriteURL bool
}

// Writable 数据源当前是否可写：已启用且配置了 Remote Write 地址。
func (t DatasourceWriteTarget) Writable() bool { return t.Enabled && t.HasWriteURL }

// DatasourceWriteTargets 数据源的写入状态（批量；不存在的不在结果中）。
type DatasourceWriteTargets interface {
	WriteTargets(ctx context.Context, ids []string) (map[string]DatasourceWriteTarget, error)
}

// RecordRuleView 记录规则及其展示信息。
type RecordRuleView struct {
	*RecordRule
	GroupName      string
	DatasourceName string
	// Writable 数据源存在、已启用且配置了 Remote Write 地址；为 false 时规则不会执行。
	Writable       bool
	CanManage      bool
	UpdateUserName string
}

// 记录规则校验出问题的字段（RecordRuleIssue.Field）。
const (
	RecordFieldName       = "name"
	RecordFieldExpr       = "expr"
	RecordFieldLabels     = "labels"
	RecordFieldDatasource = "datasource_id"
	RecordFieldGroup      = "group_id"
	RecordFieldInterval   = "interval_s"
	RecordFieldDesc       = "description"
)

// RecordRuleIssue 记录规则的一个问题。Reason 为错误码。
type RecordRuleIssue struct {
	Field   string
	Reason  string
	Message string
	// base 该问题对应的哨兵错误（保存时把第一个问题作为错误返回）。
	base *kerrors.Error
}

func (i RecordRuleIssue) err() error { return detail(i.base, "%s", i.Message) }

// RecordRuleUsecase 记录规则用例。范围规则与告警规则相同：查看要求对规则所在分组只读以上，新建 / 编辑 /
// 删除 / 启停要求可管理；换分组要求对原分组与目标分组都可管理。范围判定全部经 ScopeUsecase。
type RecordRuleUsecase struct {
	scopes  *ScopeUsecase
	reader  RecordRuleReader
	writer  RecordRuleWriter
	targets DatasourceWriteTargets
	dir     Directory
}

// NewRecordRuleUsecase new a RecordRuleUsecase.
func NewRecordRuleUsecase(scopes *ScopeUsecase, reader RecordRuleReader, writer RecordRuleWriter, targets DatasourceWriteTargets,
	dir Directory,
) *RecordRuleUsecase {
	return &RecordRuleUsecase{scopes: scopes, reader: reader, writer: writer, targets: targets, dir: dir}
}

// RecordRuleListQuery 记录规则列表查询入参。
type RecordRuleListQuery struct {
	GroupID string
	RecordRuleFilter
}

// List 分页查询记录规则（只返回数据范围内的）。
func (uc *RecordRuleUsecase) List(ctx context.Context, userID string, q RecordRuleListQuery, page, pageSize int64) ([]*RecordRuleView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	filter := q.RecordRuleFilter
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	if filter.Groups = scope.Filter(q.GroupID); filter.Groups.Empty() {
		return []*RecordRuleView{}, 0, nil
	}
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, scope, items)
	return views, total, err
}

// Get 查询记录规则详情。
func (uc *RecordRuleUsecase) Get(ctx context.Context, userID, id string) (*RecordRuleView, error) {
	scope, rule, err := uc.load(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if err := scope.requireRead(rule.Spec.GroupID); err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}

func (uc *RecordRuleUsecase) load(ctx context.Context, userID, id string) (Scope, *RecordRule, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	rule, err := uc.reader.Get(ctx, id)
	return scope, rule, err
}

// manageable 读取规则并要求对其所在分组可管理。
func (uc *RecordRuleUsecase) manageable(ctx context.Context, userID, id string) (Scope, *RecordRule, error) {
	scope, rule, err := uc.load(ctx, userID, id)
	if err != nil {
		return Scope{}, nil, err
	}
	return scope, rule, scope.requireManage(rule.Spec.GroupID)
}

func (uc *RecordRuleUsecase) view(ctx context.Context, scope Scope, rule *RecordRule) (*RecordRuleView, error) {
	views, err := uc.views(ctx, scope, []*RecordRule{rule})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// views 批量补齐展示信息（数据源状态、更新人各查一次）。停用的规则不展示执行状态。
func (uc *RecordRuleUsecase) views(ctx context.Context, scope Scope, rules []*RecordRule) ([]*RecordRuleView, error) {
	var users, sources []string
	for _, r := range rules {
		users, sources = append(users, r.UpdateUser), append(sources, r.Spec.DatasourceID)
	}
	userNames, targets := map[string]string{}, map[string]DatasourceWriteTarget{}
	var err error
	if users = uniq(users); len(users) > 0 {
		if userNames, err = uc.dir.UserNames(ctx, users); err != nil {
			return nil, err
		}
	}
	if sources = uniq(sources); len(sources) > 0 {
		if targets, err = uc.targets.WriteTargets(ctx, sources); err != nil {
			return nil, err
		}
	}
	out := make([]*RecordRuleView, 0, len(rules))
	for _, r := range rules {
		t := targets[r.Spec.DatasourceID]
		v := &RecordRuleView{
			RecordRule: r, GroupName: scope.GroupName(r.Spec.GroupID), DatasourceName: t.Name, Writable: t.Writable(),
			CanManage: scope.CanManage(r.Spec.GroupID), UpdateUserName: userNames[r.UpdateUser],
		}
		if !r.Spec.Enabled && r.Task != nil {
			cp := *r
			cp.Task, v.RecordRule = nil, &cp
		}
		out = append(out, v)
	}
	return out, nil
}

func normalizeRecordSpec(spec *RecordRuleSpec) {
	spec.Name, spec.Description, spec.Expr = strings.TrimSpace(spec.Name), strings.TrimSpace(spec.Description), strings.TrimSpace(spec.Expr)
	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}
}

// staticIssues 不依赖外部数据的问题：指标名、说明、表达式、附加标签、执行周期。
func staticIssues(spec *RecordRuleSpec) []RecordRuleIssue {
	var out []RecordRuleIssue
	add := func(field, format string, args ...any) {
		out = append(out, recordIssue(ErrRecordRuleInvalid, field, format, args...))
	}
	if spec.Name == "" || len([]rune(spec.Name)) > 128 || !mc.ValidMetricName(spec.Name) {
		add(RecordFieldName, "name must be a valid metric name (letters, digits, underscores and colons, not starting with a digit), 1-128 characters")
	}
	if len([]rune(spec.Description)) > 512 {
		add(RecordFieldDesc, "description must be at most 512 characters")
	}
	switch {
	case spec.Expr == "":
		add(RecordFieldExpr, "expression must not be empty")
	case len(spec.Expr) > maxExprBytes:
		add(RecordFieldExpr, "expression is longer than %d bytes", maxExprBytes)
	default:
		if err := checkExpr(spec.Expr); err != nil {
			add(RecordFieldExpr, "%s", truncate(errorMessage(err), 300))
		}
	}
	if err := mc.ValidateLabels(spec.Labels); err != nil {
		add(RecordFieldLabels, "labels: %v", err)
	}
	if spec.IntervalS < 15 || spec.IntervalS > 3600 {
		add(RecordFieldInterval, "interval_s must be between 15 and 3600")
	}
	return out
}

func recordIssue(base *kerrors.Error, field, format string, args ...any) RecordRuleIssue {
	return RecordRuleIssue{Field: field, Reason: base.Reason, Message: errorMessage(detail(base, format, args...)), base: base}
}

// issues 记录规则保存前的全部问题（空 = 可以保存）。old 为保存前的内容（新建 / 校验未保存的表单时为
// nil）：新选的数据源须已启用，原本就用着的数据源后来被停用不阻止保存；selfID 是规则自己的 ID，
// 判重名时排除。分组的范围判定由调用方先行完成。
func (uc *RecordRuleUsecase) issues(ctx context.Context, scope Scope, spec *RecordRuleSpec, old *RecordRuleSpec, selfID string) ([]RecordRuleIssue, error) {
	normalizeRecordSpec(spec)
	out := staticIssues(spec)
	if !scope.GroupExists(spec.GroupID) {
		out = append(out, recordIssue(ErrGroupNotFound, RecordFieldGroup, "rule group not found"))
	}
	targets, err := uc.targets.WriteTargets(ctx, []string{spec.DatasourceID})
	if err != nil {
		return nil, err
	}
	t, ok := targets[spec.DatasourceID]
	kept := old != nil && old.DatasourceID == spec.DatasourceID
	switch {
	case !ok:
		out = append(out, recordIssue(ErrRecordRuleInvalid, RecordFieldDatasource, "datasource does not exist"))
	case !t.Enabled && !kept:
		out = append(out, recordIssue(ErrRecordRuleInvalid, RecordFieldDatasource, "datasource %q is disabled", t.Name))
	case !t.HasWriteURL:
		out = append(out, recordIssue(ErrRecordRuleNoWriteURL, RecordFieldDatasource, "datasource %q has no remote write url", t.Name))
	}
	if spec.Name != "" && ok && scope.GroupExists(spec.GroupID) {
		dup, err := uc.reader.FindByName(ctx, spec.GroupID, spec.DatasourceID, spec.Name)
		if err != nil {
			return nil, err
		}
		if dup != "" && dup != selfID {
			out = append(out, recordIssue(ErrRecordRuleNameExists, RecordFieldName,
				"a record rule with this metric name already exists in this group for this datasource"))
		}
	}
	return out, nil
}

// check 保存前校验：有问题时把第一个作为错误返回。
func (uc *RecordRuleUsecase) check(ctx context.Context, scope Scope, spec *RecordRuleSpec, old *RecordRuleSpec, selfID string) error {
	issues, err := uc.issues(ctx, scope, spec, old, selfID)
	if err != nil {
		return err
	}
	if len(issues) > 0 {
		return issues[0].err()
	}
	return nil
}

// Validate 校验记录规则（不保存）：逐项返回问题。要求对所选分组只读以上（否则 ErrForbidden——不向
// 范围外的人回答「这个分组里有没有叫这个名字的规则」）。selfID 为正在编辑的规则（可空）：它在调用方
// 可读的范围内时，沿用它原来的数据源不要求仍然启用，判重名时把它自己排除。
func (uc *RecordRuleUsecase) Validate(ctx context.Context, userID string, spec RecordRuleSpec, selfID string) ([]RecordRuleIssue, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := scope.requireRead(spec.GroupID); err != nil {
		return nil, err
	}
	var old *RecordRuleSpec
	if selfID != "" {
		cur, err := uc.reader.Get(ctx, selfID)
		switch {
		case err == nil && scope.CanRead(cur.Spec.GroupID):
			old = &cur.Spec
		case err == nil || kerrors.Is(err, ErrRecordRuleNotFound):
			selfID = "" // 不存在或不可读：按新建校验，不透露它的任何信息
		default:
			return nil, err
		}
	}
	return uc.issues(ctx, scope, &spec, old, selfID)
}

// Create 新建记录规则：要求对所属分组可管理。
func (uc *RecordRuleUsecase) Create(ctx context.Context, userID string, spec RecordRuleSpec) (*RecordRuleView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := requireTargetGroup(scope, spec.GroupID); err != nil {
		return nil, err
	}
	if err := uc.check(ctx, scope, &spec, nil, ""); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	if err := uc.writer.Create(ctx, &RecordRule{ID: id, Spec: spec}, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Update 编辑记录规则（整体替换）。更换分组要求对原分组与目标分组都可管理。
func (uc *RecordRuleUsecase) Update(ctx context.Context, userID, id string, spec RecordRuleSpec, version int32) (*RecordRuleView, error) {
	scope, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if spec.GroupID != rule.Spec.GroupID {
		if err := requireTargetGroup(scope, spec.GroupID); err != nil {
			return nil, err
		}
	}
	if err := uc.check(ctx, scope, &spec, &rule.Spec, id); err != nil {
		return nil, err
	}
	// 乐观锁同时守住范围判定：判定所依据的分组若被并发修改，version 必然不同。
	if err := uc.writer.Update(ctx, &RecordRule{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Delete 删除记录规则；已写回的指标数据不受影响。
func (uc *RecordRuleUsecase) Delete(ctx context.Context, userID, id string) error {
	_, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return err
	}
	return uc.writer.Delete(ctx, id, rule.Version, userID)
}

// Toggle 启用或停用记录规则；已是目标状态时不写入。启用时数据源须仍然存在、已启用并配置了 Remote Write 地址。
func (uc *RecordRuleUsecase) Toggle(ctx context.Context, userID, id string, enabled bool) (*RecordRuleView, error) {
	scope, rule, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if rule.Spec.Enabled == enabled {
		return uc.view(ctx, scope, rule)
	}
	spec := rule.Spec
	spec.Enabled = enabled
	if enabled {
		// old 传 nil：重新启用等同于新选这个数据源，停用的数据源不能用。
		if err := uc.check(ctx, scope, &spec, nil, id); err != nil {
			return nil, err
		}
	}
	if err := uc.writer.Update(ctx, &RecordRule{ID: id, Spec: spec}, rule.Version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

func (uc *RecordRuleUsecase) reload(ctx context.Context, scope Scope, id string) (*RecordRuleView, error) {
	rule, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, rule)
}
