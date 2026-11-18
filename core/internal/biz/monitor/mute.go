package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// maxMuteScan 按「未开始 / 生效中」过滤时一次读取的屏蔽规则数上限：周期屏蔽是否在时间窗内
// 只能在应用里判定（monitorcontract.Effective.Contains），所以先取出未过期的再在内存里筛选、分页。
const maxMuteScan = 2000

// criteria 屏蔽规则与订阅规则共有的事件筛选条件（各项之间是「并且」，留空 = 不限）。标签匹配用
// monitorcontract 的已编译匹配器——与评估引擎判定屏蔽用的是同一份实现，预览结果即实际行为。
// 作用分组（含下级）由调用方另行判定。
type criteria struct {
	ruleIDs       []string
	datasourceIDs []string
	severities    []int32
	matchers      *mc.Matchers
}

func (c criteria) match(a *Alert) bool {
	if len(c.ruleIDs) > 0 && !slices.Contains(c.ruleIDs, a.RuleID) {
		return false
	}
	if len(c.datasourceIDs) > 0 && !slices.Contains(c.datasourceIDs, a.DatasourceID) {
		return false
	}
	if len(c.severities) > 0 && !slices.Contains(c.severities, a.Severity) {
		return false
	}
	return c.matchers.Match(a.Labels)
}

// MuteUsecase 屏蔽规则用例。查看要求对作用分组只读以上，变更要求可管理；作用于全部分组的
// 屏蔽规则仅超级管理员可维护（有任一分组范围的用户可见）。
type MuteUsecase struct {
	scopes *ScopeUsecase
	reader MuteReader
	writer MuteWriter
	alerts AlertReader
	stats  AlertStats
	rules  RuleReader
	names  names
	now    func() time.Time
}

// NewMuteUsecase new a MuteUsecase.
func NewMuteUsecase(scopes *ScopeUsecase, reader MuteReader, writer MuteWriter, alerts AlertReader, stats AlertStats,
	rules RuleReader, ruleLookup RuleLookup, datasources DatasourceLookup, dir Directory, schedules Schedules,
) *MuteUsecase {
	return &MuteUsecase{
		scopes: scopes, reader: reader, writer: writer, alerts: alerts, stats: stats, rules: rules,
		names: names{dir: dir, schedules: schedules, datasources: datasources, rules: ruleLookup}, now: time.Now,
	}
}

// muteState 屏蔽规则在 now 时刻的状态。
func muteState(s *MuteSpec, now time.Time) string {
	switch {
	case !s.Enabled:
		return MuteStateDisabled
	case s.EndTime != nil && !now.Before(*s.EndTime):
		return MuteStateExpired
	case s.StartTime != nil && now.Before(*s.StartTime):
		return MuteStatePending
	case s.TimeType == mc.MuteTimePeriodic && (s.Periodic == nil || !s.Periodic.Contains(now)):
		return MuteStatePending
	}
	return MuteStateActive
}

// muteVisible / muteManageable 屏蔽规则的范围判定：全局屏蔽规则有任一分组范围即可见，仅超级管理员可管理。
func muteVisible(scope Scope, groupID string) bool {
	if groupID == "" {
		return scope.HasAny()
	}
	return scope.CanRead(groupID)
}

func muteManageable(scope Scope, groupID string) bool {
	if groupID == "" {
		return scope.IsAdmin()
	}
	return scope.CanManage(groupID)
}

// MuteListQuery 屏蔽规则列表查询入参。
type MuteListQuery struct {
	GroupID string
	Keyword string
	Enabled *bool
	Mode    *int32
	State   string
}

// List 分页查询屏蔽规则。
func (uc *MuteUsecase) List(ctx context.Context, userID string, q MuteListQuery, page, pageSize int64) ([]*MuteView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	if !scope.HasAny() {
		return []*MuteView{}, 0, nil
	}
	filter := MuteFilter{Groups: scope.Filter(""), IncludeGlobal: true, Keyword: strings.TrimSpace(q.Keyword), Enabled: q.Enabled, Mode: q.Mode}
	if q.GroupID != "" {
		// 与该分组相关的屏蔽规则：作用于它、它的上级或它的下级（都可能屏蔽到该分组的事件）。
		related := slices.Concat(scope.Ancestors(q.GroupID), scope.Descendants(q.GroupID))
		ids := make([]string, 0, len(related))
		for _, id := range uniq(related) {
			if scope.CanRead(id) {
				ids = append(ids, id)
			}
		}
		filter.Groups = GroupFilter{IDs: ids}
	}
	now := uc.now()
	var mutes []*Mute
	var total int64
	switch q.State {
	case MuteStatePending, MuteStateActive:
		filter.Live = true
		all, _, err := uc.reader.List(ctx, filter, 1, maxMuteScan)
		if err != nil {
			return nil, 0, err
		}
		all = slices.DeleteFunc(all, func(m *Mute) bool { return muteState(&m.Spec, now) != q.State })
		total = int64(len(all))
		start := min((page-1)*pageSize, total)
		mutes = all[start:min(start+pageSize, total)]
	default:
		filter.State = q.State
		if mutes, total, err = uc.reader.List(ctx, filter, page, pageSize); err != nil {
			return nil, 0, err
		}
	}
	views, err := uc.views(ctx, scope, mutes)
	return views, total, err
}

// Get 查询屏蔽规则详情。
func (uc *MuteUsecase) Get(ctx context.Context, userID, id string) (*MuteView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	m, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !muteVisible(scope, m.Spec.GroupID) {
		return nil, ErrForbidden
	}
	return uc.view(ctx, scope, m)
}

func (uc *MuteUsecase) view(ctx context.Context, scope Scope, m *Mute) (*MuteView, error) {
	views, err := uc.views(ctx, scope, []*Mute{m})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

func (uc *MuteUsecase) views(ctx context.Context, scope Scope, mutes []*Mute) ([]*MuteView, error) {
	set := &refSet{}
	ids := make([]string, 0, len(mutes))
	for _, m := range mutes {
		ids = append(ids, m.ID)
		set.users = append(set.users, m.CreateUser, m.UpdateUser)
		set.rules = append(set.rules, m.Spec.RuleIDs...)
		set.datasources = append(set.datasources, m.Spec.DatasourceIDs...)
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	counts := map[string]int32{}
	if len(ids) > 0 {
		var err error
		if counts, err = uc.stats.MutedCounts(ctx, ids); err != nil {
			return nil, err
		}
	}
	now := uc.now()
	out := make([]*MuteView, 0, len(mutes))
	for _, m := range mutes {
		out = append(out, &MuteView{
			Mute: m, GroupName: scope.GroupName(m.Spec.GroupID), State: muteState(&m.Spec, now), MutedCount: counts[m.ID],
			Rules: refs(m.Spec.RuleIDs, set.ruleNames), Datasources: set.datasourceRefs(m.Spec.DatasourceIDs),
			CanManage: muteManageable(scope, m.Spec.GroupID), CreateUserName: set.userNames[m.CreateUser], UpdateUserName: set.userNames[m.UpdateUser],
		})
	}
	return out, nil
}

func muteInvalid(format string, args ...any) error { return detail(ErrMuteInvalid, format, args...) }

// validateMuteShape 不依赖外部数据的校验：名称、原因、条件个数、匹配条件、屏蔽方式与时间。
func validateMuteShape(spec *MuteSpec) (criteria, error) {
	spec.Name, spec.Reason = strings.TrimSpace(spec.Name), strings.TrimSpace(spec.Reason)
	if spec.Name == "" || len([]rune(spec.Name)) > 128 {
		return criteria{}, muteInvalid("name must be 1-128 characters")
	}
	if spec.Reason == "" || len([]rune(spec.Reason)) > 255 {
		return criteria{}, muteInvalid("reason must be 1-255 characters")
	}
	if len(spec.RuleIDs) > 200 || len(spec.DatasourceIDs) > 50 || hasDuplicate(spec.RuleIDs) || hasDuplicate(spec.DatasourceIDs) {
		return criteria{}, muteInvalid("too many or duplicated rules / datasources")
	}
	for _, s := range spec.Severities {
		if !mc.ValidSeverity(s) {
			return criteria{}, muteInvalid("severity must be 1-4")
		}
	}
	matchers, err := mc.CompileMatchers(spec.Matchers)
	if err != nil {
		return criteria{}, muteInvalid("matchers: %v", err)
	}
	if spec.Mode != mc.MuteModeNotify && spec.Mode != mc.MuteModeEvent {
		return criteria{}, muteInvalid("mode must be 1 or 2")
	}
	if spec.StartTime != nil && spec.EndTime != nil && !spec.StartTime.Before(*spec.EndTime) {
		return criteria{}, muteInvalid("start_time must be before end_time")
	}
	switch spec.TimeType {
	case mc.MuteTimeOnce:
		if spec.StartTime == nil || spec.EndTime == nil || spec.Periodic != nil {
			return criteria{}, muteInvalid("a one-off mute needs start_time and end_time and no periodic windows")
		}
	case mc.MuteTimePeriodic:
		if spec.Periodic == nil || len(spec.Periodic.Windows) == 0 {
			return criteria{}, muteInvalid("a periodic mute needs at least one time window")
		}
		// 屏蔽规则的周期只支持时间窗（「仅工作时间 / 仅非工作时间」是规则生效时间才有的方式）。
		if spec.Periodic.Mode != "" && spec.Periodic.Mode != mc.EffectiveModeWindows {
			return criteria{}, muteInvalid("periodic: mode is not supported for mute rules")
		}
		if err := spec.Periodic.Validate(); err != nil {
			return criteria{}, muteInvalid("periodic: %v", err)
		}
	default:
		return criteria{}, muteInvalid("time_type must be 1 or 2")
	}
	return criteria{ruleIDs: spec.RuleIDs, datasourceIDs: spec.DatasourceIDs, severities: spec.Severities, matchers: matchers}, nil
}

// errMuteRules 限定的规则不可用。不存在、与不在作用分组内（操作者可能无权看到它）是同一句话：
// 否则凭规则 ID 就能探出别的分组里有没有这条规则、叫什么名字。
var errMuteRules = muteInvalid("some of the selected rules do not exist in the selected group")

// rulesWithin 报告这些规则是否全部存在且位于给定分组之内。
func rulesWithin(rules []*Rule, ids, groupIDs []string) bool {
	if len(rules) != len(ids) {
		return false
	}
	return !slices.ContainsFunc(rules, func(r *Rule) bool { return !slices.Contains(groupIDs, r.Spec.GroupID) })
}

// validate 完整校验：形状 + 范围（作用分组可管理；全局仅超级管理员）+ 引用的规则与数据源存在，
// 且限定的规则须在作用分组（含下级）内——否则这条屏蔽规则永远不会命中，还会回显别的分组的规则名。
func (uc *MuteUsecase) validate(ctx context.Context, scope Scope, spec *MuteSpec) error {
	if !muteManageable(scope, spec.GroupID) {
		return ErrForbidden
	}
	if spec.GroupID != "" && !scope.GroupExists(spec.GroupID) {
		return ErrGroupNotFound
	}
	if _, err := validateMuteShape(spec); err != nil {
		return err
	}
	if len(spec.RuleIDs) > 0 {
		rules, err := uc.rules.ListByIDs(ctx, spec.RuleIDs)
		if err != nil {
			return err
		}
		// 全局屏蔽规则（仅超级管理员）可以限定任何分组的规则。
		within := scope.ReadableGroupIDs()
		if spec.GroupID != "" {
			within = scope.Descendants(spec.GroupID)
		}
		if !rulesWithin(rules, spec.RuleIDs, within) {
			return errMuteRules
		}
	}
	if len(spec.DatasourceIDs) > 0 {
		briefs, err := uc.names.datasources.Briefs(ctx, spec.DatasourceIDs)
		if err != nil {
			return err
		}
		if len(briefs) != len(spec.DatasourceIDs) {
			return muteInvalid("some of the selected datasources do not exist")
		}
	}
	return nil
}

// Create 新建屏蔽规则。
func (uc *MuteUsecase) Create(ctx context.Context, userID string, spec MuteSpec) (*MuteView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := uc.validate(ctx, scope, &spec); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	m := &Mute{ID: id, Spec: spec}
	if err := uc.writer.Create(ctx, m, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Update 编辑屏蔽规则（整体替换）：要求对原作用分组与新作用分组都可管理。
func (uc *MuteUsecase) Update(ctx context.Context, userID, id string, spec MuteSpec, version int32) (*MuteView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !muteManageable(scope, cur.Spec.GroupID) {
		return nil, ErrForbidden
	}
	if err := uc.validate(ctx, scope, &spec); err != nil {
		return nil, err
	}
	// 乐观锁同时守住范围判定：判定所依据的作用分组若被并发修改，version 必然不同。
	if err := uc.writer.Update(ctx, &Mute{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Delete 删除屏蔽规则。
func (uc *MuteUsecase) Delete(ctx context.Context, userID, id string) error {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return err
	}
	if !muteManageable(scope, cur.Spec.GroupID) {
		return ErrForbidden
	}
	return uc.writer.Delete(ctx, id, cur.Version, userID)
}

func (uc *MuteUsecase) reload(ctx context.Context, scope Scope, id string) (*MuteView, error) {
	m, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, m)
}

// Preview 保存前预览：当前活跃告警中会被该屏蔽规则命中的事件（不考虑时间范围，不落库）。
// 只在调用方可读的分组内查找。返回至多 previewItems 条与命中总数。
func (uc *MuteUsecase) Preview(ctx context.Context, userID string, spec MuteSpec) ([]*AlertView, int, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	if !muteVisible(scope, spec.GroupID) {
		return nil, 0, ErrForbidden
	}
	c, err := validateMuteShape(&spec)
	if err != nil {
		return nil, 0, err
	}
	filter := AlertFilter{
		Groups: scope.Filter(spec.GroupID), Statuses: []int32{mc.AlertFiring},
		RuleIDs: spec.RuleIDs, DatasourceIDs: spec.DatasourceIDs, Severities: spec.Severities,
	}
	return previewAlerts(ctx, scope, uc.alerts, uc.names.rules, uc.reader, uc.now(), filter, c)
}

// previewAlerts 屏蔽 / 订阅预览共用：按筛选条件扫描（至多 previewScanLimit 条）并在内存里做标签匹配。
func previewAlerts(ctx context.Context, scope Scope, alerts AlertReader, rules RuleLookup, mutes MuteReader, now time.Time, filter AlertFilter, c criteria) ([]*AlertView, int, error) {
	if filter.Groups.Empty() {
		return []*AlertView{}, 0, nil
	}
	scanned, err := alerts.Scan(ctx, filter, previewScanLimit)
	if err != nil {
		return nil, 0, err
	}
	matched := scanned[:0:0]
	for _, a := range scanned {
		if c.match(a) {
			matched = append(matched, a)
		}
	}
	total := len(matched)
	views, err := alertViews(ctx, scope, rules, mutes, now, matched[:min(total, previewItems)])
	return views, total, err
}
