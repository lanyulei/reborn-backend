package biz

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	mp "github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/google/wire"
)

// escalationProviders 升级策略、带第二期协作者的通知派发、自监控事件派发的 biz providers。
// ProvideDispatchUsecase 取代 ProviderSet 里的 NewDispatchUsecase。
var escalationProviders = wire.NewSet(NewEscalationUsecase, NewDispatchExt, ProvideDispatchUsecase, NewEngineEventUsecase)

// maxEscalationOptions 规则表单里升级策略候选项、值班分组候选项的条数上限。
const maxEscalationOptions = 500

// EscalationWriter 升级策略写仓储。每个方法一个事务，同事务 bump monitor_cfg_version 的
// monitor_escalation 行；归属分组在事务内加共享锁（与分组删除互斥）。
type EscalationWriter interface {
	// Create 同一归属下重名返回 ErrEscalationNameExists；归属分组不存在返回 ErrGroupNotFound。
	Create(ctx context.Context, e *Escalation, actor string) error
	// Update 乐观锁；不改归属分组。
	Update(ctx context.Context, e *Escalation, expectVersion int32, actor string) error
	// Delete 软删；仍被未删除的规则引用时返回 EscalationInUse（事务内计数）。
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// EscalationUsage 升级策略被规则引用的情况。
type EscalationUsage interface {
	// RuleCounts 各策略被多少条未删除的规则引用（没有引用的不在结果中）。
	RuleCounts(ctx context.Context, ids []string) (map[string]int32, error)
}

// EscalationInUse 升级策略仍被规则引用：409，metadata 带 rule_count。
func EscalationInUse(rules int) error {
	return WithMetadata(ErrEscalationInUse, map[string]string{"rule_count": strconv.Itoa(rules)})
}

// EscalationLevelRefs 一级通知对象的显示名（Depts 与 targets.depts 逐项对应）。
type EscalationLevelRefs struct {
	Users        []NamedRef
	Schedules    []NamedRef
	OncallGroups []NamedRef
	Depts        []NamedRef
}

// EscalationView 升级策略及其展示信息。
type EscalationView struct {
	*Escalation
	GroupName      string
	LevelRefs      []EscalationLevelRefs
	RuleCount      int32
	Estimate       mp.VolumeEstimate
	CanManage      bool
	UpdateUserName string
}

// EscalationOption 规则表单里可选的升级策略。
type EscalationOption struct {
	ID          string
	Name        string
	Global      bool
	GroupName   string
	LevelCount  int32
	Mode        int32
	MinSeverity int32
}

// EscalationUsecase 升级策略用例。查看要求对归属分组只读以上（全局策略有任一分组范围即可见），
// 新建 / 编辑 / 删除要求可管理；全局策略仅超级管理员可维护。范围判定全部经 ScopeUsecase。
type EscalationUsecase struct {
	scopes *ScopeUsecase
	reader EscalationReader
	writer EscalationWriter
	usage  EscalationUsage
	groups OncallGroups
	names  names
	dir    Directory
	now    func() time.Time
}

// NewEscalationUsecase new an EscalationUsecase.
func NewEscalationUsecase(scopes *ScopeUsecase, reader EscalationReader, writer EscalationWriter, usage EscalationUsage,
	groups OncallGroups, dir Directory, schedules Schedules,
) *EscalationUsecase {
	return &EscalationUsecase{
		scopes: scopes, reader: reader, writer: writer, usage: usage, groups: groups, dir: dir,
		names: names{dir: dir, schedules: schedules}, now: time.Now,
	}
}

// escalationVisible / escalationManageable 升级策略的范围判定（口径同屏蔽规则）。
func escalationVisible(scope Scope, groupID string) bool { return muteVisible(scope, groupID) }

func escalationManageable(scope Scope, groupID string) bool { return muteManageable(scope, groupID) }

func escalationInvalid(format string, args ...any) error {
	return detail(ErrEscalationInvalid, format, args...)
}

// EscalationListQuery 升级策略列表查询入参。
type EscalationListQuery struct {
	GroupID string
	Global  *bool
	Keyword string
	Enabled *bool
}

// List 分页查询升级策略：全局策略 + 数据范围内分组的策略。
func (uc *EscalationUsecase) List(ctx context.Context, userID string, q EscalationListQuery, page, pageSize int64) ([]*EscalationView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	if !scope.HasAny() {
		return []*EscalationView{}, 0, nil
	}
	filter := EscalationFilter{
		Groups: scope.Filter(q.GroupID), IncludeGlobal: q.GroupID == "", Global: q.Global,
		Keyword: strings.TrimSpace(q.Keyword), Enabled: q.Enabled,
	}
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, scope, items)
	return views, total, err
}

// Get 查询升级策略详情。
func (uc *EscalationUsecase) Get(ctx context.Context, userID, id string) (*EscalationView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	e, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !escalationVisible(scope, e.Spec.GroupID) {
		return nil, ErrForbidden
	}
	return uc.view(ctx, scope, e)
}

func (uc *EscalationUsecase) view(ctx context.Context, scope Scope, e *Escalation) (*EscalationView, error) {
	views, err := uc.views(ctx, scope, []*Escalation{e})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// oncallGroupNames 全部值班分组的名称（小集合，一次取齐）；policies 都没有引用值班分组时不查。
func (uc *EscalationUsecase) oncallGroupNames(ctx context.Context, needed bool) (map[string]string, error) {
	out := map[string]string{}
	if !needed {
		return out, nil
	}
	groups, err := uc.groups.Groups(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		out[g.ID] = g.Name
	}
	return out, nil
}

// views 批量补齐展示信息：名称各查一次、引用计数一次。
func (uc *EscalationUsecase) views(ctx context.Context, scope Scope, items []*Escalation) ([]*EscalationView, error) {
	set := &refSet{}
	ids := make([]string, 0, len(items))
	usesGroups := false
	for _, e := range items {
		ids = append(ids, e.ID)
		set.users = append(set.users, e.UpdateUser)
		for _, l := range e.Spec.Levels {
			set.users = append(set.users, l.Targets.UserIDs...)
			for _, s := range l.Targets.Schedules {
				set.schedules = append(set.schedules, s.ID)
			}
			for _, d := range l.Targets.Depts {
				set.depts = append(set.depts, d.ID)
			}
			usesGroups = usesGroups || len(l.Targets.OncallGroupIDs) > 0
		}
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	groupNames, err := uc.oncallGroupNames(ctx, usesGroups)
	if err != nil {
		return nil, err
	}
	counts := map[string]int32{}
	if len(ids) > 0 {
		if counts, err = uc.usage.RuleCounts(ctx, ids); err != nil {
			return nil, err
		}
	}
	out := make([]*EscalationView, 0, len(items))
	for _, e := range items {
		v := &EscalationView{
			Escalation: e, GroupName: scope.GroupName(e.Spec.GroupID), RuleCount: counts[e.ID],
			Estimate:  mp.EstimateVolume(policyOf(&e.Spec), 0, 0),
			CanManage: escalationManageable(scope, e.Spec.GroupID), UpdateUserName: set.userNames[e.UpdateUser],
			LevelRefs: make([]EscalationLevelRefs, 0, len(e.Spec.Levels)),
		}
		for _, l := range e.Spec.Levels {
			t := l.Targets
			v.LevelRefs = append(v.LevelRefs, EscalationLevelRefs{
				Users: refs(t.UserIDs, set.userNames), Schedules: refs(scheduleIDs(t), set.scheduleNames),
				OncallGroups: refs(t.OncallGroupIDs, groupNames), Depts: refs(deptIDs(t), set.deptNames),
			})
		}
		out = append(out, v)
	}
	return out, nil
}

// policyOf 策略里参与计算的部分（估算与引擎的升级计算用同一个结构）。
func policyOf(s *EscalationSpec) *mp.EscalationPolicy {
	return &mp.EscalationPolicy{Mode: s.Mode, MinSeverity: s.MinSeverity, Levels: s.Levels}
}

func scheduleIDs(t mc.EscalationTargets) []string {
	out := make([]string, 0, len(t.Schedules))
	for _, s := range t.Schedules {
		out = append(out, s.ID)
	}
	return out
}

func deptIDs(t mc.EscalationTargets) []string {
	out := make([]string, 0, len(t.Depts))
	for _, d := range t.Depts {
		out = append(out, d.ID)
	}
	return out
}

// validateEscalationShape 不依赖外部数据的校验：名称、说明、升级方式、最低级别、各级（after_m 递增、
// 重复间隔、次数上限、通知对象个数与重复）。保存与估算共用。
func validateEscalationShape(spec *EscalationSpec) error {
	spec.Name, spec.Description = strings.TrimSpace(spec.Name), strings.TrimSpace(spec.Description)
	if spec.Name == "" || len([]rune(spec.Name)) > 128 {
		return escalationInvalid("name must be 1-128 characters")
	}
	if len([]rune(spec.Description)) > 255 {
		return escalationInvalid("description must be at most 255 characters")
	}
	if !mc.ValidEscalationMode(spec.Mode) {
		return escalationInvalid("mode must be 1 or 2")
	}
	if !mc.ValidSeverity(spec.MinSeverity) {
		return escalationInvalid("min_severity must be 1-4")
	}
	if err := mc.ValidateEscalationLevels(spec.Levels); err != nil {
		return escalationInvalid("%v", err)
	}
	return nil
}

// validateTargets 各级引用的对象须存在：用户可用（keptUsers 里保存前就在策略上的除外）、值班表启用、
// 值班分组存在、部门存在。文件 / 请求里的 ID 格式已由接口校验。
func (uc *EscalationUsecase) validateTargets(ctx context.Context, levels []mc.EscalationLevel, keptUsers []string) error {
	set := &refSet{}
	var users, groups []string
	for _, l := range levels {
		users = append(users, l.Targets.UserIDs...)
		groups = append(groups, l.Targets.OncallGroupIDs...)
		set.schedules = append(set.schedules, scheduleIDs(l.Targets)...)
		set.depts = append(set.depts, deptIDs(l.Targets)...)
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return err
	}
	if err := requireActiveUsers(ctx, uc.dir, uniq(users), keptUsers); err != nil {
		if errors.Is(err, errInactiveUser) {
			return escalationInvalid("some of the notify users do not exist or are not active")
		}
		return err
	}
	for _, id := range set.schedules {
		if _, ok := set.scheduleNames[id]; !ok {
			return escalationInvalid("schedule %s does not exist or is disabled", id)
		}
	}
	for _, id := range set.depts {
		if _, ok := set.deptNames[id]; !ok {
			return escalationInvalid("department %s does not exist", id)
		}
	}
	groupNames, err := uc.oncallGroupNames(ctx, len(groups) > 0)
	if err != nil {
		return err
	}
	for _, id := range groups {
		if _, ok := groupNames[id]; !ok {
			return escalationInvalid("oncall group %s does not exist", id)
		}
	}
	return nil
}

// policyUsers 策略各级里指定的全部用户。
func policyUsers(levels []mc.EscalationLevel) []string {
	var out []string
	for _, l := range levels {
		out = append(out, l.Targets.UserIDs...)
	}
	return out
}

// Create 新建升级策略：要求对归属分组可管理；全局策略仅超级管理员。
func (uc *EscalationUsecase) Create(ctx context.Context, userID string, spec EscalationSpec) (*EscalationView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !escalationManageable(scope, spec.GroupID) {
		return nil, ErrForbidden
	}
	if spec.GroupID != "" && !scope.GroupExists(spec.GroupID) {
		return nil, ErrGroupNotFound
	}
	if err := validateEscalationShape(&spec); err != nil {
		return nil, err
	}
	if err := uc.validateTargets(ctx, spec.Levels, nil); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	if err := uc.writer.Create(ctx, &Escalation{ID: id, Spec: spec}, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Update 编辑升级策略（整体替换）；归属分组不可更改。
func (uc *EscalationUsecase) Update(ctx context.Context, userID, id string, spec EscalationSpec, version int32) (*EscalationView, error) {
	scope, cur, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if spec.GroupID != cur.Spec.GroupID {
		return nil, escalationInvalid("the group of an escalation policy cannot be changed")
	}
	if err := validateEscalationShape(&spec); err != nil {
		return nil, err
	}
	if err := uc.validateTargets(ctx, spec.Levels, policyUsers(cur.Spec.Levels)); err != nil {
		return nil, err
	}
	// 乐观锁同时守住范围判定：归属分组不可改，判定依据不会被并发改掉。
	if err := uc.writer.Update(ctx, &Escalation{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Delete 删除升级策略；仍被规则引用时返回 EscalationInUse。
func (uc *EscalationUsecase) Delete(ctx context.Context, userID, id string) error {
	_, cur, err := uc.manageable(ctx, userID, id)
	if err != nil {
		return err
	}
	return uc.writer.Delete(ctx, id, cur.Version, userID)
}

// manageable 读取策略并要求可管理。看不到的策略按无权限处理。
func (uc *EscalationUsecase) manageable(ctx context.Context, userID, id string) (Scope, *Escalation, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return Scope{}, nil, err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return Scope{}, nil, err
	}
	if !escalationManageable(scope, cur.Spec.GroupID) {
		return Scope{}, nil, ErrForbidden
	}
	return scope, cur, nil
}

func (uc *EscalationUsecase) reload(ctx context.Context, scope Scope, id string) (*EscalationView, error) {
	e, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, e)
}

// Estimate 估算 24 小时通知量（不保存、不访问数据）：只校验策略的形状。ruleRepeatM / ruleMaxNotify 是
// 规则自身的重复通知设置，给出后一并计入。
func (uc *EscalationUsecase) Estimate(spec EscalationSpec, ruleRepeatM, ruleMaxNotify int32) (mp.VolumeEstimate, error) {
	if spec.Name == "" {
		spec.Name = "-" // 表单还没填名称时也能估算
	}
	if err := validateEscalationShape(&spec); err != nil {
		return mp.VolumeEstimate{}, err
	}
	if ruleRepeatM != 0 && (ruleRepeatM < 5 || ruleRepeatM > 10080) || ruleMaxNotify < 0 || ruleMaxNotify > 1000 {
		return mp.VolumeEstimate{}, escalationInvalid("invalid rule repeat settings")
	}
	return mp.EstimateVolume(policyOf(&spec), ruleRepeatM, ruleMaxNotify), nil
}

// Options 某个规则分组内的规则可以引用的已启用策略：全局策略 + 归属于该分组或其上级分组的策略。
// 要求对该分组只读以上。全局在前，其余按名称排序，至多 maxEscalationOptions 条。
func (uc *EscalationUsecase) Options(ctx context.Context, userID, groupID string) ([]EscalationOption, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := scope.requireRead(groupID); err != nil {
		return nil, err
	}
	ancestors := scope.Ancestors(groupID)
	if len(ancestors) == 0 {
		return nil, ErrGroupNotFound
	}
	enabled := true
	items, _, err := uc.reader.List(ctx, EscalationFilter{Groups: GroupFilter{IDs: ancestors}, IncludeGlobal: true, Enabled: &enabled}, 1, maxEscalationOptions)
	if err != nil {
		return nil, err
	}
	out := make([]EscalationOption, 0, len(items))
	for _, e := range items {
		if !e.UsableIn(ancestors) {
			continue
		}
		out = append(out, EscalationOption{
			ID: e.ID, Name: e.Spec.Name, Global: e.Spec.GroupID == "", GroupName: scope.GroupName(e.Spec.GroupID),
			LevelCount: int32(len(e.Spec.Levels)), Mode: e.Spec.Mode, MinSeverity: e.Spec.MinSeverity, // #nosec G115 -- 至多 MaxEscalationLevels
		})
	}
	return out, nil
}

// OncallGroupOptions 值班分组候选项（最小披露），按名称排序，至多 maxEscalationOptions 个。
func (uc *EscalationUsecase) OncallGroupOptions(ctx context.Context) ([]OncallGroup, error) {
	groups, err := uc.groups.Groups(ctx)
	if err != nil {
		return nil, err
	}
	groups = slices.Clone(groups)
	slices.SortStableFunc(groups, func(a, b OncallGroup) int { return strings.Compare(a.Name, b.Name) })
	return groups[:min(len(groups), maxEscalationOptions)], nil
}
