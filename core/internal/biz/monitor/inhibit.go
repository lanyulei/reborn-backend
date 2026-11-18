package biz

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/google/wire"
)

// InhibitProviderSet 抑制规则与记录规则的 biz providers（由组合根并入 ProviderSet）。
var InhibitProviderSet = wire.NewSet(ProvideInhibitUsecase, NewRecordRuleUsecase)

// InhibitWriter 抑制规则写仓储。每个方法一个事务，同事务 bump monitor_cfg_version 的 monitor_inhibit 行。
type InhibitWriter interface {
	// Create 作用分组已不存在返回 ErrGroupNotFound。
	Create(ctx context.Context, in *Inhibit, actor string) error
	// Update 乐观锁：版本不一致返回 ErrVersionConflict，不存在返回 ErrInhibitNotFound。
	Update(ctx context.Context, in *Inhibit, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// InhibitStats 抑制规则的命中统计（运行表，引擎写）。
type InhibitStats interface {
	// InhibitedCounts 各抑制规则当前抑制着的活跃事件数；没有的不在结果中。
	InhibitedCounts(ctx context.Context, inhibitIDs []string) (map[string]int32, error)
}

// InhibitView 抑制规则及其展示信息。
type InhibitView struct {
	*Inhibit
	GroupName      string
	SourceRules    []NamedRef
	TargetRules    []NamedRef
	InhibitedCount int32
	CanManage      bool
	CreateUserName string
	UpdateUserName string
}

// InhibitPreview 抑制规则的预览：当前活跃告警里哪些是源、哪些会被抑制（各至多 previewItems 条与总数）。
type InhibitPreview struct {
	Sources     []*AlertView
	SourceTotal int
	Targets     []*AlertView
	TargetTotal int
}

// previewInhibitID 预览时这条尚未保存的抑制规则的占位 ID（只在一次预览内部使用）。
const previewInhibitID = "preview"

// InhibitUsecase 抑制规则用例。范围规则与屏蔽规则完全相同（muteVisible / muteManageable）：查看要求对
// 作用分组只读以上，变更要求可管理；作用于全部分组的抑制规则有任一分组范围即可见、仅超级管理员可维护。
type InhibitUsecase struct {
	scopes *ScopeUsecase
	reader InhibitReader
	writer InhibitWriter
	stats  InhibitStats
	alerts AlertReader
	rules  RuleReader
	mutes  MuteReader
	names  names
	// decorator 补齐预览里事件视图的第二、三期展示名；可为 nil。
	decorator AlertViewDecorator
	now       func() time.Time
}

// NewInhibitUsecase new an InhibitUsecase（预览的事件视图不带第二、三期的展示名）。
func NewInhibitUsecase(scopes *ScopeUsecase, reader InhibitReader, writer InhibitWriter, stats InhibitStats, alerts AlertReader,
	rules RuleReader, ruleLookup RuleLookup, mutes MuteReader, dir Directory,
) *InhibitUsecase {
	return &InhibitUsecase{
		scopes: scopes, reader: reader, writer: writer, stats: stats, alerts: alerts, rules: rules, mutes: mutes,
		names: names{dir: dir, rules: ruleLookup}, now: time.Now,
	}
}

// ProvideInhibitUsecase 装配用：在 NewInhibitUsecase 之上接入事件视图的装饰器。
func ProvideInhibitUsecase(scopes *ScopeUsecase, reader InhibitReader, writer InhibitWriter, stats InhibitStats, alerts AlertReader,
	rules RuleReader, ruleLookup RuleLookup, mutes MuteReader, dir Directory, decorator AlertViewDecorator,
) *InhibitUsecase {
	uc := NewInhibitUsecase(scopes, reader, writer, stats, alerts, rules, ruleLookup, mutes, dir)
	uc.decorator = decorator
	return uc
}

// InhibitListQuery 抑制规则列表查询入参。
type InhibitListQuery struct {
	GroupID string
	Keyword string
	Enabled *bool
}

// List 分页查询抑制规则。
func (uc *InhibitUsecase) List(ctx context.Context, userID string, q InhibitListQuery, page, pageSize int64) ([]*InhibitView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	if !scope.HasAny() {
		return []*InhibitView{}, 0, nil
	}
	filter := InhibitFilter{Groups: scope.Filter(""), IncludeGlobal: true, Keyword: strings.TrimSpace(q.Keyword), Enabled: q.Enabled}
	if q.GroupID != "" {
		filter.Groups = relatedGroups(scope, q.GroupID)
	}
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, scope, items)
	return views, total, err
}

// relatedGroups 与某分组相关的降噪规则所在的分组：它自己、它的上级与它的下级中可读的部分
// （作用于其中任何一个的规则都可能命中该分组的事件）。口径同屏蔽规则列表。
func relatedGroups(scope Scope, groupID string) GroupFilter {
	related := slices.Concat(scope.Ancestors(groupID), scope.Descendants(groupID))
	ids := make([]string, 0, len(related))
	for _, id := range uniq(related) {
		if scope.CanRead(id) {
			ids = append(ids, id)
		}
	}
	return GroupFilter{IDs: ids}
}

// Get 查询抑制规则详情。
func (uc *InhibitUsecase) Get(ctx context.Context, userID, id string) (*InhibitView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	in, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !muteVisible(scope, in.Spec.GroupID) {
		return nil, ErrForbidden
	}
	return uc.view(ctx, scope, in)
}

func (uc *InhibitUsecase) view(ctx context.Context, scope Scope, in *Inhibit) (*InhibitView, error) {
	views, err := uc.views(ctx, scope, []*Inhibit{in})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// views 批量补齐展示信息：名称各查一次，命中数一条分组统计。
func (uc *InhibitUsecase) views(ctx context.Context, scope Scope, items []*Inhibit) ([]*InhibitView, error) {
	set := &refSet{}
	ids := make([]string, 0, len(items))
	for _, in := range items {
		ids = append(ids, in.ID)
		set.users = append(set.users, in.CreateUser, in.UpdateUser)
		set.rules = append(append(set.rules, in.Spec.Source.RuleIDs...), in.Spec.Target.RuleIDs...)
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	counts := map[string]int32{}
	if len(ids) > 0 {
		var err error
		if counts, err = uc.stats.InhibitedCounts(ctx, ids); err != nil {
			return nil, err
		}
	}
	out := make([]*InhibitView, 0, len(items))
	for _, in := range items {
		out = append(out, &InhibitView{
			Inhibit: in, GroupName: scope.GroupName(in.Spec.GroupID), InhibitedCount: counts[in.ID],
			SourceRules: refs(in.Spec.Source.RuleIDs, set.ruleNames), TargetRules: refs(in.Spec.Target.RuleIDs, set.ruleNames),
			CanManage: muteManageable(scope, in.Spec.GroupID), CreateUserName: set.userNames[in.CreateUser], UpdateUserName: set.userNames[in.UpdateUser],
		})
	}
	return out, nil
}

func inhibitInvalid(format string, args ...any) error {
	return detail(ErrInhibitInvalid, format, args...)
}

// validateInhibitShape 不依赖外部数据的校验：名称、说明、两侧条件与相等标签（monitorcontract.ValidateInhibit，
// 与评估引擎加载规则时同一个函数——能保存的规则引擎一定会执行）。
func validateInhibitShape(spec *InhibitSpec) error {
	spec.Name, spec.Description = strings.TrimSpace(spec.Name), strings.TrimSpace(spec.Description)
	if spec.Name == "" || len([]rune(spec.Name)) > 128 {
		return inhibitInvalid("name must be 1-128 characters")
	}
	if len([]rune(spec.Description)) > 255 {
		return inhibitInvalid("description must be at most 255 characters")
	}
	if hasDuplicate(spec.Source.RuleIDs) || hasDuplicate(spec.Target.RuleIDs) {
		return inhibitInvalid("rules must not repeat")
	}
	if err := mc.ValidateInhibit(spec.Source, spec.Target, spec.EqualLabels); err != nil {
		return inhibitInvalid("%v", err)
	}
	return nil
}

// errInhibitRules 限定的规则不可用。不存在与不在作用分组内是同一句话（理由同 errMuteRules）。
var errInhibitRules = inhibitInvalid("some of the selected rules do not exist in the selected group")

// validate 完整校验：范围（作用分组可管理；全局仅超级管理员）+ 形状 + 两侧限定的规则都存在且位于
// 作用分组（含下级）内——范围外的规则产生的告警本来就不在这条抑制规则的作用范围里。
func (uc *InhibitUsecase) validate(ctx context.Context, scope Scope, spec *InhibitSpec) error {
	if !muteManageable(scope, spec.GroupID) {
		return ErrForbidden
	}
	if spec.GroupID != "" && !scope.GroupExists(spec.GroupID) {
		return ErrGroupNotFound
	}
	if err := validateInhibitShape(spec); err != nil {
		return err
	}
	ruleIDs := uniq(slices.Concat(spec.Source.RuleIDs, spec.Target.RuleIDs))
	if len(ruleIDs) == 0 {
		return nil
	}
	rules, err := uc.rules.ListByIDs(ctx, ruleIDs)
	if err != nil {
		return err
	}
	// 全局抑制规则（仅超级管理员）可以限定任何分组的规则。
	within := scope.ReadableGroupIDs()
	if spec.GroupID != "" {
		within = scope.Descendants(spec.GroupID)
	}
	if !rulesWithin(rules, ruleIDs, within) {
		return errInhibitRules
	}
	return nil
}

// Create 新建抑制规则。
func (uc *InhibitUsecase) Create(ctx context.Context, userID string, spec InhibitSpec) (*InhibitView, error) {
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
	if err := uc.writer.Create(ctx, &Inhibit{ID: id, Spec: spec}, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Update 编辑抑制规则（整体替换）：要求对原作用分组与新作用分组都可管理。
func (uc *InhibitUsecase) Update(ctx context.Context, userID, id string, spec InhibitSpec, version int32) (*InhibitView, error) {
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
	if err := uc.writer.Update(ctx, &Inhibit{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, scope, id)
}

// Delete 删除抑制规则；正被它抑制的告警由评估引擎在下一轮解除抑制。
func (uc *InhibitUsecase) Delete(ctx context.Context, userID, id string) error {
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

func (uc *InhibitUsecase) reload(ctx context.Context, scope Scope, id string) (*InhibitView, error) {
	in, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, scope, in)
}

// Preview 保存前预览（不落库）：在调用方可读的分组内扫描当前活跃告警（至多 previewScanLimit 条），给出
// 满足源条件的告警与会被抑制的告警。
//
// 判定用的就是评估引擎的那一份实现：把这一条规则交给 monitorpipeline.CompileInhibits，用「活跃且未被抑制」
// 的告警建快照（被抑制的告警不能再抑制别人），再逐个 Match——相等标签、同时满足两侧条件的告警互不抑制、
// 告警不抑制自己，都与实际行为一致。目标一侧包含此刻已被抑制的告警：编辑一条正在生效的规则时，它已经
// 抑制住的告警仍应出现在预览里。源列表按 P1 的 criteria 过滤（同一套 monitorcontract 匹配器）。
func (uc *InhibitUsecase) Preview(ctx context.Context, userID string, spec InhibitSpec) (*InhibitPreview, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !muteVisible(scope, spec.GroupID) {
		return nil, ErrForbidden
	}
	if err := validateInhibitShape(&spec); err != nil {
		return nil, err
	}
	out := &InhibitPreview{Sources: []*AlertView{}, Targets: []*AlertView{}}
	filter := AlertFilter{Groups: scope.Filter(spec.GroupID), Statuses: []int32{mc.AlertFiring}}
	if filter.Groups.Empty() {
		return out, nil
	}
	scanned, err := uc.alerts.Scan(ctx, filter, previewScanLimit)
	if err != nil {
		return nil, err
	}
	matchers, err := mc.CompileMatchers(spec.Source.Matchers)
	if err != nil {
		return nil, inhibitInvalid("source: %v", err)
	}
	source := criteria{ruleIDs: spec.Source.RuleIDs, severities: spec.Source.Severities, matchers: matchers}
	groupPath := ""
	if g, ok := scope.Group(spec.GroupID); ok {
		groupPath = g.Path
	}
	inhibitor := monitorpipeline.CompileInhibits([]monitorpipeline.InhibitRule{{
		ID: previewInhibitID, GroupPath: groupPath, Source: spec.Source, Target: spec.Target, Equal: spec.EqualLabels,
	}})
	all := make([]monitorpipeline.AlertRef, 0, len(scanned))
	active := make([]monitorpipeline.AlertRef, 0, len(scanned))
	var sources, targets []*Alert
	for _, a := range scanned {
		g, _ := scope.Group(a.GroupID)
		ref := monitorpipeline.AlertRef{ID: a.ID, RuleID: a.RuleID, GroupPath: g.Path, Severity: a.Severity, Labels: a.Labels, CIID: a.CIID}
		all = append(all, ref)
		if a.Inhibited {
			continue
		}
		active = append(active, ref)
		if source.match(a) {
			sources = append(sources, a)
		}
	}
	snap := inhibitor.Snapshot(active)
	for i, a := range scanned {
		if _, hit := snap.Match(&all[i]); hit {
			targets = append(targets, a)
		}
	}
	out.SourceTotal, out.TargetTotal = len(sources), len(targets)
	shown := slices.Concat(sources[:min(len(sources), previewItems)], targets[:min(len(targets), previewItems)])
	views, err := alertViews(ctx, scope, uc.names.rules, uc.mutes, uc.now(), shown)
	if err != nil {
		return nil, err
	}
	for _, v := range views {
		v.CanAck, v.CanManage = scope.CanRead(v.GroupID), scope.CanManage(v.GroupID)
	}
	if uc.decorator != nil {
		if err := uc.decorator.Decorate(ctx, views); err != nil {
			return nil, err
		}
	}
	n := min(len(sources), previewItems)
	out.Sources, out.Targets = views[:n], views[n:]
	return out, nil
}
