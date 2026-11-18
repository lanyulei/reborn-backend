package biz

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 订阅列表范围。
const (
	SubscriptionScopeMine = "mine"
	SubscriptionScopeAll  = "all"
)

// subscriptionPreviewWindow 订阅预览回看的历史时长。
const subscriptionPreviewWindow = 24 * time.Hour

// subscriptionHitWindow 命中次数的统计时长。
const subscriptionHitWindow = 7 * 24 * time.Hour

// SubscriptionUsecase 告警订阅规则用例。只能订阅自己可读分组内的告警；接收方超出「本人」
// （他人、值班表、群）时要求对所选分组全部可管理。
type SubscriptionUsecase struct {
	scopes *ScopeUsecase
	reader SubscriptionReader
	writer SubscriptionWriter
	logs   NotifyLogReader
	alerts AlertReader
	mutes  MuteReader
	rules  RuleReader
	names  names
	now    func() time.Time
}

// NewSubscriptionUsecase new a SubscriptionUsecase.
func NewSubscriptionUsecase(scopes *ScopeUsecase, reader SubscriptionReader, writer SubscriptionWriter, logs NotifyLogReader,
	alerts AlertReader, mutes MuteReader, rules RuleReader, ruleLookup RuleLookup, datasources DatasourceLookup, dir Directory, schedules Schedules,
) *SubscriptionUsecase {
	return &SubscriptionUsecase{
		scopes: scopes, reader: reader, writer: writer, logs: logs, alerts: alerts, mutes: mutes, rules: rules,
		names: names{dir: dir, schedules: schedules, datasources: datasources, rules: ruleLookup}, now: time.Now,
	}
}

// SubscriptionListQuery 订阅列表查询入参。
type SubscriptionListQuery struct {
	Scope   string
	Keyword string
	Enabled *bool
	GroupID string
}

// List 分页查询订阅规则：mine = 我是所有者；all = 订阅的分组在我的可读范围内（超级管理员全部）。
func (uc *SubscriptionUsecase) List(ctx context.Context, userID string, q SubscriptionListQuery, page, pageSize int64) ([]*SubscriptionView, int64, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	filter := SubscriptionFilter{Keyword: strings.TrimSpace(q.Keyword), Enabled: q.Enabled}
	if q.Scope == SubscriptionScopeMine {
		filter.OwnerID = userID
	} else {
		filter.ViewerID, filter.Groups = userID, scope.Filter("")
	}
	if q.GroupID != "" {
		// 订阅了该分组或其上级（上级的订阅覆盖到该分组）。
		if filter.CoverGroupIDs = scope.Ancestors(q.GroupID); len(filter.CoverGroupIDs) == 0 {
			return []*SubscriptionView{}, 0, nil
		}
	}
	subs, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.views(ctx, userID, scope, subs)
	return views, total, err
}

// subscriptionVisible 订阅是否对查看者可见：所有者本人，或订阅的任一分组在其可读范围内。
func subscriptionVisible(scope Scope, userID string, s *Subscription) bool {
	return s.OwnerID == userID || slices.ContainsFunc(s.Spec.GroupIDs, scope.CanRead)
}

// subscriptionManageable 订阅能否被编辑、删除：所有者本人，或对其全部订阅分组可管理。
func subscriptionManageable(scope Scope, userID string, s *Subscription) bool {
	if s.OwnerID == userID || scope.IsAdmin() {
		return true
	}
	return !slices.ContainsFunc(s.Spec.GroupIDs, func(id string) bool { return !scope.CanManage(id) })
}

// Get 查询订阅规则详情。
func (uc *SubscriptionUsecase) Get(ctx context.Context, userID, id string) (*SubscriptionView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	s, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !subscriptionVisible(scope, userID, s) {
		return nil, ErrForbidden
	}
	return uc.view(ctx, userID, scope, s)
}

func (uc *SubscriptionUsecase) view(ctx context.Context, userID string, scope Scope, s *Subscription) (*SubscriptionView, error) {
	views, err := uc.views(ctx, userID, scope, []*Subscription{s})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

// views 批量补齐展示信息、命中统计，以及所有者当前已无权访问的订阅分组。
func (uc *SubscriptionUsecase) views(ctx context.Context, userID string, scope Scope, subs []*Subscription) ([]*SubscriptionView, error) {
	set := &refSet{}
	ids := make([]string, 0, len(subs))
	var owners []string
	for _, s := range subs {
		ids = append(ids, s.ID)
		owners = append(owners, s.OwnerID)
		set.users = append(append(set.users, s.OwnerID), s.Spec.Receivers.UserIDs...)
		set.schedules = append(set.schedules, s.Spec.Receivers.ScheduleIDs...)
		set.rules = append(set.rules, s.Spec.RuleIDs...)
		set.datasources = append(set.datasources, s.Spec.DatasourceIDs...)
	}
	if err := uc.names.resolve(ctx, set); err != nil {
		return nil, err
	}
	hits := map[string]SubscriptionHit{}
	ownerScopes := map[string]Scope{}
	if len(ids) > 0 {
		var err error
		if hits, err = uc.logs.SubscriptionHits(ctx, ids, uc.now().Add(-subscriptionHitWindow)); err != nil {
			return nil, err
		}
		// 所有者各自的范围：分组树与授权只读一次，角色按所有者读取（列表页所有者通常很少）。
		if ownerScopes, err = uc.scopes.ResolveMany(ctx, uniq(owners)); err != nil {
			return nil, err
		}
	}
	out := make([]*SubscriptionView, 0, len(subs))
	for _, s := range subs {
		v := &SubscriptionView{
			Subscription: s, OwnerName: set.userNames[s.OwnerID], CanManage: subscriptionManageable(scope, userID, s),
			Rules: refs(s.Spec.RuleIDs, set.ruleNames), Datasources: set.datasourceRefs(s.Spec.DatasourceIDs),
			ReceiverUsers: refs(s.Spec.Receivers.UserIDs, set.userNames), ReceiverSchedules: refs(s.Spec.Receivers.ScheduleIDs, set.scheduleNames),
			Groups: make([]NamedRef, 0, len(s.Spec.GroupIDs)), OutOfScopeGroups: []string{},
		}
		owner := ownerScopes[s.OwnerID]
		for _, g := range s.Spec.GroupIDs {
			v.Groups = append(v.Groups, NamedRef{ID: g, Name: scope.GroupName(g), Missing: !scope.GroupExists(g)})
			if !owner.CanRead(g) || !scope.GroupExists(g) {
				v.OutOfScopeGroups = append(v.OutOfScopeGroups, g)
			}
		}
		if h, ok := hits[s.ID]; ok {
			if !h.LastTime.IsZero() {
				t := h.LastTime
				v.LastHitTime = &t
			}
			v.HitCount7d = h.Count
		}
		out = append(out, v)
	}
	return out, nil
}

func subscriptionInvalid(format string, args ...any) error {
	return detail(ErrSubscriptionInvalid, format, args...)
}

// validateSubscriptionShape 不依赖外部数据的校验，并返回编译后的筛选条件。
func validateSubscriptionShape(spec *SubscriptionSpec) (criteria, error) {
	if spec.Name = strings.TrimSpace(spec.Name); spec.Name == "" || len([]rune(spec.Name)) > 128 {
		return criteria{}, subscriptionInvalid("name must be 1-128 characters")
	}
	if n := len(spec.GroupIDs); n < 1 || n > 50 || hasDuplicate(spec.GroupIDs) {
		return criteria{}, subscriptionInvalid("select 1-50 distinct groups")
	}
	if len(spec.RuleIDs) > 200 || len(spec.DatasourceIDs) > 50 || hasDuplicate(spec.RuleIDs) || hasDuplicate(spec.DatasourceIDs) {
		return criteria{}, subscriptionInvalid("too many or duplicated rules / datasources")
	}
	if len(spec.Severities) == 0 || hasDuplicate(spec.Severities) {
		return criteria{}, subscriptionInvalid("select at least one severity")
	}
	for _, s := range spec.Severities {
		if !mc.ValidSeverity(s) {
			return criteria{}, subscriptionInvalid("severity must be 1-4")
		}
	}
	if !spec.OnFiring && !spec.OnResolved {
		return criteria{}, subscriptionInvalid("subscribe to firing, resolved or both")
	}
	matchers, err := mc.CompileMatchers(spec.Matchers)
	if err != nil {
		return criteria{}, subscriptionInvalid("matchers: %v", err)
	}
	r := &spec.Receivers
	if !r.Self && len(r.UserIDs) == 0 && len(r.ScheduleIDs) == 0 && len(r.Groups) == 0 {
		return criteria{}, subscriptionInvalid("at least one receiver is required")
	}
	if len(r.UserIDs) > 50 || len(r.ScheduleIDs) > 20 || len(r.Groups) > 10 ||
		hasDuplicate(r.UserIDs) || hasDuplicate(r.ScheduleIDs) || hasDuplicate(r.Groups) {
		return criteria{}, subscriptionInvalid("too many or duplicated receivers")
	}
	for _, g := range r.Groups {
		if !isGroupRecipient(g) || len(g) > 64 {
			return criteria{}, subscriptionInvalid("group receiver %q must start with $", g)
		}
	}
	return criteria{ruleIDs: spec.RuleIDs, datasourceIDs: spec.DatasourceIDs, severities: spec.Severities, matchers: matchers}, nil
}

// beyondSelf 接收方是否超出所有者本人（他人、值班表、群）。
func beyondSelf(r mc.Receivers, ownerID string) bool {
	if len(r.ScheduleIDs) > 0 || len(r.Groups) > 0 {
		return true
	}
	return slices.ContainsFunc(r.UserIDs, func(id string) bool { return id != ownerID })
}

// authorize 订阅内容的范围判定：所选分组须存在且操作者可读；接收方超出所有者本人、或操作者不是
// 所有者（代他人维护）时，要求对所选分组全部可管理。
func authorizeSubscription(scope Scope, actorID, ownerID string, spec *SubscriptionSpec) error {
	needManage := actorID != ownerID || beyondSelf(spec.Receivers, ownerID)
	for _, g := range spec.GroupIDs {
		if err := scope.requireRead(g); err != nil {
			return err
		}
		if !scope.GroupExists(g) {
			return ErrGroupNotFound
		}
		if needManage {
			if err := scope.requireManage(g); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateRefs 引用的规则、数据源、接收用户与值班表须存在：接收用户须是可用用户（kept 里原本就在
// 订阅里的除外——接收人离职不该让订阅从此无法保存，派发时本来就会跳过非可用用户）；限定的规则须在
// 所选分组（含下级）之内，「不存在」与「不在所选分组内」是同一句话（见 errMuteRules 的说明）。
// 所选分组已由 authorizeSubscription 确认可读，其下级随之可读。
func (uc *SubscriptionUsecase) validateRefs(ctx context.Context, scope Scope, spec *SubscriptionSpec, kept []string) error {
	set := &refSet{schedules: spec.Receivers.ScheduleIDs, datasources: spec.DatasourceIDs}
	if err := uc.names.resolve(ctx, set); err != nil {
		return err
	}
	if err := requireActiveUsers(ctx, uc.names.dir, spec.Receivers.UserIDs, kept); err != nil {
		if errors.Is(err, errInactiveUser) {
			return subscriptionInvalid("some of the receiver users do not exist or are not active")
		}
		return err
	}
	switch {
	case len(set.scheduleNames) != len(spec.Receivers.ScheduleIDs):
		return subscriptionInvalid("some of the receiver schedules do not exist or are disabled")
	case len(set.datasourceBriefs) != len(spec.DatasourceIDs):
		return subscriptionInvalid("some of the selected datasources do not exist")
	}
	if len(spec.RuleIDs) == 0 {
		return nil
	}
	rules, err := uc.rules.ListByIDs(ctx, spec.RuleIDs)
	if err != nil {
		return err
	}
	var within []string
	for _, g := range spec.GroupIDs {
		within = append(within, scope.Descendants(g)...)
	}
	if !rulesWithin(rules, spec.RuleIDs, within) {
		return subscriptionInvalid("some of the selected rules do not exist in the selected groups")
	}
	return nil
}

func normalizeSubscription(spec *SubscriptionSpec) {
	for _, p := range []*[]string{&spec.RuleIDs, &spec.DatasourceIDs, &spec.Receivers.UserIDs, &spec.Receivers.ScheduleIDs, &spec.Receivers.Groups} {
		if *p == nil {
			*p = []string{}
		}
	}
	if spec.Matchers == nil {
		spec.Matchers = []mc.Matcher{}
	}
}

// Create 新建订阅规则；当前用户成为所有者。
func (uc *SubscriptionUsecase) Create(ctx context.Context, userID string, spec SubscriptionSpec) (*SubscriptionView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if err := uc.check(ctx, scope, userID, userID, &spec, nil); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	s := &Subscription{ID: id, Spec: spec, OwnerID: userID}
	if err := uc.writer.Create(ctx, s); err != nil {
		return nil, err
	}
	return uc.reload(ctx, userID, scope, id)
}

func (uc *SubscriptionUsecase) check(ctx context.Context, scope Scope, actorID, ownerID string, spec *SubscriptionSpec, keptUsers []string) error {
	if _, err := validateSubscriptionShape(spec); err != nil {
		return err
	}
	if err := authorizeSubscription(scope, actorID, ownerID, spec); err != nil {
		return err
	}
	normalizeSubscription(spec)
	return uc.validateRefs(ctx, scope, spec, keptUsers)
}

// Update 编辑订阅规则（整体替换）：所有者本人，或对其全部订阅分组可管理的用户。
func (uc *SubscriptionUsecase) Update(ctx context.Context, userID, id string, spec SubscriptionSpec, version int32) (*SubscriptionView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !subscriptionManageable(scope, userID, cur) {
		return nil, ErrForbidden
	}
	if err := uc.check(ctx, scope, userID, cur.OwnerID, &spec, cur.Spec.Receivers.UserIDs); err != nil {
		return nil, err
	}
	if err := uc.writer.Update(ctx, &Subscription{ID: id, Spec: spec}, version, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, userID, scope, id)
}

// Delete 删除订阅规则。
func (uc *SubscriptionUsecase) Delete(ctx context.Context, userID, id string) error {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return err
	}
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return err
	}
	if !subscriptionManageable(scope, userID, cur) {
		return ErrForbidden
	}
	return uc.writer.Delete(ctx, id, cur.Version, userID)
}

func (uc *SubscriptionUsecase) reload(ctx context.Context, userID string, scope Scope, id string) (*SubscriptionView, error) {
	s, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return uc.view(ctx, userID, scope, s)
}

// Preview 保存前预览：当前活跃告警与最近 24 小时历史告警中会被命中的事件（不落库）。只在调用方
// 可读的所选分组（含下级）内查找；被屏蔽的事件不会发给订阅者，不计入。
func (uc *SubscriptionUsecase) Preview(ctx context.Context, userID string, spec SubscriptionSpec) ([]*AlertView, int, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	c, err := validateSubscriptionShape(&spec)
	if err != nil {
		return nil, 0, err
	}
	var groupIDs []string
	for _, g := range spec.GroupIDs {
		if err := scope.requireRead(g); err != nil {
			return nil, 0, err
		}
		groupIDs = append(groupIDs, scope.Filter(g).IDs...)
	}
	now := uc.now()
	since := now.Add(-subscriptionPreviewWindow)
	notMuted := false
	filter := AlertFilter{
		Groups: GroupFilter{IDs: uniq(groupIDs)}, RuleIDs: spec.RuleIDs, DatasourceIDs: spec.DatasourceIDs,
		Severities: spec.Severities, HistorySince: &since, Muted: &notMuted,
	}
	// 订了触发：窗口内触发过的事件都算（不论之后是恢复还是被关闭）；只订恢复：只算已恢复的
	// （被关闭的事件不发恢复通知）。
	if !spec.OnFiring {
		filter.Statuses = []int32{mc.AlertResolved}
	}
	return previewAlerts(ctx, scope, uc.alerts, uc.names.rules, uc.mutes, now, filter, c)
}
