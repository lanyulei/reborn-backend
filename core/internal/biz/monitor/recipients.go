package biz

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 通知对象解析与订阅匹配：通知派发（DispatchUsecase）与模拟触发共用同一份实现——「模拟出来会通知谁」
// 就是实际派发时通知谁。

// 解析通知对象时的说明种类（ResolveNote.Kind）。
const (
	// NoteScheduleEmpty 值班表此刻无人值班（或已停用 / 不存在）。
	NoteScheduleEmpty = "schedule_empty"
	// NoteDeptNoLeader 部门（及其上级）没有可用的负责人。
	NoteDeptNoLeader = "dept_no_leader"
	// NoteCIOwnerNotUser 关联资源的负责人不是可用的系统用户。
	NoteCIOwnerNotUser = "ci_owner_not_user"
	// NoteCINotLinked 规则要求通知资源负责人，但事件没有关联到资源。
	NoteCINotLinked = "ci_not_linked"
	// NoteOncallGroupNoAdmin 值班分组没有可用的管理员（或分组不存在）。
	NoteOncallGroupNoAdmin = "oncall_group_no_admin"
	// NoteUsersInactive 指定的用户里有已停用 / 不存在的，已略过。
	NoteUsersInactive = "users_inactive"
)

// ResolveNote 解析通知对象时一条需要让人知道的情况；ID 为对应对象（值班表 / 部门 / 资源 / 值班分组）。
type ResolveNote struct {
	Kind string
	ID   string
}

// RecipientResolver 把通知对象解析为可用用户。roster / groups 只有升级策略用得到，可以为 nil
// （此时升级策略里的「仅备班」与值班分组解析不出任何人）。
type RecipientResolver struct {
	dir     Directory
	leaders DeptLeaders
	duty    Duty
	roster  DutyRoster
	groups  OncallGroups
}

// NewRecipientResolver new a RecipientResolver.
func NewRecipientResolver(dir Directory, leaders DeptLeaders, duty Duty, roster DutyRoster, groups OncallGroups) *RecipientResolver {
	return &RecipientResolver{dir: dir, leaders: leaders, duty: duty, roster: roster, groups: groups}
}

// ResolveSession 一次派发（或一次模拟）内的解析会话：同一张值班表只查一次。非并发安全。
type ResolveSession struct {
	r      *RecipientResolver
	now    time.Time
	onDuty map[string][]string
	byRole map[string][2][]string
}

// Session 开始一次解析会话；now 为取值班人的时刻。
func (r *RecipientResolver) Session(now time.Time) *ResolveSession {
	return &ResolveSession{r: r, now: now, onDuty: map[string][]string{}, byRole: map[string][2][]string{}}
}

// usersOnDuty 各值班表当前值班人（主值班在前、备值班在后）；无人值班的记一条说明。
func (s *ResolveSession) usersOnDuty(ctx context.Context, scheduleIDs []string, notes *[]ResolveNote) ([]string, error) {
	var out []string
	for _, id := range scheduleIDs {
		users, ok := s.onDuty[id]
		if !ok {
			var err error
			if users, err = s.r.duty.OnDuty(ctx, id, s.now); err != nil {
				return nil, fmt.Errorf("resolve on-duty of schedule %s: %w", id, err)
			}
			s.onDuty[id] = users
		}
		if len(users) == 0 && notes != nil {
			*notes = append(*notes, ResolveNote{Kind: NoteScheduleEmpty, ID: id})
		}
		out = append(out, users...)
	}
	return out, nil
}

// deptLeaders 部门向上第 up 级（0 = 本部门）的负责人。目录在该级没有负责人时会继续向上找；该级不存在
// （上级链不够长）时退回到能找到的最高一级。
func (s *ResolveSession) deptLeaders(ctx context.Context, deptID string, up int32) ([]string, error) {
	for level := int(up) + 1; level >= 1; level-- {
		leaders, err := s.r.leaders.DeptLeaderIDs(ctx, deptID, level, "")
		if err != nil {
			return nil, fmt.Errorf("resolve leaders of dept %s: %w", deptID, err)
		}
		if len(leaders) > 0 {
			return leaders, nil
		}
	}
	return nil, nil
}

// active 去重并只保留可用用户；有被略过的记一条说明。
func (s *ResolveSession) active(ctx context.Context, users []string, notes *[]ResolveNote) ([]string, error) {
	want := uniq(users)
	if len(want) == 0 {
		return nil, nil
	}
	active, err := s.r.dir.ActiveUserIDs(ctx, want)
	if err != nil {
		return nil, err
	}
	if len(active) < len(want) && notes != nil {
		*notes = append(*notes, ResolveNote{Kind: NoteUsersInactive})
	}
	return active, nil
}

// Rule 解析规则的通知对象：指定用户、值班表当前值班人、部门负责人，以及（规则勾选了「资源负责人」时）
// 事件关联资源的负责人。ci 为事件关联的资源，未关联传 nil。返回去重后的可用用户。
func (s *ResolveSession) Rule(ctx context.Context, t mc.NotifyTargets, ci *CIEntry) ([]string, []ResolveNote, error) {
	var notes []ResolveNote
	users := slices.Clone(t.UserIDs)
	duty, err := s.usersOnDuty(ctx, t.ScheduleIDs, &notes)
	if err != nil {
		return nil, nil, err
	}
	users = append(users, duty...)
	for _, dept := range t.DeptIDs {
		leaders, err := s.deptLeaders(ctx, dept, 0)
		if err != nil {
			return nil, nil, err
		}
		if len(leaders) == 0 {
			notes = append(notes, ResolveNote{Kind: NoteDeptNoLeader, ID: dept})
		}
		users = append(users, leaders...)
	}
	owner := ""
	if t.CIOwner {
		switch {
		case ci == nil:
			notes = append(notes, ResolveNote{Kind: NoteCINotLinked})
		case !idgen.IsUUIDFold(ci.Owner):
			notes = append(notes, ResolveNote{Kind: NoteCIOwnerNotUser, ID: ci.ID})
		default:
			owner = strings.ToLower(ci.Owner)
			users = append(users, owner)
		}
	}
	active, err := s.active(ctx, users, &notes)
	if err != nil {
		return nil, nil, err
	}
	if owner != "" && !slices.Contains(active, owner) {
		notes = append(notes, ResolveNote{Kind: NoteCIOwnerNotUser, ID: ci.ID})
	}
	return active, notes, nil
}

// Subscription 解析订阅的用户接收方：指定用户、所有者本人（Self）、值班表当前值班人。通知群另算
// （Receivers.Groups），不在返回值里。
func (s *ResolveSession) Subscription(ctx context.Context, sub *Subscription) ([]string, error) {
	r := sub.Spec.Receivers
	users := slices.Clone(r.UserIDs)
	if r.Self {
		users = append(users, sub.OwnerID)
	}
	duty, err := s.usersOnDuty(ctx, r.ScheduleIDs, nil)
	if err != nil {
		return nil, err
	}
	return s.active(ctx, append(users, duty...), nil)
}

// rosterOf 值班表此刻的主 / 备值班人（一次会话内只查一次）。
func (s *ResolveSession) rosterOf(ctx context.Context, scheduleID string) (primary, backup []string, err error) {
	if cached, ok := s.byRole[scheduleID]; ok {
		return cached[0], cached[1], nil
	}
	if s.r.roster != nil {
		if primary, backup, err = s.r.roster.OnDutyByRole(ctx, scheduleID); err != nil {
			return nil, nil, fmt.Errorf("resolve on-duty of schedule %s: %w", scheduleID, err)
		}
	}
	s.byRole[scheduleID] = [2][]string{primary, backup}
	return primary, backup, nil
}

// Escalation 解析升级策略某一级的通知对象：指定用户、值班表（可仅备班）、值班分组管理员、部门负责人
// （可上溯若干级）。返回去重后的可用用户。
func (s *ResolveSession) Escalation(ctx context.Context, t mc.EscalationTargets) ([]string, []ResolveNote, error) {
	var notes []ResolveNote
	users := slices.Clone(t.UserIDs)
	for _, sch := range t.Schedules {
		primary, backup, err := s.rosterOf(ctx, sch.ID)
		if err != nil {
			return nil, nil, err
		}
		picked := backup
		if !sch.BackupOnly {
			picked = slices.Concat(primary, backup)
		}
		if len(picked) == 0 {
			notes = append(notes, ResolveNote{Kind: NoteScheduleEmpty, ID: sch.ID})
		}
		users = append(users, picked...)
	}
	for _, groupID := range t.OncallGroupIDs {
		var admins []string
		if s.r.groups != nil {
			var err error
			if admins, err = s.r.groups.AdminIDs(ctx, groupID); err != nil {
				return nil, nil, fmt.Errorf("resolve admins of oncall group %s: %w", groupID, err)
			}
		}
		if len(admins) == 0 {
			notes = append(notes, ResolveNote{Kind: NoteOncallGroupNoAdmin, ID: groupID})
		}
		users = append(users, admins...)
	}
	for _, dept := range t.Depts {
		leaders, err := s.deptLeaders(ctx, dept.ID, dept.Up)
		if err != nil {
			return nil, nil, err
		}
		if len(leaders) == 0 {
			notes = append(notes, ResolveNote{Kind: NoteDeptNoLeader, ID: dept.ID})
		}
		users = append(users, leaders...)
	}
	active, err := s.active(ctx, users, &notes)
	return active, notes, err
}

// maxCompiledSubscriptions 进程内缓存的已编译订阅匹配器个数上限，超出整体清空重建。
const maxCompiledSubscriptions = 4096

// maxSubscriptionCandidates 一次匹配至多考察的订阅数（按订阅 ID 排序取前这么多条）；命中更多时
// 其余订阅收不到这次通知，匹配时记一条带总数的告警日志。
const maxSubscriptionCandidates = 500

type compiledSubscription struct {
	version  int32
	matchers *mc.Matchers
}

// SubscriptionMatcher 找出一个事件命中的订阅：SQL 预筛（启用、事件类型、分组、规则、数据源、级别），
// 再做标签匹配与所有者数据范围判定。进程内只有「已编译匹配器」缓存，按 (订阅 ID, 版本) 取用，不承载
// 正确性；可并发使用。
type SubscriptionMatcher struct {
	subs   SubscriptionReader
	scopes *ScopeUsecase
	log    *slog.Logger

	// mu 保护 compiled：订阅 ID → 已编译的匹配器及其版本。
	mu       sync.Mutex
	compiled map[string]compiledSubscription
}

// NewSubscriptionMatcher new a SubscriptionMatcher.
func NewSubscriptionMatcher(subs SubscriptionReader, scopes *ScopeUsecase, logger *slog.Logger) *SubscriptionMatcher {
	return &SubscriptionMatcher{subs: subs, scopes: scopes, log: logger, compiled: map[string]compiledSubscription{}}
}

// matchState 一批匹配内共用的只读数据：分组树，以及已解析过的订阅所有者的数据范围。
type matchState struct {
	tree   *groupTree
	scopes map[string]Scope
}

// load 取分组树（一批只读一次）。
func (rs *matchState) load(ctx context.Context, scopes *ScopeUsecase) error {
	if rs.tree != nil {
		return nil
	}
	tree, err := scopes.loadTree(ctx)
	if err != nil {
		return err
	}
	rs.tree, rs.scopes = tree, map[string]Scope{}
	return nil
}

// resolve 把还没解析过的所有者一次批量解析掉（授权一条语句；角色按人读取，一批内每人至多一次）。
func (rs *matchState) resolve(ctx context.Context, scopes *ScopeUsecase, owners []string) error {
	missing := slices.DeleteFunc(uniq(owners), func(id string) bool { _, ok := rs.scopes[id]; return ok })
	if len(missing) == 0 {
		return nil
	}
	resolved, err := scopes.scopesFor(ctx, rs.tree, missing)
	if err != nil {
		return err
	}
	for id, sc := range resolved {
		rs.scopes[id] = sc
	}
	return nil
}

// SubscriptionEvent 待匹配的事件：所在分组、规则、数据源、级别、标签，以及通知的种类。
type SubscriptionEvent struct {
	AlertID      string
	GroupID      string
	RuleID       string
	DatasourceID string
	Severity     int32
	Labels       map[string]string
	Resolved     bool
	RepeatOnly   bool
}

// match 在给定的批状态上匹配；批状态由调用方持有（派发的一轮、模拟的一次）。
func (m *SubscriptionMatcher) match(ctx context.Context, rs *matchState, ev SubscriptionEvent) ([]*Subscription, error) {
	if err := rs.load(ctx, m.scopes); err != nil {
		return nil, err
	}
	groupIDs := rs.tree.ancestors(ev.GroupID)
	if len(groupIDs) == 0 {
		return nil, nil // 事件所在分组已删除：没有人还对它有范围
	}
	subs, total, err := m.subs.ListCandidates(ctx, SubscriptionCandidate{
		GroupIDs: groupIDs, RuleID: ev.RuleID, DatasourceID: ev.DatasourceID, Severity: ev.Severity,
		Resolved: ev.Resolved, RepeatOnly: ev.RepeatOnly,
	}, maxSubscriptionCandidates)
	if err != nil {
		return nil, err
	}
	if total > len(subs) {
		m.log.WarnContext(ctx, "monitor subscription candidates truncated: the remaining subscriptions are not notified",
			"alert_id", ev.AlertID, "group_id", ev.GroupID, "matched", total, "limit", maxSubscriptionCandidates, "skipped", total-len(subs))
	}
	subs = slices.DeleteFunc(subs, func(s *Subscription) bool { return !m.matchers(s).Match(ev.Labels) })
	if len(subs) == 0 {
		return nil, nil
	}
	owners := make([]string, 0, len(subs))
	for _, s := range subs {
		owners = append(owners, s.OwnerID)
	}
	// 所有者须对事件所在分组有只读以上范围（被移出角色后订阅自动失效，最迟下一批生效）。
	if err := rs.resolve(ctx, m.scopes, owners); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(subs, func(s *Subscription) bool { return !rs.scopes[s.OwnerID].CanRead(ev.GroupID) }), nil
}

// Match 找出事件命中的订阅（按订阅 ID 排序）与事件所在分组的完整名称。一次调用读一次分组树。
func (m *SubscriptionMatcher) Match(ctx context.Context, ev SubscriptionEvent) ([]*Subscription, error) {
	return m.match(ctx, &matchState{}, ev)
}

// matchers 取订阅的已编译匹配器：按 (ID, 版本) 缓存，订阅被编辑后版本变化即重新编译。
// 条件非法（理论上写入时已拦住）返回一个恒不命中的匹配器——宁可不通知，不误通知。
func (m *SubscriptionMatcher) matchers(s *Subscription) *mc.Matchers {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.compiled[s.ID]; ok && c.version == s.Version {
		return c.matchers
	}
	ms, err := mc.CompileMatchers(s.Spec.Matchers)
	if err != nil {
		ms = neverMatch
	}
	if len(m.compiled) >= maxCompiledSubscriptions {
		clear(m.compiled)
	}
	m.compiled[s.ID] = compiledSubscription{version: s.Version, matchers: ms}
	return ms
}

// neverMatch 恒不命中的匹配器（要求一个标签同时等于两个不同的值）。
var neverMatch = func() *mc.Matchers {
	ms, err := mc.CompileMatchers([]mc.Matcher{
		{Label: "__never__", Op: mc.OpEqual, Value: "a"}, {Label: "__never__", Op: mc.OpEqual, Value: "b"},
	})
	if err != nil {
		panic(err) // 字面量常量，初始化期即可发现
	}
	return ms
}()
