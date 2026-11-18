package biz

import (
	"context"
	"slices"
	"strings"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// groupNameSep 分组完整名称的层级分隔符。
const groupNameSep = " / "

// groupTree 规则分组树的只读索引（一次请求内构建一次；分组至多 MaxGroups 个）。
type groupTree struct {
	byID     map[string]*RuleGroup
	children map[string][]string
	ordered  []*RuleGroup
}

func newGroupTree(groups []*RuleGroup) *groupTree {
	t := &groupTree{
		byID:     make(map[string]*RuleGroup, len(groups)),
		children: make(map[string][]string, len(groups)),
		ordered:  groups,
	}
	for _, g := range groups {
		t.byID[g.ID] = g
		t.children[g.ParentID] = append(t.children[g.ParentID], g.ID)
	}
	return t
}

// ancestors 自根到自身的 ID 链（含自身），取自物化路径；分组不存在返回 nil。
func (t *groupTree) ancestors(id string) []string {
	g, ok := t.byID[id]
	if !ok {
		return nil
	}
	return strings.Split(strings.Trim(g.Path, "/"), "/")
}

// fullName 含全部上级的完整名称；分组不存在返回空串。
func (t *groupTree) fullName(id string) string {
	chain := t.ancestors(id)
	names := make([]string, 0, len(chain))
	for _, a := range chain {
		if g, ok := t.byID[a]; ok {
			names = append(names, g.Name)
		}
	}
	return strings.Join(names, groupNameSep)
}

// descendants 分组及其全部下级的 ID；分组不存在返回 nil。
func (t *groupTree) descendants(id string) []string {
	if _, ok := t.byID[id]; !ok {
		return nil
	}
	out := []string{id}
	for i := 0; i < len(out); i++ {
		out = append(out, t.children[out[i]]...)
	}
	return out
}

// Scope 用户在监控域的有效数据范围：其全部启用角色授权的并集，授权某分组即包含其全部下级；
// 超级管理员不限；没有任何授权的用户看不到任何分组。值对象，一次请求内解析一次、只读。
type Scope struct {
	admin  bool
	levels map[string]int32
	tree   *groupTree
}

// IsAdmin 是否超级管理员（可新建顶级分组、设置角色授权、维护全局屏蔽规则）。
func (s Scope) IsAdmin() bool { return s.admin }

// Level 对分组的级别：monitorcontract.ScopeNone / ScopeRead / ScopeManage。分组不存在
// （含已删除）时超级管理员仍为可管理，其余为无权限。
func (s Scope) Level(groupID string) int32 {
	if s.admin {
		return mc.ScopeManage
	}
	return s.levels[groupID]
}

// CanRead 对分组是否只读以上。
func (s Scope) CanRead(groupID string) bool { return s.Level(groupID) >= mc.ScopeRead }

// CanManage 对分组是否可管理。
func (s Scope) CanManage(groupID string) bool { return s.Level(groupID) >= mc.ScopeManage }

// HasAny 是否至少可读一个分组。
func (s Scope) HasAny() bool { return s.admin || len(s.levels) > 0 }

// ReadableGroupIDs 可读分组 ID（字典序）；超级管理员返回全部现存分组。
func (s Scope) ReadableGroupIDs() []string {
	out := make([]string, 0, len(s.tree.byID))
	for id := range s.tree.byID {
		if s.CanRead(id) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// Filter 数据层列表查询用的分组过滤：groupID 为空 = 全部可读分组（超级管理员不限，含分组已被
// 删除的历史数据）；否则为该分组及其下级中可读的部分。
func (s Scope) Filter(groupID string) GroupFilter {
	if groupID == "" {
		if s.admin {
			return GroupFilter{All: true}
		}
		return GroupFilter{IDs: s.ReadableGroupIDs()}
	}
	ids := make([]string, 0, 8)
	for _, id := range s.tree.descendants(groupID) {
		if s.CanRead(id) {
			ids = append(ids, id)
		}
	}
	return GroupFilter{IDs: ids}
}

// NotifiedFilter 「通知过我的事件」用的分组过滤：不按读范围裁剪——被通知到的人即使对事件所在分组
// 没有范围也能查看那一条事件（与事件详情的口径一致）。调用方必须同时按「接收方 = 本人」过滤；
// 本方法只负责把可选的分组筛选翻译成该分组及其下级。
func (s Scope) NotifiedFilter(groupID string) GroupFilter {
	if groupID == "" {
		return GroupFilter{All: true}
	}
	return GroupFilter{IDs: s.tree.descendants(groupID)}
}

// GroupName 分组的完整名称；分组不存在返回空串。
func (s Scope) GroupName(groupID string) string { return s.tree.fullName(groupID) }

// Group 取分组的副本；不存在（含已删除）返回 false。
func (s Scope) Group(groupID string) (RuleGroup, bool) {
	g, ok := s.tree.byID[groupID]
	if !ok {
		return RuleGroup{}, false
	}
	return *g, true
}

// Groups 全部现存分组的副本（上级在前）。只在分组列表页调用，分组至多 MaxGroups 个。
func (s Scope) Groups() []RuleGroup {
	out := make([]RuleGroup, 0, len(s.tree.ordered))
	for _, g := range s.tree.ordered {
		out = append(out, *g)
	}
	return out
}

// Ancestors 自根到该分组的 ID 链（含自身）；分组不存在返回 nil。
func (s Scope) Ancestors(groupID string) []string { return s.tree.ancestors(groupID) }

// Descendants 分组及其全部下级的 ID；分组不存在返回 nil。
func (s Scope) Descendants(groupID string) []string { return s.tree.descendants(groupID) }

// ChildCount 直属下级分组数。
func (s Scope) ChildCount(groupID string) int { return len(s.tree.children[groupID]) }

// GroupExists 分组是否存在（未删除）。
func (s Scope) GroupExists(groupID string) bool {
	_, ok := s.tree.byID[groupID]
	return ok
}

// requireRead / requireManage 统一的范围断言。
func (s Scope) requireRead(groupID string) error {
	if !s.CanRead(groupID) {
		return ErrForbidden
	}
	return nil
}

func (s Scope) requireManage(groupID string) error {
	if !s.CanManage(groupID) {
		return ErrForbidden
	}
	return nil
}

// ScopeUsecase 数据范围的唯一解析入口：所有用例经它得到 Scope，再用 Scope 判定与生成 data 层
// 的分组过滤；禁止在各用例里另写范围判断。
type ScopeUsecase struct {
	roles  RoleSource
	grants RoleScopeRepo
	groups GroupReader
}

// NewScopeUsecase new a ScopeUsecase.
func NewScopeUsecase(roles RoleSource, grants RoleScopeRepo, groups GroupReader) *ScopeUsecase {
	return &ScopeUsecase{roles: roles, grants: grants, groups: groups}
}

// Resolve 解析用户的有效范围。非可用用户、无启用角色、角色没有任何授权 → 什么都看不到。
func (uc *ScopeUsecase) Resolve(ctx context.Context, userID string) (Scope, error) {
	scopes, err := uc.ResolveMany(ctx, []string{userID})
	if err != nil {
		return Scope{}, err
	}
	return scopes[userID], nil
}

// ResolveMany 批量解析多个用户的有效范围（分组树与授权各读一次；角色按用户读取）。
func (uc *ScopeUsecase) ResolveMany(ctx context.Context, userIDs []string) (map[string]Scope, error) {
	tree, err := uc.loadTree(ctx)
	if err != nil {
		return nil, err
	}
	return uc.scopesFor(ctx, tree, userIDs)
}

// loadTree 读取分组树（一条不带子查询的轻量语句）。不针对某个用户的调用方（通知派发）用它取分组
// 名称与上级链，再把同一棵树交给 scopesFor，避免重复读取。
func (uc *ScopeUsecase) loadTree(ctx context.Context) (*groupTree, error) {
	groups, err := uc.groups.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	return newGroupTree(groups), nil
}

// scopesFor 在给定的分组树上解析这些用户的有效范围。
func (uc *ScopeUsecase) scopesFor(ctx context.Context, tree *groupTree, userIDs []string) (map[string]Scope, error) {
	out := make(map[string]Scope, len(userIDs))
	userRoles := make(map[string][]string, len(userIDs))
	var allRoles []string
	for _, userID := range userIDs {
		if _, done := out[userID]; done {
			continue
		}
		isAdmin, roleIDs, err := uc.roles.EnabledRoleIDs(ctx, userID)
		if err != nil {
			return nil, err
		}
		out[userID] = Scope{admin: isAdmin, tree: tree}
		if !isAdmin {
			userRoles[userID] = roleIDs
			allRoles = append(allRoles, roleIDs...)
		}
	}
	if len(allRoles) == 0 {
		return out, nil
	}
	slices.Sort(allRoles)
	grants, err := uc.grants.ListByRoles(ctx, slices.Compact(allRoles))
	if err != nil {
		return nil, err
	}
	byRole := make(map[string][]*RoleScope, len(grants))
	for _, g := range grants {
		byRole[g.RoleID] = append(byRole[g.RoleID], g)
	}
	for userID, roleIDs := range userRoles {
		direct := map[string]int32{}
		for _, roleID := range roleIDs {
			for _, g := range byRole[roleID] {
				direct[g.GroupID] = max(direct[g.GroupID], g.Level)
			}
		}
		out[userID] = Scope{tree: tree, levels: inheritLevels(tree, direct)}
	}
	return out, nil
}

// inheritLevels 把直接授权沿树向下传播：分组的有效级别 = 自身及全部上级授权中的最高级别。
// 已删除分组上的授权不在树里，自然失效。
func inheritLevels(tree *groupTree, direct map[string]int32) map[string]int32 {
	levels := make(map[string]int32, len(direct))
	if len(direct) == 0 {
		return levels
	}
	for id := range tree.byID {
		var level int32
		for _, a := range tree.ancestors(id) {
			level = max(level, direct[a])
		}
		if level > mc.ScopeNone {
			levels[id] = level
		}
	}
	return levels
}
