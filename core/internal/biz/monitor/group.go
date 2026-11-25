package biz

import (
	"context"
	"slices"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// GroupView 规则分组及当前用户对它的级别。
type GroupView struct {
	*RuleGroup
	FullName string
	Depth    int32
	Level    int32
	Builtin  bool
}

// RoleScopeView 角色对某分组的直接授权及展示信息。
type RoleScopeView struct {
	RoleScope
	RoleName       string
	RoleEnabled    bool
	UpdateUserName string
}

// InheritedRoleScope 从上级分组继承来的授权。
type InheritedRoleScope struct {
	RoleID    string
	RoleName  string
	Level     int32
	GroupID   string
	GroupName string
}

// GroupUsecase 规则分组与角色数据范围用例。
type GroupUsecase struct {
	scopes *ScopeUsecase
	reader GroupReader
	groups GroupWriter
	grants RoleScopeRepo
	roles  RoleDirectory
	dir    Directory
}

// NewGroupUsecase new a GroupUsecase.
func NewGroupUsecase(scopes *ScopeUsecase, reader GroupReader, groups GroupWriter, grants RoleScopeRepo, roles RoleDirectory, dir Directory) *GroupUsecase {
	return &GroupUsecase{scopes: scopes, reader: reader, groups: groups, grants: grants, roles: roles, dir: dir}
}

// groupView 生成分组视图。分组树本身不带计数（见 GroupReader.ListAll）：直属规则数取自 ruleCounts，
// 下级数由树算出。范围外的上级分组只为保持树连通，不透出说明与计数。
func groupView(scope Scope, g RuleGroup, ruleCounts map[string]int32) *GroupView {
	level := scope.Level(g.ID)
	g.RuleCount = ruleCounts[g.ID]
	g.ChildCount = int32(scope.ChildCount(g.ID)) // #nosec G115 -- 分组总数至多 MaxGroups
	if level == mc.ScopeNone {
		g.Description, g.RuleCount, g.ChildCount = "", 0, 0
	}
	return &GroupView{
		RuleGroup: &g, FullName: scope.GroupName(g.ID), Level: level,
		Depth:   int32(len(scope.Ancestors(g.ID))), // #nosec G115 -- 层级至多 MaxGroupDepth
		Builtin: g.ID == mc.RootGroupID,
	}
}

// List 当前用户可见的全部分组（上级在前）。范围外的上级分组以 Level = 0 返回，保证树连通。
func (uc *GroupUsecase) List(ctx context.Context, userID string) ([]*GroupView, bool, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, false, err
	}
	groups := scope.Groups()
	visible := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		if scope.CanRead(g.ID) {
			for _, a := range scope.Ancestors(g.ID) {
				visible[a] = struct{}{}
			}
		}
	}
	if len(visible) == 0 {
		return []*GroupView{}, scope.IsAdmin(), nil
	}
	counts, err := uc.reader.RuleCounts(ctx)
	if err != nil {
		return nil, false, err
	}
	out := make([]*GroupView, 0, len(visible))
	for _, g := range groups {
		if _, ok := visible[g.ID]; ok {
			out = append(out, groupView(scope, g, counts))
		}
	}
	return out, scope.IsAdmin(), nil
}

// Create 新建分组：下级分组要求对上级可管理，顶级分组仅超级管理员。
func (uc *GroupUsecase) Create(ctx context.Context, userID, parentID, name, description string, sort int32) (*GroupView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if name = strings.TrimSpace(name); name == "" {
		return nil, detail(ErrGroupInvalid, "group name must not be blank")
	}
	if parentID == "" {
		if !scope.IsAdmin() {
			return nil, ErrForbidden
		}
	} else {
		if !scope.GroupExists(parentID) {
			return nil, ErrGroupNotFound
		}
		if err := scope.requireManage(parentID); err != nil {
			return nil, err
		}
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	g := &RuleGroup{ID: id, ParentID: parentID, Name: name, Description: strings.TrimSpace(description), Sort: sort}
	// 层级与总数上限在 data 层的写事务里判定（与并发新建互斥）。
	if err := uc.groups.Create(ctx, g, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, userID, id)
}

// Update 修改名称、说明与排序：要求对该分组可管理。
func (uc *GroupUsecase) Update(ctx context.Context, userID, id, name, description string, sort int32) (*GroupView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !scope.GroupExists(id) {
		return nil, ErrGroupNotFound
	}
	if err := scope.requireManage(id); err != nil {
		return nil, err
	}
	if name = strings.TrimSpace(name); name == "" {
		return nil, detail(ErrGroupInvalid, "group name must not be blank")
	}
	g := &RuleGroup{ID: id, Name: name, Description: strings.TrimSpace(description), Sort: sort}
	if err := uc.groups.Update(ctx, g, userID); err != nil {
		return nil, err
	}
	return uc.reload(ctx, userID, id)
}

// Delete 删除分组：要求对该分组可管理（顶级分组仅超级管理员）；仍有下级或规则、或仍被屏蔽规则 /
// 订阅规则 / 记录规则 / 升级策略 / 抑制规则引用时拒绝。
func (uc *GroupUsecase) Delete(ctx context.Context, userID, id string) error {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return err
	}
	g, ok := scope.Group(id)
	if !ok {
		return ErrGroupNotFound
	}
	if err := scope.requireManage(id); err != nil {
		return err
	}
	if g.ParentID == "" && !scope.IsAdmin() {
		return ErrForbidden
	}
	return uc.groups.Delete(ctx, id, userID)
}

func (uc *GroupUsecase) reload(ctx context.Context, userID, id string) (*GroupView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	g, ok := scope.Group(id)
	if !ok {
		return nil, ErrGroupNotFound
	}
	counts, err := uc.reader.RuleCounts(ctx)
	if err != nil {
		return nil, err
	}
	return groupView(scope, g, counts), nil
}

// ListRoleScopes 某分组上直接授权的角色，以及从上级分组继承来的授权：要求对该分组可管理。
func (uc *GroupUsecase) ListRoleScopes(ctx context.Context, userID, groupID string) ([]*RoleScopeView, []*InheritedRoleScope, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if !scope.GroupExists(groupID) {
		return nil, nil, ErrGroupNotFound
	}
	if err := scope.requireManage(groupID); err != nil {
		return nil, nil, err
	}
	chain := scope.Ancestors(groupID)
	grants, err := uc.grants.ListByGroups(ctx, chain)
	if err != nil {
		return nil, nil, err
	}
	roles, err := uc.roleIndex(ctx)
	if err != nil {
		return nil, nil, err
	}
	direct := make([]*RoleScopeView, 0, len(grants))
	inherited := make([]*InheritedRoleScope, 0, len(grants))
	var users []string
	for _, g := range grants {
		role, ok := roles[g.RoleID]
		if !ok {
			continue // 角色已删除：授权行无害，不展示
		}
		if g.GroupID == groupID {
			direct = append(direct, &RoleScopeView{RoleScope: *g, RoleName: role.Name, RoleEnabled: role.Enabled})
			users = append(users, g.UpdateUser)
			continue
		}
		inherited = append(inherited, &InheritedRoleScope{
			RoleID: g.RoleID, RoleName: role.Name, Level: g.Level, GroupID: g.GroupID, GroupName: scope.GroupName(g.GroupID),
		})
	}
	userNames := map[string]string{}
	if users = uniq(users); len(users) > 0 {
		if userNames, err = uc.dir.UserNames(ctx, users); err != nil {
			return nil, nil, err
		}
	}
	for _, d := range direct {
		d.UpdateUserName = userNames[d.UpdateUser]
	}
	slices.SortFunc(direct, func(a, b *RoleScopeView) int { return strings.Compare(a.RoleName+a.RoleID, b.RoleName+b.RoleID) })
	// 继承的授权按离本分组由近到远排列。
	slices.SortStableFunc(inherited, func(a, b *InheritedRoleScope) int {
		return slices.Index(chain, b.GroupID) - slices.Index(chain, a.GroupID)
	})
	return direct, inherited, nil
}

// SetRoleScopes 整组替换某分组的角色授权：仅超级管理员；角色须存在且不重复。
func (uc *GroupUsecase) SetRoleScopes(ctx context.Context, userID, groupID string, items []*RoleScope) ([]*RoleScopeView, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !scope.IsAdmin() {
		return nil, ErrForbidden
	}
	if !scope.GroupExists(groupID) {
		return nil, ErrGroupNotFound
	}
	roles, err := uc.roleIndex(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(items))
	for _, it := range items {
		if _, ok := roles[it.RoleID]; !ok {
			return nil, detail(ErrRoleScopeInvalid, "role %s not found", it.RoleID)
		}
		if _, dup := seen[it.RoleID]; dup {
			return nil, detail(ErrRoleScopeInvalid, "role %s is duplicated", it.RoleID)
		}
		seen[it.RoleID] = struct{}{}
		if it.Level != mc.ScopeRead && it.Level != mc.ScopeManage {
			return nil, detail(ErrRoleScopeInvalid, "level must be 1 or 2")
		}
	}
	if err := uc.grants.Replace(ctx, groupID, items, userID); err != nil {
		return nil, err
	}
	direct, _, err := uc.ListRoleScopes(ctx, userID, groupID)
	return direct, err
}

func (uc *GroupUsecase) roleIndex(ctx context.Context) (map[string]RoleRef, error) {
	roles, err := uc.roles.ListRoles(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]RoleRef, len(roles))
	for _, r := range roles {
		out[r.ID] = r
	}
	return out, nil
}
