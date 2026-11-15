package biz

import (
	"context"
	"fmt"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 变更记录操作码（与 proto ChangeLog.action 注释一致）。
const (
	ActionGroupCreate    = "group.create"
	ActionGroupUpdate    = "group.update"
	ActionMemberAdd      = "member.add"
	ActionMemberUpdate   = "member.update"
	ActionMemberRemove   = "member.remove"
	ActionScheduleCreate = "schedule.create"
	ActionScheduleUpdate = "schedule.update"
	ActionScheduleStatus = "schedule.status"
	ActionScheduleDelete = "schedule.delete"
	ActionShiftOverride  = "shift.override"
	ActionShiftRevoke    = "shift.revoke"
	ActionShareCreate    = "share.create"
	ActionShareReset     = "share.reset"
	ActionShareDelete    = "share.delete"
)

// GroupUsecase 值班分组与成员用例。
type GroupUsecase struct {
	store  Store
	groups GroupRepo
	users  UserDirectory
}

// NewGroupUsecase new a GroupUsecase.
func NewGroupUsecase(store Store, groups GroupRepo, users UserDirectory) *GroupUsecase {
	return &GroupUsecase{store: store, groups: groups, users: users}
}

// lockManaged 行锁读取分组并校验操作者可管理（分组管理员或超级管理员）。校验放在
// 事务内、持锁之后：身份在校验与写入之间不会被并发改掉。
func lockManaged(ctx context.Context, tx Tx, actor Actor, groupID string) (*Group, error) {
	g, err := tx.LockGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if err := requireManage(ctx, tx, actor, groupID); err != nil {
		return nil, err
	}
	return g, nil
}

// requireManage 操作者须为分组管理员或超级管理员。
func requireManage(ctx context.Context, tx Tx, actor Actor, groupID string) error {
	if actor.IsAdmin {
		return nil
	}
	role, err := tx.MemberRole(ctx, groupID, actor.UserID)
	if err != nil {
		return err
	}
	if role != MemberRoleAdmin {
		return ErrForbidden
	}
	return nil
}

// writeLog 在事务内追加一条变更记录。
func writeLog(ctx context.Context, tx Tx, actor Actor, groupID, scheduleID, action, summary string, detail map[string]any) error {
	id, err := idgen.NewID()
	if err != nil {
		return fmt.Errorf("generate change log id: %w", err)
	}
	return tx.InsertChangeLog(ctx, &ChangeLog{
		ID: id, GroupID: groupID, ScheduleID: scheduleID, Action: action,
		Summary: truncate(summary, 255), Detail: detail, OperatorID: actor.UserID,
	})
}

// truncate 按字符截断（summary 列 varchar(255)）。
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// CanManage 报告 group（已含查询者身份）能否被 actor 管理。
func CanManage(actor Actor, g *Group) bool {
	return actor.IsAdmin || g.MyRole == MemberRoleAdmin
}

// Manageable 返回判定函数：actor 能否管理给定分组（超级管理员恒真）。供列表类接口
// 一次查询标注多张值班表的可管理状态。
func (uc *GroupUsecase) Manageable(ctx context.Context, actor Actor) (func(groupID string) bool, error) {
	if actor.IsAdmin {
		return func(string) bool { return true }, nil
	}
	roles, err := uc.groups.ListUserRoles(ctx, actor.UserID)
	if err != nil {
		return nil, err
	}
	return func(groupID string) bool { return roles[groupID] == MemberRoleAdmin }, nil
}

// List 分页查询分组。
func (uc *GroupUsecase) List(ctx context.Context, actor Actor, keyword string, mine bool, page, pageSize int64) ([]*Group, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.groups.List(ctx, GroupFilter{Keyword: strings.TrimSpace(keyword), ViewerID: actor.UserID, Mine: mine}, page, pageSize)
}

// Get 查询分组详情。
func (uc *GroupUsecase) Get(ctx context.Context, actor Actor, id string) (*Group, error) {
	return uc.groups.Get(ctx, id, actor.UserID)
}

// Create 新建分组；创建人自动成为管理员。
func (uc *GroupUsecase) Create(ctx context.Context, actor Actor, name, description string) (*Group, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, detail(ErrGroupInvalid, "group name is required")
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate group id: %w", err)
	}
	g := &Group{ID: id, Name: name, Description: strings.TrimSpace(description)}
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if err := tx.InsertGroup(ctx, g, actor.UserID); err != nil {
			return err
		}
		if _, err := tx.InsertMembers(ctx, id, []string{actor.UserID}, MemberRoleAdmin, actor.UserID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, id, "", ActionGroupCreate, "创建分组「"+name+"」", nil)
	})
	if err != nil {
		return nil, err
	}
	return uc.groups.Get(ctx, id, actor.UserID)
}

// Update 修改分组名称与说明。
func (uc *GroupUsecase) Update(ctx context.Context, actor Actor, id, name, description string) error {
	name, description = strings.TrimSpace(name), strings.TrimSpace(description)
	if name == "" {
		return detail(ErrGroupInvalid, "group name is required")
	}
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		g, err := lockManaged(ctx, tx, actor, id)
		if err != nil {
			return err
		}
		if g.Name == name && g.Description == description {
			return nil
		}
		if err := tx.UpdateGroup(ctx, id, name, description, actor.UserID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, id, "", ActionGroupUpdate, "修改分组信息", map[string]any{
			"before": map[string]any{"name": g.Name, "description": g.Description},
			"after":  map[string]any{"name": name, "description": description},
		})
	})
}

// Delete 删除分组：分组下仍有值班表时拒绝；公共地址随之作废。
func (uc *GroupUsecase) Delete(ctx context.Context, actor Actor, id string) error {
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, id); err != nil {
			return err
		}
		n, err := tx.CountSchedules(ctx, id)
		if err != nil {
			return err
		}
		if n > 0 {
			return ErrGroupInUse
		}
		if err := tx.DeleteShareLinksByScope(ctx, ScopeGroup, id, actor.UserID); err != nil {
			return err
		}
		return tx.DeleteGroup(ctx, id, actor.UserID)
	})
}

// ListMembers 查询分组成员；分组不存在返回 ErrGroupNotFound。
func (uc *GroupUsecase) ListMembers(ctx context.Context, actor Actor, groupID string) ([]*Member, error) {
	if _, err := uc.groups.Get(ctx, groupID, actor.UserID); err != nil {
		return nil, err
	}
	return uc.groups.ListMembers(ctx, groupID)
}

// AddMembers 批量添加成员：用户须存在且可用；已在组内的跳过；返回实际新增数。
func (uc *GroupUsecase) AddMembers(ctx context.Context, actor Actor, groupID string, userIDs []string, role int32) (int, error) {
	found, err := uc.users.Lookup(ctx, userIDs)
	if err != nil {
		return 0, err
	}
	names := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		u := found[id]
		if u == nil || !u.Available {
			return 0, detail(ErrMemberInvalid, "user %s does not exist or is not available", id)
		}
		names = append(names, u.DisplayName())
	}
	var added int
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
			return err
		}
		members, err := tx.ListMembers(ctx, groupID)
		if err != nil {
			return err
		}
		if len(members)+len(userIDs) > maxGroupMembers {
			return detail(ErrMemberInvalid, "a group can have at most %d members", maxGroupMembers)
		}
		if added, err = tx.InsertMembers(ctx, groupID, userIDs, role, actor.UserID); err != nil {
			return err
		}
		if added == 0 {
			return nil
		}
		return writeLog(ctx, tx, actor, groupID, "", ActionMemberAdd, "添加成员："+strings.Join(names, "、"),
			map[string]any{"user_ids": userIDs, "role": role})
	})
	return added, err
}

// UpdateMemberRole 修改成员身份；不能把最后一名管理员降级。
func (uc *GroupUsecase) UpdateMemberRole(ctx context.Context, actor Actor, groupID, userID string, role int32) error {
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
			return err
		}
		target, admins, err := findMember(ctx, tx, groupID, userID)
		if err != nil {
			return err
		}
		if target.Role == role {
			return nil
		}
		if target.Role == MemberRoleAdmin && admins <= 1 {
			return ErrLastAdmin
		}
		if err := tx.UpdateMemberRole(ctx, groupID, userID, role); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, groupID, "", ActionMemberUpdate, "修改成员身份："+memberName(target),
			map[string]any{"user_id": userID, "before": target.Role, "after": role})
	})
}

// RemoveMember 移出成员：仍在轮转名单内或为最后一名管理员时拒绝。
func (uc *GroupUsecase) RemoveMember(ctx context.Context, actor Actor, groupID, userID string) error {
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
			return err
		}
		target, admins, err := findMember(ctx, tx, groupID, userID)
		if err != nil {
			return err
		}
		if target.InRotation {
			return ErrMemberInUse
		}
		if target.Role == MemberRoleAdmin && admins <= 1 {
			return ErrLastAdmin
		}
		if err := tx.DeleteMember(ctx, groupID, userID); err != nil {
			return err
		}
		return writeLog(ctx, tx, actor, groupID, "", ActionMemberRemove, "移出成员："+memberName(target),
			map[string]any{"user_id": userID})
	})
}

// findMember 在事务内定位成员并统计管理员数；不在组内返回 ErrMemberNotFound。
func findMember(ctx context.Context, tx Tx, groupID, userID string) (target *Member, admins int, err error) {
	members, err := tx.ListMembers(ctx, groupID)
	if err != nil {
		return nil, 0, err
	}
	for _, m := range members {
		if m.Role == MemberRoleAdmin {
			admins++
		}
		if m.UserID == userID {
			target = m
		}
	}
	if target == nil {
		return nil, 0, ErrMemberNotFound
	}
	return target, admins, nil
}

func memberName(m *Member) string {
	if m.Nickname != "" {
		return m.Nickname
	}
	if m.Username != "" {
		return m.Username
	}
	return m.UserID
}

// Candidates 可选成员（可用的系统用户）。
func (uc *GroupUsecase) Candidates(ctx context.Context, keyword string) ([]*User, error) {
	return uc.users.Candidates(ctx, strings.TrimSpace(keyword), maxCandidates)
}

// ListChangeLogs 分页查询分组的变更记录。
func (uc *GroupUsecase) ListChangeLogs(ctx context.Context, groupID, scheduleID string, page, pageSize int64) ([]*ChangeLog, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.groups.ListChangeLogs(ctx, groupID, scheduleID, page, pageSize)
}
