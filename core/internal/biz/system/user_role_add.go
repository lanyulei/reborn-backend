package biz

import "context"

// AddRole 给用户追加一个角色绑定，已绑定则不动（幂等）；用户或角色不存在返回对应哨兵错误。
//
// 与 BindRoles 的区别：BindRoles 是「读出现有绑定 → 整组替换」里的后一半，两个调用方各自读、各自整组写回
// 会互相覆盖；这里只插入一行（唯一键 (user_id, role_id) 兜底），不读也不改其它绑定，多实例并发安全。
// 副作用口径与 BindRoles 一致：绑定只落 system_user_role，鉴权按请求现读该表，没有策略版本要推进，
// 也不吊销会话。
func (uc *UserUsecase) AddRole(ctx context.Context, userID, roleID string) error {
	if _, err := uc.reader.GetUser(ctx, userID); err != nil {
		return err
	}
	found, err := uc.roleReader.ListRolesByIDs(ctx, []string{roleID})
	if err != nil {
		return err
	}
	if len(found) != 1 {
		return ErrRoleNotFound
	}
	return uc.roles.AddUserRole(ctx, userID, roleID)
}
