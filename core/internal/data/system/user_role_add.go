package data

import (
	"context"
	"fmt"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// AddUserRole implements biz.UserRoleRepo：单语句插入一条绑定，(user_id, role_id) 已存在即不动
// （uk_system_user_role）。不读、不删其它绑定，因此与并发的另一次追加、与整组替换都不会互相丢行。
// 用户 / 角色已软删时不插入：外键只保证行存在，不看 delete_time，不拦就会给已删除的账号留下一条绑定。
func (r *userRoleRepo) AddUserRole(ctx context.Context, userID, roleID string) error {
	id, err := idgen.NewID()
	if err != nil {
		return fmt.Errorf("generate binding id: %w", err)
	}
	if _, err := r.data.Pool.Exec(ctx,
		`INSERT INTO system_user_role (id, user_id, role_id)
		 SELECT $1::uuid, u.id, ro.id
		   FROM "system_user" u, system_role ro
		  WHERE u.id = $2::uuid AND u.delete_time IS NULL
		    AND ro.id = $3::uuid AND ro.delete_time IS NULL
		 ON CONFLICT (user_id, role_id) DO NOTHING`,
		id, userID, roleID); err != nil {
		return fmt.Errorf("add user role binding: %w", err)
	}
	return nil
}
