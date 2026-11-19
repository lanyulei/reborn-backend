package conf

import (
	kitroles "github.com/lanyulei/reborn/backend/kit/roles"
)

// 角色名。
const (
	// RoleEngine 执行引擎：执行节点协议、租约清扫、日志清理（需要数据库与主密钥）。
	RoleEngine = "engine"
	// RoleRunner 执行节点：领取任务并运行 Ansible（不连接数据库、不持有主密钥）。
	RoleRunner = "runner"
)

// Roles 启用的角色集合。
type Roles map[string]struct{}

// NewRoles 解析角色：环境变量 SERVER_ROLES 优先，其次 server.roles，两者皆空 = 全部角色；
// 未知角色名直接装配失败。
func NewRoles(c *Server) (Roles, error) {
	set, _, err := kitroles.Resolve(c.GetRoles(), RoleEngine, RoleRunner)
	if err != nil {
		return nil, err
	}
	return Roles(set), nil
}

// Has 报告角色是否启用。
func (r Roles) Has(role string) bool {
	_, ok := r[role]
	return ok
}

// List 按固定顺序列出已启用角色（日志用）。
func (r Roles) List() []string {
	out := make([]string, 0, 2)
	for _, role := range []string{RoleEngine, RoleRunner} {
		if r.Has(role) {
			out = append(out, role)
		}
	}
	return out
}
