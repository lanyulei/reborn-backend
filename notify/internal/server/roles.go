// Package server 构造传输层与后台 worker 的装配：角色门控（SERVER_ROLES）、
// HTTP/gRPC 服务器、HMAC 鉴权中间件、健康检查与指标端点。
package server

import (
	"os"
	"strings"

	"github.com/lanyulei/reborn/backend/notify/internal/conf"
)

// 角色名（设计 §2.3；relay 保留给 NATS 骨干）。
const (
	RoleAPI         = "api"
	RoleRelay       = "relay"
	RoleRouter      = "router"
	RoleSender      = "sender"
	RoleDlqWorker   = "dlqworker"
	RoleMaintenance = "maintenance"
)

// Roles 启用的角色集合。
type Roles map[string]struct{}

// NewRoles 解析角色：环境变量 SERVER_ROLES 优先，其次 conf.pipeline.roles，
// 两者皆空 = 全部角色；未知角色名忽略（启动日志提示）。
func NewRoles(c *conf.Pipeline) Roles {
	list := c.GetRoles()
	if env := strings.TrimSpace(os.Getenv("SERVER_ROLES")); env != "" {
		list = strings.Split(env, ",")
	}
	roles := Roles{}
	for _, r := range list {
		if r = strings.TrimSpace(strings.ToLower(r)); r != "" {
			roles[r] = struct{}{}
		}
	}
	if len(roles) == 0 {
		for _, r := range []string{RoleAPI, RoleRouter, RoleSender, RoleDlqWorker, RoleMaintenance} {
			roles[r] = struct{}{}
		}
	}
	return roles
}

// Has 报告角色是否启用。
func (r Roles) Has(role string) bool {
	_, ok := r[role]
	return ok
}

// List 按固定顺序列出已启用角色（日志用）。
func (r Roles) List() []string {
	out := make([]string, 0, len(r))
	for _, role := range []string{RoleAPI, RoleRelay, RoleRouter, RoleSender, RoleDlqWorker, RoleMaintenance} {
		if r.Has(role) {
			out = append(out, role)
		}
	}
	return out
}
