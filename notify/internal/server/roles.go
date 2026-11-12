// Package server 构造传输层与后台 worker 的装配：角色门控（SERVER_ROLES）、
// HTTP/gRPC 服务器、HMAC 鉴权中间件、健康检查与指标端点。
package server

import (
	"fmt"
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

// knownRoles 合法角色名。
var knownRoles = map[string]struct{}{
	RoleAPI: {}, RoleRelay: {}, RoleRouter: {}, RoleSender: {}, RoleDlqWorker: {}, RoleMaintenance: {},
}

// NewRoles 解析角色：环境变量 SERVER_ROLES 优先，其次 conf.pipeline.roles，
// 两者皆空 = 全部角色。未知角色名直接装配失败（此前静默忽略：`SERVER_ROLES=sendr`
// 会起一个只有 HTTP 的进程，看似健康却不发送）。
func NewRoles(c *conf.Pipeline) (Roles, error) {
	list := c.GetRoles()
	if env := strings.TrimSpace(os.Getenv("SERVER_ROLES")); env != "" {
		list = strings.Split(env, ",")
	}
	roles, err := ParseRoles(list)
	if err != nil {
		return nil, err
	}
	// 缺省全角色在 nats 骨干下含 relay（pg 下 relay 为 no-op，不加）。
	if len(trimmed(list)) == 0 && strings.EqualFold(strings.TrimSpace(c.GetBackbone()), "nats") {
		roles[RoleRelay] = struct{}{}
	}
	return roles, nil
}

func trimmed(list []string) []string {
	var out []string
	for _, r := range list {
		if strings.TrimSpace(r) != "" {
			out = append(out, r)
		}
	}
	return out
}

// ParseRoles 解析角色名列表（大小写不敏感、忽略空白；空 = 全部角色）。
func ParseRoles(list []string) (Roles, error) {
	roles := Roles{}
	for _, r := range list {
		r = strings.TrimSpace(strings.ToLower(r))
		if r == "" {
			continue
		}
		if _, ok := knownRoles[r]; !ok {
			return nil, fmt.Errorf("unknown pipeline role %q (known: api, relay, router, sender, dlqworker, maintenance)", r)
		}
		roles[r] = struct{}{}
	}
	if len(roles) == 0 {
		for _, r := range []string{RoleAPI, RoleRouter, RoleSender, RoleDlqWorker, RoleMaintenance} {
			roles[r] = struct{}{}
		}
	}
	return roles, nil
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
