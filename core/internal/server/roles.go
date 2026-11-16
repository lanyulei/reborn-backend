package server

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// 进程角色：同一份程序可以只承担其中一部分职责，分开部署、各自扩缩容。
const (
	// RoleAPI 对外提供业务 HTTP 接口。
	RoleAPI = "api"
	// RoleWorker 运行后台循环（值班、附件清理、流程作业、工单 SLA 巡检）。
	RoleWorker = "worker"
)

// rolesEnv 角色环境变量（与 cmdb / notify 服务同名同语义）。
const rolesEnv = "SERVER_ROLES"

// Roles 本进程启用的角色集合。
type Roles map[string]struct{}

// AllRoles 全部角色（缺省，即单进程部署）。
func AllRoles() Roles {
	return Roles{RoleAPI: {}, RoleWorker: {}}
}

// NewRoles 解析角色：环境变量 SERVER_ROLES（逗号分隔）优先，其次 conf.server.roles，两者皆空 =
// 全部角色。出现不认识的角色名直接报错——拼错一个字母就悄悄少跑一类职责，比启动失败危险得多。
func NewRoles(c *conf.Server, logger *slog.Logger) (Roles, error) {
	list := c.GetRoles()
	if env := strings.TrimSpace(os.Getenv(rolesEnv)); env != "" {
		list = strings.Split(env, ",")
	}
	roles := Roles{}
	for _, r := range list {
		switch r = strings.TrimSpace(strings.ToLower(r)); r {
		case "":
		case RoleAPI, RoleWorker:
			roles[r] = struct{}{}
		default:
			return nil, fmt.Errorf("unknown server role %q: want %s or %s", r, RoleAPI, RoleWorker)
		}
	}
	if len(roles) == 0 {
		roles = AllRoles()
	}
	logger.Info("server roles enabled", "roles", roles.List())
	if !roles.Has(RoleWorker) {
		// 只承担接口的进程不跑后台循环：定时节点、任务超时、自动任务、SLA 通知、值班提醒都靠
		// 别的进程里的 worker 角色推进。这里明确提醒一次（另有 /metrics 的积压指标可告警）。
		logger.Warn("background loops are disabled in this process; make sure another process runs the worker role")
	}
	return roles, nil
}

// Has 报告角色是否启用。
func (r Roles) Has(role string) bool {
	_, ok := r[role]
	return ok
}

// List 按固定顺序列出已启用的角色（日志用）。
func (r Roles) List() []string {
	out := make([]string, 0, 2)
	for _, role := range []string{RoleAPI, RoleWorker} {
		if r.Has(role) {
			out = append(out, role)
		}
	}
	return out
}
