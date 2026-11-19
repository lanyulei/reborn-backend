// Package roles 解析进程角色：同一份程序按角色启用其中一部分职责，分开部署、各自扩缩容。
// 各服务的角色名不同，解析规则相同：环境变量 SERVER_ROLES（逗号分隔）优先于配置文件，
// 两者皆空 = 全部角色，出现不认识的角色名直接报错。
package roles

import (
	"fmt"
	"os"
	"strings"
)

// Env 角色环境变量（全部服务同名同语义）。
const Env = "SERVER_ROLES"

// Set 启用的角色集合。
type Set map[string]struct{}

// Resolve 解析角色：环境变量 SERVER_ROLES 优先，其次 confRoles；大小写不敏感、忽略空白与空项。
// 未知角色名报错——拼错一个字母就悄悄少跑一类职责，比启动失败危险得多。explicit 报告是否
// 显式指定了角色；未指定时返回 known 全集。
func Resolve(confRoles []string, known ...string) (set Set, explicit bool, err error) {
	list := confRoles
	if v := strings.TrimSpace(os.Getenv(Env)); v != "" {
		list = strings.Split(v, ",")
	}
	set = Set{}
	for _, r := range list {
		r = strings.TrimSpace(strings.ToLower(r))
		if r == "" {
			continue
		}
		if !contains(known, r) {
			return nil, false, fmt.Errorf("unknown role %q (known: %s)", r, strings.Join(known, ", "))
		}
		set[r] = struct{}{}
	}
	if len(set) > 0 {
		return set, true, nil
	}
	for _, r := range known {
		set[r] = struct{}{}
	}
	return set, false, nil
}

// Has 报告角色是否启用。
func (s Set) Has(role string) bool {
	_, ok := s[role]
	return ok
}

// List 按 order 给出的顺序列出已启用的角色（日志用）。
func (s Set) List(order ...string) []string {
	out := make([]string, 0, len(s))
	for _, r := range order {
		if s.Has(r) {
			out = append(out, r)
		}
	}
	return out
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
