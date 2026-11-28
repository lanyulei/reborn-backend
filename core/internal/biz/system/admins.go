package biz

import "context"

// AdminReader 超管目录只读仓储接口（由 data 层实现，依赖倒置）：供流程引擎的「系统管理员」
// 审批人规则取人。
type AdminReader interface {
	// ListActiveAdminIDs 全部可用的超级管理员 ID（存在、启用、未删除），按 ID 排序。
	ListActiveAdminIDs(ctx context.Context) ([]string, error)
}

// AdminDirectory 「系统管理员」审批人规则的组织目录：流程引擎定义的 Admins 端口由它实现
// （单向依赖 workflow → system 的只读查询）。
type AdminDirectory struct {
	repo AdminReader
}

// NewAdminDirectory new an AdminDirectory。
func NewAdminDirectory(repo AdminReader) *AdminDirectory {
	return &AdminDirectory{repo: repo}
}

// AdminUserIDs 当前全部可用的超级管理员 ID。
func (d *AdminDirectory) AdminUserIDs(ctx context.Context) ([]string, error) {
	return d.repo.ListActiveAdminIDs(ctx)
}
