package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// Role 是角色领域模型（DO）；与 proto 实体、data 层 PO 显式转换，禁止跨层直传。
// Code 为业务标识，创建后不可变。
type Role struct {
	ID         string
	Name       string
	Code       string
	Status     int32
	BizScope   []string // 可管理的通知 biz 列表；空 = 不限
	CreateTime time.Time
	UpdateTime time.Time
}

// RoleFilter 是角色列表过滤条件；Status 为 nil 表示不过滤。
type RoleFilter struct {
	Keyword string
	Status  *int32
}

// RoleUpdate 是角色部分更新；nil 字段不更新（Code 不可更新）。BizScope 非 nil
// 时整组替换（空切片 = 清空 = 不限）。
type RoleUpdate struct {
	Name     *string
	Status   *int32
	BizScope *[]string
}

// 角色域哨兵错误：reason 与 api/system/v1 的 ErrorReason 枚举对齐。
var (
	// ErrRoleNotFound 目标角色不存在。
	ErrRoleNotFound = errors.NotFound(v1.ErrorReason_ROLE_NOT_FOUND.String(), "role not found")
	// ErrRoleCodeExists 角色标识已被占用。
	ErrRoleCodeExists = errors.Conflict(v1.ErrorReason_ROLE_CODE_EXISTS.String(), "role code already exists")
)

// RoleReader 是角色只读仓储接口，由 data 层实现（依赖倒置）。
type RoleReader interface {
	// GetRole 按 ID 查询未删除角色；不存在返回 ErrRoleNotFound。
	GetRole(ctx context.Context, id string) (*Role, error)
	// ListRoles 分页查询未删除角色；page 从 1 起，pageSize 上限 100，按创建时间倒序。
	ListRoles(ctx context.Context, filter RoleFilter, page, pageSize int64) ([]*Role, int64, error)
	// ListRolesByIDs 按 ID 集合查询未删除角色（绑定校验用），自动去重输入。
	ListRolesByIDs(ctx context.Context, ids []string) ([]*Role, error)
}

// RoleWriter 是角色写仓储接口，由 data 层实现（依赖倒置）。
type RoleWriter interface {
	// CreateRole 创建角色；code 冲突返回 ErrRoleCodeExists。
	CreateRole(ctx context.Context, role *Role) error
	// UpdateRole 按 ID 部分更新未删除角色；不存在返回 ErrRoleNotFound。
	UpdateRole(ctx context.Context, id string, upd RoleUpdate) error
	// DeleteRole 软删角色并同事务物理删除其全部用户绑定与授权行；不存在返回 ErrRoleNotFound。
	DeleteRole(ctx context.Context, id string) error
}

// UserRoleRepo 是用户-角色绑定仓储接口，由 data 层实现（依赖倒置）。
type UserRoleRepo interface {
	// BindUserRoles 事务内全量覆盖用户绑定（先物理删除旧绑定再写入新绑定）。
	BindUserRoles(ctx context.Context, userID string, roleIDs []string) error
	// ListUserRoles 查询用户当前绑定的角色，按 code 升序。
	ListUserRoles(ctx context.Context, userID string) ([]*Role, error)
}

// RoleUsecase 是角色用例：承载业务规则与事务边界。
type RoleUsecase struct {
	reader RoleReader
	writer RoleWriter
	authz  *Authorizer
}

// NewRoleUsecase new a RoleUsecase.
func NewRoleUsecase(reader RoleReader, writer RoleWriter, authz *Authorizer) *RoleUsecase {
	return &RoleUsecase{reader: reader, writer: writer, authz: authz}
}

// Create 创建角色：生成 ID、填充审计时间后落库；返回完整角色实体。
func (uc *RoleUsecase) Create(ctx context.Context, role *Role) (*Role, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	role.ID = id
	role.CreateTime, role.UpdateTime = now, now
	if err := uc.writer.CreateRole(ctx, role); err != nil {
		return nil, err
	}
	return role, nil
}

// Update 部分更新角色（name/status）；不存在返回 ErrRoleNotFound。
func (uc *RoleUsecase) Update(ctx context.Context, id string, upd RoleUpdate) error {
	return uc.writer.UpdateRole(ctx, id, upd)
}

// Delete 软删角色并级联物理删除其用户绑定；不存在返回 ErrRoleNotFound。
func (uc *RoleUsecase) Delete(ctx context.Context, id string) error {
	if err := uc.writer.DeleteRole(ctx, id); err != nil {
		return err
	}
	// 删角色级联清理其策略行，刷新本实例快照（WithoutCancel：删除已提交，
	// 刷新不得随请求取消而跳过；刷新失败不回报 500——删除已成功，客户端
	// 重试只会得到 404；其余实例在 policy_ttl 内收敛）。
	uc.authz.ReloadAfterCommit(context.WithoutCancel(ctx))
	return nil
}

// Get 查询角色详情。
func (uc *RoleUsecase) Get(ctx context.Context, id string) (*Role, error) {
	return uc.reader.GetRole(ctx, id)
}

// List 分页查询角色；分页参数先规整再委托仓储。
func (uc *RoleUsecase) List(ctx context.Context, filter RoleFilter, page, pageSize int64) ([]*Role, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.reader.ListRoles(ctx, filter, page, pageSize)
}
