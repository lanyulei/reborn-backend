// Package service 实现 proto 生成的服务接口：只做参数编排、模型转换与错误透传，
// 业务规则全部收敛在 biz（禁止 import internal/data 与任何驱动）。
package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// UserService 实现 api/system/v1 生成的 UserServiceServer。
type UserService struct {
	uc *biz.UserUsecase
}

// NewUserService new a UserService.
func NewUserService(uc *biz.UserUsecase) *UserService {
	return &UserService{uc: uc}
}

// CreateUser implements proto UserService/CreateUser：创建超管账号须由超管
// 操作（见 requireAdmin）。
func (s *UserService) CreateUser(ctx context.Context, req *v1.CreateUserRequest) (*v1.CreateUserResponse, error) {
	if req.GetIsAdmin() {
		operator, err := UserIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.requireAdmin(ctx, operator); err != nil {
			return nil, err
		}
	}
	status := biz.StatusEnabled
	if req.Status != nil {
		status = *req.Status
	}
	user, err := s.uc.Create(ctx, &biz.User{
		Username: req.GetUsername(),
		Password: req.GetPassword(),
		Nickname: req.GetNickname(),
		Email:    req.GetEmail(),
		Phone:    req.GetPhone(),
		Gender:   req.GetGender(),
		IsAdmin:  req.GetIsAdmin(),
		Status:   status,
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateUserResponse{User: toProtoUser(user)}, nil
}

// UpdateUser implements proto UserService/UpdateUser：携带 is_admin 时走
// 超管标记变更守卫（仅超管可授予/撤销，且不能改自己的）；其余资料字段
// 不受限（用户可改自己的昵称等）。
func (s *UserService) UpdateUser(ctx context.Context, req *v1.UpdateUserRequest) (*v1.UpdateUserResponse, error) {
	if req.IsAdmin != nil {
		if err := s.guardAdminFlagChange(ctx, req.GetId(), req.GetIsAdmin()); err != nil {
			return nil, err
		}
	}
	upd := biz.UserUpdate{
		Nickname: req.Nickname,
		Email:    req.Email,
		Phone:    req.Phone,
		Gender:   req.Gender,
		IsAdmin:  req.IsAdmin,
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	return &v1.UpdateUserResponse{}, nil
}

// UpdateUserStatus implements proto UserService/UpdateUserStatus：不能停用
// 自己；停用超管须由超管操作。
func (s *UserService) UpdateUserStatus(ctx context.Context, req *v1.UpdateUserStatusRequest) (*v1.UpdateUserStatusResponse, error) {
	if err := s.guardMutation(ctx, req.GetId(), true); err != nil {
		return nil, err
	}
	if err := s.uc.UpdateStatus(ctx, req.GetId(), req.GetStatus()); err != nil {
		return nil, err
	}
	return &v1.UpdateUserStatusResponse{}, nil
}

// ResetUserPassword implements proto UserService/ResetUserPassword：重置
// 超管密码须由超管操作（否则持「重置密码」权限者可接管超管账号）；可重置
// 自己的密码。
func (s *UserService) ResetUserPassword(ctx context.Context, req *v1.ResetUserPasswordRequest) (*v1.ResetUserPasswordResponse, error) {
	if err := s.guardMutation(ctx, req.GetId(), false); err != nil {
		return nil, err
	}
	if err := s.uc.ResetPassword(ctx, req.GetId(), req.GetPassword()); err != nil {
		return nil, err
	}
	return &v1.ResetUserPasswordResponse{}, nil
}

// DeleteUser implements proto UserService/DeleteUser：不能删除自己；删除
// 超管须由超管操作。
func (s *UserService) DeleteUser(ctx context.Context, req *v1.DeleteUserRequest) (*v1.DeleteUserResponse, error) {
	if err := s.guardMutation(ctx, req.GetId(), true); err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteUserResponse{}, nil
}

// GetUser implements proto UserService/GetUser.
func (s *UserService) GetUser(ctx context.Context, req *v1.GetUserRequest) (*v1.GetUserResponse, error) {
	user, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetUserResponse{User: toProtoUser(user)}, nil
}

// ListUsers implements proto UserService/ListUsers.
func (s *UserService) ListUsers(ctx context.Context, req *v1.ListUsersRequest) (*v1.ListUsersResponse, error) {
	filter := biz.UserFilter{Keyword: req.GetKeyword(), Status: req.Status}
	users, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListUsersResponse{
		Items: make([]*v1.User, 0, len(users)),
		Total: total,
	}
	for _, u := range users {
		resp.Items = append(resp.Items, toProtoUser(u))
	}
	return resp, nil
}

// BindUserRoles implements proto UserService/BindUserRoles.
func (s *UserService) BindUserRoles(ctx context.Context, req *v1.BindUserRolesRequest) (*v1.BindUserRolesResponse, error) {
	if err := s.uc.BindRoles(ctx, req.GetId(), req.GetRoleIds()); err != nil {
		return nil, err
	}
	return &v1.BindUserRolesResponse{}, nil
}

// ListUserRoles implements proto UserService/ListUserRoles.
func (s *UserService) ListUserRoles(ctx context.Context, req *v1.ListUserRolesRequest) (*v1.ListUserRolesResponse, error) {
	roles, err := s.uc.ListRoles(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	resp := &v1.ListUserRolesResponse{Items: make([]*v1.Role, 0, len(roles))}
	for _, r := range roles {
		resp.Items = append(resp.Items, toProtoRole(r))
	}
	return resp, nil
}

// guardMutation 是用户管理写操作的操作者侧守卫（身份来自传输层 claims，
// 故在 service 层判定；RBAC 中间件只回答「有没有该接口权限」，回答不了
// 「对谁」）：
//   - selfForbidden 时禁止以自己为目标（ErrUserSelfOperation）——否则唯一
//     超管可把自己停用/删除，系统无人可登录；
//   - 目标是超管时操作者须为超管（PERMISSION_DENIED）——否则仅持用户管理
//     接口权限的普通角色可删除/停用/接管全部超管账号。
//
// 目标不存在返回 ErrUserNotFound（与原写路径同码）。
func (s *UserService) guardMutation(ctx context.Context, targetID string, selfForbidden bool) error {
	operator, err := UserIDFromContext(ctx)
	if err != nil {
		return err
	}
	target, err := s.uc.Get(ctx, targetID)
	if err != nil {
		return err
	}
	if selfForbidden && target.ID == operator {
		return biz.ErrUserSelfOperation
	}
	if target.IsAdmin {
		return s.requireAdmin(ctx, operator)
	}
	return nil
}

// guardAdminFlagChange 守卫 is_admin 标记变更：仅超管可授予/撤销（否则持
// 「编辑用户」权限者可把自己提升为超管，整体绕过 RBAC）；不能改自己的
// 标记（唯一超管自我降级即锁死系统）——值未变化的自我提交放行，便于
// 前端整表单回填。
func (s *UserService) guardAdminFlagChange(ctx context.Context, targetID string, isAdmin bool) error {
	operator, err := UserIDFromContext(ctx)
	if err != nil {
		return err
	}
	if err := s.requireAdmin(ctx, operator); err != nil {
		return err
	}
	if targetID == operator {
		target, err := s.uc.Get(ctx, targetID)
		if err != nil {
			return err
		}
		if target.IsAdmin != isAdmin {
			return biz.ErrUserSelfOperation
		}
	}
	return nil
}

// requireAdmin 要求操作者为超管；操作者不存在按仓储错误透传（fail closed）。
func (s *UserService) requireAdmin(ctx context.Context, operator string) error {
	admin, err := s.uc.IsAdminUser(ctx, operator)
	if err != nil {
		return err
	}
	if !admin {
		return v1.ErrorPermissionDenied("only administrators may manage administrator accounts")
	}
	return nil
}

// toProtoUser 将领域模型（DO）显式转换为 proto 实体；不含密码字段。
func toProtoUser(u *biz.User) *v1.User {
	if u == nil {
		return nil
	}
	return &v1.User{
		Id:         u.ID,
		Username:   u.Username,
		Nickname:   u.Nickname,
		Email:      u.Email,
		Phone:      u.Phone,
		Gender:     u.Gender,
		IsAdmin:    u.IsAdmin,
		Status:     u.Status,
		CreateTime: timestamppb.New(u.CreateTime),
		UpdateTime: timestamppb.New(u.UpdateTime),
	}
}
