// Package service 实现 proto 生成的服务接口：只做参数编排、模型转换与错误透传，
// 业务规则全部收敛在 biz（禁止 import internal/data 与任何驱动）。
package service

import (
	"context"

	v1 "github.com/lanyulei/monitor/backend/core/api/system/v1"
	"github.com/lanyulei/monitor/backend/core/internal/biz"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// UserService 实现 api/system/v1 生成的 UserServiceServer。
type UserService struct {
	v1.UnimplementedUserServiceServer

	uc *biz.UserUsecase
}

// NewUserService new a UserService.
func NewUserService(uc *biz.UserUsecase) *UserService {
	return &UserService{uc: uc}
}

// CreateUser implements proto UserService/CreateUser.
func (s *UserService) CreateUser(ctx context.Context, req *v1.CreateUserRequest) (*v1.CreateUserResponse, error) {
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

// UpdateUser implements proto UserService/UpdateUser.
func (s *UserService) UpdateUser(ctx context.Context, req *v1.UpdateUserRequest) (*v1.UpdateUserResponse, error) {
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

// UpdateUserStatus implements proto UserService/UpdateUserStatus.
func (s *UserService) UpdateUserStatus(ctx context.Context, req *v1.UpdateUserStatusRequest) (*v1.UpdateUserStatusResponse, error) {
	if err := s.uc.UpdateStatus(ctx, req.GetId(), req.GetStatus()); err != nil {
		return nil, err
	}
	return &v1.UpdateUserStatusResponse{}, nil
}

// ResetUserPassword implements proto UserService/ResetUserPassword.
func (s *UserService) ResetUserPassword(ctx context.Context, req *v1.ResetUserPasswordRequest) (*v1.ResetUserPasswordResponse, error) {
	if err := s.uc.ResetPassword(ctx, req.GetId(), req.GetPassword()); err != nil {
		return nil, err
	}
	return &v1.ResetUserPasswordResponse{}, nil
}

// DeleteUser implements proto UserService/DeleteUser.
func (s *UserService) DeleteUser(ctx context.Context, req *v1.DeleteUserRequest) (*v1.DeleteUserResponse, error) {
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
