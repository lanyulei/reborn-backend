package service

import (
	"context"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RoleService 实现 api/system/v1 生成的 RoleServiceServer。
type RoleService struct {
	uc   *biz.RoleUsecase
	perm *biz.PermissionUsecase
}

// NewRoleService new a RoleService.
func NewRoleService(uc *biz.RoleUsecase, perm *biz.PermissionUsecase) *RoleService {
	return &RoleService{uc: uc, perm: perm}
}

// CreateRole implements proto RoleService/CreateRole.
func (s *RoleService) CreateRole(ctx context.Context, req *v1.CreateRoleRequest) (*v1.CreateRoleResponse, error) {
	status := biz.RoleStatusEnabled
	if req.Status != nil {
		status = *req.Status
	}
	role, err := s.uc.Create(ctx, &biz.Role{
		Name:     req.GetName(),
		Code:     req.GetCode(),
		Status:   status,
		BizScope: req.GetBizScope(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateRoleResponse{Role: toProtoRole(role)}, nil
}

// UpdateRole implements proto RoleService/UpdateRole.
func (s *RoleService) UpdateRole(ctx context.Context, req *v1.UpdateRoleRequest) (*v1.UpdateRoleResponse, error) {
	upd := biz.RoleUpdate{Name: req.Name, Status: req.Status}
	if req.BizScope != nil {
		scope := req.GetBizScope().GetItems()
		if scope == nil {
			scope = []string{}
		}
		upd.BizScope = &scope
	}
	if err := s.uc.Update(ctx, req.GetId(), upd); err != nil {
		return nil, err
	}
	return &v1.UpdateRoleResponse{}, nil
}

// DeleteRole implements proto RoleService/DeleteRole.
func (s *RoleService) DeleteRole(ctx context.Context, req *v1.DeleteRoleRequest) (*v1.DeleteRoleResponse, error) {
	if err := s.uc.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteRoleResponse{}, nil
}

// GetRole implements proto RoleService/GetRole.
func (s *RoleService) GetRole(ctx context.Context, req *v1.GetRoleRequest) (*v1.GetRoleResponse, error) {
	role, err := s.uc.Get(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRoleResponse{Role: toProtoRole(role)}, nil
}

// ListRoles implements proto RoleService/ListRoles.
func (s *RoleService) ListRoles(ctx context.Context, req *v1.ListRolesRequest) (*v1.ListRolesResponse, error) {
	filter := biz.RoleFilter{Keyword: req.GetKeyword(), Status: req.Status}
	roles, total, err := s.uc.List(ctx, filter, req.GetPage(), int64(req.GetPageSize()))
	if err != nil {
		return nil, err
	}
	resp := &v1.ListRolesResponse{
		Items: make([]*v1.Role, 0, len(roles)),
		Total: total,
	}
	for _, r := range roles {
		resp.Items = append(resp.Items, toProtoRole(r))
	}
	return resp, nil
}

// GetRolePermissions implements proto RoleService/GetRolePermissions：
// 返回三类直接授权（菜单/页面元素/接口码）。
func (s *RoleService) GetRolePermissions(ctx context.Context, req *v1.GetRolePermissionsRequest) (*v1.GetRolePermissionsResponse, error) {
	grants, err := s.perm.GetGrants(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRolePermissionsResponse{
		Menus:    grants.Menus,
		Elements: grants.Elements,
		Apis:     grants.Apis,
	}, nil
}

// SetRolePermissions implements proto RoleService/SetRolePermissions：先校验
// 接口码在可枚举集合内（400 OPERATION_INVALID；菜单/元素码在用例层对目录
// 校验），再全量覆盖直接授权、物化接口策略（仅来自 apis，菜单/元素只控
// 可见性）并在同一事务直插 permission.change 通知事件（notify 设计 §11），
// 最后刷新策略快照。操作者取自 JWT claims（本端点必经鉴权中间件；提取
// 失败按空处理，事件载荷省略 operator 字段）。
func (s *RoleService) SetRolePermissions(ctx context.Context, req *v1.SetRolePermissionsRequest) (*v1.SetRolePermissionsResponse, error) {
	if err := validateOperations(req.GetApis()); err != nil {
		return nil, err
	}
	var operator string
	if id, err := UserIDFromContext(ctx); err == nil {
		operator = id
	}
	grants := biz.GrantSet{
		Menus:    req.GetMenus(),
		Elements: req.GetElements(),
		Apis:     req.GetApis(),
	}
	if err := s.perm.SetGrants(ctx, req.GetId(), grants, operator); err != nil {
		return nil, err
	}
	return &v1.SetRolePermissionsResponse{}, nil
}

// ListOperations implements proto RoleService/ListOperations。
// 由缓存快照逐条新建 proto 对象返回（快照是包内值切片，不外泄引用）。
func (s *RoleService) ListOperations(_ context.Context, _ *v1.ListOperationsRequest) (*v1.ListOperationsResponse, error) {
	src := listOperations()
	items := make([]*v1.Operation, 0, len(src))
	for _, op := range src {
		items = append(items, &v1.Operation{
			Name:   op.Name,
			Code:   op.Code,
			Title:  op.Title,
			Path:   op.Path,
			Method: op.Method,
		})
	}
	return &v1.ListOperationsResponse{Operations: items}, nil
}

// ListPermissionCatalog implements proto RoleService/ListPermissionCatalog：
// 组装 biz 权限目录（级联结构）与 proto 反射枚举的接口展示名，每条新建
// proto 对象返回（目录是包内注册表，不外泄引用）。
func (s *RoleService) ListPermissionCatalog(_ context.Context, _ *v1.ListPermissionCatalogRequest) (*v1.ListPermissionCatalogResponse, error) {
	titles := make(map[string]operation, 64)
	for _, op := range listOperations() {
		titles[op.Name] = op
	}
	toProtoOperation := func(method string) *v1.Operation {
		op := titles[method]
		return &v1.Operation{
			Name:   method,
			Code:   biz.OperationCode(method),
			Title:  op.Title,
			Path:   op.Path,
			Method: op.Method,
		}
	}
	menus := biz.Catalog()
	items := make([]*v1.CatalogMenu, 0, len(menus))
	for _, menu := range menus {
		protoMenu := &v1.CatalogMenu{
			Code: menu.Code,
			Name: menu.Name,
		}
		for _, method := range menu.DefaultOps {
			protoMenu.DefaultOperations = append(protoMenu.DefaultOperations, toProtoOperation(method))
		}
		for _, el := range menu.Elements {
			protoElement := &v1.CatalogElement{Code: el.Code, Name: el.Name}
			for _, method := range el.Ops {
				protoElement.Operations = append(protoElement.Operations, toProtoOperation(method))
			}
			protoMenu.Elements = append(protoMenu.Elements, protoElement)
		}
		items = append(items, protoMenu)
	}
	return &v1.ListPermissionCatalogResponse{Menus: items}, nil
}

// toProtoRole 将领域模型（DO）显式转换为 proto 实体。
func toProtoRole(r *biz.Role) *v1.Role {
	if r == nil {
		return nil
	}
	return &v1.Role{
		Id:         r.ID,
		Name:       r.Name,
		Code:       r.Code,
		Status:     r.Status,
		BizScope:   r.BizScope,
		CreateTime: timestamppb.New(r.CreateTime),
		UpdateTime: timestamppb.New(r.UpdateTime),
	}
}
