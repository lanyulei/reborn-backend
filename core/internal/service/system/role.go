package service

import (
	"context"
	"slices"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// AdminResolver 判定操作者是否超管并读取其角色绑定（由 biz.UserUsecase
// 实现，wire 自动注入）。
type AdminResolver interface {
	IsAdminUser(ctx context.Context, id string) (bool, error)
	// ListRoles 查询用户已绑定的角色（授权守卫计算操作者授权上限用）。
	ListRoles(ctx context.Context, userID string) ([]*biz.Role, error)
}

// RoleService 实现 api/system/v1 生成的 RoleServiceServer。
type RoleService struct {
	uc    *biz.RoleUsecase
	perm  *biz.PermissionUsecase
	admin AdminResolver
}

// NewRoleService new a RoleService.
func NewRoleService(uc *biz.RoleUsecase, perm *biz.PermissionUsecase, admin AdminResolver) *RoleService {
	return &RoleService{uc: uc, perm: perm, admin: admin}
}

// guardBuiltinWrite 内置角色写操作守卫：目标为内置角色时要求操作者为超管
// （缺少身份按无效令牌拒绝；操作者不存在按仓储错误透传，fail closed）。
// 非内置角色不做额外限制，沿用接口级 RBAC。
func (s *RoleService) guardBuiltinWrite(ctx context.Context, roleID string) error {
	role, err := s.uc.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if !role.Builtin {
		return nil
	}
	operator, err := UserIDFromContext(ctx)
	if err != nil {
		return err
	}
	isAdmin, err := s.admin.IsAdminUser(ctx, operator)
	if err != nil {
		return err
	}
	if !isAdmin {
		return v1.ErrorPermissionDenied("only administrators may manage the built-in role")
	}
	return nil
}

// operatorRoles 解析操作者身份：超管返回 (true, nil)；否则返回其启用角色
// （与 authz 判定同口径，停用角色不计）。缺少身份按无效令牌拒绝；操作者
// 不存在按仓储错误透传（fail closed）。
func (s *RoleService) operatorRoles(ctx context.Context) (bool, []*biz.Role, error) {
	operator, err := UserIDFromContext(ctx)
	if err != nil {
		return false, nil, err
	}
	isAdmin, err := s.admin.IsAdminUser(ctx, operator)
	if err != nil || isAdmin {
		return isAdmin, nil, err
	}
	roles, err := s.admin.ListRoles(ctx, operator)
	if err != nil {
		return false, nil, err
	}
	return false, slices.DeleteFunc(roles, func(r *biz.Role) bool { return r.Status != biz.RoleStatusEnabled }), nil
}

// guardGrantCeiling 授权上限守卫：非超管只能授予自己已持有的菜单/元素/
// 接口授权（否则持「角色授权」权限者可给自己所在角色勾选任意接口，整体
// 绕过 RBAC）。目标角色已有的授权项可原样保留（不算新授予），便于在
// 他人配置的角色上做收缩。超管不受限。
func (s *RoleService) guardGrantCeiling(ctx context.Context, roleID string, grants biz.GrantSet) error {
	isAdmin, roles, err := s.operatorRoles(ctx)
	if err != nil || isAdmin {
		return err
	}
	roleIDs := make([]string, 0, len(roles))
	for _, r := range roles {
		roleIDs = append(roleIDs, r.ID)
	}
	held, err := s.perm.GrantsOfRoles(ctx, roleIDs)
	if err != nil {
		return err
	}
	current, err := s.perm.GetGrants(ctx, roleID)
	if err != nil {
		return err
	}
	if !withinCeiling(grants.Menus, held.Menus, current.Menus) ||
		!withinCeiling(grants.Elements, held.Elements, current.Elements) ||
		!withinCeiling(grants.Apis, held.Apis, current.Apis) {
		return v1.ErrorPermissionDenied("cannot grant permissions beyond your own")
	}
	return nil
}

// guardEnableCeiling 启用上限守卫：角色当前已是启用态则放行（非状态变更）；
// 否则非超管须持有该角色的全部菜单 / 元素 / 接口授权。超管不受限。
func (s *RoleService) guardEnableCeiling(ctx context.Context, roleID string) error {
	isAdmin, roles, err := s.operatorRoles(ctx)
	if err != nil || isAdmin {
		return err
	}
	target, err := s.uc.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if target.Status == biz.RoleStatusEnabled {
		return nil
	}
	roleIDs := make([]string, 0, len(roles))
	for _, r := range roles {
		roleIDs = append(roleIDs, r.ID)
	}
	held, err := s.perm.GrantsOfRoles(ctx, roleIDs)
	if err != nil {
		return err
	}
	grants, err := s.perm.GetGrants(ctx, roleID)
	if err != nil {
		return err
	}
	if !withinCeiling(grants.Menus, held.Menus, nil) ||
		!withinCeiling(grants.Elements, held.Elements, nil) ||
		!withinCeiling(grants.Apis, held.Apis, nil) {
		return v1.ErrorPermissionDenied("cannot enable a role whose permissions exceed your own")
	}
	return nil
}

// guardBizScope biz 范围上限守卫：非超管只能把角色 biz_scope 设为自身可
// 管理范围内的值（空 = 不限，须操作者自身不限）。目标角色当前为不限时
// 任何取值都是收缩，放行；已有的范围项可原样保留。超管不受限。
func (s *RoleService) guardBizScope(ctx context.Context, roleID string, scope []string) error {
	isAdmin, roles, err := s.operatorRoles(ctx)
	if err != nil || isAdmin {
		return err
	}
	held := make([]string, 0, 8)
	for _, r := range roles {
		if len(r.BizScope) == 0 {
			return nil // 操作者自身不限
		}
		held = append(held, r.BizScope...)
	}
	role, err := s.uc.Get(ctx, roleID)
	if err != nil {
		return err
	}
	if len(role.BizScope) == 0 {
		return nil
	}
	if len(scope) == 0 || !withinCeiling(scope, held, role.BizScope) {
		return v1.ErrorPermissionDenied("cannot grant biz scope beyond your own")
	}
	return nil
}

// withinCeiling 报告 requested 的每一项是否都在 held 或 current 之内。
func withinCeiling(requested, held, current []string) bool {
	for _, code := range requested {
		if !slices.Contains(held, code) && !slices.Contains(current, code) {
			return false
		}
	}
	return true
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
	if err := s.guardBuiltinWrite(ctx, req.GetId()); err != nil {
		return nil, err
	}
	upd := biz.RoleUpdate{Name: req.Name, Status: req.Status}
	// 启用角色 = 让它的全部授权生效：非超管须已持有该角色的全部授权，否则
	// 可以把一个权限超出自己的已停用角色重新启用（并通过绑定间接提权）。
	if req.Status != nil && req.GetStatus() == biz.RoleStatusEnabled {
		if err := s.guardEnableCeiling(ctx, req.GetId()); err != nil {
			return nil, err
		}
	}
	if req.BizScope != nil {
		scope := req.GetBizScope().GetItems()
		if scope == nil {
			scope = []string{}
		}
		if err := s.guardBizScope(ctx, req.GetId(), scope); err != nil {
			return nil, err
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
// 最后刷新策略快照。操作者取自 JWT claims（本端点必经鉴权中间件；缺少
// 身份按无效令牌拒绝）。非超管受授权上限守卫约束（见 guardGrantCeiling）。
func (s *RoleService) SetRolePermissions(ctx context.Context, req *v1.SetRolePermissionsRequest) (*v1.SetRolePermissionsResponse, error) {
	if err := s.guardBuiltinWrite(ctx, req.GetId()); err != nil {
		return nil, err
	}
	if err := validateOperations(req.GetApis()); err != nil {
		return nil, err
	}
	operator, err := UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	grants := biz.GrantSet{
		Menus:    req.GetMenus(),
		Elements: req.GetElements(),
		Apis:     req.GetApis(),
	}
	if err := s.guardGrantCeiling(ctx, req.GetId(), grants); err != nil {
		return nil, err
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
		Builtin:    r.Builtin,
		CreateTime: timestamppb.New(r.CreateTime),
		UpdateTime: timestamppb.New(r.UpdateTime),
	}
}
