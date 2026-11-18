package biz

import "context"

// AccessResolver 解析调用者的数据范围（启用角色在 k8s_role_scope 里的授权并集；超管不限）。
// 审计、变更单、转发前的集群可见性检查共用它。
type AccessResolver struct {
	roles  RoleSource
	grants GrantSource
}

// NewAccessResolver new an AccessResolver，并挂到 PlaneUsecase 上：转发前先确认调用者在该集群上有授权。
func NewAccessResolver(roles RoleSource, grants GrantSource, planes *PlaneUsecase) *AccessResolver {
	r := &AccessResolver{roles: roles, grants: grants}
	planes.access = r
	return r
}

// Resolve 调用者的数据范围。
func (r *AccessResolver) Resolve(ctx context.Context, userID string) (Access, error) {
	isAdmin, roleIDs, err := r.roles.EnabledRoleIDs(ctx, userID)
	if err != nil {
		return Access{}, err
	}
	return r.forRoles(ctx, isAdmin, roleIDs)
}

func (r *AccessResolver) forRoles(ctx context.Context, isAdmin bool, roleIDs []string) (Access, error) {
	if isAdmin {
		return Access{All: true}, nil
	}
	if len(roleIDs) == 0 {
		return NewAccess(nil), nil
	}
	grants, err := r.grants.Grants(ctx, roleIDs)
	if err != nil {
		return Access{}, err
	}
	return NewAccess(grants), nil
}

// AllowsObject 调用者能否查看该集群该命名空间里对象的内容（审计、变更单、终端会话）。比 AllowsNamespace
// 严格：集群级对象与平台配置（namespace 为空）要求「全部命名空间」的授权；没有集群的记录只有超管可见。
func (a Access) AllowsObject(clusterID, namespace string) bool {
	if a.All {
		return true
	}
	ca := a.Clusters[clusterID]
	if ca == nil || clusterID == "" {
		return false
	}
	if ca.AllNamespaces {
		return true
	}
	if namespace == "" {
		return false
	}
	_, ok := ca.Namespaces[namespace]
	return ok
}
