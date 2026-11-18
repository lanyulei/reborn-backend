package biz

import (
	"context"
	"encoding/json"

	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// maxRoleScopes 单个角色的数据范围条数上限。
const maxRoleScopes = 500

// ScopeUsecase 角色数据范围用例。这里只维护配置；同步为集群内的 RoleBinding 与权限判定都在数据面与集群。
type ScopeUsecase struct {
	repo ScopeRepo
	dir  Directory
}

// NewScopeUsecase new a ScopeUsecase.
func NewScopeUsecase(repo ScopeRepo, dir Directory) *ScopeUsecase {
	return &ScopeUsecase{repo: repo, dir: dir}
}

// Get 读取某个角色的数据范围。
func (uc *ScopeUsecase) Get(ctx context.Context, roleID string) ([]*RoleScope, error) {
	return uc.repo.Get(ctx, roleID)
}

// validateScopes 校验一组数据范围：命名空间名合法、档位 1-4、集群管理员只能授予全部命名空间、
// 同一集群同一命名空间不得重复。
func validateScopes(items []*RoleScope) error {
	if len(items) > maxRoleScopes {
		return detail(ErrScopeInvalid, "at most %d scope items per role", maxRoleScopes)
	}
	seen := make(map[[2]string]struct{}, len(items))
	for _, it := range items {
		switch {
		case it == nil || it.ClusterID == "":
			return detail(ErrScopeInvalid, "cluster is required")
		case it.Namespace != kc.AllNamespaces && !clusterNamePattern.MatchString(it.Namespace):
			return detail(ErrScopeInvalid, "invalid namespace %q", it.Namespace)
		case kc.LevelClusterRole(it.Level) == "":
			return detail(ErrScopeInvalid, "level must be 1-4")
		case it.Level == kc.LevelClusterAdmin && it.Namespace != kc.AllNamespaces:
			return detail(ErrScopeInvalid, "cluster admin can only be granted on all namespaces (*)")
		}
		key := [2]string{it.ClusterID, it.Namespace}
		if _, dup := seen[key]; dup {
			return detail(ErrScopeInvalid, "duplicate scope for the same cluster and namespace %q", it.Namespace)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// Replace 整组替换某个角色的数据范围，返回保存后的内容。
func (uc *ScopeUsecase) Replace(ctx context.Context, caller Caller, roleID string, items []*RoleScope) ([]*RoleScope, error) {
	if err := validateScopes(items); err != nil {
		return nil, err
	}
	type entry struct {
		ClusterID string `json:"clusterId"`
		Namespace string `json:"namespace"`
		Level     int32  `json:"level"`
	}
	entries := make([]entry, 0, len(items))
	for _, it := range items {
		entries = append(entries, entry{it.ClusterID, it.Namespace, it.Level})
	}
	raw, _ := json.Marshal(map[string]any{"roleId": roleID, "items": entries})
	audit := newAuditEntry(ctx, uc.dir, caller, kc.AuditResourceRoleScope, kc.ActionScopeReplace, raw)
	if err := uc.repo.Replace(ctx, roleID, items, audit); err != nil {
		return nil, err
	}
	return uc.repo.Get(ctx, roleID)
}

// ScopeInvalid 数据范围不合法的具体原因（data 层在写事务内判定，如引用了不存在的集群）。
func ScopeInvalid(format string, args ...any) error {
	return detail(ErrScopeInvalid, format, args...)
}
