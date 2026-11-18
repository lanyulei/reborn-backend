package biz

import "context"

// 本文件收拢 k8s 域对其他域的全部依赖：只读、单向，一律是在此定义的窄接口，由组合根
// （internal/biz/biz.go）绑定到 system / notify 域的用例。其他域不依赖 k8s。

// RoleSource 用户的启用角色（system 域 AuthzUsecase 实现）。
type RoleSource interface {
	// EnabledRoleIDs 超管返回 (true, nil)；非可用用户返回 (false, 空)。
	EnabledRoleIDs(ctx context.Context, userID string) (isAdmin bool, roleIDs []string, err error)
}

// Accounts 账号状态（system 域 AuthzUsecase 实现）。
type Accounts interface {
	// Active 用户是否存在且可用；不存在返回 error。
	Active(ctx context.Context, userID string) (bool, error)
}

// Directory 用户显示名（system 域 DirectoryUsecase 实现）。
type Directory interface {
	// UserNames 批量取用户显示名；不存在的用户不在结果中。
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
}

// SecretSealer 凭据加密（data 层以 kit/secretbox 实现）。只有 Seal：core 不解密集群凭据。
type SecretSealer interface {
	// Seal 加密明文；主密钥未配置返回 ErrSecretUnavailable。
	Seal(plaintext string) ([]byte, error)
}
