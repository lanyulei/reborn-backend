// Package data 实现 notify 域仓储（PostgreSQL + pgx）；驱动 import 仅允许在本层，
// 连接池由顶层 internal/data 共享。
package data

import (
	"errors"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/secretbox"
)

// SecretBox 渠道凭据加密器：kit/secretbox（AES-256-GCM，key = sha256(主密钥)，
// nonce||ct）的 core 侧封装——**只提供 Seal**（只写不读由调用面约束），未配置主密钥
// 时 Seal 返回业务哨兵 ErrChannelSecretUnavailable（可选能力，不阻断启动）。
type SecretBox struct {
	box *secretbox.Box
}

// NewSecretBox 由启动配置构造。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Seal 加密明文凭据。
func (b *SecretBox) Seal(plaintext string) ([]byte, error) {
	out, err := b.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrChannelSecretUnavailable
	}
	return out, err
}
