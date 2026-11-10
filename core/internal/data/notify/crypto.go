// Package data 实现 notify 域仓储（PostgreSQL + pgx）；驱动 import 仅允许在本层，
// 连接池由顶层 internal/data 共享。
package data

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
)

// SecretBox 渠道凭据加密器：AES-256-GCM，密文布局 = nonce(12B) || ciphertext。
// 主密钥为任意非空字节串，经 SHA-256 派生 32 字节 AES 密钥（密钥来自环境变量
// 注入的随机值而非低熵口令，无需 PBKDF 系拉伸）。aead 为 nil 表示未配置主密钥：
// Seal 返回 ErrChannelSecretUnavailable，无 secret 渠道的读写不受影响。
type SecretBox struct {
	aead cipher.AEAD
}

// NewSecretBox 由启动配置构造。未配置主密钥返回未启用的 box 而非报错——渠道
// 凭据加密是可选能力，缺失时在写入带 secret 的渠道处快速失败（绝不静默落明文），
// 不阻断整个服务启动。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	material := c.GetMaster().GetKey()
	if material == "" {
		return &SecretBox{}, nil
	}
	key := sha256.Sum256([]byte(material))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("notify secretbox init cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("notify secretbox init gcm: %w", err)
	}
	return &SecretBox{aead: aead}, nil
}

// Seal 加密明文凭据；未配置主密钥时返回业务哨兵错误。
func (b *SecretBox) Seal(plaintext string) ([]byte, error) {
	if b.aead == nil {
		return nil, biz.ErrChannelSecretUnavailable
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("notify secretbox nonce: %w", err)
	}
	return b.aead.Seal(nonce, nonce, []byte(plaintext), nil), nil
}
