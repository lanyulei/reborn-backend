package data

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/notify/internal/conf"
)

// SecretBox 渠道凭据解密器——与 core `internal/data/notify/crypto.go` 完全同构
// 的契约：AES-256-GCM，密钥 = SHA-256(主密钥字节串)，密文布局 nonce(12B)||ct，
// 无附加数据。主密钥为空时 Open 返回 ErrMasterKeyUnavailable（带 secret 渠道
// 标记不健康，无 secret 渠道不受影响）。
type SecretBox struct {
	aead cipher.AEAD
}

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = errors.New("notify master key unavailable")

// NewSecretBox 由配置构造。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	material := c.GetMaster().GetKey()
	if material == "" {
		return &SecretBox{}, nil
	}
	key := sha256.Sum256([]byte(material))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("init secret cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("init secret gcm: %w", err)
	}
	return &SecretBox{aead: aead}, nil
}

// Open 解密 nonce||ciphertext。
func (b *SecretBox) Open(sealed []byte) (string, error) {
	if b.aead == nil {
		return "", ErrMasterKeyUnavailable
	}
	n := b.aead.NonceSize()
	if len(sealed) < n+b.aead.Overhead() {
		return "", errors.New("sealed secret too short")
	}
	plain, err := b.aead.Open(nil, sealed[:n], sealed[n:], nil)
	if err != nil {
		return "", fmt.Errorf("open secret: %w", err)
	}
	return string(plain), nil
}

// Seal 加密（测试与契约验证用；生产写路径在 core）。
func (b *SecretBox) Seal(plaintext string, nonce []byte) ([]byte, error) {
	if b.aead == nil {
		return nil, ErrMasterKeyUnavailable
	}
	if len(nonce) != b.aead.NonceSize() {
		return nil, errors.New("bad nonce size")
	}
	return b.aead.Seal(append([]byte(nil), nonce...), nonce, []byte(plaintext), nil), nil
}
