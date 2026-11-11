// Package secretbox 凭据加密：AES-256-GCM，密钥 = SHA-256(主密钥字节串)（主密钥
// 来自环境变量注入的随机值而非低熵口令，无需 PBKDF 拉伸），密文布局 nonce(12B)||ct，
// 无附加数据。core 写路径只调用 Seal、管道读路径只调用 Open——"只写不读"由调用面
// 约束。主密钥为空时构造成功但 Seal/Open 返回 ErrUnavailable（可选能力，不阻断启动）。
package secretbox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

// ErrUnavailable 主密钥未配置。
var ErrUnavailable = errors.New("secretbox: master key unavailable")

// Box 加密器。
type Box struct {
	aead cipher.AEAD
}

// New 由主密钥构造；空串 → 未启用的 Box。
func New(masterKey string) (*Box, error) {
	if masterKey == "" {
		return &Box{}, nil
	}
	key := sha256.Sum256([]byte(masterKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("secretbox init cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secretbox init gcm: %w", err)
	}
	return &Box{aead: aead}, nil
}

// Enabled 报告是否已配置主密钥。
func (b *Box) Enabled() bool { return b.aead != nil }

// Seal 用随机 nonce 加密。
func (b *Box) Seal(plaintext string) ([]byte, error) {
	if b.aead == nil {
		return nil, ErrUnavailable
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secretbox nonce: %w", err)
	}
	return b.SealWithNonce(plaintext, nonce)
}

// SealWithNonce 用给定 nonce 加密（确定性，测试与契约向量用）。
func (b *Box) SealWithNonce(plaintext string, nonce []byte) ([]byte, error) {
	if b.aead == nil {
		return nil, ErrUnavailable
	}
	if len(nonce) != b.aead.NonceSize() {
		return nil, errors.New("secretbox: bad nonce size")
	}
	return b.aead.Seal(append([]byte(nil), nonce...), nonce, []byte(plaintext), nil), nil
}

// Open 解密 nonce||ciphertext。
func (b *Box) Open(sealed []byte) (string, error) {
	if b.aead == nil {
		return "", ErrUnavailable
	}
	n := b.aead.NonceSize()
	if len(sealed) < n+b.aead.Overhead() {
		return "", errors.New("secretbox: sealed secret too short")
	}
	plain, err := b.aead.Open(nil, sealed[:n], sealed[n:], nil)
	if err != nil {
		return "", fmt.Errorf("secretbox open: %w", err)
	}
	return string(plain), nil
}
