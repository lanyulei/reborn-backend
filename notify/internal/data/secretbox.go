package data

import (
	"github.com/lanyulei/reborn/backend/kit/secretbox"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"
)

// SecretBox 渠道凭据解密器 = kit/secretbox.Box（与 core 写路径同一实现：AES-256-GCM，
// key = sha256(主密钥)，nonce||ct）。管道只调用 Open（读路径）。
type SecretBox = secretbox.Box

// ErrMasterKeyUnavailable 主密钥未配置。
var ErrMasterKeyUnavailable = secretbox.ErrUnavailable

// NewSecretBox 由配置构造（主密钥为空 → 未启用：带 secret 渠道标记不健康）。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	return secretbox.New(c.GetMaster().GetKey())
}
