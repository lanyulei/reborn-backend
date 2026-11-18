package k8scontract

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// 身份头：core 向数据面的每个请求都带。数据面只信任签名正确且在时效内的身份，不校验用户令牌。
const (
	IdentityHeader  = "X-Reborn-Identity"
	SignatureHeader = "X-Reborn-Signature"
	// ClusterHeader 目标集群 ID（与集群无关的操作不带）。
	ClusterHeader = "X-Reborn-Cluster"
	// IdentityTTL 身份头的有效期；流式连接只在建立时校验。
	IdentityTTL = 60 * time.Second
	// MinInternalKeyBytes 内部密钥的最小长度。
	MinInternalKeyBytes = 32
)

// Identity 发起操作的平台用户。
type Identity struct {
	UserID   string   `json:"userId"`
	Username string   `json:"username"`
	RoleIDs  []string `json:"roleIds"`
	IsAdmin  bool     `json:"isAdmin"`
	ClientIP string   `json:"clientIp,omitempty"`
	// RequestID 请求 ID，写入审计。
	RequestID string `json:"requestId,omitempty"`
	// IssuedAt 签发时间（Unix 秒）。
	IssuedAt int64 `json:"issuedAt"`
	// Emergency 紧急变更（持有免审批权限的用户在需审批的集群上直接执行），写入审计。
	Emergency bool `json:"emergency,omitempty"`
	// ChangeID 关联的变更单（数据面执行已批准的变更时由其自行构造，不经 core 转发）。
	ChangeID string `json:"changeId,omitempty"`
}

// 身份校验错误。
var (
	ErrIdentityMissing = errors.New("identity header missing")
	ErrIdentityInvalid = errors.New("identity signature invalid")
	ErrIdentityExpired = errors.New("identity expired")
	ErrInternalKey     = errors.New("internal key must be at least 32 bytes")
)

// SignIdentity 把身份写入请求头并签名。
func SignIdentity(h http.Header, key []byte, id Identity) error {
	if len(key) < MinInternalKeyBytes {
		return ErrInternalKey
	}
	raw, err := json.Marshal(id)
	if err != nil {
		return err
	}
	enc := base64.RawURLEncoding.EncodeToString(raw)
	h.Set(IdentityHeader, enc)
	h.Set(SignatureHeader, base64.RawURLEncoding.EncodeToString(sign(key, enc)))
	return nil
}

// VerifyIdentity 校验请求头里的身份：签名正确、未过期（允许 5 秒的时钟超前）、带用户 ID。
func VerifyIdentity(h http.Header, key []byte, now time.Time) (Identity, error) {
	var id Identity
	if len(key) < MinInternalKeyBytes {
		return id, ErrInternalKey
	}
	enc, sig := h.Get(IdentityHeader), h.Get(SignatureHeader)
	if enc == "" || sig == "" {
		return id, ErrIdentityMissing
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, sign(key, enc)) {
		return id, ErrIdentityInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return id, ErrIdentityInvalid
	}
	if err := json.Unmarshal(raw, &id); err != nil || id.UserID == "" {
		return Identity{}, ErrIdentityInvalid
	}
	issued := time.Unix(id.IssuedAt, 0)
	if now.Sub(issued) > IdentityTTL || issued.Sub(now) > 5*time.Second {
		return Identity{}, ErrIdentityExpired
	}
	return id, nil
}

func sign(key []byte, payload string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return mac.Sum(nil)
}
