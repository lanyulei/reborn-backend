// Package ingressauth 定义通知管道 ingress 的 HMAC 鉴权协议（生产者侧签名 +
// 服务端校验共用同一规范串构造），供外部生产者 SDK 与管道 server 复用。
//
// 协议：
//
//	Authorization: HMAC <producer_id>:<sig>:<ts>
//	key  = SHA-256(token 明文)            // token 形如 npt_xxx；明文永不上线，服务端库存为该密钥的主密钥封存值
//	sig  = hex(HMAC-SHA256(key, canonical))
//	canonical = ts + "\n" + METHOD + "\n" + PATH + "\n" + hex(sha256(body))
//
// ts 为 Unix 秒；服务端校验 |now-ts| ≤ 时间窗（缺省 ±5min），并在时间窗内拒绝重复
// 出现的同一签名（防重放；事件全部带 dedup_key 的请求除外——原样重发由 dedup 门闩
// 幂等应答）。生产者每次请求（含不带 dedup_key 的重试）都应以当前时间重新签名。body
// 哈希把签名绑定到请求体。数据面只承诺 HTTP（gRPC 无法等价绑定请求体，不暴露）。
package ingressauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Scheme 是 Authorization 头的方案名。
const Scheme = "HMAC"

// ErrMalformed 头格式不符合 "HMAC <id>:<sig>:<ts>"。
var ErrMalformed = errors.New("ingressauth: malformed authorization header")

// Credential 是已解析的 Authorization 头。
type Credential struct {
	ProducerID string
	Signature  string
	Timestamp  int64
}

// DeriveKey 由 token 明文派生 HMAC 密钥（= SHA-256(token)；服务端 token_hash 列存其封存值）。
func DeriveKey(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// Canonical 构造待签名规范串。
func Canonical(ts int64, method, path string, body []byte) string {
	bodySum := sha256.Sum256(body)
	return strconv.FormatInt(ts, 10) + "\n" + method + "\n" + path + "\n" + hex.EncodeToString(bodySum[:])
}

// Sign 计算签名（hex）。
func Sign(key []byte, canonical string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

// Header 生产者侧便捷构造：给定 producer_id 与 token 明文，生成完整 Authorization 头值。
func Header(producerID, token, method, path string, body []byte, now time.Time) string {
	ts := now.Unix()
	sig := Sign(DeriveKey(token), Canonical(ts, method, path, body))
	return fmt.Sprintf("%s %s:%s:%d", Scheme, producerID, sig, ts)
}

// Parse 解析 Authorization 头值。
func Parse(header string) (Credential, error) {
	scheme, rest, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(scheme, Scheme) {
		return Credential{}, ErrMalformed
	}
	parts := strings.Split(strings.TrimSpace(rest), ":")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return Credential{}, ErrMalformed
	}
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Credential{}, ErrMalformed
	}
	return Credential{ProducerID: parts[0], Signature: parts[1], Timestamp: ts}, nil
}

// Verify 用库存密钥校验签名（常量时间比较）。
func Verify(key []byte, canonical, signature string) bool {
	want := Sign(key, canonical)
	return hmac.Equal([]byte(want), []byte(signature))
}
