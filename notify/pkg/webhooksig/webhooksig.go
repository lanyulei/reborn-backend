// Package webhooksig 是通知管道 webhook 出站签名的接收端校验助手（供下游系统 import，
// 不依赖 internal）：头 X-Notify-Signature = "sha256=" + hex(HMAC-SHA256(secret, ts + "." + body))，
// ts 为 X-Notify-Timestamp（Unix 秒）。
package webhooksig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// 出站请求头名。
const (
	HeaderSignature = "X-Notify-Signature"
	HeaderTimestamp = "X-Notify-Timestamp"
	HeaderSendKey   = "X-Notify-Send-Key"
	HeaderEventID   = "X-Notify-Event-Id"
)

// Sign 计算签名头值。
func Sign(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify 常量时间比较签名（不校验时间窗）。
func Verify(secret, ts string, body []byte, header string) bool {
	return hmac.Equal([]byte(Sign(secret, ts, body)), []byte(header))
}

// VerifyWithin 校验签名且 ts 距 now 不超过 skew（重放防护）；ts 非法视为失败。
func VerifyWithin(secret, ts string, body []byte, header string, now time.Time, skew time.Duration) bool {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if d := now.Sub(time.Unix(sec, 0)); d > skew || d < -skew {
		return false
	}
	return Verify(secret, ts, body, header)
}
