// Package sigv4 AWS Signature Version 4 请求签名（S3 兼容对象存储、云资源采集共用）。
// 纯计算、不发请求：调用方自己设置请求头并把返回值放进 Authorization。
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// Algorithm 签名算法标识。
const Algorithm = "AWS4-HMAC-SHA256"

// DateFormat X-Amz-Date 头的时间格式（UTC）。
const DateFormat = "20060102T150405Z"

func hmacSHA256(key []byte, msg string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msg))
	return mac.Sum(nil)
}

// HashHex 请求体的 SHA-256（hex），即签名里的 payload hash。
func HashHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Authorization 计算 Authorization 头。path 为规范 URI（已转义），query 为规范查询串；
// headers 为参与签名的头（名 → 值，全部参与）；payloadHash 为请求体 SHA-256 hex；
// 时间取 now（UTC，须与 X-Amz-Date 头一致）。
func Authorization(method, path, query string, headers map[string]string, payloadHash string, now time.Time, region, service, accessKey, secret string) string {
	lower := make(map[string]string, len(headers))
	names := make([]string, 0, len(headers))
	for k, v := range headers {
		name := strings.ToLower(k)
		lower[name] = v
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + strings.TrimSpace(lower[k]) + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{method, path, query, canonicalHeaders.String(), signedHeaders, payloadHash}, "\n")
	date := now.UTC().Format("20060102")
	scope := date + "/" + region + "/" + service + "/aws4_request"
	stringToSign := strings.Join([]string{Algorithm, now.UTC().Format(DateFormat), scope, HashHex([]byte(canonicalRequest))}, "\n")
	key := hmacSHA256([]byte("AWS4"+secret), date)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, service)
	key = hmacSHA256(key, "aws4_request")
	return Algorithm + " Credential=" + accessKey + "/" + scope + ", SignedHeaders=" + signedHeaders +
		", Signature=" + hex.EncodeToString(hmacSHA256(key, stringToSign))
}
