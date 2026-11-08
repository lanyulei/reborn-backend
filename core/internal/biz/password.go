package biz

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
	"golang.org/x/crypto/argon2"
)

// b64FieldPrefix 标识前端 Base64 编码的字段值（标准带 padding 编码）。
const b64FieldPrefix = "b64$"

// 明文密码长度契约（原 proto min_len/max_len，因编码值放宽 proto 上限后
// 收敛到解码后断言）。
const (
	minPasswordLen = 8
	maxPasswordLen = 64
)

// 密码字段解码哨兵：统一 reason FIELD_DECODE，前端可按 reason 一致提示。
var (
	// ErrFieldDecode 值携带 b64$ 前缀但 Base64 解码失败。
	ErrFieldDecode = errors.BadRequest("FIELD_DECODE", "invalid base64")
	// ErrPasswordLength 解码后的明文密码长度越界。
	ErrPasswordLength = errors.BadRequest("FIELD_DECODE", "password must be 8-64 characters")
)

// DecodeField 解码前端编码的字符串字段：b64$ 前缀 → Base64 解码；
// 无前缀原样返回（兼容直传明文的旧客户端，双向可混用）。
func DecodeField(s string) (string, error) {
	if !strings.HasPrefix(s, b64FieldPrefix) {
		return s, nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(s, b64FieldPrefix))
	if err != nil {
		return "", ErrFieldDecode
	}
	return string(raw), nil
}

// decodePassword 解码密码字段并断言明文长度 8-64：proto 层只能约束编码值
// （max_len 256），明文长度契约在解码后统一生效。
func decodePassword(s string) (string, error) {
	plain, err := DecodeField(s)
	if err != nil {
		return "", err
	}
	if n := len(plain); n < minPasswordLen || n > maxPasswordLen {
		return "", ErrPasswordLength
	}
	return plain, nil
}

// Argon2id 参数契约：盐与密钥长度固定，时/空/并行参数从配置注入，
// 构造时校验不得低于 OWASP Password Storage Cheat Sheet 下限。
const (
	argon2Version = 0x13 // PHC version 19
	argon2SaltLen = 16
	argon2KeyLen  = 32
	// OWASP 下限：m ≥ 19 MiB、t ≥ 2、p ≥ 1；低于此线的配置视为配置错误。
	minArgon2MemoryKB    = 19456
	minArgon2Iterations  = 2
	minArgon2Parallelism = 1
	// maxArgon2Parallelism 是并行度上限（uint8 边界），超出视为配置/数据错误。
	maxArgon2Parallelism = 255
)

// PasswordHasher 是 Argon2id 密码哈希器；输出与校验均使用 PHC 标准格式串
// （$argon2id$v=19$m=...,t=...,p=...$<salt>$<hash>），盐为每次随机的 16 字节。
// 哈希串自带参数，Verify 按串内参数重新派生，天然支持参数在线升级后的旧串校验。
type PasswordHasher struct {
	time        uint32
	memoryKB    uint32
	parallelism uint8
}

// NewPasswordHasher 按配置构造哈希器；参数缺失或低于 OWASP 下限返回错误，
// 在 wire 装配阶段快速失败，禁止弱参数静默生效。
func NewPasswordHasher(c *conf.Security) (*PasswordHasher, error) {
	p := c.GetPassword()
	if p == nil {
		return nil, fmt.Errorf("password hasher config is required")
	}
	memoryKB := p.GetMemoryKb()
	iterations := p.GetIterations()
	parallelism := p.GetParallelism()
	if memoryKB < minArgon2MemoryKB {
		return nil, fmt.Errorf("password memory_kb %d is below OWASP minimum %d", memoryKB, minArgon2MemoryKB)
	}
	if iterations < minArgon2Iterations {
		return nil, fmt.Errorf("password iterations %d is below OWASP minimum %d", iterations, minArgon2Iterations)
	}
	if parallelism < minArgon2Parallelism {
		return nil, fmt.Errorf("password parallelism %d is below OWASP minimum %d", parallelism, minArgon2Parallelism)
	}
	if parallelism > maxArgon2Parallelism {
		return nil, fmt.Errorf("password parallelism %d exceeds maximum %d", parallelism, maxArgon2Parallelism)
	}
	return &PasswordHasher{
		time:        iterations,
		memoryKB:    memoryKB,
		parallelism: uint8(parallelism),
	}, nil
}

// Hash 对明文密码生成 Argon2id PHC 串；随机盐来自 crypto/rand。
func (h *PasswordHasher) Hash(password string) (string, error) {
	salt := make([]byte, argon2SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("read random salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, h.time, h.memoryKB, h.parallelism, argon2KeyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2Version, h.memoryKB, h.time, h.parallelism,
		enc.EncodeToString(salt), enc.EncodeToString(key)), nil
}

// Verify 常数时间校验明文密码与 PHC 串是否匹配；格式非法一律返回 false。
// 当前供测试与后续登录用例使用。
func (h *PasswordHasher) Verify(password, phc string) bool {
	p, err := parsePHC(phc)
	if err != nil {
		return false
	}
	key := argon2.IDKey([]byte(password), p.salt, p.time, p.memoryKB, p.parallelism, p.keyLen)
	return subtle.ConstantTimeCompare(key, p.key) == 1
}

// phcParams 是解析后的 PHC 串参数。
type phcParams struct {
	time        uint32
	memoryKB    uint32
	parallelism uint8
	keyLen      uint32
	salt        []byte
	key         []byte
}

// parsePHC 解析 argon2id PHC 串；仅接受 version 19。
func parsePHC(phc string) (phcParams, error) {
	parts := strings.Split(phc, "$")
	// Split 结果：["", "argon2id", "v=19", "m=..,t=..,p=..", salt, key]
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return phcParams{}, fmt.Errorf("invalid argon2 phc string")
	}
	version, err := strconv.ParseUint(strings.TrimPrefix(parts[2], "v="), 10, 32)
	if err != nil || version != argon2Version {
		return phcParams{}, fmt.Errorf("unsupported argon2 version %q", parts[2])
	}
	kv := map[string]uint32{}
	for _, f := range strings.Split(parts[3], ",") {
		name, val, ok := strings.Cut(f, "=")
		if !ok {
			return phcParams{}, fmt.Errorf("invalid argon2 params %q", f)
		}
		n, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return phcParams{}, fmt.Errorf("invalid argon2 param %q", f)
		}
		kv[name] = uint32(n)
	}
	enc := base64.RawStdEncoding
	salt, err := enc.DecodeString(parts[4])
	if err != nil {
		return phcParams{}, fmt.Errorf("invalid argon2 salt: %w", err)
	}
	key, err := enc.DecodeString(parts[5])
	if err != nil {
		return phcParams{}, fmt.Errorf("invalid argon2 key: %w", err)
	}
	// 参数完整性防御：缺项或低于 argon2 内部下限（memory ≥ 8×threads）的串
	// 不得进入派生函数（x/crypto 会 panic）。
	threads := kv["p"]
	keyLen := len(key)
	if kv["t"] == 0 || kv["m"] == 0 || threads == 0 {
		return phcParams{}, fmt.Errorf("incomplete argon2 params %q", parts[3])
	}
	if threads > maxArgon2Parallelism {
		return phcParams{}, fmt.Errorf("argon2 parallelism %d exceeds maximum %d", threads, maxArgon2Parallelism)
	}
	if kv["m"] < 8*threads {
		return phcParams{}, fmt.Errorf("argon2 memory %d below 8x parallelism %d", kv["m"], threads)
	}
	if len(salt) < 8 || keyLen == 0 {
		return phcParams{}, fmt.Errorf("argon2 salt/key length invalid")
	}
	if keyLen > math.MaxInt32 {
		return phcParams{}, fmt.Errorf("argon2 key length %d invalid", keyLen)
	}
	return phcParams{
		time:        kv["t"],
		memoryKB:    kv["m"],
		parallelism: uint8(threads),
		keyLen:      uint32(keyLen),
		salt:        salt,
		key:         key,
	}, nil
}
