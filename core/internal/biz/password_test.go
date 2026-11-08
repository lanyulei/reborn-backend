package biz

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
)

// testHasherConfig 返回测试用 Argon2id 参数（OWASP 下限，加速用例）。
func testHasherConfig() *conf.Security {
	return &conf.Security{Password: &conf.Security_Password{
		MemoryKb:    minArgon2MemoryKB,
		Iterations:  minArgon2Iterations,
		Parallelism: minArgon2Parallelism,
	}}
}

func TestNewPasswordHasher(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  *conf.Security
	}{
		{name: "nil security", cfg: nil},
		{name: "nil password", cfg: &conf.Security{}},
		{
			name: "memory below floor",
			cfg: &conf.Security{Password: &conf.Security_Password{
				MemoryKb: 1024, Iterations: 3, Parallelism: 1,
			}},
		},
		{
			name: "iterations below floor",
			cfg: &conf.Security{Password: &conf.Security_Password{
				MemoryKb: 19456, Iterations: 1, Parallelism: 1,
			}},
		},
		{
			name: "parallelism below floor",
			cfg: &conf.Security{Password: &conf.Security_Password{
				MemoryKb: 19456, Iterations: 2, Parallelism: 0,
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewPasswordHasher(tc.cfg); err == nil {
				t.Fatalf("NewPasswordHasher(%v) error = nil, want config error", tc.cfg)
			}
		})
	}

	h, err := NewPasswordHasher(testHasherConfig())
	if err != nil {
		t.Fatalf("NewPasswordHasher() error = %v", err)
	}
	if h == nil {
		t.Fatal("NewPasswordHasher() = nil")
	}
}

func TestPasswordHasher_Hash(t *testing.T) {
	t.Parallel()
	h, err := NewPasswordHasher(testHasherConfig())
	if err != nil {
		t.Fatalf("NewPasswordHasher() error = %v", err)
	}

	phc, err := h.Hash("s3cret-password")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	// PHC 格式：$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		t.Fatalf("Hash() = %q, want argon2id PHC format", phc)
	}
	if want := "m=19456,t=2,p=1"; parts[3] != want {
		t.Fatalf("Hash() params = %q, want %q", parts[3], want)
	}

	// 随机盐：同一明文两次哈希必须产生不同串。
	again, err := h.Hash("s3cret-password")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	if again == phc {
		t.Fatal("Hash() twice produced identical strings, salt is not random")
	}
}

func TestPasswordHasher_Verify(t *testing.T) {
	t.Parallel()
	h, err := NewPasswordHasher(testHasherConfig())
	if err != nil {
		t.Fatalf("NewPasswordHasher() error = %v", err)
	}
	phc, err := h.Hash("s3cret-password")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}

	if !h.Verify("s3cret-password", phc) {
		t.Fatal("Verify(correct) = false, want true")
	}
	if h.Verify("wrong-password", phc) {
		t.Fatal("Verify(wrong) = true, want false")
	}

	// 篡改密文后必须校验失败。
	tampered := phc[:len(phc)-2] + "AA"
	if h.Verify("s3cret-password", tampered) {
		t.Fatal("Verify(tampered key) = true, want false")
	}
}

func TestPasswordHasher_VerifyMalformed(t *testing.T) {
	t.Parallel()
	h, err := NewPasswordHasher(testHasherConfig())
	if err != nil {
		t.Fatalf("NewPasswordHasher() error = %v", err)
	}
	for _, bad := range []string{
		"",
		"plaintext",
		"$argon2i$v=19$m=19456,t=2,p=1$c2FsdA$a2V5",
		"$argon2id$v=16$m=19456,t=2,p=1$c2FsdA$a2V5",
		"$argon2id$v=19$m=19456,t=2$c2FsdA$a2V5",
		"$argon2id$v=19$m=19456,t=2,p=1$!!$a2V5",
		"$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$!!",
	} {
		if h.Verify("s3cret-password", bad) {
			t.Fatalf("Verify(%q) = true, want false", bad)
		}
	}
}

// Verify 按串内参数派生：不同参数配置的哈希器可互相校验（参数在线升级兼容）。
func TestPasswordHasher_VerifyAcrossParams(t *testing.T) {
	t.Parallel()
	strong, err := NewPasswordHasher(&conf.Security{Password: &conf.Security_Password{
		MemoryKb: 32768, Iterations: 3, Parallelism: 2,
	}})
	if err != nil {
		t.Fatalf("NewPasswordHasher(strong) error = %v", err)
	}
	weak, err := NewPasswordHasher(testHasherConfig())
	if err != nil {
		t.Fatalf("NewPasswordHasher(weak) error = %v", err)
	}

	phc, err := strong.Hash("s3cret-password")
	if err != nil {
		t.Fatalf("Hash() error = %v", err)
	}
	if !weak.Verify("s3cret-password", phc) {
		t.Fatal("Verify(strong-hash by weak hasher) = false, want true")
	}
}

// TestDecodeField 解码契约：b64$ 前缀走标准 Base64 解码，无前缀原样返回。
func TestDecodeField(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr error
	}{
		{"plaintext passthrough", "plain-secret", "plain-secret", nil},
		{"empty passthrough", "", "", nil},
		{"b64 decoded", "b64$" + base64.StdEncoding.EncodeToString([]byte("plain-secret")), "plain-secret", nil},
		{"b64 multibyte", "b64$" + base64.StdEncoding.EncodeToString([]byte("密码p@ss")), "密码p@ss", nil},
		{"invalid base64", "b64$!!not-base64!!", "", ErrFieldDecode},
	}
	for _, tc := range cases {
		got, err := DecodeField(tc.in)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: DecodeField() error = %v, want %v", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: DecodeField() = (%q, %v), want (%q, nil)", tc.name, got, err, tc.want)
		}
	}
}

// TestDecodePassword 明文长度契约收敛在解码后：8-64 之外拒绝，边界值放行。
func TestDecodePassword(t *testing.T) {
	t.Parallel()
	b64 := func(plain string) string { return "b64$" + base64.StdEncoding.EncodeToString([]byte(plain)) }
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr error
	}{
		{"plaintext valid", "plain-secret", "plain-secret", nil},
		{"encoded valid", b64("plain-secret"), "plain-secret", nil},
		{"plaintext short", "1234567", "", ErrPasswordLength},
		{"decoded short", b64("1234567"), "", ErrPasswordLength},
		{"decoded 8 boundary", b64("12345678"), "12345678", nil},
		{"decoded 64 boundary", b64(strings.Repeat("a", 64)), strings.Repeat("a", 64), nil},
		{"decoded 65 too long", b64(strings.Repeat("a", 65)), "", ErrPasswordLength},
		{"bad base64", "b64$!!", "", ErrFieldDecode},
	}
	for _, tc := range cases {
		got, err := decodePassword(tc.in)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: decodePassword() error = %v, want %v", tc.name, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: decodePassword() = (%q, %v), want (%q, nil)", tc.name, got, err, tc.want)
		}
	}
}
