package biz

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/monitor/backend/core/internal/conf"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/types/known/durationpb"
)

// newTestSigner 按给定有效期构造签发器（密钥满足构造下限，仅测试用）。
func newTestSigner(t *testing.T, accessTTL, refreshTTL time.Duration) *TokenSigner {
	t.Helper()
	s, err := NewTokenSigner(&conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     "unit-test-signing-key-0123456789abcdef",
			AccessTtl:  durationpb.New(accessTTL),
			RefreshTtl: durationpb.New(refreshTTL),
			Issuer:     "unit-test",
		},
	})
	if err != nil {
		t.Fatalf("NewTokenSigner() error = %v", err)
	}
	return s
}

// newDefaultSigner 返回 15m/24h 参数的签发器（与 testfake.NewSigner 同参，
// 供 auth 用例复用）。
func newDefaultSigner(t *testing.T) *TokenSigner {
	t.Helper()
	return newTestSigner(t, 15*time.Minute, 24*time.Hour)
}

// TestNewTokenSigner 锁定构造校验契约：装配期拒绝弱密钥与不合理 TTL。
func TestNewTokenSigner(t *testing.T) {
	t.Parallel()
	valid := &conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     strings.Repeat("k", 32),
			AccessTtl:  durationpb.New(15 * time.Minute),
			RefreshTtl: durationpb.New(24 * time.Hour),
			Issuer:     "monitor",
		},
	}
	cases := []struct {
		name    string
		mutate  func(*conf.Security)
		wantErr bool
	}{
		{"valid", func(*conf.Security) {}, false},
		{"jwt nil", func(c *conf.Security) { c.Jwt = nil }, true},
		{"secret too short", func(c *conf.Security) { c.Jwt.Secret = strings.Repeat("k", 31) }, true},
		{"secret too long", func(c *conf.Security) { c.Jwt.Secret = strings.Repeat("k", 513) }, true},
		{"secret at bounds", func(c *conf.Security) { c.Jwt.Secret = strings.Repeat("k", 512) }, false},
		{"access_ttl below floor", func(c *conf.Security) { c.Jwt.AccessTtl = durationpb.New(59 * time.Second) }, true},
		{"refresh_ttl equal access", func(c *conf.Security) { c.Jwt.RefreshTtl = durationpb.New(15 * time.Minute) }, true},
		{"refresh_ttl below access", func(c *conf.Security) { c.Jwt.RefreshTtl = durationpb.New(time.Minute) }, true},
		{"empty issuer", func(c *conf.Security) { c.Jwt.Issuer = "" }, true},
	}
	for _, tc := range cases {
		c := &conf.Security{Jwt: valid.GetJwt()}
		tc.mutate(c)
		_, err := NewTokenSigner(c)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: NewTokenSigner() error = %v, wantErr = %v", tc.name, err, tc.wantErr)
		}
	}
	// nil 安全：GetJwt 对 nil 接收者返回 nil，须报错而非 panic。
	if _, err := NewTokenSigner(nil); err == nil {
		t.Error("NewTokenSigner(nil) error = nil, want error")
	}
}

// TestTokenSigner_SignParseRoundTrip 签发与解析互为镜像：声明完整回读。
func TestTokenSigner_SignParseRoundTrip(t *testing.T) {
	t.Parallel()
	s := newDefaultSigner(t)

	token, expiresIn, err := s.SignAccess("user-1", "alice", time.Now())
	if err != nil {
		t.Fatalf("SignAccess() error = %v", err)
	}
	if expiresIn != int64(s.AccessTTL()/time.Second) {
		t.Fatalf("SignAccess() expiresIn = %d, want %d", expiresIn, s.AccessTTL()/time.Second)
	}
	claims, err := s.ParseAccess(token)
	if err != nil {
		t.Fatalf("ParseAccess() error = %v", err)
	}
	if claims.Subject != "user-1" || claims.Username != "alice" || claims.Issuer != "unit-test" {
		t.Fatalf("ParseAccess() claims = %+v", claims)
	}
	if claims.ID == "" || claims.IssuedAt == nil || claims.ExpiresAt == nil {
		t.Fatalf("ParseAccess() standard claims incomplete: %+v", claims.RegisteredClaims)
	}
	if d := claims.ExpiresAt.Sub(claims.IssuedAt.Time); d != s.AccessTTL() {
		t.Fatalf("exp-iat = %s, want %s", d, s.AccessTTL())
	}
}

// TestTokenSigner_ParseRejects 拒绝路径：过期、篡改、换密钥、alg 混淆。
func TestTokenSigner_ParseRejects(t *testing.T) {
	t.Parallel()
	s := newDefaultSigner(t)
	now := time.Now()

	expired, _, err := s.SignAccess("user-1", "alice", now.Add(-s.AccessTTL()-time.Second))
	if err != nil {
		t.Fatalf("SignAccess(expired) error = %v", err)
	}
	if _, err := s.ParseAccess(expired); !errors.Is(err, jwt.ErrTokenExpired) {
		t.Fatalf("ParseAccess(expired) error = %v, want ErrTokenExpired", err)
	}

	valid, _, err := s.SignAccess("user-1", "alice", now)
	if err != nil {
		t.Fatalf("SignAccess() error = %v", err)
	}
	// 篡改负载首字符（破坏签名）。
	tampered := valid[:len(valid)-8] + "tampered"
	if _, err := s.ParseAccess(tampered); err == nil {
		t.Fatal("ParseAccess(tampered) error = nil, want error")
	}

	// 换密钥签发：同参数不同密钥，签名不匹配。
	other, err := NewTokenSigner(&conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     "another-signing-key-0123456789abcdef",
			AccessTtl:  durationpb.New(s.AccessTTL()),
			RefreshTtl: durationpb.New(s.RefreshTTL()),
			Issuer:     "unit-test",
		},
	})
	if err != nil {
		t.Fatalf("NewTokenSigner(other) error = %v", err)
	}
	foreign, _, err := other.SignAccess("user-1", "alice", now)
	if err != nil {
		t.Fatalf("SignAccess(foreign) error = %v", err)
	}
	if _, err := s.ParseAccess(foreign); err == nil {
		t.Fatal("ParseAccess(foreign key) error = nil, want error")
	}

	// alg 混淆：HS512 签名（同密钥），解析白名单只接受 HS256。
	confused, err := jwt.NewWithClaims(jwt.SigningMethodHS512, jwt.RegisteredClaims{
		Subject: "user-1", Issuer: "unit-test",
	}).SignedString([]byte("unit-test-signing-key-0123456789abcdef"))
	if err != nil {
		t.Fatalf("sign HS512 token: %v", err)
	}
	if _, err := s.ParseAccess(confused); err == nil {
		t.Fatal("ParseAccess(HS512) error = nil, want error")
	}

	// KeyFunc 直接拒绝非 HMAC 方法族（不依赖解析器前置白名单）。
	if _, err := s.KeyFunc()(&jwt.Token{Method: jwt.SigningMethodRS256}); err == nil {
		t.Fatal("KeyFunc(RS256) error = nil, want error")
	}
}

// TestTokenSigner_ClaimsFactoryDistinct 每次必须返回新实例（中间件并发解析
// 复用同一 Claims 会跨请求串写）。
func TestTokenSigner_ClaimsFactoryDistinct(t *testing.T) {
	t.Parallel()
	s := newDefaultSigner(t)
	f := s.ClaimsFactory()
	if f() == f() {
		t.Fatal("ClaimsFactory() returned same instance twice")
	}
	if _, ok := f().(*AccessClaims); !ok {
		t.Fatal("ClaimsFactory() must construct *AccessClaims")
	}
}

var refreshTokenPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// TestTokenSigner_NewRefreshToken 刷新令牌原文：64 hex、每次唯一。
func TestTokenSigner_NewRefreshToken(t *testing.T) {
	t.Parallel()
	s := newDefaultSigner(t)
	seen := make(map[string]struct{}, 100)
	for range 100 {
		raw, err := s.NewRefreshToken()
		if err != nil {
			t.Fatalf("NewRefreshToken() error = %v", err)
		}
		if !refreshTokenPattern.MatchString(raw) {
			t.Fatalf("NewRefreshToken() = %q, want 64 hex chars", raw)
		}
		if _, ok := seen[raw]; ok {
			t.Fatalf("NewRefreshToken() duplicated %q", raw)
		}
		seen[raw] = struct{}{}
	}
}

// TestHashRefreshToken 哈希确定性与原文不可逆推（等价于固定向量校验）。
func TestHashRefreshToken(t *testing.T) {
	t.Parallel()
	a, b := hashRefreshToken("token-a"), hashRefreshToken("token-a")
	if a != b {
		t.Fatalf("hashRefreshToken() not deterministic: %q != %q", a, b)
	}
	if a == hashRefreshToken("token-b") {
		t.Fatal("hashRefreshToken() collision for distinct inputs")
	}
	if !refreshTokenPattern.MatchString(a) {
		t.Fatalf("hashRefreshToken() = %q, want 64 hex chars", a)
	}
	if strings.Contains(a, "token-a") {
		t.Fatal("hashRefreshToken() leaked raw token")
	}
}

func BenchmarkTokenSigner_SignAccess(b *testing.B) {
	s, err := NewTokenSigner(&conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     "bench-signing-key-0123456789abcdef",
			AccessTtl:  durationpb.New(15 * time.Minute),
			RefreshTtl: durationpb.New(24 * time.Hour),
			Issuer:     "bench",
		},
	})
	if err != nil {
		b.Fatalf("NewTokenSigner() error = %v", err)
	}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := s.SignAccess("bench-user", "bench", now); err != nil {
			b.Fatalf("SignAccess() error = %v", err)
		}
	}
}

func BenchmarkTokenSigner_ParseAccess(b *testing.B) {
	s, err := NewTokenSigner(&conf.Security{
		Jwt: &conf.Security_Jwt{
			Secret:     "bench-signing-key-0123456789abcdef",
			AccessTtl:  durationpb.New(15 * time.Minute),
			RefreshTtl: durationpb.New(24 * time.Hour),
			Issuer:     "bench",
		},
	})
	if err != nil {
		b.Fatalf("NewTokenSigner() error = %v", err)
	}
	token, _, err := s.SignAccess("bench-user", "bench", time.Now())
	if err != nil {
		b.Fatalf("SignAccess() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := s.ParseAccess(token); err != nil {
			b.Fatalf("ParseAccess() error = %v", err)
		}
	}
}
