package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// 未配置可信代理（环境变量为空、只有空白、或没有一条有效条目）时，取来源地址的口径与此前逐例一致。
func TestTrustedProxiesUnsetKeepsLegacyBehaviour(t *testing.T) {
	t.Parallel()
	cases := []struct {
		remote string
		xff    []string
	}{
		{"203.0.113.9:4711", []string{"1.2.3.4"}},
		{"203.0.113.9:4711", nil},
		{"127.0.0.1:5000", []string{"198.51.100.7"}},
		{"10.0.0.2:5000", []string{"1.2.3.4, 198.51.100.7"}},
		{"10.0.0.2:5000", []string{"1.2.3.4", "198.51.100.7"}},
		{"127.0.0.1:5000", []string{"not-an-ip"}},
		{"[::1]:5000", nil},
		{"bogus", []string{"1.2.3.4"}},
	}
	for _, raw := range []string{"", "  ", " , ", "not-a-cidr, 10.0.0.0/99"} {
		tp := parseTrustedProxies(raw, slog.New(slog.DiscardHandler))
		if tp != nil {
			t.Fatalf("parseTrustedProxies(%q) = %+v, want nil (legacy mode)", raw, tp)
		}
		for _, tc := range cases {
			want := systemservice.ResolveClientIP(tc.remote, tc.xff)
			if got := tp.clientIP(tc.remote, tc.xff); got != want {
				t.Errorf("env %q: clientIP(%q, %v) = %q, want legacy %q", raw, tc.remote, tc.xff, got, want)
			}
		}
	}
}

// 配置后：直连的调用方伪造 X-Forwarded-For 换不来新的键；来自可信代理的请求取最右的非可信跳。
func TestTrustedProxiesStrictMode(t *testing.T) {
	t.Parallel()
	tp := parseTrustedProxies("10.0.0.0/24, 192.168.1.10 ,fd00::/8,::ffff:172.16.0.0/108", slog.New(slog.DiscardHandler))
	if tp == nil {
		t.Fatal("parseTrustedProxies() = nil, want strict mode")
	}
	cases := []struct {
		name   string
		remote string
		xff    []string
		want   string
	}{
		// 直连：对端不在清单里，哪怕是私网 / 回环地址（此前会采信 XFF）。
		{"direct private peer with forged xff", "10.9.9.9:4711", []string{"1.2.3.4"}, "10.9.9.9"},
		{"direct loopback peer with forged xff", "127.0.0.1:4711", []string{"1.2.3.4"}, "127.0.0.1"},
		{"direct public peer with forged xff", "203.0.113.9:4711", []string{"1.2.3.4"}, "203.0.113.9"},
		{"forged xff naming a trusted proxy", "10.9.9.9:4711", []string{"10.0.0.5"}, "10.9.9.9"},
		// 经可信代理。
		{"one trusted proxy", "10.0.0.5:4711", []string{"198.51.100.7"}, "198.51.100.7"},
		{"client-forged leftmost ignored", "10.0.0.5:4711", []string{"1.2.3.4, 198.51.100.7"}, "198.51.100.7"},
		{"chain of trusted proxies skipped", "192.168.1.10:4711", []string{"1.2.3.4, 198.51.100.7, 10.0.0.9"}, "198.51.100.7"},
		{"multiple headers joined in order", "10.0.0.5:4711", []string{"198.51.100.7", "10.0.0.9"}, "198.51.100.7"},
		{"untrusted private hop is the key", "10.0.0.5:4711", []string{"1.2.3.4, 10.9.9.9"}, "10.9.9.9"},
		{"ipv6 trusted proxy", "[fd00::1]:4711", []string{"2001:db8::7"}, "2001:db8::7"},
		{"ipv4-mapped peer matches v4 entry", "[::ffff:10.0.0.5]:4711", []string{"198.51.100.7"}, "198.51.100.7"},
		{"ipv4-mapped cidr entry", "172.16.1.1:4711", []string{"198.51.100.7"}, "198.51.100.7"},
		// 退回对端地址。
		{"trusted proxy without xff", "10.0.0.5:4711", nil, "10.0.0.5"},
		{"all hops trusted", "10.0.0.5:4711", []string{"10.0.0.7, 10.0.0.9"}, "10.0.0.5"},
		{"garbage hop stops the walk", "10.0.0.5:4711", []string{"198.51.100.7, not-an-ip"}, "10.0.0.5"},
		{"garbage behind a trusted hop", "10.0.0.5:4711", []string{"198.51.100.7, bogus, 10.0.0.9"}, "10.0.0.5"},
		{"unparseable remote", "bogus", []string{"1.2.3.4"}, ""},
	}
	for _, tc := range cases {
		if got := tp.clientIP(tc.remote, tc.xff); got != tc.want {
			t.Errorf("%s: clientIP() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// 解析不了的条目记 WARN 后忽略，其余条目照常生效。
func TestTrustedProxiesInvalidEntriesAreWarned(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	tp := parseTrustedProxies("10.0.0.0/24,nonsense,300.1.1.1", slog.New(slog.NewTextHandler(buf, nil)))
	if tp == nil || len(tp.nets) != 1 {
		t.Fatalf("parseTrustedProxies() = %+v, want exactly the one valid entry", tp)
	}
	out := buf.String()
	if strings.Count(out, "level=WARN") != 2 || !strings.Contains(out, "nonsense") || !strings.Contains(out, "300.1.1.1") {
		t.Fatalf("log = %s, want one WARN per invalid entry", out)
	}
	if got := tp.clientIP("10.0.0.5:1", []string{"198.51.100.7"}); got != "198.51.100.7" {
		t.Fatalf("clientIP() = %q, want the forwarded hop", got)
	}
}

// 环境变量经 trustedProxiesFromEnv 读入。
func TestTrustedProxiesFromEnv(t *testing.T) {
	t.Setenv(trustedProxiesEnv, "10.0.0.0/24")
	tp := trustedProxiesFromEnv(slog.New(slog.DiscardHandler))
	if got := tp.clientIP("127.0.0.1:1", []string{"1.2.3.4"}); got != "127.0.0.1" {
		t.Fatalf("clientIP() = %q, want the peer (loopback is not in the list)", got)
	}
	t.Setenv(trustedProxiesEnv, "")
	if tp := trustedProxiesFromEnv(slog.New(slog.DiscardHandler)); tp != nil {
		t.Fatalf("trustedProxiesFromEnv() = %+v, want nil when unset", tp)
	}
}
