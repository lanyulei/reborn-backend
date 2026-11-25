package netguard

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestBlockedIP(t *testing.T) {
	t.Parallel()
	blocked := []string{
		"127.0.0.1", "127.255.255.254", "::1", "10.0.0.1", "172.16.5.5", "172.31.255.255", "192.168.1.1", "fc00::1", "fd12::1",
		"169.254.169.254", "fe80::1", "224.0.0.1", "ff02::1", "0.0.0.0", "::", "100.64.0.9", "100.127.255.255",
		"0.1.2.3", "198.18.0.1", "198.19.255.255", "240.0.0.1", "255.255.255.255",
		"64:ff9b::a00:1", "2002:a00:1::1",
		"::ffff:10.0.0.1", "::ffff:127.0.0.1", "::ffff:100.64.0.1",
	}
	for _, s := range blocked {
		if !BlockedIP(net.ParseIP(s)) {
			t.Errorf("%s must be blocked", s)
		}
	}
	allowed := []string{"8.8.8.8", "1.1.1.1", "100.128.0.1", "172.32.0.1", "198.17.255.255", "198.20.0.1", "2606:4700::1111", "::ffff:8.8.8.8", "2001:db9::1"}
	for _, s := range allowed {
		if BlockedIP(net.ParseIP(s)) {
			t.Errorf("%s must be allowed", s)
		}
	}
	if !BlockedIP(nil) {
		t.Error("nil must be blocked")
	}
}

func TestUnsafeHost(t *testing.T) {
	t.Parallel()
	for _, h := range []string{"", "localhost", "LOCALHOST.", "a.localhost", "127.0.0.1", "[::1]", "::1", "10.0.0.1", "64:ff9b::7f00:1", "0.0.0.0"} {
		if !UnsafeHost(h) {
			t.Errorf("%q must be unsafe", h)
		}
	}
	for _, h := range []string{"hooks.example.com", "notlocalhost", "8.8.8.8", "2606:4700::1111", "localhost.example.com"} {
		if UnsafeHost(h) {
			t.Errorf("%q must be allowed", h)
		}
	}
}

type fakeResolver map[string][]net.IP

func (f fakeResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	out := make([]net.IPAddr, 0, len(f[host]))
	for _, ip := range f[host] {
		out = append(out, net.IPAddr{IP: ip})
	}
	return out, nil
}

func TestDialer(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cerr := ln.Close(); cerr != nil {
			t.Errorf("close listener: %v", cerr)
		}
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	ctx := context.Background()

	d := NewDialer(time.Second)
	d.Resolver = fakeResolver{
		"rebind.example.com": {net.ParseIP("127.0.0.1")},
		// 混合记录：有一个内网地址就整体拒绝，不去连那个公网地址。
		"mixed.example.com": {net.ParseIP("8.8.8.8"), net.ParseIP("10.0.0.1")},
		"nat64.example.com": {net.ParseIP("64:ff9b::7f00:1")},
		"empty.example.com": nil,
	}
	for _, host := range []string{"rebind.example.com", "mixed.example.com", "nat64.example.com"} {
		if _, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, port)); !errors.Is(err, ErrHostBlocked) {
			t.Errorf("%s: error = %v, want ErrHostBlocked", host, err)
		}
	}
	// 字面 IP 同样经解析器校验（标准解析器对字面 IP 原样返回）。
	d2 := NewDialer(time.Second)
	if _, err := d2.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port)); !errors.Is(err, ErrHostBlocked) {
		t.Errorf("literal loopback: error = %v, want ErrHostBlocked", err)
	}
	if _, err := d.DialContext(ctx, "tcp", net.JoinHostPort("empty.example.com", port)); err == nil || errors.Is(err, ErrHostBlocked) {
		t.Errorf("no address: error = %v, want a plain error", err)
	}
	if _, err := d.DialContext(ctx, "tcp", "no-port"); err == nil {
		t.Error("malformed address must fail")
	}
	d.AllowInsecure = true
	conn, err := d.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("AllowInsecure dial error = %v", err)
	}
	_ = conn.Close()
}
