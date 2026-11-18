// Package netguard 出站请求的目标地址防护（SSRF）：服务端按用户配置的地址发起请求时
// （Webhook 回调、通知渠道等），不允许指向本机、内网或经转换前缀可绕到内网的地址。
//
// 两道门共用一份规则：保存配置时校验字面量（UnsafeHost）；每次连接时对解析出的真实地址
// 再校验（Dialer.DialContext，免疫 DNS 重绑定）。
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

// ErrHostBlocked 目标解析到不允许的地址：确定性失败，重试无益。
var ErrHostBlocked = errors.New("host resolves to a private or local address")

// blockedNets 除回环 / 私网 / 链路本地 / 组播 / 未指定之外，同样不允许的地址段：经它们可以
// 绕到内网（NAT64、6to4 把 IPv4 地址嵌在 IPv6 里），或本就不是可路由的公网地址。
var blockedNets = func() []*net.IPNet {
	cidrs := []string{
		"0.0.0.0/8",     // 「本网络」
		"100.64.0.0/10", // 运营商级 NAT
		"198.18.0.0/15", // 基准测试
		"240.0.0.0/4",   // 保留（含广播）
		"64:ff9b::/96",  // NAT64
		"2002::/16",     // 6to4
	}
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(err) // 常量表写错即启动期暴露
		}
		out = append(out, n)
	}
	return out
}()

// BlockedIP 报告地址是否不允许作为出站请求的目标。nil 视为不允许。
func BlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	for _, n := range blockedNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// UnsafeHost 对主机名做字面量校验（保存配置时的第一道门）：空、localhost / *.localhost、
// 以及属于 BlockedIP 的字面 IP 不允许。其它主机名不在这里解析——解析结果在连接时校验。
func UnsafeHost(host string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "" || h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return true
	}
	if ip := net.ParseIP(strings.Trim(h, "[]")); ip != nil {
		return BlockedIP(ip)
	}
	return false
}

// Resolver 域名解析（*net.Resolver 满足；测试可替换）。
type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// Dialer 带地址防护的拨号器：先解析、逐地址校验，再连接。任一解析地址不允许即整体拒绝
// （不能「挑一个公网地址连」——对端可返回混合记录诱导）。零值不可用，经 NewDialer 构造。
type Dialer struct {
	// Resolver 域名解析器。
	Resolver Resolver
	// AllowInsecure 跳过地址校验（仅测试脚手架使用）。
	AllowInsecure bool

	dialer *net.Dialer
}

// NewDialer 构造拨号器；timeout 为单次建连超时。
func NewDialer(timeout time.Duration) *Dialer {
	return &Dialer{Resolver: net.DefaultResolver, dialer: &net.Dialer{Timeout: timeout}}
}

// DialContext 可直接用作 http.Transport.DialContext。目标不允许时返回的错误包着
// ErrHostBlocked，信息里带主机名与命中的地址（不含 URL 的路径 / 查询串）。
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if d.AllowInsecure {
		return d.dialer.DialContext(ctx, network, addr)
	}
	ips, err := d.Resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("host %q resolves to no address", host)
	}
	for _, ip := range ips {
		if BlockedIP(ip.IP) {
			return nil, fmt.Errorf("%w: %s → %s", ErrHostBlocked, host, ip.IP)
		}
	}
	var last error
	for _, ip := range ips {
		conn, err := d.dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}
