package server

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strings"

	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// trustedProxiesEnv 可信代理清单的环境变量：逗号分隔的 CIDR 或单个 IP（如 "10.0.0.0/8,192.168.1.10,fd00::/8"）。
// 配置后，集群代理入口只在 TCP 对端属于清单时才采信 X-Forwarded-For；未配置时沿用既有口径。
const trustedProxiesEnv = "KRATOS_TRUSTED_PROXIES"

// trustedProxies 可信代理清单；nil / 空清单表示未启用严格模式。构造后只读，可并发使用。
type trustedProxies struct {
	nets []netip.Prefix
}

// trustedProxiesFromEnv 读取并解析 KRATOS_TRUSTED_PROXIES。解析不了的条目记 WARN 后忽略；没有任何有效条目
// 时返回 nil（不启用严格模式）——严格模式下清单为空等于谁的 X-Forwarded-For 都不信，反向代理后面的全部
// 用户会并成同一个限速键、一人打满全员被拒，宁可退回既有口径并在日志里说明。
func trustedProxiesFromEnv(logger *slog.Logger) *trustedProxies {
	return parseTrustedProxies(os.Getenv(trustedProxiesEnv), logger)
}

// parseTrustedProxies 是 trustedProxiesFromEnv 的纯解析部分。
func parseTrustedProxies(raw string, logger *slog.Logger) *trustedProxies {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	ctx := context.Background() // 启动期日志，没有请求上下文
	tp := &trustedProxies{}
	for item := range strings.SplitSeq(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if p, err := netip.ParsePrefix(item); err == nil {
			// 规整成网络地址并去掉 IPv4-mapped 形态，与 contains 里对地址的处理一致。
			tp.nets = append(tp.nets, netip.PrefixFrom(p.Addr().Unmap(), unmappedBits(p)).Masked())
			continue
		}
		if a, err := netip.ParseAddr(item); err == nil {
			a = a.Unmap()
			tp.nets = append(tp.nets, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		logger.WarnContext(ctx, "ignoring invalid trusted proxy entry", "env", trustedProxiesEnv, "entry", item)
	}
	if len(tp.nets) == 0 {
		logger.WarnContext(ctx, "no valid trusted proxy entry, X-Forwarded-For handling stays in legacy mode", "env", trustedProxiesEnv)
		return nil
	}
	logger.InfoContext(ctx, "k8s proxy entry trusts X-Forwarded-For only from the configured proxies", "env", trustedProxiesEnv, "entries", len(tp.nets))
	return tp
}

// unmappedBits 前缀长度：::ffff:a.b.c.d/N（N ≥ 96）换算成 IPv4 的 N-96，其余原样。
func unmappedBits(p netip.Prefix) int {
	if p.Addr().Is4In6() && p.Bits() >= 96 {
		return p.Bits() - 96
	}
	return p.Bits()
}

// contains 地址是否属于可信代理。
func (tp *trustedProxies) contains(a netip.Addr) bool {
	a = a.Unmap()
	for _, n := range tp.nets {
		if n.Contains(a) {
			return true
		}
	}
	return false
}

// clientIP 集群代理入口的来源地址（认证失败限速的键，也是令牌的最近使用地址与审计来源）。
//
// 未配置可信代理：与 systemservice.ResolveClientIP 完全一致（对端是回环 / 私网地址时取 X-Forwarded-For
// 最右一跳）。配置后：对端不在清单里 → 忽略 X-Forwarded-For，取对端地址——直连的调用方伪造该头换不来新的
// 限速键；对端在清单里 → 从右往左跳过可信代理，取第一个非可信跳（它是最外层可信代理亲眼看到的对端，客户端
// 伪造不了）。遇到解析不了的跳、或整条链都是可信代理，退回对端地址。
func (tp *trustedProxies) clientIP(remoteAddr string, forwardedFor []string) string {
	if tp == nil || len(tp.nets) == 0 {
		return systemservice.ResolveClientIP(remoteAddr, forwardedFor)
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	peer, ok := parseHop(host)
	if !ok {
		return ""
	}
	if !tp.contains(peer) {
		return peer.String()
	}
	hops := strings.Split(strings.Join(forwardedFor, ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, ok := parseHop(hops[i])
		if !ok {
			break
		}
		if !tp.contains(hop) {
			return hop.String()
		}
	}
	return peer.String()
}

// parseHop 解析一跳地址（去空白、兼容 "[v6]"、去掉 zone 与 IPv4-mapped 形态）。
func parseHop(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap().WithZone(""), true
}
