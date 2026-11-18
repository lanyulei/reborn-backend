// Package agent 是部署在目标集群（或其转发节点）上的 Agent：主动连到数据面的隧道入口，
// 把隧道上的每条流当作一条 HTTP/1.1 连接，反向代理到**启动时配置的那一个** API Server。
//
// 安全边界：流里不携带目标地址——请求里的绝对 URI、Host 头一律被忽略；不跟随跳转；拒绝 CONNECT；
// 入站的 Authorization 一律丢弃，换上 Agent 自己的凭据；Impersonate-* 头原样保留（数据面据此以用户身份访问）。
package agent

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"

	"k8s.io/client-go/rest"
)

// dropHeaders 入站请求里一律丢弃的头：凭据只能来自 Agent 自己的配置。
var dropHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"}

// NewProxy 由集群访问配置构造反向代理。普通请求可用 HTTP/2；协议升级（终端）走仅 HTTP/1.1 的传输层。
func NewProxy(cfg *rest.Config, log *slog.Logger) (http.Handler, error) {
	target, err := url.Parse(cfg.Host)
	if err != nil || target.Host == "" || (target.Scheme != "https" && target.Scheme != "http") {
		return nil, fmt.Errorf("api server address %q is not an http(s) URL", cfg.Host)
	}
	target = &url.URL{Scheme: target.Scheme, Host: target.Host, Path: strings.TrimRight(target.Path, "/")}
	base := rest.CopyConfig(cfg)
	base.Impersonate = rest.ImpersonationConfig{} // 身份只来自隧道另一端的 Impersonate-* 头
	rt, err := rest.TransportFor(base)
	if err != nil {
		return nil, fmt.Errorf("build api server transport: %w", err)
	}
	h1cfg := rest.CopyConfig(base)
	h1cfg.NextProtos = []string{"http/1.1"}
	h1, err := rest.TransportFor(h1cfg)
	if err != nil {
		return nil, fmt.Errorf("build api server upgrade transport: %w", err)
	}
	mk := func(t http.RoundTripper) *httputil.ReverseProxy {
		return &httputil.ReverseProxy{
			Transport:     t,
			FlushInterval: -1, // watch、日志等长响应逐块下发
			Rewrite: func(pr *httputil.ProxyRequest) {
				// 目标恒为配置的 API Server：入站请求的 scheme / host / Host 头不参与。
				pr.Out.URL.Scheme, pr.Out.URL.Host = target.Scheme, target.Host
				pr.Out.URL.Path = target.Path + pr.In.URL.Path
				pr.Out.URL.RawPath = ""
				pr.Out.URL.RawQuery = pr.In.URL.RawQuery
				pr.Out.URL.User = nil
				pr.Out.Host = target.Host
				for _, h := range dropHeaders {
					pr.Out.Header.Del(h)
				}
			},
			ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
				log.Warn("api server request failed", "error", err)
				http.Error(w, "agent: api server unreachable", http.StatusBadGateway)
			},
		}
	}
	plain, upgrade := mk(rt), mk(h1)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			http.Error(w, "agent: CONNECT is not allowed", http.StatusMethodNotAllowed)
			return
		}
		// 路径必须是规范形式：API Server 地址带路径前缀时（经网关接入的集群），.. 能跳出这个前缀。
		if p := r.URL.Path; !strings.HasPrefix(p, "/") || (path.Clean(p) != p && path.Clean(p)+"/" != p) {
			http.Error(w, "agent: malformed request path", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Upgrade") != "" {
			upgrade.ServeHTTP(w, r)
			return
		}
		plain.ServeHTTP(w, r)
	}), nil
}
