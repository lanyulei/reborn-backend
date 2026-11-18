package server

import (
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

// pprofEnv 置为 1 时在内部接口监听上挂出 /debug/pprof/*（规模验证与线上排查用）。缺省关闭，且只应答
// 来自本机回环地址的请求：堆转储里有集群的访问令牌、内部签名密钥与仓库口令，不能让能连到内部接口的
// 任何一方读走。排查时在数据面所在主机上访问（或经 SSH 端口转发）。
const pprofEnv = "K8S_PPROF"

// loopbackOnly 只放行对端地址是回环地址的请求。
func loopbackOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if ip := net.ParseIP(host); err != nil || ip == nil || !ip.IsLoopback() {
			http.Error(w, "pprof is only available from the loopback interface", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func registerPprof(srv *khttp.Server) {
	if os.Getenv(pprofEnv) != "1" {
		return
	}
	srv.Handle("/debug/pprof/", loopbackOnly(http.HandlerFunc(pprof.Index)))
	srv.Handle("/debug/pprof/profile", loopbackOnly(http.HandlerFunc(pprof.Profile)))
	srv.Handle("/debug/pprof/trace", loopbackOnly(http.HandlerFunc(pprof.Trace)))
	srv.Handle("/debug/pprof/symbol", loopbackOnly(http.HandlerFunc(pprof.Symbol)))
	for _, name := range []string{"heap", "allocs", "goroutine", "mutex", "block", "threadcreate"} {
		srv.Handle("/debug/pprof/"+name, loopbackOnly(pprof.Handler(name)))
	}
}
