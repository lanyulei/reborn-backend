package kube

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ biz.KubeProxier = (*Manager)(nil)

// kubeProxyDropHeaders 不转给集群的请求头：凭据与身份只来自连接配置与用户身份传输层
// （Authorization 由 bearerRoundTripper 覆盖、Impersonate-* 由 userRoundTripper 重建，这里再删一遍是纵深防御），
// 平台内部头与浏览器上下文头与集群无关。
var kubeProxyDropHeaders = []string{
	"Authorization", "Cookie", "Proxy-Authorization",
	k8scontract.IdentityHeader, k8scontract.SignatureHeader, k8scontract.ClusterHeader, k8scontract.KubeTokenHeader,
	"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-Ip",
	"X-Remote-User", "X-Remote-Group",
}

// upgradeTransport 按请求选择传输层：协议升级（exec / attach）只能走 HTTP/1.1。两者都是用户身份的传输层。
type upgradeTransport struct {
	h2, h1 http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t upgradeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Upgrade") != "" {
		return t.h1.RoundTrip(r)
	}
	return t.h2.RoundTrip(r)
}

// KubeHandler implements biz.KubeProxier：把请求（r.URL.Path 为集群 API 路径）以 id 的身份原样转给集群。
// 只能经用户身份的传输层发出：没有合法身份的请求不会离开本进程。响应不缓冲（watch、日志逐块刷出）。
func (m *Manager) KubeHandler(cl *biz.Cluster, id biz.Identity) (http.Handler, error) {
	c, err := m.conn(cl.ID)
	if err != nil {
		return nil, err
	}
	if err := validIdentity(id); err != nil {
		return nil, err
	}
	target, err := url.Parse(c.host)
	if err != nil {
		return nil, biz.ErrClusterOffline("cluster address is invalid")
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// 不用 SetURL 的路径拼接语义之外的任何入站信息：目标主机恒为集群地址。
			pr.Out.URL.Scheme, pr.Out.URL.Host = target.Scheme, target.Host
			pr.Out.URL.Path = strings.TrimRight(target.Path, "/") + pr.In.URL.Path
			pr.Out.URL.RawPath = ""
			pr.Out.Host = target.Host
			for _, h := range kubeProxyDropHeaders {
				pr.Out.Header.Del(h)
			}
			stripImpersonation(pr.Out.Header)
		},
		Transport:     upgradeTransport{h2: c.userRT, h1: c.userH1},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			be := biz.AsError(mapError(err))
			code := be.Code
			if code < 400 || be.Reason == k8scontract.ReasonUnspecified {
				code = http.StatusBadGateway
			}
			writeKubeStatus(w, code, "cluster request failed: "+be.Message)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 生命周期：跟随请求，也跟随集群连接（断开集群时在途的 watch / exec 随之结束）。
		ctx, cancel := context.WithCancel(context.WithValue(r.Context(), identityKey{}, id))
		defer cancel()
		stop := context.AfterFunc(c.ctx, cancel)
		defer stop()
		rp.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}

// writeKubeStatus 以集群 API 的 Status 对象写出错误（kubectl 能直接展示 message）。
func writeKubeStatus(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure, Message: message, Code: int32(code), //nolint:gosec // HTTP 状态码
		Reason: metav1.StatusReason(http.StatusText(code)),
	})
}
