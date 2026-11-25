package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	k8sv1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	k8sservice "github.com/lanyulei/reborn/backend/core/internal/service/k8s"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

const (
	// k8sProxyPrefix 集群代理入口：/api/v1/k8s/proxy/{集群 ID}/<集群 API 路径>。
	k8sProxyPrefix = "/api/v1/k8s/proxy/"
	// k8sFileUploadSuffix 文件上传路由的后缀：请求体上限由数据面按文件传输上限约束，不受通用的 8MiB 限制。
	k8sFileUploadSuffix = "/pod/file:upload"
	// k8sRawMaxDuration 一条原始通道（watch、exec、文件传输）在 core 侧的硬上限。
	k8sRawMaxDuration = 8*time.Hour + 5*time.Minute
)

// k8sBodyLimit 请求体上限过滤器：通用的 8MiB；容器文件上传除外（流式转给数据面，由其按配置的上限截断）。
func k8sBodyLimit(next http.Handler) http.Handler {
	limited := httpx.LimitRequestBody(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v1/k8s/cluster/") && strings.HasSuffix(r.URL.Path, k8sFileUploadSuffix) {
			next.ServeHTTP(w, r)
			return
		}
		limited.ServeHTTP(w, r)
	})
}

// k8sRawTransport 到数据面原始通道的传输层：内网直连、不压缩（流式响应不得被缓冲）、不设响应头超时
// （上传与 exec 的响应头在很久之后才到）。
func k8sRawTransport() *http.Transport {
	return &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
}

// k8sDropHeaders 不转给数据面的请求头：用户的凭据到 core 为止；身份只由 core 签名的头表达。
var k8sDropHeaders = []string{
	"Authorization", "Cookie", "Proxy-Authorization", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto",
	"X-Real-Ip", kc.IdentityHeader, kc.SignatureHeader, kc.ClusterHeader, kc.KubeTokenHeader,
}

// k8sRawProxy 把请求不缓冲地转给数据面的 base + path（查询串沿用入站请求），带上 core 签名的身份头。
// 入站的凭据头、身份头与 Impersonate-* 一律丢弃。onNotOwner 在数据面应答「不是我负责」时调用。
func k8sRawProxy(tr http.RoundTripper, base, path string, signed http.Header, onNotOwner func()) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme, pr.Out.URL.Host, pr.Out.URL.Path, pr.Out.URL.RawPath = target.Scheme, target.Host, path, ""
			pr.Out.Host = target.Host
			for _, h := range k8sDropHeaders {
				pr.Out.Header.Del(h)
			}
			for k := range pr.Out.Header {
				if strings.HasPrefix(http.CanonicalHeaderKey(k), "Impersonate-") {
					pr.Out.Header.Del(k)
				}
			}
			for k, v := range signed {
				pr.Out.Header[k] = v
			}
		},
		Transport:     tr,
		FlushInterval: -1,
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusMisdirectedRequest && onNotOwner != nil {
				onNotOwner()
			}
			resp.Header.Del("X-Reborn-Reason")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			if onNotOwner != nil {
				onNotOwner()
			}
			writeKubeStatus(w, http.StatusBadGateway, "the k8s data plane is unreachable, retry shortly")
		},
	}, nil
}

// writeKubeStatus 以集群 API 的 Status 对象写出错误：kubectl 直接展示 message。
func writeKubeStatus(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{}, "status": "Failure",
		"message": message, "reason": http.StatusText(code), "code": code,
	})
}

// k8sDetach 让长请求脱离服务器的请求超时与读写截止时间：只在客户端断开、停机或到达硬上限时结束。
func k8sDetach(base context.Context, w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc) {
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), k8sRawMaxDuration)
	stopClient := context.AfterFunc(r.Context(), func() {
		if errors.Is(context.Cause(r.Context()), context.Canceled) {
			cancel()
		}
	})
	stopShutdown := context.AfterFunc(base, cancel)
	return ctx, func() { stopClient(); stopShutdown(); cancel() }
}

// registerK8sGovernance 绑定容器平台的集群代理入口（限时 kubeconfig）与容器文件传输。
//
// 集群代理入口不经中间件链：它的凭据不是登录令牌，而是限时 kubeconfig 的 Bearer 令牌，由
// GovernanceService.AuthorizeKube 逐请求校验（令牌、集群、账号状态、限速、审批策略）；通过后把请求
// 原样、不缓冲地转给负责该集群的数据面实例，由后者以该用户的身份访问集群。错误以集群 API 的 Status
// 对象返回，kubectl 能直接展示。请求不记访问日志（请求体里有 Secret 与清单内容），写操作由数据面审计。
//
// 文件传输走中间件链（登录、接口授权、参数校验），通过后同样不缓冲地转给数据面。
func registerK8sGovernance(srv *khttp.Server, gov *k8sservice.GovernanceService, logger *slog.Logger) {
	base, stop := context.WithCancel(context.Background())
	srv.RegisterOnShutdown(stop)
	tr := k8sRawTransport()
	srv.RegisterOnShutdown(tr.CloseIdleConnections)
	// 可信代理清单（KRATOS_TRUSTED_PROXIES）：未配置时取来源地址的口径与此前完全一致。
	proxies := trustedProxiesFromEnv(logger)

	srv.HandlePrefix(k8sProxyPrefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, k8sProxyPrefix)
		clusterID, kubePath, _ := strings.Cut(rest, "/")
		kubePath = "/" + kubePath
		bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || len(clusterID) != 36 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeKubeStatus(w, http.StatusUnauthorized, "a kubeconfig token issued by the platform is required")
			return
		}
		if !kc.KubePathAllowed(kubePath) {
			writeKubeStatus(w, http.StatusForbidden, "this path is not accessible through the cluster proxy")
			return
		}
		ip := proxies.clientIP(r.RemoteAddr, r.Header.Values("X-Forwarded-For"))
		route, err := gov.AuthorizeKube(r.Context(), bearer, clusterID, r.Method, kubePath, ip)
		if err != nil {
			ke := kerrors.FromError(err)
			if ke.Code >= 500 && ke.Code != http.StatusServiceUnavailable {
				logger.WarnContext(r.Context(), "k8s proxy authorize failed", "reason", ke.Reason, "error", err)
			}
			if ke.Code == http.StatusUnauthorized {
				w.Header().Set("WWW-Authenticate", "Bearer")
			}
			msg := ke.Message
			if ke.Code >= 500 && ke.Code != http.StatusServiceUnavailable {
				msg = "internal error, see the service log" // 未认证的入口不回显内部错误原文
			}
			writeKubeStatus(w, int(ke.Code), msg)
			return
		}
		defer route.Release()
		rp, err := k8sRawProxy(tr, route.Base, kc.InternalKubePath+kubePath, route.Header, func() { gov.InvalidateRoute(clusterID) })
		if err != nil {
			writeKubeStatus(w, http.StatusBadGateway, "the k8s data plane advertises an invalid address")
			return
		}
		ctx, cancel := k8sDetach(base, w, r)
		defer cancel()
		rp.ServeHTTP(w, r.WithContext(ctx))
	}))

	r := srv.Route("/api/v1/k8s")
	r.GET("/cluster/{cluster_id}/pod/file:download", k8sFileHandler(base, tr, gov, k8sv1.OperationK8SFileServiceDownloadPodFile,
		kc.InternalFileDownloadPath, func() *k8sv1.DownloadPodFileRequest { return &k8sv1.DownloadPodFileRequest{} },
		func(in *k8sv1.DownloadPodFileRequest) (string, url.Values) {
			return in.GetClusterId(), url.Values{
				kc.FileQueryNamespace: {in.GetNamespace()}, kc.FileQueryPod: {in.GetPod()},
				kc.FileQueryContainer: {in.GetContainer()}, kc.FileQueryPath: {in.GetPath()},
			}
		}))
	r.POST("/cluster/{cluster_id}/pod/file:upload", k8sFileHandler(base, tr, gov, k8sv1.OperationK8SFileServiceUploadPodFile,
		kc.InternalFileUploadPath, func() *k8sv1.UploadPodFileRequest { return &k8sv1.UploadPodFileRequest{} },
		func(in *k8sv1.UploadPodFileRequest) (string, url.Values) {
			return in.GetClusterId(), url.Values{
				kc.FileQueryNamespace: {in.GetNamespace()}, kc.FileQueryPod: {in.GetPod()}, kc.FileQueryContainer: {in.GetContainer()},
				kc.FileQueryPath: {in.GetPath()}, kc.FileQueryName: {in.GetName()},
			}
		}))
}

// k8sFileHandler 文件传输处理函数：从查询串与路径绑定参数 → 中间件链预检（登录 / 授权 / 校验）→
// 把请求体与响应体不缓冲地在浏览器与数据面之间转发。参数以校验后的值重新编码，不透传原始查询串。
func k8sFileHandler[T any](base context.Context, tr http.RoundTripper, gov *k8sservice.GovernanceService, operation, planePath string,
	newReq func() T, target func(T) (clusterID string, query url.Values),
) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		in := newReq()
		if err := ctx.BindQuery(in); err != nil {
			return err
		}
		if err := ctx.BindVars(in); err != nil {
			return err
		}
		khttp.SetOperation(ctx, operation)
		var authed context.Context
		preflight := ctx.Middleware(func(c context.Context, _ any) (any, error) {
			authed = c
			return nil, nil
		})
		if _, err := preflight(ctx, in); err != nil {
			return err
		}
		if authed == nil {
			return k8sv1.ErrorK8sUnspecified("authorization context missing")
		}
		clusterID, query := target(in)
		planeBase, signed, err := gov.FileRoute(authed, clusterID)
		if err != nil {
			return err
		}
		rp, err := k8sRawProxy(tr, planeBase, planePath, signed, func() { gov.InvalidateRoute(clusterID) })
		if err != nil {
			return k8sv1.ErrorK8sPlaneUnavailable("data plane instance advertises an invalid address")
		}
		rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			gov.InvalidateRoute(clusterID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": http.StatusServiceUnavailable, "reason": k8sv1.ErrorReason_K8S_PLANE_UNAVAILABLE.String(),
				"message": "the k8s data plane is unreachable, retry shortly",
			})
		}
		req := ctx.Request()
		runCtx, cancel := k8sDetach(base, ctx.Response(), req)
		defer cancel()
		out := req.WithContext(runCtx)
		out.URL = &url.URL{Path: req.URL.Path, RawQuery: query.Encode()}
		rp.ServeHTTP(ctx.Response(), out)
		return nil
	}
}
