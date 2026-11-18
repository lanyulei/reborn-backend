package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// GovService 治理类内部接口：集群 API 的原始透传（限时 kubeconfig）与容器文件传输。
// 身份校验与内部接口相同（core 签名的身份头）；请求体与响应体不缓冲。
type GovService struct {
	internal *InternalService
	gateway  *biz.KubeGateway
	files    *biz.FileTransfer
}

// NewGovService new a GovService.
func NewGovService(internal *InternalService, gateway *biz.KubeGateway, files *biz.FileTransfer) *GovService {
	return &GovService{internal: internal, gateway: gateway, files: files}
}

// Register 把接口挂到路由上。
func (s *GovService) Register(handlePrefix func(prefix string, h http.Handler), handle func(path string, h http.Handler)) {
	handlePrefix(k8scontract.InternalKubePath+"/", http.HandlerFunc(s.serveKube))
	handle(k8scontract.InternalFileDownloadPath, http.HandlerFunc(s.serveDownload))
	handle(k8scontract.InternalFileUploadPath, http.HandlerFunc(s.serveUpload))
}

// UnlimitedBody 报告该路径的请求体不受通用的 8MiB 上限约束（文件上传自带大小上限）。
func UnlimitedBody(path string) bool { return path == k8scontract.InternalFileUploadPath }

type connCtxKey struct{}

// KeepConnContext 过滤器：把「未加请求超时」的请求上下文存进请求里。必须挂在 khttp.Filter 上——它先于
// 服务器的超时中间件执行，此时的上下文只在客户端断开（或服务器关停）时结束。
func KeepConnContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), connCtxKey{}, r.Context())))
	})
}

// detach 让长请求脱离服务器的请求超时（khttp.Timeout 作用于全部路由）：只在客户端断开时取消。
// 跟随的是 KeepConnContext 存下的连接上下文，而不是带超时的请求上下文：后者在超时触发后就结束了，
// 之后客户端再断开便无从得知，上游的 watch / 日志流会一直挂到集群那边超时。
func detach(r *http.Request) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	if base, ok := r.Context().Value(connCtxKey{}).(context.Context); ok {
		stop := context.AfterFunc(base, cancel)
		return ctx, func() { stop(); cancel() }
	}
	// 没有挂过滤器（单元测试）：退回到按取消原因区分。
	stop := context.AfterFunc(r.Context(), func() {
		if errors.Is(context.Cause(r.Context()), context.Canceled) {
			cancel()
		}
	})
	return ctx, func() { stop(); cancel() }
}

// fileIdleTimeout 文件传输的空闲时限：对端这么久不收 / 不发一个字节即中断（挂起的客户端不应无限占着
// 到容器的 exec 连接）。
const fileIdleTimeout = 2 * time.Minute

// idleWriter 每次写之前把写截止时间往后推一个空闲时限。
type idleWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (i idleWriter) Write(p []byte) (int, error) {
	_ = i.rc.SetWriteDeadline(time.Now().Add(fileIdleTimeout))
	return i.w.Write(p)
}

// idleReader 每次读之前把读截止时间往后推一个空闲时限。
type idleReader struct {
	r  io.Reader
	rc *http.ResponseController
}

func (i idleReader) Read(p []byte) (int, error) {
	_ = i.rc.SetReadDeadline(time.Now().Add(fileIdleTimeout))
	return i.r.Read(p)
}

// writeKubeStatus 以集群 API 的 Status 对象写出错误：kubectl 能直接展示 message。
// X-Reborn-Reason 带上平台的错误原因，core 据此识别归属变化（K8S_NOT_OWNER）。
func writeKubeStatus(w http.ResponseWriter, err error) {
	be := biz.AsError(err)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(reasonHeader, be.Reason)
	w.WriteHeader(be.Code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"kind": "Status", "apiVersion": "v1", "metadata": map[string]any{}, "status": "Failure",
		"message": be.Message, "reason": http.StatusText(be.Code), "code": be.Code,
	})
}

// reasonHeader 错误响应里平台错误原因的头。
const reasonHeader = "X-Reborn-Reason"

func (s *GovService) serveKube(w http.ResponseWriter, r *http.Request) {
	id, err := s.internal.identify(r)
	if err != nil {
		writeKubeStatus(w, err)
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	ctx, cancel := detach(r)
	defer cancel()
	out := r.Clone(ctx)
	out.URL.Path = strings.TrimPrefix(r.URL.Path, k8scontract.InternalKubePath)
	out.URL.RawPath = ""
	if err := s.gateway.Serve(w, out, r.Header.Get(k8scontract.ClusterHeader), id, r.Header.Get(k8scontract.KubeTokenHeader)); err != nil {
		writeKubeStatus(w, err)
	}
}

func fileRef(r *http.Request) biz.FileRef {
	q := r.URL.Query()
	return biz.FileRef{
		Namespace: q.Get(k8scontract.FileQueryNamespace), Pod: q.Get(k8scontract.FileQueryPod),
		Container: q.Get(k8scontract.FileQueryContainer), Path: q.Get(k8scontract.FileQueryPath),
	}
}

func (s *GovService) serveDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.internal.writeError(w, biz.NewError(http.StatusMethodNotAllowed, k8scontract.ReasonInvalidArgument, "GET required"))
		return
	}
	id, err := s.internal.identify(r)
	if err != nil {
		s.internal.writeError(w, err)
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})
	ctx, cancel := detach(r)
	defer cancel()
	started := false
	begin := func(name string, isTar bool, size int64) io.Writer {
		started = true
		h := w.Header()
		h.Set("Content-Type", "application/octet-stream")
		if isTar {
			h.Set("Content-Type", "application/x-tar")
		}
		h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
		h.Set("X-Content-Type-Options", "nosniff")
		if size >= 0 {
			h.Set("Content-Length", fmt.Sprint(size))
		}
		w.WriteHeader(http.StatusOK)
		return idleWriter{w: w, rc: rc}
	}
	err = s.files.Download(ctx, r.Header.Get(k8scontract.ClusterHeader), id, fileRef(r), begin)
	if err == nil {
		return
	}
	if !started {
		s.internal.writeError(w, err)
		return
	}
	// 内容已开始下发：无法再改状态码，直接中断连接，客户端会看到传输不完整。
	panic(http.ErrAbortHandler)
}

func (s *GovService) serveUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.internal.writeError(w, biz.NewError(http.StatusMethodNotAllowed, k8scontract.ReasonInvalidArgument, "POST required"))
		return
	}
	id, err := s.internal.identify(r)
	if err != nil {
		s.internal.writeError(w, err)
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	ctx, cancel := detach(r)
	defer cancel()
	body := idleReader{r: http.MaxBytesReader(w, r.Body, s.files.MaxBytes()), rc: rc}
	name := r.URL.Query().Get(k8scontract.FileQueryName)
	if err := s.files.Upload(ctx, r.Header.Get(k8scontract.ClusterHeader), id, fileRef(r), name, r.ContentLength, body); err != nil {
		s.internal.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"size": r.ContentLength})
}
