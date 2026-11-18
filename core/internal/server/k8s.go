package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	k8sv1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	k8sservice "github.com/lanyulei/reborn/backend/core/internal/service/k8s"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
	kerrors "github.com/go-kratos/kratos/v3/errors"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
	"google.golang.org/protobuf/proto"
)

// 容器平台流式接口参数。
const (
	// k8sStreamMaxDuration 单个 SSE 流在 core 侧的硬上限（数据面对日志流另有 1 小时上限并会下发结束事件，
	// 前端据此续接；这里是兜底，略大于它）。
	k8sStreamMaxDuration = 65 * time.Minute
	// k8sStreamMaxLine 转发 SSE 时单行的上限：日志行 16KB，JSON 转义后仍远小于它；超过即中断该流。
	k8sStreamMaxLine = 1 << 20
	// k8sExecConnectTimeout 核销票据并连上数据面的时限。
	k8sExecConnectTimeout = 35 * time.Second
	// k8sExecMaxDuration 终端连接在 core 侧的硬上限（数据面的会话上限是 8 小时）。
	k8sExecMaxDuration = kc.ExecMaxDuration + 5*time.Minute
	// k8sExecWriteTimeout 向任一端写一帧的时限：对端卡死时不让转发协程永久阻塞。
	k8sExecWriteTimeout = 30 * time.Second
	// k8sExecReadLimit 单帧上限（契约的帧上限 + 类型字节与余量）。
	k8sExecReadLimit = kc.ExecMaxFrameSize + 16
)

// registerK8sStreams 以原生路由绑定容器平台的流式接口与 Web 终端。
//
// 与助手对话同理（见 registerAssistantStream）：protoc-gen-go-http 只支持一元响应，SSE 与 WebSocket 无法
// 经生成代码绑定。四个 SSE 接口的契约仍由 proto 定义（K8sStreamService：路径一致、进入授权枚举、
// 生成 Operation 常量），handler 复刻生成代码的流程：Bind → BindVars → SetOperation → 中间件链
// （recovery / tracing / logging / validate / ratelimit / auth / authz）。中间件链只包住毫秒级的预检，
// 流式转发在链外进行。
//
// 停机：srv 开始关闭时取消全部在途的流与终端连接（前端会自动续接），不让它们拖住优雅退出。
func registerK8sStreams(srv *khttp.Server, streams *k8sservice.StreamService, allowedOrigins []string, logger *slog.Logger) {
	base, stop := context.WithCancel(context.Background())
	srv.RegisterOnShutdown(stop)

	r := srv.Route("/api/v1/k8s")
	r.POST("/cluster/{cluster_id}/watch", k8sStreamHandler(base, k8sv1.OperationK8SStreamServiceWatchResources,
		func() *k8sv1.WatchResourcesRequest { return &k8sv1.WatchResourcesRequest{} }, streams.WatchResources))
	r.POST("/cluster/{cluster_id}/pod/log", k8sStreamHandler(base, k8sv1.OperationK8SStreamServiceStreamPodLog,
		func() *k8sv1.StreamPodLogRequest { return &k8sv1.StreamPodLogRequest{} }, streams.StreamPodLog))
	r.POST("/cluster/{cluster_id}/workload/log", k8sStreamHandler(base, k8sv1.OperationK8SStreamServiceStreamWorkloadLog,
		func() *k8sv1.StreamWorkloadLogRequest { return &k8sv1.StreamWorkloadLogRequest{} }, streams.StreamWorkloadLog))
	r.POST("/cluster/{cluster_id}/node:drain", k8sStreamHandler(base, k8sv1.OperationK8SStreamServiceDrainNode,
		func() *k8sv1.DrainNodeRequest { return &k8sv1.DrainNodeRequest{} }, streams.DrainNode))
	r.GET("/exec", k8sExecHandler(base, streams, allowedOrigins, logger))
}

// k8sStreamHandler 构造 SSE 转发处理函数：预检（鉴权 / 授权 / 校验）→ 向数据面建立流 → 转发。
// 数据面的流建立之前的失败（无权限、集群不可达、并发超限等）以标准 JSON 错误与 HTTP 状态码返回；
// 响应头在流建立成功后才写出。
func k8sStreamHandler[T proto.Message](base context.Context, operation string, newReq func() T,
	open func(ctx context.Context, req T) (io.ReadCloser, error),
) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		in := newReq()
		if err := ctx.Bind(in); err != nil {
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
		flusher, ok := ctx.Response().(http.Flusher)
		if !ok {
			return k8sv1.ErrorK8sUnspecified("streaming not supported by response writer")
		}
		// 流式阶段脱离请求 ctx（它带着服务器的请求超时），改挂停机信号与硬上限；身份等值仍沿用预检后的 ctx。
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(authed), k8sStreamMaxDuration)
		defer cancel()
		stopOnShutdown := context.AfterFunc(base, cancel)
		defer stopOnShutdown()

		body, err := open(runCtx, in)
		if err != nil {
			return err
		}
		// 取消时关闭上游：读数据面的阻塞读随之返回。
		closeBody := sync.OnceFunc(func() { _ = body.Close() })
		defer closeBody()
		stopOnCancel := context.AfterFunc(runCtx, closeBody)
		defer stopOnCancel()

		// 请求体已读完：清除连接读截止时间（Server.ReadTimeout），否则它在流式期间到点会中断连接。
		_ = http.NewResponseController(ctx.Response()).SetReadDeadline(time.Time{})
		w := ctx.Response()
		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache, no-transform")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		relaySSE(runCtx, cancel, body, w, flusher)
		return nil
	}
}

// relaySSE 把数据面的 SSE 逐事件转给浏览器：按行读，遇到空行（事件边界）立即刷出。在事件边界之间
// 定时插入注释行心跳——既防代理空闲断连，也借写失败发现浏览器已断开（请求 ctx 不可用于此）。
func relaySSE(ctx context.Context, cancel context.CancelFunc, body io.Reader, w http.ResponseWriter, flusher http.Flusher) {
	var mu sync.Mutex // 串行化写出；boundary 只在持锁时读写
	boundary := true
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(ssePingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				var err error
				if boundary {
					if _, err = w.Write([]byte(": ping\n\n")); err == nil {
						flusher.Flush()
					}
				}
				mu.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	reader := bufio.NewReaderSize(body, 32<<10)
	for {
		line, err := readLine(reader, k8sStreamMaxLine)
		if len(line) > 0 {
			mu.Lock()
			_, werr := w.Write(line)
			boundary = len(bytes.TrimRight(line, "\r\n")) == 0
			if werr == nil && boundary {
				flusher.Flush()
			}
			mu.Unlock()
			if werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	cancel()
	<-done
	mu.Lock()
	flusher.Flush()
	mu.Unlock()
}

// errLineTooLong SSE 单行超过上限。
var errLineTooLong = errors.New("sse line too long")

// readLine 读一行（含行尾换行）；超过 limit 返回 errLineTooLong。末尾没有换行的残行原样返回并带上 io.EOF。
func readLine(r *bufio.Reader, limit int) ([]byte, error) {
	var out []byte
	for {
		chunk, err := r.ReadSlice('\n')
		out = append(out, chunk...)
		if len(out) > limit {
			return nil, errLineTooLong
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return out, err
	}
}

// k8sExecHandler Web 终端：GET /api/v1/k8s/exec?ticket=…（WebSocket）。
//
// 浏览器的 WebSocket 不能带 Authorization 头，所以凭 OpenExecTicket 签发的一次性票据认证：
// 本入口免 JWT、免接口授权（OperationExecConnect 同时在两份豁免清单里），仍经过中间件链的
// recovery / tracing / logging / ratelimit。握手时校验 Origin（同源，或配置里列出的主机），
// 防止别的站点借用户浏览器发起连接。升级成功后才核销票据：失败原因以状态帧告知后关闭。
// 之后在浏览器与数据面之间逐帧转发（二进制帧，格式见 kit/k8scontract 的 Frame*），任一端结束即两端都关闭。
func k8sExecHandler(base context.Context, streams *k8sservice.StreamService, allowedOrigins []string, logger *slog.Logger) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		khttp.SetOperation(ctx, systemservice.OperationK8sExecConnect)
		var authed context.Context
		// 预检不带请求内容：票据不进请求日志。
		preflight := ctx.Middleware(func(c context.Context, _ any) (any, error) {
			authed = c
			return nil, nil
		})
		if _, err := preflight(ctx, nil); err != nil {
			return err
		}
		if authed == nil {
			return k8sv1.ErrorK8sUnspecified("request context missing")
		}
		ticket := ctx.Request().URL.Query().Get("ticket")
		if ticket == "" {
			return bizk8s.ErrTicketInvalid
		}
		clientIP := systemservice.ClientIP(authed)
		_ = http.NewResponseController(ctx.Response()).SetReadDeadline(time.Time{})
		client, err := websocket.Accept(ctx.Response(), ctx.Request(), &websocket.AcceptOptions{
			OriginPatterns:  allowedOrigins,
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			// Accept 已写出错误响应（Origin 不符为 403，非 WebSocket 请求为 400/426）。
			return nil
		}
		client.SetReadLimit(k8sExecReadLimit)
		defer func() { _ = client.CloseNow() }()

		runCtx, cancel := context.WithTimeout(context.WithoutCancel(authed), k8sExecMaxDuration)
		defer cancel()
		stopOnShutdown := context.AfterFunc(base, cancel)
		defer stopOnShutdown()

		connectCtx, connectCancel := context.WithTimeout(runCtx, k8sExecConnectTimeout)
		plane, err := streams.ConnectExec(connectCtx, ticket, clientIP)
		connectCancel()
		if err != nil {
			ke := kerrors.FromError(err)
			if ke.Code >= 500 {
				logger.WarnContext(runCtx, "k8s exec connect failed", "reason", ke.Reason, "error", err)
			}
			status, _ := json.Marshal(kc.ExecStatus{State: kc.ExecClosed, Reason: ke.Reason, Message: ke.Message})
			wctx, wcancel := context.WithTimeout(runCtx, 5*time.Second)
			_ = client.Write(wctx, websocket.MessageBinary, append([]byte{kc.FrameStatus}, status...))
			wcancel()
			_ = client.Close(websocket.StatusPolicyViolation, ke.Reason)
			return nil
		}
		defer func() { _ = plane.Close() }()

		relayExec(runCtx, cancel, client, plane)
		_ = client.Close(websocket.StatusNormalClosure, "")
		return nil
	}
}

// relayExec 在浏览器与数据面之间双向转发帧，任一方向结束（对端关闭、出错、超限、停机）即取消另一方向。
func relayExec(ctx context.Context, cancel context.CancelFunc, client *websocket.Conn, plane bizk8s.ExecConn) {
	var wg sync.WaitGroup
	wg.Add(2)
	// 浏览器 → 数据面
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			typ, data, err := client.Read(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageBinary || len(data) == 0 {
				continue
			}
			wctx, wcancel := context.WithTimeout(ctx, k8sExecWriteTimeout)
			err = plane.Write(wctx, data)
			wcancel()
			if err != nil {
				return
			}
		}
	}()
	// 数据面 → 浏览器
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			data, err := plane.Read(ctx)
			if err != nil {
				return
			}
			wctx, wcancel := context.WithTimeout(ctx, k8sExecWriteTimeout)
			err = client.Write(wctx, websocket.MessageBinary, data)
			wcancel()
			if err != nil {
				return
			}
		}
	}()
	// 取消后两条连接的阻塞读都会因 ctx 结束而返回。
	wg.Wait()
}
