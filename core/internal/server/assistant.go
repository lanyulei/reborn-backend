package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	assistantv1 "github.com/lanyulei/reborn/backend/core/api/assistant/v1"
	bizassistant "github.com/lanyulei/reborn/backend/core/internal/biz/assistant"
	assistantservice "github.com/lanyulei/reborn/backend/core/internal/service/assistant"

	"github.com/go-kratos/kratos/v3/errors"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
	"google.golang.org/protobuf/proto"
)

// SSE 流参数。
const (
	// ssePingInterval 注释行心跳间隔：探测客户端断开并防止代理空闲断连。
	ssePingInterval = 15 * time.Second
	// sseMaxDuration 单个流的硬上限（biz 层另有可配置的单轮 timeout，此为兜底）。
	sseMaxDuration = 15 * time.Minute
)

// registerAssistantStream 以原生路由绑定 AssistantChatService 的 Chat/Confirm。
//
// 这是本服务唯一不经生成代码绑定的 HTTP 方法（AGENTS §5 的有意偏离）：
// protoc-gen-go-http 只支持一元"请求→一次性响应"，无法表达 SSE 流式输出。
// 契约仍由 proto 定义（路径与注解一致、进入授权枚举、生成 Operation 常量与
// swagger），handler 复刻生成代码流程：Bind → SetOperation → 中间件链
// （recovery/tracing/logging/validate/ratelimit/auth/authz）。中间件链只包裹
// 一次毫秒级"预检"并立即返回，流式阶段在链外进行——限流/日志/tracing 看到的
// 是短请求，分钟级长连接不会拉偏它们的统计，也不触碰既有中间件链配置。
func registerAssistantStream(srv *khttp.Server, chat *assistantservice.AssistantChatService) {
	r := srv.Route("/api/v1/assistant")
	r.POST("/chat", streamHandler(assistantv1.OperationAssistantChatServiceChat,
		func() *assistantv1.ChatRequest { return &assistantv1.ChatRequest{} }, chat.StreamChat))
	r.POST("/confirm", streamHandler(assistantv1.OperationAssistantChatServiceConfirm,
		func() *assistantv1.ConfirmRequest { return &assistantv1.ConfirmRequest{} }, chat.StreamConfirm))
}

// streamHandler 构造 SSE 路由处理函数：预检（鉴权/授权/校验）→ 流式运行。
// 响应头延迟到首个事件才写出：轮次开始前的失败（停用/会话不存在/忙）仍以
// 标准 JSON 错误与 HTTP 状态码返回；流式开始后的失败以 error 事件下发。
func streamHandler[T proto.Message](operation string, newReq func() T,
	run func(ctx context.Context, req T, emit func(bizassistant.Event) error) error,
) khttp.HandlerFunc {
	return func(ctx khttp.Context) error {
		in := newReq()
		if err := ctx.Bind(in); err != nil {
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
			return errors.InternalServer("ASSISTANT_UNSPECIFIED", "authorization context missing")
		}
		w := newSSEWriter(ctx.Response())
		if w == nil {
			return errors.InternalServer("ASSISTANT_UNSPECIFIED", "streaming not supported by response writer")
		}
		// 请求体已读完：清除连接读截止时间（Server.ReadTimeout），否则它在
		// 流式期间到点会让 net/http 后台读报错并取消请求 ctx。尽力而为：
		// 底层 writer 不支持时忽略（流式运行本就脱离请求 ctx 的取消）。
		_ = http.NewResponseController(ctx.Response()).SetReadDeadline(time.Time{})
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(authed), sseMaxDuration)
		defer cancel()
		emit := func(ev bizassistant.Event) error {
			data, err := json.Marshal(ev.Payload)
			if err != nil {
				return err
			}
			if err := w.event(ev.Type, data); err != nil {
				cancel()
				return err
			}
			return nil
		}
		w.startPing(runCtx, cancel)
		err := run(runCtx, in, emit)
		w.stopPing()
		if err != nil && !w.started() {
			// 尚未写出任何事件：按普通错误响应（状态码随 reason 映射）。
			return err
		}
		return nil
	}
}

// sseWriter 串行化 SSE 写出（事件与心跳来自不同 goroutine）。
type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	mu      sync.Mutex
	headers bool
	stop    chan struct{}
	done    chan struct{}
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	return &sseWriter{w: w, flusher: flusher}
}

// writeHeaders 首次写出时设置 SSE 响应头（200）。
func (s *sseWriter) writeHeaders() {
	if s.headers {
		return
	}
	s.headers = true
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
}

func (s *sseWriter) started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers
}

// event 写出一个事件（event: 名称 + data: JSON）。
func (s *sseWriter) event(name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeHeaders()
	if _, err := s.w.Write([]byte("event: " + name + "\ndata: ")); err != nil {
		return err
	}
	if _, err := s.w.Write(data); err != nil {
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// ping 写出注释行心跳；仅在响应头已写出后才发送。
func (s *sseWriter) ping() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.headers {
		return nil
	}
	if _, err := s.w.Write([]byte(": ping\n\n")); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// startPing 启动心跳 goroutine；写失败视为客户端断开，取消运行。
func (s *sseWriter) startPing(ctx context.Context, cancel context.CancelFunc) {
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(ssePingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.ping(); err != nil {
					cancel()
					return
				}
			}
		}
	}()
}

func (s *sseWriter) stopPing() {
	close(s.stop)
	<-s.done
}
