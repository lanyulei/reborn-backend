// Package service 容器平台数据面的接口层：内部接口（core → 数据面）的 HTTP 处理。只做身份校验、
// 解码与编排，业务在 biz。协议见 kit/k8scontract/api.go。
package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewAgentService, NewInternalService, NewGovService)

// InternalKey core 与数据面之间的身份头签名密钥。
type InternalKey []byte

// InternalService 内部接口。
type InternalService struct {
	key  InternalKey
	ops  *biz.Ops
	exec *biz.ExecManager
	log  *slog.Logger
}

// NewInternalService new an InternalService。密钥不足 32 字节时仍可启动，但所有请求都会被拒绝（401）。
func NewInternalService(key InternalKey, ops *biz.Ops, exec *biz.ExecManager, log *slog.Logger) *InternalService {
	if len(key) < k8scontract.MinInternalKeyBytes {
		log.Error("plane.internal_key is missing or shorter than 32 bytes: every internal request will be rejected")
	}
	return &InternalService{key: key, ops: ops, exec: exec, log: log}
}

// Register 把内部接口挂到路由上。
func (s *InternalService) Register(handlePrefix func(prefix string, h http.Handler), handle func(path string, h http.Handler)) {
	handlePrefix(k8scontract.InternalOpPath, http.HandlerFunc(s.serveOp))
	handlePrefix(k8scontract.InternalStreamPath, http.HandlerFunc(s.serveStream))
	handle(k8scontract.InternalExecPath, http.HandlerFunc(s.serveExec))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *InternalService) writeError(w http.ResponseWriter, err error) {
	be := biz.AsError(err)
	if be.Code >= 500 && be.Reason == k8scontract.ReasonUnspecified {
		s.log.Error("internal request failed", "error", err)
	}
	writeJSON(w, be.Code, k8scontract.ErrorBody{Code: be.Code, Reason: be.Reason, Message: be.Message})
}

// identify 校验身份头。任何失败都以同一个 401 应答，细节只进日志。
func (s *InternalService) identify(r *http.Request) (biz.Identity, error) {
	id, err := k8scontract.VerifyIdentity(r.Header, s.key, time.Now())
	if err != nil {
		s.log.Warn("internal identity rejected", "error", err, "remote", r.RemoteAddr)
		return id, biz.NewError(http.StatusUnauthorized, k8scontract.ReasonIdentity, "internal identity is missing, invalid or expired")
	}
	return id, nil
}

func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(r.Body) // 上限由 httpx.LimitRequestBody（8MiB）约束
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, biz.NewError(http.StatusRequestEntityTooLarge, k8scontract.ReasonInvalidArgument, "request body too large")
		}
		return nil, biz.ErrInvalid("cannot read request body")
	}
	return body, nil
}

func (s *InternalService) serveOp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, biz.NewError(http.StatusMethodNotAllowed, k8scontract.ReasonInvalidArgument, "POST required"))
		return
	}
	id, err := s.identify(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	body, err := readBody(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	op := strings.TrimPrefix(r.URL.Path, k8scontract.InternalOpPath)
	if op == k8scontract.OpClusterTest {
		var req k8scontract.ClusterTestRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, biz.ErrInvalid("malformed request body"))
			return
		}
		writeJSON(w, http.StatusOK, s.ops.TestCluster(r.Context(), req))
		return
	}
	if k8scontract.HelmClusterless(op) {
		resp, err := s.ops.CallGlobal(r.Context(), id, op, body)
		if err != nil {
			s.writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp, err := s.ops.Call(r.Context(), r.Header.Get(k8scontract.ClusterHeader), id, op, body)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// sseWriter 串行化 SSE 写出（事件与心跳来自不同 goroutine）。响应头延迟到首个事件：
// 流开始前的失败仍以普通 JSON 错误与状态码返回。
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController

	mu      sync.Mutex
	started bool
	failed  bool
}

func (s *sseWriter) event(event, id string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return errors.New("stream closed")
	}
	if !s.started {
		s.started = true
		h := s.w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache, no-transform")
		h.Set("X-Accel-Buffering", "no")
		s.w.WriteHeader(http.StatusOK)
	}
	var b strings.Builder
	if id != "" {
		b.WriteString("id: " + id + "\n")
	}
	b.WriteString("event: " + event + "\ndata: ")
	b.Write(raw) // JSON 编码后不含裸换行
	b.WriteString("\n\n")
	_ = s.rc.SetWriteDeadline(time.Now().Add(30 * time.Second)) // 对端不读时不无限期阻塞
	if _, err := io.WriteString(s.w, b.String()); err != nil {
		s.failed = true
		return err
	}
	if err := s.rc.Flush(); err != nil {
		s.failed = true
		return err
	}
	return nil
}

func (s *sseWriter) isStarted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

// close 由 handler 在返回前调用。handler 返回后 net/http 会回收 ResponseWriter
// （内部缓冲置 nil），迟到的写（如并发中的心跳事件）会触发 panic：在锁内标记关闭，
// 之后 event 一律失败，不再触碰 ResponseWriter。
func (s *sseWriter) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
}

// ssePing 心跳间隔：让中间层不因空闲断开连接，也让本端及时发现对端已断开。
const ssePing = 20 * time.Second

func (s *InternalService) serveStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, biz.NewError(http.StatusMethodNotAllowed, k8scontract.ReasonInvalidArgument, "POST required"))
		return
	}
	id, err := s.identify(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	body, err := readBody(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	rc := http.NewResponseController(w)
	// 请求体已读完：清除读截止时间；流的时长由业务层的上限控制，不受服务器请求超时约束。
	_ = rc.SetReadDeadline(time.Time{})
	// 流只在客户端断开时结束，不受服务器的请求超时约束（见 detach）。
	ctx, cancel := detach(r)
	defer cancel()

	sw := &sseWriter{w: w, rc: rc}
	// 必须先于 handler 返回把流标记为关闭：心跳 goroutine 可能正持有锁在写，
	// 等它落地后 handler 才能返回，否则写会打到已回收的 ResponseWriter 上（panic）。
	defer sw.close()
	emit := func(event, id string, data any) error {
		if err := sw.event(event, id, data); err != nil {
			cancel()
			return err
		}
		return nil
	}
	done := make(chan struct{})
	go func() { // 退出路径：done 关闭或 ctx 取消
		t := time.NewTicker(ssePing)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if sw.isStarted() {
					_ = emit(k8scontract.EventPing, "", k8scontract.Empty{})
				}
			}
		}
	}()
	op := strings.TrimPrefix(r.URL.Path, k8scontract.InternalStreamPath)
	err = s.ops.Stream(ctx, r.Header.Get(k8scontract.ClusterHeader), id, op, body, emit)
	close(done)
	if err == nil {
		return
	}
	if !sw.isStarted() {
		s.writeError(w, err)
		return
	}
	be := biz.AsError(err)
	_ = sw.event(k8scontract.EventEnd, "", k8scontract.StreamEnd{Reason: k8scontract.EndError, Message: be.Message})
}

// execPing WebSocket 心跳间隔。
const execPing = 20 * time.Second

func (s *InternalService) serveExec(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("params"))
	var params k8scontract.ExecParams
	if err != nil || json.Unmarshal(raw, &params) != nil {
		s.writeError(w, biz.ErrInvalid("malformed exec params"))
		return
	}
	// 只有 core 会连这里（身份头已校验），不是浏览器：不做 Origin 校验。
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ws.SetReadLimit(k8scontract.ExecMaxFrameSize + 1)
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	defer func() { _ = ws.CloseNow() }()

	var wmu sync.Mutex
	sink := func(typ byte, payload []byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		wctx, wcancel := context.WithTimeout(ctx, 15*time.Second)
		defer wcancel()
		return ws.Write(wctx, websocket.MessageBinary, biz.EncodeFrame(typ, payload))
	}
	h, err := s.exec.Attach(ctx, r.Header.Get(k8scontract.ClusterHeader), id, params, sink)
	if err != nil {
		be := biz.AsError(err)
		st, _ := json.Marshal(k8scontract.ExecStatus{State: k8scontract.ExecClosed, Reason: be.Reason, Message: be.Message})
		_ = sink(k8scontract.FrameStatus, st)
		_ = ws.Close(websocket.StatusPolicyViolation, be.Reason)
		return
	}
	defer h.Detach()

	go func() { // 退出路径：会话结束或本连接结束
		t := time.NewTicker(execPing)
		defer t.Stop()
		for {
			select {
			case <-h.Done():
				// 会话结束：closed 状态帧已由会话写出，正常关闭连接让读循环返回。
				_ = ws.Close(websocket.StatusNormalClosure, "")
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if sink(k8scontract.FramePing, nil) != nil {
					cancel()
					return
				}
			}
		}
	}()
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			// 正常关闭 = 用户主动关掉终端：结束会话。其余（网络中断、core 重启）= 保留会话等待接回。
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				h.Close()
			}
			return
		}
		typ, payload, err := biz.DecodeFrame(data)
		if err != nil {
			continue
		}
		switch typ {
		case k8scontract.FrameInput:
			if h.Input(payload) != nil {
				return
			}
		case k8scontract.FrameResize:
			var rs k8scontract.ExecResize
			if json.Unmarshal(payload, &rs) == nil && rs.Cols > 0 && rs.Rows > 0 {
				h.Resize(rs)
			}
		}
	}
}
