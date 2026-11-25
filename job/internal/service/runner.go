// Package service 实现引擎面向执行节点的 HTTP 协议（kit/jobcontract：四个 POST 接口，JSON 请求 / 应答）。
// 薄编排层：鉴权、解码、调用 biz 用例、把用例错误映射为协议状态码；不含业务规则，不 import data。
//
// 状态码（契约）：200 成功；400 请求不合法；401 令牌无效；403 节点已停用；409 租约已失效；
// 413 请求体超限；其余 5xx 由节点按可重试处理。接入令牌与任务内容（含凭据明文）不写日志。
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"

	"github.com/google/wire"
)

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewRunnerService)

// RunnerService 执行节点协议的 HTTP 处理器。
type RunnerService struct {
	uc  *biz.RunnerUsecase
	log *slog.Logger
}

// NewRunnerService new a RunnerService.
func NewRunnerService(uc *biz.RunnerUsecase, logger *slog.Logger) *RunnerService {
	return &RunnerService{uc: uc, log: logger}
}

// Routes 返回协议路径到处理器的映射（server 层据此注册路由）。
func (s *RunnerService) Routes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		jobcontract.RunnerPathHeartbeat: s.handle(s.heartbeat),
		jobcontract.RunnerPathClaim:     s.handle(s.claim),
		jobcontract.RunnerPathReport:    s.handle(s.report),
		jobcontract.RunnerPathFinish:    s.handle(s.finish),
	}
}

// endpoint 一个协议接口：解码请求体、调用用例，返回应答体。
type endpoint func(ctx context.Context, r *biz.Runner, body io.Reader) (any, error)

// handle 包装公共流程：只接受 POST → Bearer 令牌鉴权 → 调用接口 → 编码应答或错误。
func (s *RunnerService) handle(ep endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		ctx := req.Context()
		runner, err := s.uc.Authenticate(ctx, bearerToken(req))
		if err != nil {
			s.fail(w, req, "", err)
			return
		}
		out, err := ep(ctx, runner, req.Body)
		if err != nil {
			s.fail(w, req, runner.ID, err)
			return
		}
		claim, _ := out.(*jobcontract.ClaimResponse)
		claimed := claim != nil && claim.Task != nil
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		err = json.NewEncoder(w).Encode(out)
		if err == nil && claimed {
			err = delivered(ctx, w)
		}
		if err != nil {
			s.log.WarnContext(ctx, "write runner api response failed", "path", req.URL.Path, "runner_id", runner.ID, "error", err)
			if claimed {
				s.uc.Unclaim(ctx, claim.Task.ExecutionID, claim.Task.LeaseToken)
			}
		}
	}
}

// delivered 尽力确认带任务的领取应答送到了节点：Encode 不报错只说明数据进了 net/http 的缓冲。
// 把缓冲推给连接（连接已断时在这里报错），再看请求 ctx——节点在领取期间断开的话，net/http 的后台读
// 会取消它（前提是请求体已读尽，见 claim）。两者都只能发现「到此刻为止」的断开，之后才断的仍要等租约过期。
func delivered(ctx context.Context, w http.ResponseWriter) error {
	if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return ctx.Err()
}

func (s *RunnerService) heartbeat(ctx context.Context, r *biz.Runner, body io.Reader) (any, error) {
	var in jobcontract.HeartbeatRequest
	if err := decode(body, &in); err != nil {
		return nil, err
	}
	return s.uc.Heartbeat(ctx, r, &in)
}

// claim 领取没有请求内容（请求体可为空），但要先把请求体读尽（监听层已限长）：net/http 读到请求体
// 末尾才启动后台读，之后节点断开才会取消请求 ctx。不读的话，节点在领取期间断开时应答照样「写成功」、
// ctx 也没有错，从未运行过的执行要等租约过期，再被判成「结果未知：作业可能仍在运行」。
func (s *RunnerService) claim(ctx context.Context, r *biz.Runner, body io.Reader) (any, error) {
	if _, err := io.Copy(io.Discard, body); err != nil {
		return nil, bodyError(err)
	}
	task, err := s.uc.Claim(ctx, r)
	if err != nil {
		return nil, err
	}
	return &jobcontract.ClaimResponse{Task: task}, nil
}

func (s *RunnerService) report(ctx context.Context, r *biz.Runner, body io.Reader) (any, error) {
	var in jobcontract.ReportRequest
	if err := decode(body, &in); err != nil {
		return nil, err
	}
	return s.uc.Report(ctx, r, &in)
}

func (s *RunnerService) finish(ctx context.Context, r *biz.Runner, body io.Reader) (any, error) {
	var in jobcontract.FinishRequest
	if err := decode(body, &in); err != nil {
		return nil, err
	}
	if err := s.uc.Finish(ctx, r, &in); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

// errBodyTooLarge 请求体超过监听层设置的上限。
var errBodyTooLarge = errors.New("request body too large")

// decode 解码 JSON 请求体；未知字段忽略（新旧版本节点共存）。
func decode(body io.Reader, v any) error {
	if err := json.NewDecoder(body).Decode(v); err != nil {
		return bodyError(err)
	}
	return nil
}

// bodyError 把读取 / 解码请求体的错误映射为协议错误：超限 413，其余 400。
func bodyError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return errBodyTooLarge
	}
	return fmt.Errorf("%w: malformed json body", biz.ErrInvalidRequest)
}

// bearerToken 取 Authorization: Bearer <令牌>；没有或格式不对返回空串（随后鉴权失败）。
func bearerToken(req *http.Request) string {
	const prefix = "bearer "
	h := req.Header.Get("Authorization")
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

// fail 把用例错误映射为协议状态码。5xx 记错误日志（含原因），应答体只给笼统说明：
// 内部错误可能带表名、语句片段，不外泄。
func (s *RunnerService) fail(w http.ResponseWriter, req *http.Request, runnerID string, err error) {
	switch {
	case errors.Is(err, biz.ErrUnauthorized):
		writeError(w, http.StatusUnauthorized, "invalid runner token")
	case errors.Is(err, biz.ErrRunnerDisabled):
		writeError(w, http.StatusForbidden, "runner is disabled")
	case errors.Is(err, biz.ErrLeaseLost):
		writeError(w, http.StatusConflict, "execution lease lost")
	case errors.Is(err, biz.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errBodyTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
	default:
		s.log.ErrorContext(req.Context(), "runner api request failed", "path", req.URL.Path, "runner_id", runnerID, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// writeError 写 JSON 错误应答 {"error": "..."}。
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg}) // 写失败无可补救
}
