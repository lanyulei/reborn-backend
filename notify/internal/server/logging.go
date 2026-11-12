package server

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

// ingressRequestResult 按 HTTP 语义归类请求结果（指标 result 标签）。
func ingressRequestResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case kerrors.Code(err) == http.StatusUnauthorized:
		return "unauthorized"
	case kerrors.Code(err) == http.StatusTooManyRequests:
		return "quota_exceeded"
	case kerrors.Code(err) == http.StatusBadRequest:
		return "invalid"
	default:
		return "error"
	}
}

// ingressLogSampleEvery ingress 成功请求日志采样率（1/N）。
const ingressLogSampleEvery = 100

// ingressLogging ingress 操作的请求日志：失败恒记（WARN），成功按 1/N 采样（INFO，
// 带采样率字段）——1 万/s 接入下逐条 INFO 日志本身就是吞吐瓶颈；生产者/事件数
// 等计量走指标而非日志。
func ingressLogging(logger *slog.Logger, sampleEvery uint64, observe func(result string)) middleware.Middleware {
	if sampleEvery == 0 {
		sampleEvery = 1
	}
	if observe == nil {
		observe = func(string) {}
	}
	var seq atomic.Uint64
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			reply, err := handler(ctx, req)
			observe(ingressRequestResult(err))
			operation := ""
			if tr, ok := transport.FromServerContext(ctx); ok {
				operation = tr.Operation()
			}
			if err != nil {
				logger.WarnContext(ctx, "ingress request failed", "operation", operation, "latency_ms", time.Since(start).Milliseconds(), "error", err)
				return reply, err
			}
			if seq.Add(1)%sampleEvery == 0 {
				logger.InfoContext(ctx, "ingress request", "operation", operation, "latency_ms", time.Since(start).Milliseconds(), "sampled", sampleEvery)
			}
			return reply, nil
		}
	}
}
