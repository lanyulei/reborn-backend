package server

import (
	"context"
	"io"
	"net/http"

	"github.com/lanyulei/reborn/backend/kit/httpx"
	ingressv1 "github.com/lanyulei/reborn/backend/notify/api/ingress/v1"
	"github.com/lanyulei/reborn/backend/notify/internal/biz"

	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

type producerKey struct{}

// ProducerFromContext 取鉴权中间件注入的生产者。
func ProducerFromContext(ctx context.Context) (*biz.Producer, bool) {
	p, ok := ctx.Value(producerKey{}).(*biz.Producer)
	return p, ok
}

// WithProducer 注入生产者（测试脚手架用）。
func WithProducer(ctx context.Context, p *biz.Producer) context.Context {
	return context.WithValue(ctx, producerKey{}, p)
}

// hmacMiddleware ingress 鉴权（仅 HTTP）：以 METHOD/PATH/body 哈希构造规范串
// （body 由 codec 回放，解码后仍可读）。非 HTTP 传输一律 401（数据面只承诺 HTTP）。
func hmacMiddleware(auth *biz.ProducerAuth) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return nil, biz.ErrUnauthorized
			}
			r, ok := khttp.RequestFromServerContext(ctx)
			if !ok || r == nil {
				return nil, biz.ErrUnauthorized
			}
			header := tr.RequestHeader().Get("Authorization")
			var body []byte
			if r.Body != nil && r.Body != http.NoBody {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, httpx.ErrBadRequest(err)
				}
				body = data
			}
			ad, err := auth.Verify(ctx, header, r.Method, r.URL.Path, body)
			if err != nil {
				return nil, err
			}
			// 重放：带 dedup_key 的交给 dedup 门闩幂等应答（客户端原样重发不受影响），
			// 否则拒绝——无门闩兜底的事件会被重复落库。
			if ad.Replayed && !dedupGuarded(req) {
				return nil, biz.ErrReplayed
			}
			resp, err := handler(WithProducer(ctx, ad.Producer), req)
			if err != nil {
				ad.Forget() // 未受理（校验/配额/落库失败）：允许原样重发
			}
			return resp, err
		}
	}
}

// dedupGuarded 报告请求内的事件是否全部携带 dedup_key（重放时由门闩保证幂等）。
func dedupGuarded(req any) bool {
	switch r := req.(type) {
	case *ingressv1.SendEventRequest:
		return r.GetEvent().GetDedupKey() != ""
	case *ingressv1.BatchSendRequest:
		for _, e := range r.GetEvents() {
			if e.GetDedupKey() == "" {
				return false
			}
		}
		return len(r.GetEvents()) > 0
	default:
		return false
	}
}
