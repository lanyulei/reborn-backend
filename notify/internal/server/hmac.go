package server

import (
	"context"
	"io"
	"net/http"

	"github.com/lanyulei/reborn/backend/kit/httpx"
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
			producer, err := auth.Verify(ctx, header, r.Method, r.URL.Path, body)
			if err != nil {
				return nil, err
			}
			return handler(WithProducer(ctx, producer), req)
		}
	}
}
