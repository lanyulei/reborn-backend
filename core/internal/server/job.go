package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	jobv1 "github.com/lanyulei/reborn/backend/core/api/job/v1"
	jobservice "github.com/lanyulei/reborn/backend/core/internal/service/job"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	khttp "github.com/go-kratos/kratos/v3/transport/http"
)

// jobOpenFireMaxBody 开放触发接口的请求体上限：流程服务节点的请求体带流程变量，1MiB 足够。
const jobOpenFireMaxBody = 1 << 20

// registerJobOpen 以原生路由绑定作业中心的开放触发接口（POST /api/v1/open/job/trigger/{id}）。
//
// 不经生成代码绑定的原因：认证方式之一是流程 Webhook 的 HMAC 签名，它对「原始请求体」计算，
// 而生成的绑定只给出解码后的消息。接口不走用户登录（见 authExempt），凭请求头里的调用令牌或签名认证，
// 校验在用例层完成；其余中间件（recovery / tracing / logging / 限流）照常经过。
func registerJobOpen(srv *khttp.Server, triggers *jobservice.TriggerService) {
	srv.Route("/api/v1/open/job").POST("/trigger/{id}", func(ctx khttp.Context) error {
		khttp.SetOperation(ctx, systemservice.OperationJobOpenFire)
		req := ctx.Request()
		// 先读原始请求体（签名用）；上限之外的内容直接拒绝，不截断后继续。
		body, err := io.ReadAll(io.LimitReader(req.Body, jobOpenFireMaxBody+1))
		if err != nil || len(body) > jobOpenFireMaxBody {
			return jobv1.ErrorJobTriggerInvalid("request body is unreadable or larger than 1MiB")
		}
		headers := jobservice.OpenFireHeaders{
			Authorization: req.Header.Get("Authorization"),
			Timestamp:     req.Header.Get("X-Workflow-Timestamp"),
			Signature:     req.Header.Get("X-Workflow-Signature"),
			WorkflowJobID: req.Header.Get("X-Workflow-Job-Id"),
		}
		id := ctx.Vars().Get("id")
		// 预检不带请求内容：令牌与签名不进请求日志。
		handler := ctx.Middleware(func(c context.Context, _ any) (any, error) {
			return triggers.OpenFire(c, id, headers, body)
		})
		out, err := handler(ctx, nil)
		if err != nil {
			return err
		}
		w := ctx.Response()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		return json.NewEncoder(w).Encode(out)
	})
}
