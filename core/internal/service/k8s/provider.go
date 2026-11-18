// Package service 容器平台域 proto 接口实现：只做身份提取、参数编排与 proto ↔ 领域模型 / 契约结构的转换。
// 资源类接口把请求转换为 kit/k8scontract 的请求结构后交给 biz.PlaneUsecase，响应体是数据面返回的 JSON，
// 以 google.api.HttpBody 原样透传。任何响应都不含集群凭据。
package service

import (
	"context"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/go-kratos/kratos/v3/transport"
	"github.com/google/wire"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is 容器平台域 service providers。
var ProviderSet = wire.NewSet(NewClusterService, NewScopeService, NewAuditService, NewResourceService, NewWorkloadService, NewStreamService, NewHelmRepoService, NewHelmReleaseService, NewInsightService, NewChangeService, NewGovernanceService)

// callerFrom 从传输上下文提取调用者：用户 ID 取自鉴权后的 claims，来源 IP 用 system 域的统一口径。
func callerFrom(ctx context.Context) (biz.Caller, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return biz.Caller{}, err
	}
	caller := biz.Caller{UserID: userID, ClientIP: systemservice.ClientIP(ctx)}
	// 紧急变更的意愿由请求头表达；是否允许在 biz 层按接口授权判定。
	if tr, ok := transport.FromServerContext(ctx); ok && tr.RequestHeader().Get(kc.EmergencyHeader) == "1" {
		caller.Emergency = true
	}
	return caller, nil
}

// jsonBody 把数据面返回的 JSON 包成 HttpBody 原样透传。
func jsonBody(raw []byte) *httpbody.HttpBody {
	return &httpbody.HttpBody{ContentType: "application/json", Data: raw}
}

// optTime proto 时间戳 → *time.Time（未设置为 nil）。
func optTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

// protoTime *time.Time → proto 时间戳（nil 保持为空）。
func protoTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
