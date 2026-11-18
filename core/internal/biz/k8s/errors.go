// Package biz 容器平台域（k8s）管理面的业务核心：集群登记、Agent 令牌、角色数据范围、操作审计查询、
// 终端票据，以及把资源类请求连同调用者身份转给数据面。定义仓储与数据面访问的窄接口（依赖倒置），
// 零驱动、零传输层 import。
//
// 数据面（backend/k8s）是独立进程：两边共享的只有 k8s_* 表与 kit/k8scontract。安全边界：
//   - 直连凭据只写不读——本包只调用 SecretSealer.Seal，没有解密入口；解密只发生在数据面；
//   - core 不连接任何集群：凡需要访问集群的动作（含测试连接）一律经 Plane 转给数据面；
//   - 数据面只信任 core 签名的身份（Identity）：身份一律由 PlaneUsecase 从登录用户实时解析
//     （启用角色、是否超管），不接受调用方自带的角色；
//   - 数据范围不在本包判定：能否对具体资源操作由集群按该身份判定。
package biz

import (
	"fmt"

	v1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误：reason 来自 api/k8s/v1 错误码枚举，HTTP 状态随枚举映射。
var (
	ErrVersionConflict   = errors.Conflict(v1.ErrorReason_K8S_VERSION_CONFLICT.String(), "modified by someone else, please refresh and retry")
	ErrSecretUnavailable = errors.ServiceUnavailable(v1.ErrorReason_K8S_SECRET_UNAVAILABLE.String(), "credential master key is not configured")
	ErrClusterNameExists = errors.Conflict(v1.ErrorReason_K8S_CLUSTER_NAME_EXISTS.String(), "cluster name already exists")
	ErrClusterInvalid    = errors.BadRequest(v1.ErrorReason_K8S_CLUSTER_INVALID.String(), "invalid cluster")
	ErrScopeInvalid      = errors.BadRequest(v1.ErrorReason_K8S_SCOPE_INVALID.String(), "invalid role scope")
	ErrAuditNotFound     = errors.NotFound(v1.ErrorReason_K8S_AUDIT_NOT_FOUND.String(), "audit record not found")
	ErrTimeRangeInvalid  = errors.BadRequest(v1.ErrorReason_K8S_TIME_RANGE_INVALID.String(), "invalid time range")
	ErrTicketInvalid     = errors.Unauthorized(v1.ErrorReason_K8S_TICKET_INVALID.String(), "terminal ticket is invalid or expired")
	ErrAgentUnconfigured = errors.ServiceUnavailable(v1.ErrorReason_K8S_AGENT_UNCONFIGURED.String(), "agent tunnel endpoint or image is not configured (k8s.agent.server_url / k8s.agent.image)")
	ErrClusterNotFound   = errors.NotFound(v1.ErrorReason_K8S_CLUSTER_NOT_FOUND.String(), "cluster not found")
	ErrDebugForbidden    = errors.Forbidden(v1.ErrorReason_K8S_FORBIDDEN.String(), "debug containers require the debug-container permission")
	ErrExecDisabled      = errors.Forbidden(v1.ErrorReason_K8S_EXEC_DISABLED.String(), "terminal is disabled on this cluster")
	ErrPlaneUnavailable  = errors.ServiceUnavailable(v1.ErrorReason_K8S_PLANE_UNAVAILABLE.String(), "no k8s data plane instance is available")
	ErrInvalidArgument   = errors.BadRequest(v1.ErrorReason_K8S_INVALID_ARGUMENT.String(), "invalid argument")
)

// detail 以哨兵错误的 code/reason 构造带具体说明的错误（errors.Is 按 code+reason 匹配，
// 前端按 message 展示原因）。说明里不得出现凭据与令牌内容。
func detail(base *errors.Error, format string, args ...any) error {
	return errors.New(int(base.Code), base.Reason, fmt.Sprintf(format, args...))
}

// PlaneUnavailable 数据面不可用的具体原因（未配置内部密钥、没有存活实例、连接失败等）。
func PlaneUnavailable(format string, args ...any) error {
	return detail(ErrPlaneUnavailable, format, args...)
}

// PlaneError 把数据面返回的错误体（kit/k8scontract.ErrorBody）还原为同 code、同 reason 的业务错误。
// reason 不在本域错误码枚举内（数据面与 core 版本不一致）时按 502 K8S_UPSTREAM_ERROR 处理。
func PlaneError(code int, reason, message string) error {
	if _, ok := v1.ErrorReason_value[reason]; !ok || code < 400 || code > 599 {
		return errors.New(502, v1.ErrorReason_K8S_UPSTREAM_ERROR.String(), message)
	}
	return errors.New(code, reason, message)
}

// IsNotOwner 报告错误是否为「该数据面实例不负责此集群」（调用方刷新归属后重试一次）。
func IsNotOwner(err error) bool {
	e := errors.FromError(err)
	return e != nil && e.Reason == v1.ErrorReason_K8S_NOT_OWNER.String()
}
