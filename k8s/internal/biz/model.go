package biz

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Identity 发起操作的平台用户（core 签名后随请求带入）。
type Identity = k8scontract.Identity

// Cluster 集群登记（k8s_cluster 的只读视图）。Credential 是密文，只有 data 层解密。
type Cluster struct {
	ID           string
	Name         string
	DisplayName  string
	Env          string
	AccessMode   int32
	APIServer    string
	ProxyURL     string
	CACert       string
	Credential   []byte
	ExecDisabled bool
	// Version / UpdateTime 合起来标识一次配置：任一变化即重建连接并重新自检。
	Version    int32
	UpdateTime time.Time
}

// Generation 配置代次：变化即需要重建连接。
func (c *Cluster) Generation() string {
	return fmt.Sprintf("%d/%d", c.Version, c.UpdateTime.UnixNano())
}

// Scope 一条角色数据范围（k8s_role_scope）。
type Scope struct {
	RoleID    string
	Namespace string
	Level     int32
}

// Error 带 HTTP 状态码与原因的业务错误；service 层据此写出 k8scontract.ErrorBody。
type Error struct {
	Code    int
	Reason  string
	Message string
}

// Error implements error.
func (e *Error) Error() string { return e.Reason + ": " + e.Message }

// NewError 构造业务错误。
func NewError(code int, reason, format string, args ...any) *Error {
	return &Error{Code: code, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// AsError 把任意错误归一为业务错误：非业务错误一律 500 且不外泄内部细节。
func AsError(err error) *Error {
	var be *Error
	if errors.As(err, &be) {
		return be
	}
	return &Error{Code: http.StatusInternalServerError, Reason: k8scontract.ReasonUnspecified, Message: "internal error, see the service log"}
}

// 常用错误构造器。
func errInvalid(format string, args ...any) *Error {
	return NewError(http.StatusBadRequest, k8scontract.ReasonInvalidArgument, format, args...)
}

// ErrInvalid 请求参数不合法（400）。
func ErrInvalid(format string, args ...any) *Error { return errInvalid(format, args...) }

// ErrUnsupported 该资源不支持此操作（400）。
func ErrUnsupported(format string, args ...any) *Error {
	return NewError(http.StatusBadRequest, k8scontract.ReasonUnsupported, format, args...)
}

// ErrClusterOffline 集群不可达或未通过接入自检（503）。
func ErrClusterOffline(format string, args ...any) *Error {
	return NewError(http.StatusServiceUnavailable, k8scontract.ReasonClusterOffline, format, args...)
}

// ErrCacheSyncing 缓存建立中（503）。
func ErrCacheSyncing(format string, args ...any) *Error {
	return NewError(http.StatusServiceUnavailable, k8scontract.ReasonCacheSyncing, format, args...)
}

// ErrForbidden 数据范围不足（403）。
func ErrForbidden(format string, args ...any) *Error {
	return NewError(http.StatusForbidden, k8scontract.ReasonForbidden, format, args...)
}

// ErrNotFound 对象不存在（404）。
func ErrNotFound(format string, args ...any) *Error {
	return NewError(http.StatusNotFound, k8scontract.ReasonNotFound, format, args...)
}

var (
	errClusterNotFound = &Error{Code: http.StatusNotFound, Reason: k8scontract.ReasonClusterNotFound, Message: "cluster not found or disabled"}
	errNotOwner        = &Error{Code: http.StatusMisdirectedRequest, Reason: k8scontract.ReasonNotOwner, Message: "this instance does not own the cluster"}
	errTooManyStreams  = &Error{Code: http.StatusTooManyRequests, Reason: k8scontract.ReasonTooManyStreams, Message: "too many concurrent streams or terminal sessions"}
	errExecDisabled    = &Error{Code: http.StatusForbidden, Reason: k8scontract.ReasonExecDisabled, Message: "terminal is disabled on this cluster"}
)

// Observer 业务指标观察者（server.Metrics 实现）。
type Observer interface {
	// AuditFailed 一次审计写入失败。
	AuditFailed()
	// StreamDelta 打开（+1）或关闭（-1）一条流；kind 为 log / watch / drain / exec。
	StreamDelta(kind string, delta int)
}
