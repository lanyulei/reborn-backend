package kube

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// mapError 把集群与网络错误归一为业务错误，保留集群返回的原文。
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var be *biz.Error
	if errors.As(err, &be) {
		return be
	}
	if errors.Is(err, errNoIdentity) {
		return biz.NewError(http.StatusInternalServerError, k8scontract.ReasonUnspecified, "internal error: request without identity")
	}
	var st apierrors.APIStatus
	if errors.As(err, &st) {
		s := st.Status()
		msg := s.Message
		if msg == "" {
			msg = err.Error()
		}
		switch s.Code {
		case http.StatusBadRequest:
			return biz.NewError(http.StatusBadRequest, k8scontract.ReasonInvalidArgument, "%s", msg)
		case http.StatusForbidden:
			return biz.NewError(http.StatusForbidden, k8scontract.ReasonForbidden, "%s", msg)
		case http.StatusNotFound:
			return biz.NewError(http.StatusNotFound, k8scontract.ReasonNotFound, "%s", msg)
		case http.StatusConflict:
			return biz.NewError(http.StatusConflict, k8scontract.ReasonConflict, "%s", msg)
		case http.StatusUnprocessableEntity:
			return biz.NewError(http.StatusUnprocessableEntity, k8scontract.ReasonRejected, "%s", msg)
		case http.StatusMethodNotAllowed, http.StatusUnsupportedMediaType, http.StatusNotAcceptable:
			return biz.NewError(http.StatusBadRequest, k8scontract.ReasonUnsupported, "%s", msg)
		case http.StatusTooManyRequests:
			// 驱逐被 PodDisruptionBudget 拒绝、或集群限流：都属于「现在不行，稍后可重试」。
			return biz.NewError(http.StatusConflict, k8scontract.ReasonConflict, "%s", msg)
		case http.StatusUnauthorized:
			return biz.NewError(http.StatusBadGateway, k8scontract.ReasonUpstream, "cluster rejected the platform credential: %s", msg)
		}
		return biz.NewError(http.StatusBadGateway, k8scontract.ReasonUpstream, "%s", msg)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return biz.NewError(http.StatusBadGateway, k8scontract.ReasonUpstream, "request to the cluster timed out or was cancelled")
	}
	var ne net.Error
	if errors.Is(err, ErrAgentOffline) || errors.As(err, &ne) {
		return biz.ErrClusterOffline("%s", err.Error())
	}
	return biz.NewError(http.StatusBadGateway, k8scontract.ReasonUpstream, "%s", err.Error())
}
