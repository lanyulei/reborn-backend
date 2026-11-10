package biz

import (
	"context"
	"errors"
	"fmt"
	"time"

	ingressv1 "github.com/lanyulei/reborn/backend/notify/api/ingress/v1"
	"github.com/lanyulei/reborn/backend/notify/pkg/ingressauth"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// ingress 哨兵错误（reason 与 api/ingress/v1 ErrorReason 对齐）。
var (
	// ErrUnauthorized 鉴权失败统一口径（防枚举：不区分生产者不存在/停用/过期/签名错）。
	ErrUnauthorized = kerrors.Unauthorized(ingressv1.ErrorReason_INGRESS_UNAUTHORIZED.String(), "unauthorized")
	// ErrForbidden token scope 不允许（biz 不一致 / 事件类型不在白名单）。
	ErrForbidden = kerrors.Forbidden(ingressv1.ErrorReason_INGRESS_FORBIDDEN.String(), "producer scope does not allow this event")
	// ErrEventInvalid 事件参数不合法。
	ErrEventInvalid = kerrors.BadRequest(ingressv1.ErrorReason_INGRESS_EVENT_INVALID.String(), "invalid event")
	// ErrQuotaExceeded 生产者配额超限（整批未落库）。
	ErrQuotaExceeded = kerrors.New(429, ingressv1.ErrorReason_INGRESS_QUOTA_EXCEEDED.String(), "producer quota exceeded")
)

// ProducerAuth 生产者 HMAC 鉴权（协议见 pkg/ingressauth）：生产者快照来自
// ConfigCache（停用/轮换 ≤10s 生效，与设计一致）。
type ProducerAuth struct {
	cfg  *ConfigCache
	skew time.Duration
	now  func() time.Time
}

// NewProducerAuth new a ProducerAuth（缺省时间窗 ±5min）。
func NewProducerAuth(cfg *ConfigCache) *ProducerAuth {
	return &ProducerAuth{cfg: cfg, skew: 5 * time.Minute, now: time.Now}
}

// WithClockSkew 覆盖时间窗。
func (a *ProducerAuth) WithClockSkew(d time.Duration) *ProducerAuth {
	if d > 0 {
		a.skew = d
	}
	return a
}

// Verify 校验 Authorization 头：解析 → 时间窗 → 生产者存在/启用/未过期 →
// 常量时间比对签名；全部失败归一为 ErrUnauthorized。
func (a *ProducerAuth) Verify(_ context.Context, header, method, path string, body []byte) (*Producer, error) {
	cred, err := ingressauth.Parse(header)
	if err != nil {
		return nil, ErrUnauthorized
	}
	now := a.now()
	ts := time.Unix(cred.Timestamp, 0)
	if ts.Before(now.Add(-a.skew)) || ts.After(now.Add(a.skew)) {
		return nil, ErrUnauthorized
	}
	snap := a.cfg.Current()
	if snap == nil {
		return nil, errors.New("config snapshot not loaded")
	}
	p := snap.Producers[cred.ProducerID]
	if p == nil || !p.Enabled || (p.ExpireTime != nil && !p.ExpireTime.After(now)) || len(p.TokenHash) == 0 {
		return nil, ErrUnauthorized
	}
	if !ingressauth.Verify(p.TokenHash, ingressauth.Canonical(cred.Timestamp, method, path, body), cred.Signature) {
		return nil, ErrUnauthorized
	}
	return p, nil
}

// Authorize 校验事件是否在生产者 scope 内：biz 一致且事件类型命中白名单（空=不限）。
func (p *Producer) Authorize(bizID, eventType string) error {
	if bizID != p.BizID {
		return fmt.Errorf("%w: biz %q not bound to producer", ErrForbidden, bizID)
	}
	if len(p.EventWhitelist) == 0 {
		return nil
	}
	for _, pattern := range p.EventWhitelist {
		if MatchPattern(pattern, eventType) {
			return nil
		}
	}
	return fmt.Errorf("%w: event_type %q not in whitelist", ErrForbidden, eventType)
}
