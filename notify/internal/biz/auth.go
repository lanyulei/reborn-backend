package biz

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	// ErrReplayed 时间窗内重复出现的已签名请求且无 dedup_key 兜底（对外同 401 口径）。
	ErrReplayed      = fmt.Errorf("%w: replayed request without dedup_key", ErrUnauthorized)
	ErrQuotaExceeded = kerrors.New(429, ingressv1.ErrorReason_INGRESS_QUOTA_EXCEEDED.String(), "producer quota exceeded")
)

// ProducerAuth 生产者 HMAC 鉴权（协议见 pkg/ingressauth）：生产者快照来自
// ConfigCache（停用/轮换 ≤10s 生效，与设计一致）。验签通过的 (生产者, 签名) 记入
// 进程内防重放表：时间窗内同一签名再次出现即重放（签名覆盖时间戳，合法客户端每次
// 请求的签名都不同；原样重发除外，见 Admission.Replayed）。
type ProducerAuth struct {
	cfg    *ConfigCache
	skew   time.Duration
	now    func() time.Time
	replay *replayGuard
	// store 跨副本共享的防重放登记（PG）；nil 时只有进程内表（单副本 / 测试）。
	store ReplayStore
}

// ReplayStore 跨副本共享的防重放登记：进程内表只覆盖本实例，多副本部署时同一
// 已签名请求打到另一副本需要靠它识别。实现须原子（主键冲突即重放）。
type ReplayStore interface {
	// RegisterReplay 登记 (生产者, 签名)，保留到 until；首次登记返回 true。
	RegisterReplay(ctx context.Context, producerID, signature string, until time.Time) (bool, error)
	// ForgetReplay 撤销登记（请求未被受理时调用）。
	ForgetReplay(ctx context.Context, producerID, signature string) error
}

// NewProducerAuth new a ProducerAuth（缺省时间窗 ±5min）；store 可为 nil。
func NewProducerAuth(cfg *ConfigCache, store ReplayStore) *ProducerAuth {
	return &ProducerAuth{cfg: cfg, skew: 5 * time.Minute, now: time.Now, replay: newReplayGuard(replayGuardMax), store: store}
}

// WithClockSkew 覆盖时间窗。
func (a *ProducerAuth) WithClockSkew(d time.Duration) *ProducerAuth {
	if d > 0 {
		a.skew = d
	}
	return a
}

// Admission 一次验签通过的请求。
type Admission struct {
	Producer *Producer
	// Replayed 时间窗内本实例已受理过同一签名的请求。调用方须拒绝不带 dedup_key 的
	// 重放（否则事件被重复落库）；带 dedup_key 的放行——由 ingress 的 dedup 门闩幂等
	// 应答，客户端原样重发已签名请求（超时重试）不受影响。
	Replayed bool

	guard *replayGuard
	store ReplayStore
	key   replayKey
}

// Forget 撤销本次请求的防重放登记：请求最终未被受理（处理出错）时调用，客户端可
// 原样重发。重放请求（Replayed）不持有登记，调用为 no-op。
func (ad *Admission) Forget() {
	if ad.guard == nil || ad.Replayed {
		return
	}
	ad.guard.forget(ad.key)
	if ad.store != nil {
		// 撤销用脱离请求取消的短时 ctx：请求已失败 / 已取消，登记仍须撤掉，否则
		// 客户端在时间窗内无法原样重发。撤销失败只会让重发被拒（需重新签名），不影响正确性。
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ad.store.ForgetReplay(ctx, ad.key.producer, ad.key.sig)
	}
}

// Verify 校验 Authorization 头：解析 → 时间窗 → 生产者存在/启用/未过期 →
// 常量时间比对签名 → 防重放登记；鉴权失败全部归一为 ErrUnauthorized。
func (a *ProducerAuth) Verify(ctx context.Context, header, method, path string, body []byte) (*Admission, error) {
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
	// 验签通过 ⇒ 签名恰为小写 hex(HMAC)，无等价变体；键取生产者 + 签名。登记保留到
	// 该时间戳滑出时间窗（之后由时间窗校验拒绝）。
	key := replayKey{producer: p.ID, sig: cred.Signature}
	until := ts.Add(a.skew)
	replayed := a.replay.seen(key, until, now, 2*a.skew)
	// 本实例未见过：再到共享存储登记，识别其它副本已受理的同一请求。
	if !replayed && a.store != nil {
		fresh, err := a.store.RegisterReplay(ctx, p.ID, cred.Signature, until)
		if err != nil {
			a.replay.forget(key) // 未完成登记：不留下半截状态
			return nil, err
		}
		if !fresh {
			// 其它副本受理过：这次请求不持有登记（Forget 为 no-op）。本实例的进程内
			// 登记一并撤掉，以共享存储为准——否则持有方撤销登记后，原样重发打到
			// 本实例仍会被进程内表误判为重放。
			a.replay.forget(key)
			return &Admission{Producer: p, Replayed: true}, nil
		}
	}
	return &Admission{Producer: p, Replayed: replayed, guard: a.replay, store: a.store, key: key}, nil
}

// replayGuardMax 防重放表单代容量上限（两代合计至多 2× 条目，约数十 MB）。
const replayGuardMax = 1 << 17

type replayKey struct{ producer, sig string }

// replayGuard 进程内防重放表（**按实例**：多副本部署时同一请求在每个副本各可被受理
// 一次；进程重启后表清空——跨实例/跨重启的幂等仍靠 dedup_key 门闩）。两代 map 轮转：
// 每 ttl（= 2×时间窗，登记所需的最长寿命）或当前代写满时换代，条目至少存活一个
// ttl、内存有界、无后台 goroutine。写满提前换代会提前遗忘最旧的一代（宁可放过
// 极端流量下的旧重放，也不拒绝新请求）。
type replayGuard struct {
	mu        sync.Mutex
	cur, prev map[replayKey]int64 // 值 = 登记失效时刻（Unix 秒）
	rotated   time.Time
	max       int
}

func newReplayGuard(max int) *replayGuard {
	return &replayGuard{cur: map[replayKey]int64{}, max: max}
}

// seen 报告 key 是否已登记且未失效；未登记则登记（原子：并发的两个相同请求恰有一个
// 得到 false）。
func (g *replayGuard) seen(key replayKey, until, now time.Time, ttl time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.Sub(g.rotated) >= ttl || len(g.cur) >= g.max {
		g.prev, g.cur, g.rotated = g.cur, make(map[replayKey]int64), now
	}
	exp, ok := g.cur[key]
	if !ok {
		exp, ok = g.prev[key]
	}
	if ok && now.Unix() <= exp {
		return true
	}
	g.cur[key] = until.Unix()
	return false
}

func (g *replayGuard) forget(key replayKey) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.cur, key)
	delete(g.prev, key)
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
