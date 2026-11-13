package biz

import (
	"strings"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// ErrLoginThrottled 登录失败次数过多被临时锁定（429）；reason 与全局限流
// 中间件（kratos ratelimit）同口径，不新增 proto ErrorReason。
var ErrLoginThrottled = errors.New(429, "RATELIMIT", "too many failed login attempts, please try again later")

// 登录失败限速参数：按（用户名, 来源 IP）对、按来源 IP、按用户名三级计数（纯内存、
// 单实例口径，多实例部署时阈值按实例数放大）。主阈值挂在（用户名, IP）对上：别人
// 从其它地址输错密码不会把正主锁在门外（否则任何人都能持续锁定指定账号）。IP 阈值
// 高于它：NAT 出口后多个用户共享同一 IP。用户名全局阈值最高，只兜分布式爆破。
const (
	loginMaxFailuresPerUser       = 5
	loginMaxFailuresPerIP         = 20
	loginMaxFailuresPerUserGlobal = 50
	// loginFailureWindow 失败计数窗口：距上次失败超过该时长后计数清零。
	loginFailureWindow = 10 * time.Minute
	// loginLockout 达到阈值后的锁定时长。
	loginLockout = time.Minute
	// loginLimiterMaxEntries 计数表容量上限（防随机用户名撑爆内存）。
	loginLimiterMaxEntries = 10000
)

// loginAttempt 是单个键（用户名或 IP）的失败计数状态。
type loginAttempt struct {
	failures    int
	lastFailure time.Time
	lockedUntil time.Time
}

// loginLimiter 登录失败限速器：阈值内的连续失败触发短时锁定，锁定期内的
// 登录在密码校验前直接拒绝（不再消耗 Argon2id）。有界 map + 惰性过期。
type loginLimiter struct {
	mu      sync.Mutex
	entries map[string]*loginAttempt
	now     func() time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{entries: make(map[string]*loginAttempt, 64), now: time.Now}
}

// loginLimiterKeys 返回（用户名, IP）对键、用户名全局键与 IP 键（IP 取不到时无 IP 键，
// 对键退化为仅用户名）；用户名不区分大小写与首尾空白，防变体绕过计数。
func loginLimiterKeys(username, ip string) (pairKey, userKey, ipKey string) {
	name := strings.ToLower(strings.TrimSpace(username))
	pairKey, userKey = "p:"+name+"|"+ip, "u:"+name
	if ip != "" {
		ipKey = "i:" + ip
	}
	return pairKey, userKey, ipKey
}

// blocked 报告（用户名, IP）对、用户名全局或 IP 是否处于锁定期。
func (l *loginLimiter) blocked(username, ip string) bool {
	pairKey, userKey, ipKey := loginLimiterKeys(username, ip)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range []string{pairKey, userKey, ipKey} {
		if e, ok := l.entries[key]; ok && now.Before(e.lockedUntil) {
			return true
		}
	}
	return false
}

// fail 记一次失败；达到阈值即锁定并清零计数（锁定结束后重新累计）。
func (l *loginLimiter) fail(username, ip string) {
	pairKey, userKey, ipKey := loginLimiterKeys(username, ip)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bump(pairKey, loginMaxFailuresPerUser, now)
	l.bump(userKey, loginMaxFailuresPerUserGlobal, now)
	if ipKey != "" {
		l.bump(ipKey, loginMaxFailuresPerIP, now)
	}
}

// succeed 登录成功后清零该（用户名, IP）对的失败计数。IP 计数不清（同一来源对其它
// 账号的失败仍应累计）；用户名全局计数也不清（分布式爆破期间正主登录不应替攻击者
// 重置额度），靠窗口自然过期。
func (l *loginLimiter) succeed(username, ip string) {
	pairKey, _, _ := loginLimiterKeys(username, ip)
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, pairKey)
}

func (l *loginLimiter) bump(key string, limit int, now time.Time) {
	e, ok := l.entries[key]
	if !ok {
		if len(l.entries) >= loginLimiterMaxEntries {
			l.evict(now)
		}
		e = &loginAttempt{}
		l.entries[key] = e
	} else if now.Sub(e.lastFailure) > loginFailureWindow {
		e.failures = 0
	}
	e.failures++
	e.lastFailure = now
	if e.failures >= limit {
		e.failures = 0
		e.lockedUntil = now.Add(loginLockout)
	}
}

// evict 容量触顶时清理已过期条目；仍无空位则任意淘汰一条（保持有界优先
// 于计数精确）。
func (l *loginLimiter) evict(now time.Time) {
	for key, e := range l.entries {
		if now.Sub(e.lastFailure) > loginFailureWindow && !now.Before(e.lockedUntil) {
			delete(l.entries, key)
		}
	}
	if len(l.entries) < loginLimiterMaxEntries {
		return
	}
	for key := range l.entries {
		delete(l.entries, key)
		return
	}
}
