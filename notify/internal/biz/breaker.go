package biz

import (
	"sync"
	"time"
)

// 熔断参数（设计 §4.6）：1 分钟滑窗、≥10 样本、失败率 >50% → 打开 60s，半开放 1 条探测。
const (
	breakerWindow     = time.Minute
	breakerMinSamples = 10
	breakerThreshold  = 0.5
	breakerOpenFor    = 60 * time.Second
)

// Breaker 进程内按渠道实例的熔断器（P1 口径：N 副本 = N 组独立状态与探针，
// AGENTS.md 明示；跨实例共享留 P2）。
type Breaker struct {
	mu     sync.Mutex
	states map[string]*breakerState
	now    func() time.Time
}

type breakerState struct {
	samples   []sample // 滑窗样本
	openUntil time.Time
	probing   bool // 半开：已放出 1 条探测，等待结果
}

type sample struct {
	at time.Time
	ok bool
}

// NewBreaker new a Breaker.
func NewBreaker() *Breaker {
	return &Breaker{states: make(map[string]*breakerState), now: time.Now}
}

// Allow 报告渠道当前是否允许发送：关闭→允许；打开→拒绝；打开到期→半开放 1 条探测。
func (b *Breaker) Allow(channelID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state(channelID)
	now := b.now()
	if st.openUntil.IsZero() {
		return true
	}
	if now.Before(st.openUntil) {
		return false
	}
	if st.probing {
		return false
	}
	st.probing = true
	return true
}

// Record 记录一次发送结果并重算状态。
func (b *Breaker) Record(channelID string, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state(channelID)
	now := b.now()
	if st.probing {
		st.probing = false
		if ok {
			st.openUntil = time.Time{}
			st.samples = nil
			return
		}
		st.openUntil = now.Add(breakerOpenFor)
		return
	}
	st.samples = append(st.samples, sample{at: now, ok: ok})
	cutoff := now.Add(-breakerWindow)
	i := 0
	for i < len(st.samples) && st.samples[i].at.Before(cutoff) {
		i++
	}
	st.samples = st.samples[i:]
	if len(st.samples) < breakerMinSamples {
		return
	}
	failed := 0
	for _, s := range st.samples {
		if !s.ok {
			failed++
		}
	}
	if float64(failed)/float64(len(st.samples)) > breakerThreshold {
		st.openUntil = now.Add(breakerOpenFor)
		st.samples = nil
	}
}

// Open 报告渠道是否处于打开状态（指标用）。
func (b *Breaker) Open(channelID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := b.state(channelID)
	return !st.openUntil.IsZero() && b.now().Before(st.openUntil)
}

func (b *Breaker) state(id string) *breakerState {
	st, ok := b.states[id]
	if !ok {
		st = &breakerState{}
		b.states[id] = st
	}
	return st
}
