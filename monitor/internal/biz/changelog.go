package biz

import (
	"sync"
	"time"
)

// failureRemind 故障持续期间再次提醒的间隔。
const failureRemind = 10 * time.Minute

// failureLog 给「按对象的故障日志」限频：只在状态变化时（正常 → 故障、故障 → 正常）和故障持续每
// failureRemind 一次时放行，其余轮次静默。只记录处于故障中的对象，内存占用与故障对象数成正比。
// 纯粹的日志降噪，进程内状态，不承担任何正确性。并发安全。
type failureLog struct {
	mu      sync.Mutex
	failing map[string]time.Time // 对象 → 上次记日志的时刻
}

// remind 报告是否该为对象 key 的故障记一条日志：每个对象每 failureRemind 至多一次，不跟踪恢复
// （用于「部分请求失败、部分成功」会来回翻转的场景）。条目数与对象数（数据源数）成正比。
func (l *failureLog) remind(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last, ok := l.failing[key]; ok && now.Sub(last) < failureRemind {
		return false
	}
	if l.failing == nil {
		l.failing = make(map[string]time.Time)
	}
	l.failing[key] = now
	return true
}

// observe 报告对象 key 本轮的状态，返回是否该记日志：failed 为真时记 WARN，为假时记「已恢复」。
func (l *failureLog) observe(key string, failed bool, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	last, was := l.failing[key]
	switch {
	case failed && was && now.Sub(last) < failureRemind:
		return false
	case failed:
		if l.failing == nil {
			l.failing = make(map[string]time.Time)
		}
		l.failing[key] = now
		return true
	case was:
		delete(l.failing, key)
		return true
	}
	return false
}
