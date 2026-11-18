package biz

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

// 日志续接游标：<行时间戳 RFC3339Nano>/<该时间戳下的第几行，从 0 起>。
// 只有时间戳不足以定位：同一纳秒可以有多行（批量写出、时钟精度不足的运行时）。续接时以游标的时间戳
// 作为起点向集群重新请求，再丢弃「时间戳更早」以及「时间戳相同且序号不大于游标」的行，做到不丢不重。

// LogCursor 一行日志的位置。
type LogCursor struct {
	Time time.Time
	Seq  int
}

// String 游标的文本形式（log 事件的 id）。
func (c LogCursor) String() string {
	return c.Time.UTC().Format(time.RFC3339Nano) + "/" + strconv.Itoa(c.Seq)
}

// ParseLogCursor 解析游标；格式不对返回 ok = false。
func ParseLogCursor(s string) (LogCursor, bool) {
	ts, seq, ok := strings.Cut(s, "/")
	if !ok {
		return LogCursor{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return LogCursor{}, false
	}
	n, err := strconv.Atoi(seq)
	if err != nil || n < 0 {
		return LogCursor{}, false
	}
	return LogCursor{Time: t, Seq: n}, true
}

// LogTracker 为一条日志流的每一行计算游标，并在续接时判定哪些行已经发过。非并发安全。
type LogTracker struct {
	resume    LogCursor
	hasResume bool
	last      time.Time
	seq       int
	started   bool
}

// NewLogTracker 由请求里的游标构造；游标为空或非法时从头开始。
func NewLogTracker(cursor string) *LogTracker {
	c, ok := ParseLogCursor(cursor)
	return &LogTracker{resume: c, hasResume: ok}
}

// Since 续接时向集群请求的起始时间（游标的时间戳）；不是续接返回 ok = false。
func (t *LogTracker) Since() (time.Time, bool) { return t.resume.Time, t.hasResume }

// Next 推进到时间戳为 ts 的下一行，返回它的游标以及是否应当下发（false = 续接前已发过）。
// 行必须按集群返回的顺序逐行送入，包括被关键字过滤掉的行——序号按原始行计数。
func (t *LogTracker) Next(ts time.Time) (cursor string, emit bool) {
	if t.started && ts.Equal(t.last) {
		t.seq++
	} else {
		t.last, t.seq, t.started = ts, 0, true
	}
	cur := LogCursor{Time: ts, Seq: t.seq}
	if t.hasResume {
		if ts.Before(t.resume.Time) || (ts.Equal(t.resume.Time) && t.seq <= t.resume.Seq) {
			return cur.String(), false
		}
	}
	return cur.String(), true
}

// ParseLogLine 拆出带时间戳的日志行（timestamps=true：<RFC3339Nano> <正文>）。
// 时间戳无法解析时 ok = false，调用方应把整行当正文并沿用上一行的时间。
func ParseLogLine(line []byte) (ts time.Time, text []byte, ok bool) {
	head, rest, found := bytes.Cut(line, []byte{' '})
	if !found {
		head, rest = line, nil
	}
	t, err := time.Parse(time.RFC3339Nano, string(head))
	if err != nil {
		return time.Time{}, line, false
	}
	return t, rest, true
}
