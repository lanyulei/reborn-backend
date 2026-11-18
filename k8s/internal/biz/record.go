package biz

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// recorder 终端录制：只记输出与窗口大小，不记输入（用户在提示符下键入的口令不会回显，因而不会进入录制；
// 会回显的输入作为输出被记录——与旁观者在屏幕上看到的一致）。
//
// 输出路径（session.Write）上只做内存追加；压缩与落库由会话的管理循环在热路径之外完成。
// 缓冲有上限：落库跟不上（数据库故障）或会话超过录制上限时停止录制并标记不完整，不影响终端本身。
type recorder struct {
	start time.Time
	max   int64

	mu        sync.Mutex
	buf       bytes.Buffer // 未落库的事件行
	total     int64        // 已录制的输出字节数
	truncated bool
	dirty     bool // truncated 状态尚未落库
	seq       int
	lastFlush time.Time
}

// recordBufferCap 未落库缓冲的上限（落库持续失败时的保护）。
const recordBufferCap = 4 << 20

func newRecorder(maxBytes int64, now time.Time) *recorder {
	return &recorder{start: now, max: maxBytes, lastFlush: now}
}

func (r *recorder) line(typ, data string) {
	raw, _ := json.Marshal([]any{time.Since(r.start).Milliseconds(), typ, data})
	r.buf.Write(raw)
	r.buf.WriteByte('\n')
}

// output 记录一段终端输出。
func (r *recorder) output(p []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.truncated {
		return
	}
	if r.total+int64(len(p)) > r.max || r.buf.Len() > recordBufferCap {
		r.truncated, r.dirty = true, true
		return
	}
	r.total += int64(len(p))
	r.line(k8scontract.RecordOutput, base64.StdEncoding.EncodeToString(p))
}

// lost 一块录制内容落库失败：录制从此不完整。
func (r *recorder) lost() {
	r.mu.Lock()
	r.truncated, r.dirty = true, true
	r.mu.Unlock()
}

// resize 记录窗口大小变化。
func (r *recorder) resize(cols, rows uint16) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.truncated {
		return
	}
	cols, rows = ClampTermSize(cols, rows)
	r.line(k8scontract.RecordResize, fmt.Sprintf("%dx%d", cols, rows))
}

// take 取出一块待落库的内容（gzip）。force 为 false 时只在缓冲够大或距上次落库够久时取。
// 没有内容但 truncated 状态待落库时返回 nil 块与 ok = true。
func (r *recorder) take(now time.Time, force bool) (seq int, chunk []byte, truncated, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.buf.Len() == 0 {
		if r.dirty {
			r.dirty = false
			return 0, nil, true, true
		}
		return 0, nil, false, false
	}
	if !force && r.buf.Len() < k8scontract.RecordFlushBytes && now.Sub(r.lastFlush) < k8scontract.RecordFlushInterval {
		return 0, nil, false, false
	}
	var out bytes.Buffer
	zw := gzip.NewWriter(&out)
	_, _ = zw.Write(r.buf.Bytes())
	_ = zw.Close()
	r.buf.Reset()
	r.seq++
	r.lastFlush = now
	truncated = r.truncated
	r.dirty = false
	return r.seq, out.Bytes(), truncated, true
}

// WithRecording 设置终端录制：maxBytes ≤ 0 关闭录制。启动期调用。
func (m *ExecManager) WithRecording(maxBytes int64) {
	m.limits.RecordMax = max(maxBytes, 0)
}
