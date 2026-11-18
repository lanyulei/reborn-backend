package runner

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	// chunkTarget 日志块的目标大小：攒够即封块（远小于协议上限 MaxLogChunkBytes）。
	chunkTarget = 32 << 10
	// bufferLimit 未确认日志的内存上限（每个作业）：到达后 Append 阻塞，背压经管道传给子进程。
	bufferLimit = 4 << 20
	// reportMaxBytes 单次上报携带的日志字节数上限（控制请求体大小，远低于引擎的 8MiB 上限）。
	reportMaxBytes = 1 << 20
)

// errDiscarded 缓冲已被丢弃（租约失效，作业作废）。
var errDiscarded = errors.New("log buffer discarded")

// logBuffer 作业日志的有界缓冲：输出先攒进 cur，攒够或定时封成带序号的块；封好的块保留到
// 引擎确认为止，供失败后从确认序号之后重传。缓冲满时 Append 阻塞（背压），绝不丢弃日志。
type logBuffer struct {
	target int
	limit  int

	// mu 保护以下全部字段。不变式：sealed 按 Seq 连续递增，首块 Seq = acked + 1（非空时），
	// 末块 Seq = nextSeq − 1；size = len(cur) + Σ len(sealed[i].Content)。
	mu        sync.Mutex
	cur       []byte
	sealed    []jobcontract.LogChunk
	size      int
	nextSeq   int64
	acked     int64
	discarded bool

	// space 有空间腾出（或缓冲被丢弃）时得到一个信号；ready 有新块封好时得到一个信号。容量均为 1。
	space chan struct{}
	ready chan struct{}
}

func newLogBuffer(target, limit int) *logBuffer {
	return &logBuffer{
		target: min(max(target, utf8.UTFMax), jobcontract.MaxLogChunkBytes), limit: limit, nextSeq: 1,
		space: make(chan struct{}, 1), ready: make(chan struct{}, 1),
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Ready 有新块可上报时可读。
func (b *logBuffer) Ready() <-chan struct{} { return b.ready }

// Append 追加一段已掩码的文本（须为合法 UTF-8）。缓冲已满时阻塞到有空间为止；
// ctx 取消返回其错误，缓冲被丢弃返回 errDiscarded。单次追加允许越过上限（上限是软上限，
// 超出量以一次追加为界）。
func (b *logBuffer) Append(ctx context.Context, text string) error {
	if text == "" {
		return nil
	}
	for {
		b.mu.Lock()
		if b.discarded {
			b.mu.Unlock()
			return errDiscarded
		}
		if b.size < b.limit {
			b.cur = append(b.cur, text...)
			b.size += len(text)
			sealed := b.sealLocked(false)
			if b.size < b.limit {
				signal(b.space) // 仍有空间：把信号传给其他等待者
			}
			b.mu.Unlock()
			if sealed {
				signal(b.ready)
			}
			return nil
		}
		b.mu.Unlock()
		select {
		case <-b.space:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Flush 把尚未封块的内容封成块（定时调用与收尾时调用）。
func (b *logBuffer) Flush() {
	b.mu.Lock()
	sealed := b.sealLocked(true)
	b.mu.Unlock()
	if sealed {
		signal(b.ready)
	}
}

// sealLocked 封块：cur 达到目标大小时切出一块；force 时把剩余内容也封掉。调用方持锁。
func (b *logBuffer) sealLocked(force bool) bool {
	sealed := false
	for len(b.cur) >= b.target || (force && len(b.cur) > 0) {
		n := cutPoint(b.cur, b.target)
		b.sealed = append(b.sealed, jobcontract.LogChunk{Seq: b.nextSeq, Content: string(b.cur[:n])})
		b.nextSeq++
		b.cur = b.cur[n:]
		sealed = true
	}
	if len(b.cur) == 0 {
		b.cur = nil // 释放底层数组（切片前移后旧内容仍被引用）
	}
	return sealed
}

// cutPoint 在 buf 的前 target 字节里选切点：优先最后一个换行之后；没有换行则退到字符边界，
// 不把一个 UTF-8 字符切成两半。len(buf) ≤ target 时整段切出。
func cutPoint(buf []byte, target int) int {
	if len(buf) <= target {
		return len(buf)
	}
	if i := bytes.LastIndexByte(buf[:target], '\n'); i >= 0 {
		return i + 1
	}
	n := target
	for n > 0 && !utf8.RuneStart(buf[n]) {
		n--
	}
	if n == 0 {
		return target // 不是合法 UTF-8（不应出现）：按字节切，保证前进
	}
	return n
}

// Pending 返回待上报的块（从最早未确认的块起，至多 maxChunks 块、约 maxBytes 字节，至少一块）。
func (b *logBuffer) Pending(maxChunks, maxBytes int) []jobcontract.LogChunk {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	n := 0
	for n < len(b.sealed) && n < maxChunks {
		total += len(b.sealed[n].Content)
		if n > 0 && total > maxBytes {
			break
		}
		n++
	}
	return append([]jobcontract.LogChunk(nil), b.sealed[:n]...)
}

// Ack 引擎已确认到 seq：丢弃不大于它的块并唤醒被背压阻塞的写入方。seq 不前进时是空操作。
func (b *logBuffer) Ack(seq int64) {
	b.mu.Lock()
	if seq > b.nextSeq-1 {
		seq = b.nextSeq - 1 // 引擎确认了尚未产生的序号（不应出现）：按已产生的算
	}
	if seq <= b.acked {
		b.mu.Unlock()
		return
	}
	drop := int(seq - b.acked)
	for _, c := range b.sealed[:drop] {
		b.size -= len(c.Content)
	}
	b.sealed = append([]jobcontract.LogChunk(nil), b.sealed[drop:]...)
	b.acked = seq
	b.mu.Unlock()
	signal(b.space)
}

// Acked 已确认的最大序号。
func (b *logBuffer) Acked() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.acked
}

// Drained 报告是否全部日志都已封块并被确认。
func (b *logBuffer) Drained() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.cur) == 0 && len(b.sealed) == 0
}

// Discard 丢弃全部内容并让此后的 Append 立即失败（租约失效：这些日志已无处可交）。
func (b *logBuffer) Discard() {
	b.mu.Lock()
	b.discarded = true
	b.cur, b.sealed, b.size = nil, nil, 0
	b.mu.Unlock()
	signal(b.space)
}
