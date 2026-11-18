package kube

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Stream implements biz.Streamer.
func (m *Manager) Stream(ctx context.Context, cl *biz.Cluster, id biz.Identity, op string, body []byte, emit biz.Emit) ([]*biz.AuditEvent, error) {
	c, err := m.conn(cl.ID)
	if err != nil {
		return nil, err
	}
	// 流随连接结束：归属转移或集群移除时不留悬挂的流。
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	u, err := c.asUser(sctx, id)
	if err != nil {
		return nil, err
	}
	switch op {
	case k8scontract.StreamWatch:
		req, err := decode[k8scontract.WatchRequest](body)
		if err != nil {
			return nil, err
		}
		return nil, mapError(u.watch(req, emit))
	case k8scontract.StreamPodLog:
		req, err := decode[k8scontract.PodLogRequest](body)
		if err != nil {
			return nil, err
		}
		return nil, mapError(u.podLog(req, emit))
	case k8scontract.StreamWorkloadLog:
		req, err := decode[k8scontract.WorkloadLogRequest](body)
		if err != nil {
			return nil, err
		}
		return nil, mapError(u.workloadLog(req, emit))
	case k8scontract.StreamDrain:
		req, err := decode[k8scontract.DrainRequest](body)
		if err != nil {
			return nil, err
		}
		audits, err := u.drain(req, emit)
		return audits, mapError(err)
	}
	return nil, biz.ErrInvalid("unknown stream %q", op)
}

func (u *userClients) podLog(req k8scontract.PodLogRequest, emit biz.Emit) error {
	if req.Namespace == "" || req.Pod == "" {
		return biz.ErrInvalid("namespace and pod are required")
	}
	rc, err := u.openLog(req)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	// 日志流已打开：先发一个心跳把响应头写出去（容器暂时没有输出时，客户端也能确认流已建立）。
	if emit(k8scontract.EventPing, "", k8scontract.Empty{}) != nil {
		return nil
	}
	err = pumpLog(u.ctx, rc, biz.NewLogTracker(req.Cursor), req.Keyword, "", "", emit)
	return endLog(u.ctx, err, emit)
}

// endLog 日志流收尾：读到末尾（容器结束或非跟随模式）下发 completed；读取出错下发 error；
// ctx 结束（客户端断开或到达时长上限）不下发，由上层处理。
func endLog(ctx context.Context, err error, emit biz.Emit) error {
	if ctx.Err() != nil || errors.Is(err, errEmit) {
		return nil
	}
	end := k8scontract.StreamEnd{Reason: k8scontract.EndCompleted}
	if err != nil {
		end = k8scontract.StreamEnd{Reason: k8scontract.EndError, Message: biz.AuditErrorText(mapError(err))}
	}
	_ = emit(k8scontract.EventEnd, "", end)
	return nil
}

// openLog 以用户身份打开日志流。恒带时间戳（游标需要）。续接时以游标时间为起点、不限行数：
// 起点之前与游标之前的行由 LogTracker 丢弃。
func (u *userClients) openLog(req k8scontract.PodLogRequest) (io.ReadCloser, error) {
	opts := &corev1.PodLogOptions{Container: req.Container, Follow: req.Follow, Previous: req.Previous, Timestamps: true}
	if since, ok := biz.NewLogTracker(req.Cursor).Since(); ok {
		t := metav1.NewTime(since)
		opts.SinceTime = &t
	} else {
		tail := req.TailLines
		if tail <= 0 {
			tail = k8scontract.DefaultTailLines
		}
		if tail > k8scontract.MaxTailLines {
			tail = k8scontract.MaxTailLines
		}
		opts.TailLines = &tail
		if req.SinceSeconds > 0 {
			opts.SinceSeconds = &req.SinceSeconds
		}
	}
	return u.kube.CoreV1().Pods(req.Namespace).GetLogs(req.Pod, opts).Stream(u.ctx)
}

// errEmit 对端已断开。
var errEmit = errors.New("stream receiver gone")

// readLogLine 读一行（不含换行）；超过 MaxLogLineBytes + 时间戳余量的部分丢弃并标记截断。
func readLogLine(r *bufio.Reader, limit int) (line []byte, truncated bool, err error) {
	for {
		chunk, rerr := r.ReadSlice('\n')
		room := limit - len(line)
		if len(chunk) <= room {
			line = append(line, chunk...)
		} else {
			line = append(line, chunk[:room]...)
			if dropped := chunk[room:]; len(dropped) != 1 || dropped[0] != '\n' {
				truncated = true
			}
		}
		if errors.Is(rerr, bufio.ErrBufferFull) {
			continue
		}
		if n := len(line); n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		return line, truncated, rerr
	}
}

// timestampRoom 行首时间戳（RFC3339Nano + 空格）占用的字节上限。
const timestampRoom = 40

// pumpLog 逐行读取并下发。返回 nil = 读到末尾。
func pumpLog(ctx context.Context, rc io.Reader, tracker *biz.LogTracker, keyword, pod, container string, emit biz.Emit) error {
	r := bufio.NewReaderSize(rc, 32<<10)
	var last time.Time
	for {
		raw, truncated, err := readLogLine(r, k8scontract.MaxLogLineBytes+timestampRoom)
		if len(raw) > 0 || err == nil {
			ts, text, ok := biz.ParseLogLine(raw)
			if ok {
				last = ts
			} else {
				ts = last // 没有时间戳的行（集群的错误提示等）：沿用上一行的时间
			}
			if len(text) > k8scontract.MaxLogLineBytes {
				text, truncated = text[:k8scontract.MaxLogLineBytes], true
			}
			cursor, send := tracker.Next(ts)
			if send && (keyword == "" || strings.Contains(string(text), keyword)) {
				line := k8scontract.LogLine{Pod: pod, Container: container, Time: ts, Text: strings.ToValidUTF8(string(text), "�"), Truncated: truncated}
				if emit(k8scontract.EventLog, cursor, line) != nil {
					return errEmit
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

var logWorkloads = []k8scontract.GVR{
	k8scontract.GVRDeployment, k8scontract.GVRStatefulSet, k8scontract.GVRDaemonSet, k8scontract.GVRReplicaSet, k8scontract.GVRJob,
}

// workloadLog 工作负载级日志：同时跟随其下的多个 Pod（最新的 MaxWorkloadLogPods 个）。流建立后新出现的
// Pod 不会自动加入；客户端在流结束或手动刷新时重新订阅即可。
func (u *userClients) workloadLog(req k8scontract.WorkloadLogRequest, emit biz.Emit) error {
	if err := checkRef(req.Ref, true); err != nil {
		return err
	}
	if err := supports(req.Ref, logWorkloads); err != nil {
		return err
	}
	rc, _, err := u.resource(req.Ref)
	if err != nil {
		return err
	}
	w, err := rc.Get(u.ctx, req.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	raw := nestedMap(w.Object, "spec", "selector")
	if raw == nil {
		return biz.ErrUnsupported("%s has no pod selector", req.Ref.Name)
	}
	var ls metav1.LabelSelector
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &ls); err != nil {
		return biz.ErrInvalid("invalid pod selector")
	}
	sel, err := metav1.LabelSelectorAsSelector(&ls)
	if err != nil {
		return biz.ErrInvalid("invalid pod selector")
	}
	list, err := u.kube.CoreV1().Pods(req.Ref.Namespace).List(u.ctx, metav1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return err
	}
	pods := list.Items
	sort.Slice(pods, func(i, j int) bool { return pods[i].CreationTimestamp.After(pods[j].CreationTimestamp.Time) })
	omitted := 0
	if len(pods) > k8scontract.MaxWorkloadLogPods {
		omitted = len(pods) - k8scontract.MaxWorkloadLogPods
		pods = pods[:k8scontract.MaxWorkloadLogPods]
	}
	names := make([]string, 0, len(pods))
	for i := range pods {
		names = append(names, pods[i].Name)
	}
	var mu sync.Mutex // 多个 Pod 并发下发：串行化 emit
	safe := func(event, id string, data any) error {
		mu.Lock()
		defer mu.Unlock()
		return emit(event, id, data)
	}
	if safe(k8scontract.EventPods, "", k8scontract.LogPods{Pods: names, Omitted: omitted}) != nil {
		return nil
	}
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func(pod string) { // 退出路径：日志流读到末尾，或 u.ctx 取消后读取返回
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					u.c.m.log.Error("workload log follower panicked", "pod", pod, "panic", r)
				}
			}()
			preq := k8scontract.PodLogRequest{
				Namespace: req.Ref.Namespace, Pod: pod, Container: req.Container, Follow: true,
				TailLines: req.TailLines, SinceSeconds: req.SinceSeconds, Cursor: req.Cursors[pod],
			}
			stream, err := u.openLog(preq)
			if err != nil {
				// 单个 Pod 打不开（容器尚未启动等）不影响其余 Pod：作为一行提示下发。
				_ = safe(k8scontract.EventLog, "", k8scontract.LogLine{Pod: pod, Time: time.Now(), Text: "[log unavailable] " + biz.AuditErrorText(mapError(err))})
				return
			}
			defer func() { _ = stream.Close() }()
			_ = pumpLog(u.ctx, stream, biz.NewLogTracker(preq.Cursor), req.Keyword, pod, req.Container, safe)
		}(name)
	}
	wg.Wait()
	return endLog(u.ctx, nil, safe)
}
