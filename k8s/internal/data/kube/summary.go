package kube

import (
	"context"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	_ biz.ClusterSummarizer = (*Manager)(nil)
	_ biz.EventSource       = (*Manager)(nil)
)

// milliCPU 解析 CPU 量为毫核；无法解析返回 0。
func milliCPU(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.MilliValue()
}

// quantityBytes 解析内存量为字节；无法解析返回 0。
func quantityBytes(s string) int64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return q.Value()
}

const (
	summaryPageSize = 500
	// summaryMaxPages 冷路径单类资源至多读取的页数（500 × 300 = 15 万个对象），超出标记为不完整。
	summaryMaxPages = 300
)

// eachEntry 遍历某类资源的全部对象（平台身份）。缓存已建立时读缓存（warm = true，纯内存）；
// 否则向集群分页读取，逐页转换后丢弃原文——内存占用与一页的大小成正比，不随集群规模增长。
func (c *conn) eachEntry(ctx context.Context, gvr k8scontract.GVR, fn func(e *biz.Entry)) (warm, complete bool, err error) {
	if store := c.cache.peek(gvr); store != nil {
		for _, e := range store.snapshot(nil) {
			fn(e)
		}
		return true, true, nil
	}
	convert := converterFor(gvr)
	rc := c.platDyn.Resource(toSchema(gvr))
	opts := metav1.ListOptions{Limit: summaryPageSize}
	for page := 0; page < summaryMaxPages; page++ {
		list, err := rc.List(ctx, opts)
		if err != nil {
			return false, false, mapError(err)
		}
		for i := range list.Items {
			fn(convert(&list.Items[i]))
		}
		if opts.Continue = list.GetContinue(); opts.Continue == "" {
			return false, true, nil
		}
	}
	return false, false, nil
}

// Summary implements biz.ClusterSummarizer.
//
// 开销：节点与三类工作负载各一次遍历，Pod 一次遍历。Pod 缓存已建立（有人正在看这个集群）时全部是内存
// 计算；否则是 ⌈对象数 / 500⌉ 次列表请求（1200 节点 / 5 万 Pod 的集群约 100 余次，受该集群的客户端
// 限速约束），由调用方以 5 分钟的间隔执行。不会为了摘要去建立任何缓存。
func (m *Manager) Summary(ctx context.Context, clusterID string) (k8scontract.ClusterSummary, bool, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return k8scontract.ClusterSummary{}, false, err
	}
	b := biz.NewSummaryBuilder()
	if _, ok, err := c.eachEntry(ctx, k8scontract.GVRNode, b.AddNode); err != nil {
		return k8scontract.ClusterSummary{}, false, err
	} else if !ok {
		b.MarkPartial()
	}
	for kind, gvr := range map[string]k8scontract.GVR{
		"Deployment": k8scontract.GVRDeployment, "StatefulSet": k8scontract.GVRStatefulSet, "DaemonSet": k8scontract.GVRDaemonSet,
	} {
		if _, ok, err := c.eachEntry(ctx, gvr, func(e *biz.Entry) { b.AddWorkload(kind, e) }); err != nil {
			return k8scontract.ClusterSummary{}, false, err
		} else if !ok {
			b.MarkPartial()
		}
	}
	warm, ok, err := c.eachEntry(ctx, k8scontract.GVRPod, b.AddPod)
	if err != nil {
		return k8scontract.ClusterSummary{}, false, err
	}
	if !ok {
		b.MarkPartial()
	}
	return b.Result(), warm, nil
}

// Warnings implements biz.EventSource：当前集群里的 Warning 事件（平台身份，分页读取）。
func (m *Manager) Warnings(ctx context.Context, clusterID string, limit int) ([]biz.WarningEvent, bool, error) {
	c, err := m.conn(clusterID)
	if err != nil {
		return nil, false, err
	}
	var out []biz.WarningEvent
	opts := metav1.ListOptions{FieldSelector: "type=Warning", Limit: 500}
	for {
		list, err := c.platKube.CoreV1().Events(metav1.NamespaceAll).List(ctx, opts)
		if err != nil {
			return nil, false, mapError(err)
		}
		for i := range list.Items {
			ev := &list.Items[i]
			w := biz.WarningEvent{
				UID: string(ev.UID), Namespace: ev.InvolvedObject.Namespace, Kind: ev.InvolvedObject.Kind, Name: ev.InvolvedObject.Name,
				Reason: ev.Reason, Message: ev.Message, Source: ev.Source.Component, Count: int64(ev.Count),
				FirstTime: ev.FirstTimestamp.Time, LastTime: ev.LastTimestamp.Time,
			}
			if ev.Series != nil && int64(ev.Series.Count) > w.Count {
				w.Count = int64(ev.Series.Count)
				if w.LastTime.Before(ev.Series.LastObservedTime.Time) {
					w.LastTime = ev.Series.LastObservedTime.Time
				}
			}
			if w.LastTime.IsZero() {
				w.LastTime = ev.EventTime.Time
			}
			if w.LastTime.IsZero() {
				w.LastTime = ev.CreationTimestamp.Time
			}
			if w.FirstTime.IsZero() {
				w.FirstTime = w.LastTime
			}
			if w.Source == "" {
				w.Source = ev.ReportingController
			}
			out = append(out, w)
			if len(out) >= limit {
				return out, true, nil
			}
		}
		if opts.Continue = list.Continue; opts.Continue == "" {
			return out, false, nil
		}
	}
}
