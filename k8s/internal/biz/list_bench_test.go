package biz

import (
	"fmt"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// benchEntries 造 n 个接近真实 Pod 列表行的条目（200 个命名空间、1200 个节点）。
func benchEntries(n int) []*Entry {
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	states := []string{k8scontract.StateOK, k8scontract.StateOK, k8scontract.StateOK, k8scontract.StatePending, k8scontract.StateAbnormal}
	out := make([]*Entry, n)
	for i := range out {
		app := fmt.Sprintf("svc-%04d", i%2500)
		state := states[(i*7)%len(states)]
		node := fmt.Sprintf("kwok-node-%04d", (i*31)%1200)
		out[i] = &Entry{
			Row: k8scontract.Row{
				Namespace: fmt.Sprintf("scale-ns-%03d", i%200), Name: fmt.Sprintf("%s-7d9f8c6b5-%05x", app, i*2654435761%1048576),
				UID: fmt.Sprintf("uid-%d", i), CreateTime: base.Add(time.Duration(i*37%86400) * time.Second), State: state,
				Cells: map[string]any{
					"status": k8scontract.StateCell{State: state, Text: "Running"}, "ready": "2/2", "restarts": int64(i % 17),
					"node": node, "ip": fmt.Sprintf("10.%d.%d.%d", i>>16&255, i>>8&255, i&255),
				},
			},
			Labels: map[string]string{"app": app, "tier": []string{"frontend", "backend", "worker", "gateway"}[i%4]},
			Node:   node, Owners: []string{fmt.Sprintf("rs-%d", i%2500)},
		}
	}
	return out
}

// BenchmarkSelectEntries 5 万个 Pod 上一次列表请求的筛选 + 排序 + 分页（不含快照、权限询问与编码）。
func BenchmarkSelectEntries(b *testing.B) {
	entries := benchEntries(50000)
	cases := []struct {
		name string
		q    ListQuery
	}{
		{"default-name-page1", ListQuery{Page: 1, PageSize: 20}},
		{"deep-page", ListQuery{Page: 2000, PageSize: 20}},
		{"keyword-narrow", ListQuery{Keyword: "svc-0042", Page: 1, PageSize: 20}},
		{"keyword-wide", ListQuery{Keyword: "svc-0", Page: 1, PageSize: 20}},
		{"state-abnormal", ListQuery{State: k8scontract.StateAbnormal, Page: 1, PageSize: 20}},
		{"node", ListQuery{Node: "kwok-node-0042", Page: 1, PageSize: 20}},
		{"sort-restarts-desc", ListQuery{SortBy: "restarts", SortDesc: true, Page: 1, PageSize: 20}},
		{"sort-node", ListQuery{SortBy: "node", Page: 1, PageSize: 20}},
		{"sort-status", ListQuery{SortBy: "status", Page: 1, PageSize: 20}},
		{"sort-createTime-desc", ListQuery{SortBy: "createTime", SortDesc: true, Page: 1, PageSize: 20}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				SelectEntries(entries, c.q)
			}
		})
	}
}
