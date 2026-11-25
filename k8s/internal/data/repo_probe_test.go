package data

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 探活结果里的集群版本按列宽截断（k8s_cluster_runtime.k8s_version 是 varchar(32)）：超长的 GitVersion
// 原先原样写入，被 PostgreSQL 拒绝（22001），整条探活结果落不了库，集群恒为「未知 / 不在线」。
func TestProbeArgsClipVersionToColumnWidth(t *testing.T) {
	t.Parallel()
	if k8sVersionColumn != 32 {
		t.Fatalf("column width = %d, want 32", k8sVersionColumn)
	}
	long := "v1.35.1-eks-" + strings.Repeat("a", 40)
	rec := biz.ProbeRecord{
		ClusterID: "c1", Status: k8scontract.ClusterOnline,
		Info: biz.ProbeInfo{Version: long, NodeTotal: 3, NodeReady: 2},
	}
	// 8 个参数，与 SQL 的 $1…$8 一一对应。
	args := probeArgs("i1", rec)
	want := []any{"c1", "i1", k8scontract.ClusterOnline, true, long[:k8sVersionColumn], 3, 2, ""}
	if len(args) != len(want) {
		t.Fatalf("args = %d, want %d: %v", len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("arg %d ($%d) = %#v, want %#v", i, i+1, args[i], want[i])
		}
	}
	// 不超长：原样写入。
	rec.Info.Version = "v1.35.1"
	if args = probeArgs("i1", rec); args[4] != "v1.35.1" {
		t.Fatalf("a short version must be written as is: %#v", args[4])
	}
	// 探活失败：不覆盖版本与节点数（ok = false），带出错误信息。
	rec.Error = "dial tcp: i/o timeout"
	if args = probeArgs("i1", rec); args[3] != false || args[7] != rec.Error {
		t.Fatalf("failed probe: %#v", args)
	}
	// 截断不切断多字节字符：20 个汉字（60 字节）→ 10 个（30 字节）。
	args = probeArgs("i1", biz.ProbeRecord{Info: biz.ProbeInfo{Version: strings.Repeat("版", 20)}})
	if got, _ := args[4].(string); got != strings.Repeat("版", 10) || !utf8.ValidString(got) {
		t.Fatalf("truncation must not split a multi-byte character: %#v", args[4])
	}
}
