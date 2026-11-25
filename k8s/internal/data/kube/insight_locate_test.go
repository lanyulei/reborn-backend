package kube

import (
	"net/netip"
	"slices"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// nodeEntry 一个节点：主机名 + 若干内网地址（顺序即 status.addresses 里的顺序）+ 一个外网地址。
func nodeEntry(t *testing.T, name string, internalIPs ...string) *biz.Entry {
	t.Helper()
	addresses := []any{
		map[string]any{"type": "Hostname", "address": name},
		map[string]any{"type": "ExternalIP", "address": "203.0.113.9"},
	}
	for _, ip := range internalIPs {
		addresses = append(addresses, map[string]any{"type": "InternalIP", "address": ip})
	}
	obj := map[string]any{
		"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": name},
		"status": map[string]any{"addresses": addresses},
	}
	return converterFor(k8scontract.GVRNode)(&unstructured.Unstructured{Object: obj})
}

// 主机定位比对节点的全部 InternalIP（不只第一个），IPv4-mapped 的写法两侧归一后再比。
func TestLocateNodeByAnyInternalIP(t *testing.T) {
	t.Parallel()
	dual := nodeEntry(t, "dual", "10.0.0.1", "fd00::1")
	mapped := nodeEntry(t, "mapped", "::ffff:10.0.0.9")
	single := nodeEntry(t, "single", "192.168.1.5")

	// 缓存条目记下全部内网地址；列表的「内网 IP」列仍是第一个。
	if got := dual.InternalIPs; !slices.Equal(got, []string{"10.0.0.1", "fd00::1"}) {
		t.Fatalf("dual-stack node addresses = %v", got)
	}
	if got := single.Row.Cells["internalIP"]; got != "192.168.1.5" {
		t.Fatalf("the column must keep the first internal address, got %v", got)
	}

	entries := []*biz.Entry{dual, mapped, single}
	addr := func(s string) netip.Addr {
		t.Helper()
		a, err := netip.ParseAddr(s)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	for ip, want := range map[string]*biz.Entry{
		"10.0.0.1":           dual,   // 第一个内网地址
		"fd00::1":            dual,   // 第二个内网地址（原先只比对第一个，定位不到）
		"::ffff:10.0.0.1":    dual,   // 查询是 IPv4-mapped 的写法（原先与 10.0.0.1 不相等）
		"10.0.0.9":           mapped, // 缓存里是 IPv4-mapped 的写法
		"::ffff:10.0.0.9":    mapped,
		"192.168.1.5":        single,
		"::ffff:192.168.1.5": single,
	} {
		if got := nodeByIP(entries, addr(ip)); got != want {
			t.Errorf("nodeByIP(%s) = %v, want %s", ip, nameOf(got), want.Row.Name)
		}
	}
	// 别的地址、外网地址、带 zone 的写法都定位不到（主机名 / CIDR 在 locateNode 入口就被拒，走不到这里）。
	for _, ip := range []string{"10.0.0.2", "fd00::2", "203.0.113.9", "fd00::1%eth0"} {
		if got := nodeByIP(entries, addr(ip)); got != nil {
			t.Errorf("nodeByIP(%s) = %s, want no match", ip, nameOf(got))
		}
	}
	if nodeByIP(nil, addr("10.0.0.1")) != nil {
		t.Error("no nodes, no match")
	}
}

func nameOf(e *biz.Entry) string {
	if e == nil {
		return "<nil>"
	}
	return e.Row.Name
}
