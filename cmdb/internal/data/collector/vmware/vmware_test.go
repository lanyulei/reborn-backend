package vmware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/vmware/govmomi/simulator"
)

type memSink struct{ items []*biz.Resource }

func (s *memSink) Put(_ context.Context, r *biz.Resource) error {
	s.items = append(s.items, r)
	return nil
}

// vcsim：默认拓扑（数据中心 / 集群 / 主机 / 虚拟机），校验主机与虚拟机映射及关系声明。
func TestCollector_Discover(t *testing.T) {
	model := simulator.VPX()
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(model.Remove)
	srv := model.Service.NewServer()
	t.Cleanup(srv.Close)
	user := srv.URL.User
	srv.URL.User = nil
	cfg := fmt.Sprintf(`{"url":%q,"username":%q,"insecure":true}`, srv.URL.String(), user.Username())
	password, _ := user.Password()

	c := New(5 * time.Second)
	sink := &memSink{}
	if err := c.Discover(context.Background(), &biz.Source{Provider: cmdbcontract.ProviderVMware, Config: []byte(cfg)}, password, sink); err != nil {
		t.Fatalf("Discover() error = %v", err)
	}
	var hosts, vms, linked int
	hostIDs := map[string]bool{}
	for _, r := range sink.items {
		switch r.ModelCode {
		case cmdbcontract.ModelPhysicalServer:
			hosts++
			hostIDs[r.ExternalID] = true
			if r.Name == "" || r.Attrs["cpu_cores"] == nil {
				t.Fatalf("host missing basics: %+v", r)
			}
		case cmdbcontract.ModelVirtualMachine:
			vms++
			if r.Name == "" || r.ExternalID == "" || r.Attrs["vcpu"] == nil {
				t.Fatalf("vm missing basics: %+v", r)
			}
			if len(r.Relations) == 1 {
				linked++
				rel := r.Relations[0]
				if rel.Type != cmdbcontract.RelationHostRunsVM || rel.Direction != cmdbcontract.DirectionIncoming || rel.PeerModel != cmdbcontract.ModelPhysicalServer {
					t.Fatalf("bad relation: %+v", rel)
				}
			}
		default:
			t.Fatalf("unexpected model %s", r.ModelCode)
		}
	}
	if hosts == 0 || vms == 0 || linked != vms {
		t.Fatalf("hosts=%d vms=%d linked=%d", hosts, vms, linked)
	}
	for _, r := range sink.items {
		for _, rel := range r.Relations {
			if !hostIDs[rel.PeerExternalID] {
				t.Fatalf("vm relation points to unknown host %s", rel.PeerExternalID)
			}
		}
	}
	// 主机顺序在虚拟机之前（对账先建主机再建关系）。
	if sink.items[0].ModelCode != cmdbcontract.ModelPhysicalServer {
		t.Fatal("hosts must be emitted before vms")
	}
	// 不可达地址：连接失败而非挂起（请求级超时）。
	bad := `{"url":"https://127.0.0.1:1/sdk","username":"u","insecure":true}`
	if err := c.Discover(context.Background(), &biz.Source{Provider: cmdbcontract.ProviderVMware, Config: []byte(bad)}, "p", &memSink{}); err == nil {
		t.Fatal("unreachable vcenter must fail")
	}
}
