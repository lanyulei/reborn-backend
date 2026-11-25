package vmware

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
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

// 只读请求的重试：网络类错误重试至多 maxRetries 次；vCenter 明确给出的故障不重试；ctx 取消即停。
func TestCollector_Retry(t *testing.T) {
	t.Parallel()
	c := &Collector{backoff: time.Millisecond}
	ctx := context.Background()
	transient := errors.New("Post \"https://vc/sdk\": context deadline exceeded")

	calls := 0
	if err := c.retry(ctx, func() error { calls++; return transient }); !errors.Is(err, transient) || calls != maxRetries {
		t.Fatalf("transient error: calls=%d err=%v, want %d attempts", calls, err, maxRetries)
	}
	calls = 0
	if err := c.retry(ctx, func() error {
		if calls++; calls < 2 {
			return transient
		}
		return nil
	}); err != nil || calls != 2 {
		t.Fatalf("recover on retry: calls=%d err=%v", calls, err)
	}
	for name, fault := range map[string]error{
		"soap fault": soap.WrapSoapFault(&soap.Fault{String: "NoPermission"}),
		"vim fault":  soap.WrapVimFault(&types.NoPermission{}),
	} {
		calls = 0
		if err := c.retry(ctx, func() error { calls++; return fault }); err == nil || calls != 1 {
			t.Fatalf("%s must not be retried: calls=%d err=%v", name, calls, err)
		}
	}
	// ctx 已取消：首次失败后不再重试。
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	calls = 0
	if err := c.retry(cancelled, func() error { calls++; return transient }); err == nil || calls != 1 {
		t.Fatalf("cancelled ctx: calls=%d err=%v", calls, err)
	}
	// 退避期间取消：返回 ctx 的错误。
	slow := &Collector{backoff: time.Minute}
	waiting, cancelWait := context.WithCancel(ctx)
	calls = 0
	err := slow.retry(waiting, func() error {
		if calls++; calls == 1 {
			time.AfterFunc(10*time.Millisecond, cancelWait)
		}
		return transient
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancel during backoff: calls=%d err=%v", calls, err)
	}
}
