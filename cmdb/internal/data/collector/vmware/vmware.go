// Package vmware vCenter 采集器：govmomi（vSphere SOAP API）ContainerView 一次性拉取
// HostSystem 与 VirtualMachine 的所需属性，输出固定映射到内置类型 physical_server /
// virtual_machine，并声明 host_runs_vm 关系（虚拟机 → 所在主机）。
package vmware

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/soap"
)

// maxRetries 单个只读请求的尝试次数上限（与其它采集器一致：首发 + 2 次重试）。
const maxRetries = 3

// Collector implements biz.Collector（provider=vmware）。
type Collector struct {
	timeout time.Duration
	// backoff 重试退避的基数（第 n 次重试前等 n × backoff）。
	backoff time.Duration
}

// New new a Collector；timeout 为单次 SOAP 请求超时。ContainerView 一次请求拉取全部主机 / 虚拟机的
// 属性，耗时随 vCenter 规模增长，调用方应给出比普通 REST 调用宽得多的值（见 collector.NewRegistry）；
// 整次采集仍受调用方 ctx（collect_timeout）约束。
func New(timeout time.Duration) *Collector {
	return &Collector{timeout: timeout, backoff: 500 * time.Millisecond}
}

// Provider implements biz.Collector.
func (c *Collector) Provider() string { return cmdbcontract.ProviderVMware }

var (
	hostProps = []string{"name", "summary.hardware", "hardware.systemInfo", "config.network.vnic"}
	vmProps   = []string{
		"name", "config.instanceUuid", "config.guestFullName", "config.hardware.numCPU", "config.hardware.memoryMB",
		"guest.ipAddress", "runtime.host", "runtime.powerState",
	}
)

// Discover implements biz.Collector：先主机后虚拟机（对账顺序保证关系两端都已存在）。
func (c *Collector) Discover(ctx context.Context, src *biz.Source, credential string, sink biz.Sink) error {
	cfg, err := cmdbcontract.DecodeConfig(cmdbcontract.ProviderVMware, src.Config)
	if err != nil {
		return err
	}
	vc, ok := cfg.(*cmdbcontract.VMwareConfig)
	if !ok {
		return errors.New("vmware: unexpected config type")
	}
	u, err := soap.ParseURL(vc.URL)
	if err != nil {
		return fmt.Errorf("vmware: parse url: %w", err)
	}
	soapClient := soap.NewClient(u, vc.Insecure)
	soapClient.Timeout = c.timeout
	var client *vim25.Client
	if err := c.retry(ctx, func() (err error) {
		client, err = vim25.NewClient(ctx, soapClient)
		return err
	}); err != nil {
		return fmt.Errorf("vmware: connect: %w", err)
	}
	sm := session.NewManager(client)
	if err := sm.Login(ctx, url.UserPassword(vc.Username, credential)); err != nil {
		return fmt.Errorf("vmware: login: %w", err)
	}
	defer func() { _ = sm.Logout(context.WithoutCancel(ctx)) }()

	vm := view.NewManager(client)
	cv, err := vm.CreateContainerView(ctx, client.ServiceContent.RootFolder, []string{"HostSystem", "VirtualMachine"}, true)
	if err != nil {
		return fmt.Errorf("vmware: create view: %w", err)
	}
	defer func() { _ = cv.Destroy(context.WithoutCancel(ctx)) }()

	var hosts []mo.HostSystem
	if err := c.retry(ctx, func() error {
		hosts = nil // 上一次尝试可能已填了一部分
		return cv.Retrieve(ctx, []string{"HostSystem"}, hostProps, &hosts)
	}); err != nil {
		return fmt.Errorf("vmware: retrieve hosts: %w", err)
	}
	for i := range hosts {
		if err := sink.Put(ctx, hostResource(&hosts[i])); err != nil {
			return err
		}
	}
	var vms []mo.VirtualMachine
	if err := c.retry(ctx, func() error {
		vms = nil
		return cv.Retrieve(ctx, []string{"VirtualMachine"}, vmProps, &vms)
	}); err != nil {
		return fmt.Errorf("vmware: retrieve vms: %w", err)
	}
	for i := range vms {
		if err := sink.Put(ctx, vmResource(&vms[i])); err != nil {
			return err
		}
	}
	return nil
}

func hostResource(h *mo.HostSystem) *biz.Resource {
	attrs := map[string]any{"vendor": "", "model": "", "serial_number": ""}
	if hw := h.Summary.Hardware; hw != nil {
		attrs["vendor"], attrs["model"] = hw.Vendor, hw.Model
		attrs["cpu_cores"] = int(hw.NumCpuCores)
		attrs["memory_gb"] = int(hw.MemorySize / (1 << 30))
		attrs["uuid"] = hw.Uuid
	}
	if h.Hardware != nil {
		if sn := h.Hardware.SystemInfo.SerialNumber; sn != "" {
			attrs["serial_number"] = sn
		}
		if v := h.Hardware.SystemInfo.Vendor; v != "" {
			attrs["vendor"] = v
		}
	}
	if h.Config != nil && h.Config.Network != nil {
		for _, nic := range h.Config.Network.Vnic {
			if nic.Spec.Ip != nil && nic.Spec.Ip.IpAddress != "" {
				attrs["ip"] = nic.Spec.Ip.IpAddress
				break
			}
		}
	}
	for k, v := range attrs {
		if s, ok := v.(string); ok && s == "" {
			delete(attrs, k)
		}
	}
	return &biz.Resource{ModelCode: cmdbcontract.ModelPhysicalServer, ExternalID: h.Self.Value, Name: h.Name, Attrs: attrs}
}

// retry 只读请求（建连取 ServiceContent、Retrieve）的重试：网络错误 / 超时 / 5xx 重试，至多
// maxRetries 次、线性退避，口径同其它采集器。vCenter 明确给出的 SOAP / Vim 故障（鉴权、权限、
// 参数）重试也不会变，直接返回。登录不走这里：它会创建会话，不是幂等的只读请求。
func (c *Collector) retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * c.backoff):
			}
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if soap.IsSoapFault(err) || soap.IsVimFault(err) || ctx.Err() != nil {
			break
		}
	}
	return lastErr
}

func vmResource(v *mo.VirtualMachine) *biz.Resource {
	attrs := map[string]any{"power_state": strings.TrimPrefix(string(v.Runtime.PowerState), "powered")}
	externalID := v.Self.Value
	if v.Config != nil {
		if v.Config.InstanceUuid != "" {
			externalID = v.Config.InstanceUuid
		}
		attrs["os"] = v.Config.GuestFullName
		attrs["vcpu"] = int(v.Config.Hardware.NumCPU)
		attrs["memory_gb"] = int(v.Config.Hardware.MemoryMB / 1024)
	}
	if v.Guest != nil && v.Guest.IpAddress != "" {
		attrs["ip"] = v.Guest.IpAddress
	}
	res := &biz.Resource{ModelCode: cmdbcontract.ModelVirtualMachine, ExternalID: externalID, Name: v.Name, Attrs: attrs}
	if v.Runtime.Host != nil {
		res.Relations = []cmdbcontract.StagingRelation{{
			Type: cmdbcontract.RelationHostRunsVM, Direction: cmdbcontract.DirectionIncoming,
			PeerModel: cmdbcontract.ModelPhysicalServer, PeerExternalID: v.Runtime.Host.Value,
		}}
	}
	return res
}
