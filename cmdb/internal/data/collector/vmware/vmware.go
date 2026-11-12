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

// Collector implements biz.Collector（provider=vmware）。
type Collector struct {
	timeout time.Duration
}

// New new a Collector；timeout 为单次 SOAP 请求超时。
func New(timeout time.Duration) *Collector { return &Collector{timeout: timeout} }

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
	client, err := vim25.NewClient(ctx, soapClient)
	if err != nil {
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
	if err := cv.Retrieve(ctx, []string{"HostSystem"}, hostProps, &hosts); err != nil {
		return fmt.Errorf("vmware: retrieve hosts: %w", err)
	}
	for i := range hosts {
		if err := sink.Put(ctx, hostResource(&hosts[i])); err != nil {
			return err
		}
	}
	var vms []mo.VirtualMachine
	if err := cv.Retrieve(ctx, []string{"VirtualMachine"}, vmProps, &vms); err != nil {
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
