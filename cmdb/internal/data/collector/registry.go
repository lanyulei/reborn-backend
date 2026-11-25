// Package collector 采集器注册表：每个提供方一个子包实现 biz.Collector，在此装配。
// 新增厂商 = 新增一个子包 + 在 NewRegistry 追加一行。
package collector

import (
	"time"

	"github.com/lanyulei/reborn/backend/cmdb/internal/biz"
	"github.com/lanyulei/reborn/backend/cmdb/internal/conf"
	"github.com/lanyulei/reborn/backend/cmdb/internal/data/collector/aliyun"
	"github.com/lanyulei/reborn/backend/cmdb/internal/data/collector/aws"
	"github.com/lanyulei/reborn/backend/cmdb/internal/data/collector/tencent"
	"github.com/lanyulei/reborn/backend/cmdb/internal/data/collector/vmware"
)

// defaultRequestTimeout 单次出站请求超时缺省值（AGENTS §7：≤3s）。
const defaultRequestTimeout = 3 * time.Second

// vmwareRequestTimeout vCenter 单次 SOAP 请求超时的下限。三朵云是逐页（≤100 条）的 REST 调用，
// 3s 足够；vCenter 采集器用 ContainerView 一次请求取回全部主机 / 虚拟机的属性，数千台规模下
// 普遍超过 3s——沿用 request_timeout 的话每次同步都稳定超时，而不是偶发。整次采集仍受
// collect_timeout（任务 ctx）约束，这里只是不让单次请求先于它超时。request_timeout 配得更大时以配置为准。
const vmwareRequestTimeout = 2 * time.Minute

// NewRegistry 按配置构造全部采集器。
func NewRegistry(c *conf.Sync) []biz.Collector {
	timeout := c.GetWorker().GetRequestTimeout().AsDuration()
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return []biz.Collector{
		aliyun.New(timeout),
		tencent.New(timeout),
		aws.New(timeout),
		vmware.New(max(timeout, vmwareRequestTimeout)),
	}
}
