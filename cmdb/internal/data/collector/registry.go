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
		vmware.New(timeout),
	}
}
