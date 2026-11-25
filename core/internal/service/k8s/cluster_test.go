package service

import (
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
)

// 集群的任何响应都不返回代理账号：编辑 / 详情 / 列表都经 toProtoCluster，代理地址里的账号密码一律换成占位。
func TestToProtoClusterMasksProxyAccount(t *testing.T) {
	t.Parallel()
	const stored = "http://alice:s3cret@proxy.local:3128" //nolint:gosec // 测试数据
	for name, c := range map[string]*biz.Cluster{
		"with account":  {Name: "a", ProxyURL: stored},
		"without":       {Name: "b", ProxyURL: "http://proxy.local:3128"},
		"already blank": {Name: "c"},
	} {
		got := toProtoCluster(c).GetSpec().GetProxyUrl()
		switch name {
		case "with account":
			if got != "http://REDACTED@proxy.local:3128" {
				t.Errorf("%s: proxy = %q, want the masked form", name, got)
			}
		default:
			if got != c.ProxyURL {
				t.Errorf("%s: proxy = %q, want %q unchanged", name, got, c.ProxyURL)
			}
		}
	}
}
