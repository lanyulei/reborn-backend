package biz

import (
	"context"
	"net/netip"
	"time"

	bizcmdb "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	bizmonitor "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

// k8sMetrics 把 monitor 域的数据源查询适配为容器平台的指标来源。只读、单向（k8s → monitor）：
// 查询经 monitor 的统一出站入口（并发额度、超时、地址策略、凭据解密都在那里），容器平台不接触数据源凭据。
type k8sMetrics struct {
	datasources *bizmonitor.DatasourceUsecase
}

// NewK8sMetricsSource 构造容器平台的指标来源。
func NewK8sMetricsSource(datasources *bizmonitor.DatasourceUsecase) bizk8s.MetricsSource {
	return k8sMetrics{datasources: datasources}
}

// QueryRange implements bizk8s.MetricsSource：固定模板都是聚合成一条序列的查询，取第一条。
func (m k8sMetrics) QueryRange(ctx context.Context, datasourceID, expr string, start, end time.Time, step time.Duration) ([][2]float64, error) {
	res, err := m.datasources.QueryRange(ctx, datasourceID, expr, start, end, step)
	if err != nil {
		return nil, err
	}
	if len(res.Series) == 0 {
		return nil, nil
	}
	points := make([][2]float64, 0, len(res.Series[0].Points))
	for _, p := range res.Series[0].Points {
		points = append(points, [2]float64{float64(p.Time.Unix()), p.Value})
	}
	return bizk8s.SanitizePoints(points), nil
}

// Enabled implements bizk8s.MetricsSource.
func (m k8sMetrics) Enabled(ctx context.Context, datasourceID string) (bool, error) {
	items, err := m.datasources.ListOptions(ctx, true)
	if err != nil {
		return false, err
	}
	for _, d := range items {
		if d.ID == datasourceID {
			return true, nil
		}
	}
	return false, nil
}

// k8sHosts 把 cmdb 域的全局搜索适配为「按 IP 找主机资源」。只读、单向（k8s → cmdb）；结果受调用者的
// CMDB 数据范围约束，敏感字段照常脱敏。不向 CMDB 写入任何容器平台的数据。
type k8sHosts struct {
	scopes *bizcmdb.ScopeUsecase
	cis    *bizcmdb.CIUsecase
}

// NewK8sHostFinder 构造容器平台的主机查找适配器。
func NewK8sHostFinder(scopes *bizcmdb.ScopeUsecase, cis *bizcmdb.CIUsecase) bizk8s.HostFinder {
	return k8sHosts{scopes: scopes, cis: cis}
}

// FindByIP implements bizk8s.HostFinder：全局搜索按子串匹配，这里只保留某个属性值恰好等于该 IP 的资源
// （按地址比较，不受书写形式影响），至多 5 个。
func (h k8sHosts) FindByIP(ctx context.Context, userID, ip string) ([]bizk8s.HostCI, error) {
	want, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, nil
	}
	scope, err := h.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	filter := bizcmdb.SearchFilter{Query: ip, Limit: bizcmdb.SearchMaxLimit}
	if !scope.Unrestricted {
		if !scope.ModelUnrestricted {
			if len(scope.Models) == 0 {
				return nil, nil
			}
			for code := range scope.Models {
				filter.ModelCodes = append(filter.ModelCodes, code)
			}
		}
		if !scope.EnvUnrestricted {
			for env := range scope.Envs {
				filter.Envs = append(filter.Envs, env)
			}
			if len(filter.Envs) == 0 {
				return nil, nil
			}
		}
	}
	hits, err := h.cis.Search(ctx, filter)
	if err != nil {
		return nil, err
	}
	var out []bizk8s.HostCI
	seen := map[string]struct{}{}
	for _, hit := range hits {
		got, err := netip.ParseAddr(hit.Value)
		if err != nil || got != want || !scope.Allows(hit.CI.ModelCode, hit.CI.Env) {
			continue
		}
		if _, dup := seen[hit.CI.ID]; dup {
			continue
		}
		seen[hit.CI.ID] = struct{}{}
		out = append(out, bizk8s.HostCI{
			ID: hit.CI.ID, Name: hit.CI.Name, ModelCode: hit.CI.ModelCode, ModelName: hit.CI.ModelName, Env: hit.CI.Env,
			Status: hit.CI.Status, Owner: hit.CI.Owner, OwnerName: hit.CI.OwnerName,
		})
		if len(out) == 5 {
			break
		}
	}
	return out, nil
}
