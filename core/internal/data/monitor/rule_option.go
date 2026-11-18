package data

import (
	"context"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"

	"github.com/google/wire"
)

var _ biz.CIEnvLister = (*ciEnvRepo)(nil)

// ruleP2Providers 规则第二、三期的 data providers（由包的 ProviderSet 引用）。
var ruleP2Providers = wire.NewSet(NewCIEnvRepo, wire.Bind(new(biz.CIEnvLister), new(*ciEnvRepo)))

type ciEnvRepo struct {
	data *basedata.Data
}

// NewCIEnvRepo new a ciEnvRepo.
func NewCIEnvRepo(data *basedata.Data) *ciEnvRepo { return &ciEnvRepo{data: data} }

// ListEnvs implements biz.CIEnvLister。monitor_ci_index 是 CMDB 资源在监控侧的物化副本（本域的表）。
func (r *ciEnvRepo) ListEnvs(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.data.Pool.Query(ctx,
		"SELECT DISTINCT env FROM monitor_ci_index WHERE valid AND env <> '' ORDER BY env LIMIT $1", limit)
	if err != nil {
		return nil, fmt.Errorf("list monitor ci envs: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var env string
		if err := rows.Scan(&env); err != nil {
			return nil, fmt.Errorf("scan monitor ci env: %w", err)
		}
		out = append(out, env)
	}
	return out, rows.Err()
}
