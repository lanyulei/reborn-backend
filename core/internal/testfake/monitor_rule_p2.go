package testfake

import (
	"context"
	"slices"
	"sync"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
)

var _ biz.CIEnvLister = (*MonitorCIEnvs)(nil)

// MonitorCIEnvs 资源索引里出现过的环境取值的内存桩（biz.CIEnvLister）。零值可用。
type MonitorCIEnvs struct {
	mu   sync.Mutex
	envs []string
}

// Set 设定环境取值（空串与重复项被忽略，与真实实现一致）。
func (m *MonitorCIEnvs) Set(envs ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.envs = m.envs[:0]
	for _, e := range envs {
		if e != "" && !slices.Contains(m.envs, e) {
			m.envs = append(m.envs, e)
		}
	}
	slices.Sort(m.envs)
}

// ListEnvs implements biz.CIEnvLister.
func (m *MonitorCIEnvs) ListEnvs(_ context.Context, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.envs[:min(len(m.envs), limit)]), nil
}
