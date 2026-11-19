// Package bootstrap 收纳各服务 main 里相同的启动步骤：配置加载（共享基线 + 服务配置 + 环境变量）。
// 仅 cmd 层 import。
package bootstrap

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"
)

// EnvPrefix 环境变量前缀：KRATOS_FOO_BAR 去掉前缀后作为 ${FOO_BAR} 占位符的取值。
const EnvPrefix = "KRATOS"

// sharedYAML 全部服务共用的配置基线（各服务必须一致的键只在这里写一次）。
//
//go:embed shared.yaml
var sharedYAML []byte

// Load 按「共享基线 → confPath（文件或目录）→ KRATOS_ 环境变量」的顺序合并配置并解码到 out
// （服务的 conf.Bootstrap）。后面的源覆盖前面的同名键，嵌套段逐键合并。配置只在启动时读取一次。
func Load(confPath string, out any) error {
	c := config.New(config.WithSource(sharedSource{}, file.NewSource(confPath), env.NewSource(EnvPrefix)))
	if err := c.Load(); err != nil {
		_ = c.Close()
		return fmt.Errorf("load config: %w", err)
	}
	if err := c.Scan(out); err != nil {
		_ = c.Close()
		return fmt.Errorf("scan config: %w", err)
	}
	// 成功后不 Close：Close 会让每个配置源的 watcher 各打一条带 error 字段的日志（"watcher's ctx
	// cancel"），出现在每次启动的开头容易被误读为故障；watcher 随进程退出即可。
	return nil
}

// sharedSource 把内嵌的共享基线作为一个配置源。
type sharedSource struct{}

// Load implements config.Source.
func (sharedSource) Load() ([]*config.KeyValue, error) {
	return []*config.KeyValue{{Key: "shared.yaml", Format: "yaml", Value: sharedYAML}}, nil
}

// Watch implements config.Source：内嵌内容不会变化，Next 阻塞到 Stop。
func (sharedSource) Watch() (config.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &staticWatcher{ctx: ctx, cancel: cancel}, nil
}

type staticWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *staticWatcher) Next() ([]*config.KeyValue, error) {
	<-w.ctx.Done()
	return nil, w.ctx.Err()
}

func (w *staticWatcher) Stop() error {
	w.cancel()
	return nil
}
