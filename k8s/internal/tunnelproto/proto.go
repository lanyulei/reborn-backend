// Package tunnelproto 数据面与 Agent 两端共用的隧道参数（路径、头、子协议见 kit/k8scontract）。
// 独立成包：Agent 二进制不必为了一份配置引入数据面的其余代码。
package tunnelproto

import (
	"io"
	"time"

	"github.com/hashicorp/yamux"
)

// 隧道参数。心跳 15 秒，写超时 30 秒：对端无响应至多 45 秒判定断开。
const (
	KeepAliveInterval = 15 * time.Second
	WriteTimeout      = 30 * time.Second
	StreamOpenTimeout = 20 * time.Second
	// StreamWindow 单条流的接收窗口：大列表与日志走同一条流，窗口过小会明显拖慢。
	StreamWindow = 1 << 20
)

// SessionConfig 两端共用的 yamux 配置。
func SessionConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = KeepAliveInterval
	cfg.ConnectionWriteTimeout = WriteTimeout
	cfg.StreamOpenTimeout = StreamOpenTimeout
	cfg.MaxStreamWindowSize = StreamWindow
	cfg.LogOutput = io.Discard
	return cfg
}
