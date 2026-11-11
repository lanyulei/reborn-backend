// Package migrations 把 SQL 迁移脚本随二进制打包：go:embed 不能引用上级目录，
// 故本文件与脚本同目录。消费方为 cmd/migrate（golang-migrate iofs 源）。
package migrations

import "embed"

// FS 全部迁移脚本（NNNN_name.up.sql / NNNN_name.down.sql）。
//
//go:embed *.up.sql *.down.sql
var FS embed.FS
