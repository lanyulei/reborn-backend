package biz

import (
	"crypto/md5" //nolint:gosec // 非密码学用途：仅派生接口稳定标识（code），不承担机密性/完整性
	"encoding/hex"
)

// OperationCode 由操作的 proto full method 派生稳定标识：md5 的 32 位小写
// hex。纯函数、无状态，name 不变则 code 永不变；授权存储与 authz 判定统一
// 使用此值，规避 full method 字符串（含 "/" 与 "."）在 URL/日志/异构系统间
// 的兼容问题。
// 定义在 biz 层：与判定链同源；service 层操作枚举与 server 层中间件经
// 本函数换算（service→biz 单向依赖方向不变）。
func OperationCode(name string) string {
	// 弱哈希无害：仅作稳定标识派生（用户指定），不承担机密性/完整性。
	sum := md5.Sum([]byte(name)) //nolint:gosec // 非密码学用途，仅派生接口标识
	return hex.EncodeToString(sum[:])
}
