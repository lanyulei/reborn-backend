package service

// ResolveClientIP 导出 resolveClientIP：不经中间件链的原生路由（没有传输上下文）按同一口径取客户端 IP。
func ResolveClientIP(remoteAddr string, forwardedFor []string) string {
	return resolveClientIP(remoteAddr, forwardedFor)
}
