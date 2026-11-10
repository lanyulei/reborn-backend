package server

import (
	"testing"

	service "github.com/lanyulei/reborn/backend/core/internal/service/system"
)

// TestAuthExemptWithinAuthzExempt 守护两份豁免清单的一致性：免认证的方法必须
// 同时免授权（authExempt ⊆ authzExemptOperations，前者在 server/auth.go、后者
// 在 service/system/operations.go 独立维护）。漏同步时请求会通过 JWT 校验后
// 被 RBAC 拒绝——本测试在提交前拦截该漂移。
func TestAuthExemptWithinAuthzExempt(t *testing.T) {
	for op := range authExempt {
		if !service.IsAuthzExempt(op) {
			t.Errorf("operation %q 免认证但未免授权：请同步 internal/service/system/operations.go 的 authzExemptOperations", op)
		}
	}
}
