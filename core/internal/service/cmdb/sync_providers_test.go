package service

import (
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/cmdb/v1"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/httpx"

	"google.golang.org/protobuf/types/known/structpb"
)

// 采集源的 provider 校验必须覆盖契约里的全部提供方：此前只放行 aliyun / vmware，
// 腾讯云与 AWS 的采集器和配置结构都已就绪，采集源却建不出来。
func TestCreateSourceAcceptsEveryContractProvider(t *testing.T) {
	t.Parallel()
	for _, p := range cmdbcontract.Providers() {
		err := httpx.ProtoValidate(&v1.CreateSourceRequest{Name: "src-" + p, Provider: p, Config: &structpb.Struct{}, Credential: "placeholder-secret"})
		if err != nil {
			t.Errorf("CreateSourceRequest(provider=%q) validation = %v, want nil", p, err)
		}
	}
	if err := httpx.ProtoValidate(&v1.CreateSourceRequest{Name: "s", Provider: "unknown-cloud", Config: &structpb.Struct{}, Credential: "placeholder-secret"}); err == nil {
		t.Error("CreateSourceRequest with an unknown provider must be rejected")
	}
}
