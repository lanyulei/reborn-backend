package server

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	k8sv1 "github.com/lanyulei/reborn/backend/core/api/k8s/v1"
	"github.com/lanyulei/reborn/backend/kit/httpx"

	"github.com/go-kratos/kratos/v3/middleware/logging"
)

// 访问日志（真实的 logging 中间件 + 服务所用的脱敏 handler）不得出现容器平台请求里的凭据与内容：
// 集群令牌 / 证书私钥、kubeconfig、带账号的代理地址、对象 YAML、Helm values、仓库密码。
func TestK8sRequestLogHasNoSecrets(t *testing.T) {
	t.Parallel()
	const leak = "S3CR3T-LEAK"
	secret := leak
	reqs := []any{
		&k8sv1.CreateClusterRequest{
			Spec:       &k8sv1.ClusterSpec{Name: "visible-name", ProxyUrl: "http://user:" + leak + "@proxy:3128"},
			Credential: &k8sv1.ClusterCredential{Token: leak, ClientCert: leak, ClientKey: "-----BEGIN EC PRIVATE KEY-----\n" + leak},
		},
		&k8sv1.TestClusterRequest{ApiServer: "https://visible-name", Credential: &k8sv1.ClusterCredential{ClientKey: leak}},
		&k8sv1.ParseKubeconfigRequest{Kubeconfig: "users:\n- name: a\n  user:\n    token: " + leak + "\n    client-key-data: " + leak},
		&k8sv1.ApplyObjectRequest{ClusterId: "visible-name", Yaml: "kind: Secret\nstringData:\n  password: " + leak},
		&k8sv1.ApplyManifestRequest{Yaml: "kind: Secret\ndata: {k: " + leak + "}"},
		&k8sv1.InstallHelmReleaseRequest{Values: "db:\n  password: " + leak},
		&k8sv1.UpgradeHelmReleaseRequest{Values: leak},
		&k8sv1.CreateHelmRepoRequest{Secret: &secret},
	}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{ReplaceAttr: httpx.MaskSensitiveAttr}))
	handler := logging.Server(logger)(func(context.Context, any) (any, error) { return nil, nil })
	for _, req := range reqs {
		if _, err := handler(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	logged := buf.String()
	if strings.Count(logged, "server request") != len(reqs) {
		t.Fatalf("expected %d request log lines: %s", len(reqs), logged)
	}
	if strings.Contains(logged, leak) {
		t.Fatalf("secret leaked into the request log: %s", logged)
	}
	if !strings.Contains(logged, "visible-name") {
		t.Fatalf("non-sensitive fields should stay for ops: %s", logged)
	}
}
