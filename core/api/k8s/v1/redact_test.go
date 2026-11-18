package v1

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestRedact(t *testing.T) {
	t.Parallel()
	const leak = "LEAK-ME"
	sec := leak
	cred := &ClusterCredential{Token: leak, ClientCert: leak, ClientKey: leak}
	cases := []interface {
		proto.Message
		Redact() string
	}{
		&CreateClusterRequest{Spec: &ClusterSpec{Name: "keep-name", ProxyUrl: "http://u:" + leak + "@p:1"}, Credential: cred},
		&UpdateClusterRequest{Spec: &ClusterSpec{Name: "keep-name", ProxyUrl: leak}, Credential: cred},
		&ParseKubeconfigRequest{Kubeconfig: "users:\n- user: {token: " + leak + "}"},
		&TestClusterRequest{ApiServer: "https://keep-name", ProxyUrl: leak, Credential: cred},
		&CreateHelmRepoRequest{Secret: &sec},
		&UpdateHelmRepoRequest{Secret: &sec},
		&TestHelmRepoRequest{Secret: &sec},
		&PreviewHelmReleaseRequest{Values: "password: " + leak},
		&InstallHelmReleaseRequest{Values: leak},
		&UpgradeHelmReleaseRequest{Values: leak},
		&PreviewObjectRequest{Yaml: "stringData: {p: " + leak + "}"},
		&ApplyObjectRequest{Yaml: leak},
		&PreviewManifestRequest{Yaml: leak},
		&ApplyManifestRequest{Yaml: leak},
	}
	for _, c := range cases {
		out := c.Redact()
		if strings.Contains(out, leak) {
			t.Errorf("%T leaks: %s", c, out)
		}
		if !strings.Contains(out, "***") {
			t.Errorf("%T has no redaction marker: %s", c, out)
		}
	}
	// 原消息不被改动；非敏感字段保留。
	req := cases[0].(*CreateClusterRequest)
	if req.GetCredential().GetToken() != leak || !strings.Contains(req.Redact(), "keep-name") {
		t.Fatalf("redact must not mutate the request and must keep other fields")
	}
}
