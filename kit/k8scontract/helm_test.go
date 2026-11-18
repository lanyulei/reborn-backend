package k8scontract

import "testing"

func TestNormalizeHelmRepoURL(t *testing.T) {
	t.Parallel()
	ok := []struct {
		typ  int32
		in   string
		cred bool
		want string
	}{
		{HelmRepoHTTP, "https://charts.example.com/stable/", true, "https://charts.example.com/stable"},
		{HelmRepoHTTP, " http://10.0.0.8:8080 ", false, "http://10.0.0.8:8080"},
		{HelmRepoOCI, "oci://harbor.local/library", true, "oci://harbor.local/library"},
	}
	for _, c := range ok {
		got, err := NormalizeHelmRepoURL(c.typ, c.in, c.cred)
		if err != nil || got != c.want {
			t.Errorf("NormalizeHelmRepoURL(%d, %q) = %q, %v; want %q", c.typ, c.in, got, err, c.want)
		}
	}
	bad := []struct {
		typ  int32
		in   string
		cred bool
	}{
		{HelmRepoHTTP, "http://charts.example.com", true}, // 凭据走明文
		{HelmRepoHTTP, "https://user:pw@charts.example.com", false},
		{HelmRepoHTTP, "https://charts.example.com/?a=1", false},
		{HelmRepoHTTP, "https://charts.example.com/#x", false},
		{HelmRepoHTTP, "oci://harbor.local/x", false},
		{HelmRepoOCI, "https://harbor.local/x", false},
		{HelmRepoHTTP, "file:///etc/passwd", false},
		{HelmRepoHTTP, "https:///nohost", false},
		{HelmRepoHTTP, "https://a.example.com/../x", false},
		{9, "https://a.example.com", false},
		{HelmRepoHTTP, "", false},
	}
	for _, c := range bad {
		if got, err := NormalizeHelmRepoURL(c.typ, c.in, c.cred); err == nil {
			t.Errorf("NormalizeHelmRepoURL(%d, %q) = %q, want error", c.typ, c.in, got)
		}
	}
}

func TestHelmNames(t *testing.T) {
	t.Parallel()
	for _, n := range []string{"web", "my-app.v2", "a"} {
		if !ValidHelmReleaseName(n) {
			t.Errorf("release name %q must be valid", n)
		}
	}
	for _, n := range []string{"", "Web", "-a", "a_b", "a/b", string(make([]byte, 54))} {
		if ValidHelmReleaseName(n) {
			t.Errorf("release name %q must be invalid", n)
		}
	}
	for _, n := range []string{"nginx", "bitnami/nginx", "a.b_c"} {
		if !ValidHelmChartName(n) {
			t.Errorf("chart name %q must be valid", n)
		}
	}
	for _, n := range []string{"", "../x", "a/../b", "a//b", "/abs", "a b", "a?b"} {
		if ValidHelmChartName(n) {
			t.Errorf("chart name %q must be invalid", n)
		}
	}
	if !ValidHelmVersion("1.2.3-rc.1+build") || ValidHelmVersion("") || ValidHelmVersion("1 2") || ValidHelmVersion("../1") {
		t.Error("version validation mismatch")
	}
	if !HelmClusterless(OpHelmCharts) || HelmClusterless(OpHelmList) || !IsHelmWrite(OpHelmUninstall) || IsHelmWrite(OpHelmPreview) || !IsHelmOp(OpHelmGet) || IsHelmOp(OpList) {
		t.Error("helm op classification mismatch")
	}
}
