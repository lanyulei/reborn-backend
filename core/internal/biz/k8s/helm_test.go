package biz

import (
	"context"
	"errors"
	"testing"

	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"
)

func TestHelmRepoInputNormalize(t *testing.T) {
	t.Parallel()
	in := HelmRepoInput{Name: " charts ", Type: kc.HelmRepoHTTP, URL: "https://charts.example.com/stable/ ", Username: "u"}
	if err := in.normalize(true); err != nil {
		t.Fatal(err)
	}
	if in.Name != "charts" || in.URL != "https://charts.example.com/stable" {
		t.Fatalf("normalized = %+v", in)
	}
	bad := []struct {
		in  HelmRepoInput
		has bool
	}{
		{HelmRepoInput{Type: kc.HelmRepoHTTP, URL: "https://a.example.com"}, false},                            // 缺名称
		{HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "http://a.example.com", Username: "u"}, true},    // 凭据走明文
		{HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "https://a.example.com"}, true},                  // 有密码没用户名
		{HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "https://u:p@a.example.com"}, false},             //nolint:gosec // 测试数据：内嵌凭据必须被拒绝
		{HelmRepoInput{Name: "a", Type: kc.HelmRepoOCI, URL: "https://a.example.com"}, false},                  // 类型与协议不符
		{HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "https://a.example.com", CACert: "nope"}, false}, // CA 不是 PEM
	}
	for i, c := range bad {
		if err := c.in.normalize(c.has); !errors.Is(err, ErrHelmRepoInvalid) {
			t.Errorf("case %d: err = %v, want ErrHelmRepoInvalid", i, err)
		}
	}
	plain := HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "http://10.0.0.8:8080"}
	if err := plain.normalize(false); err != nil {
		t.Fatalf("anonymous http repo must be allowed: %v", err)
	}
}

type helmStore struct {
	cur   *HelmRepo
	saved *HelmRepo
	sec   *[]byte
}

func (s *helmStore) List(context.Context, string, bool, int64, int64) ([]*HelmRepo, int64, error) {
	return nil, 0, nil
}

func (s *helmStore) Get(context.Context, string) (*HelmRepo, error) {
	if s.cur == nil {
		return nil, ErrHelmRepoNotFound
	}
	return s.cur, nil
}

func (s *helmStore) Save(_ context.Context, r *HelmRepo, secret *[]byte, _ int32, _ *AuditEntry) error {
	s.saved, s.sec = r, secret
	return nil
}

type helmSealer struct{}

func (helmSealer) Seal(p string) ([]byte, error) { return []byte("sealed:" + p), nil }

type helmDir struct{}

func (helmDir) UserNames(context.Context, []string) (map[string]string, error) { return nil, nil }

func TestHelmUpdateSecretRules(t *testing.T) {
	t.Parallel()
	store := &helmStore{cur: &HelmRepo{ID: "r1", Name: "a", Type: kc.HelmRepoHTTP, URL: "https://a.example.com", Username: "u", HasSecret: true, Version: 3}}
	uc := NewHelmUsecase(store, nil, helmSealer{}, helmDir{}, nil)
	ctx := context.Background()
	in := HelmRepoInput{Name: "a", Type: kc.HelmRepoHTTP, URL: "https://a.example.com/charts", Username: "u", Enabled: true}

	// 不传密码：沿用（secret 指针为 nil）
	if _, err := uc.Update(ctx, Caller{UserID: "x"}, "r1", in, nil, 3); err != nil {
		t.Fatal(err)
	}
	if store.sec != nil {
		t.Fatal("omitted secret must keep the saved one")
	}
	// 主机变了却沿用已保存的密码：拒绝（否则密码会被发往新主机）
	moved := in
	moved.URL = "https://evil.example.net/charts"
	if _, err := uc.Update(ctx, Caller{UserID: "x"}, "r1", moved, nil, 3); !errors.Is(err, ErrHelmRepoInvalid) {
		t.Fatalf("host change with kept secret: err = %v", err)
	}
	// 主机变了并重新给出密码：允许
	pw := "new"
	if _, err := uc.Update(ctx, Caller{UserID: "x"}, "r1", moved, &pw, 3); err != nil {
		t.Fatal(err)
	}
	if store.sec == nil || string(*store.sec) != "sealed:new" {
		t.Fatalf("secret = %v", store.sec)
	}
	// 空串：清除；清除后不再要求 https
	empty := ""
	plain := in
	plain.URL = "http://a.example.com"
	if _, err := uc.Update(ctx, Caller{UserID: "x"}, "r1", plain, &empty, 3); err != nil {
		t.Fatal(err)
	}
	if store.sec == nil || len(*store.sec) != 0 {
		t.Fatalf("empty secret must clear: %v", store.sec)
	}
	// 类型不可改
	oci := in
	oci.Type, oci.URL = kc.HelmRepoOCI, "oci://a.example.com/x"
	if _, err := uc.Update(ctx, Caller{UserID: "x"}, "r1", oci, nil, 3); !errors.Is(err, ErrHelmRepoInvalid) {
		t.Fatalf("type change: err = %v", err)
	}
}

func TestCheckRelease(t *testing.T) {
	t.Parallel()
	if err := CheckRelease("web", "bitnami/nginx", "1.2.3"); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][3]string{{"Web", "", ""}, {"web", "../x", ""}, {"web", "nginx", "1 2"}} {
		if err := CheckRelease(c[0], c[1], c[2]); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("CheckRelease(%v) = %v", c, err)
		}
	}
}
