package cmdbcontract

import (
	"errors"
	"testing"
)

func TestDecodeConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		provider string
		raw      string
		ok       bool
	}{
		{"aliyun ok", ProviderAliyun, `{"region_id":"cn-hangzhou","access_key_id":"LTAI"}`, true},
		{"aliyun custom endpoint", ProviderAliyun, `{"region_id":"cn-hangzhou","access_key_id":"LTAI","endpoint":"https://ecs.example.com"}`, true},
		{"aliyun missing region", ProviderAliyun, `{"access_key_id":"LTAI"}`, false},
		{"aliyun secret in config", ProviderAliyun, `{"region_id":"r","access_key_id":"k","access_key_secret":"s"}`, false},
		{"aliyun bad endpoint", ProviderAliyun, `{"region_id":"r","access_key_id":"k","endpoint":"ftp://x"}`, false},
		{"vmware ok", ProviderVMware, `{"url":"https://vc.example.com/sdk","username":"admin@vsphere.local","insecure":true}`, true},
		{"vmware embedded creds", ProviderVMware, `{"url":"https://u:p@vc.example.com/sdk","username":"a"}`, false},
		{"vmware missing user", ProviderVMware, `{"url":"https://vc.example.com/sdk"}`, false},
		{"unknown provider", "gcp", `{}`, false},
		{"empty", ProviderAliyun, ``, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DecodeConfig(tc.provider, []byte(tc.raw))
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrConfigInvalid) {
				t.Fatalf("expected ErrConfigInvalid, got %v", err)
			}
		})
	}
}

func TestSourceOf(t *testing.T) {
	t.Parallel()
	if s := SourceOf("abc"); s != "sync:abc" || !IsSyncSource(s) || IsSyncSource(SourceManual) {
		t.Fatalf("source helpers broken: %q", s)
	}
	if len(ProviderModels(ProviderVMware)) != 2 || ProviderModels("x") != nil {
		t.Fatal("ProviderModels mapping broken")
	}
}
