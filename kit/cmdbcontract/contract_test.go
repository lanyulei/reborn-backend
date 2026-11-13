package cmdbcontract

import (
	"errors"
	"testing"
)

func TestUniqueKey(t *testing.T) {
	t.Parallel()
	got := UniqueKey([]string{"serial", "ip"}, map[string]any{"ip": "10.0.0.1", "serial": "SN1", "other": 1})
	if got != "ip=10.0.0.1|serial=SN1" {
		t.Fatalf("UniqueKey = %q", got)
	}
	if got := UniqueKey([]string{"ip"}, map[string]any{"ip": nil}); got != "" {
		t.Fatalf("nil value must be skipped, got %q", got)
	}
	if got := UniqueKey(nil, map[string]any{"ip": "x"}); got != "" {
		t.Fatalf("no unique fields must yield empty key, got %q", got)
	}
}

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

// UniqueKey 的数值文本须与 jsonb `->>` 一致（SQL 重算与数据面对账以它为准）。
func TestUniqueKeyNumberText(t *testing.T) {
	got := UniqueKey([]string{"n", "f", "s", "b"}, map[string]any{
		"n": float64(1000000), "f": 1.5, "s": "x", "b": true,
	})
	want := "b=true|f=1.5|n=1000000|s=x"
	if got != want {
		t.Fatalf("UniqueKey = %q, want %q", got, want)
	}
}
