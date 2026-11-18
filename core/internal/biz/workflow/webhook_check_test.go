package biz

import (
	"errors"
	"testing"
)

// 端点地址的保存校验：须为 https、不带账号口令，且不是 localhost 或指向回环 / 内网
// （含经 NAT64、6to4 嵌入的）字面地址。规则与通知渠道共用 kit/netguard。
func TestCheckWebhook(t *testing.T) {
	t.Parallel()
	for _, u := range []string{
		"http://hooks.example.com/x", "https://user:pw@hooks.example.com/x", "https:///x", "https://hooks.example.com/x#frag",
		"https://localhost/x", "https://a.localhost/x", "https://127.0.0.1/x", "https://[::1]/x", "https://10.0.0.5/x",
		"https://169.254.169.254/x", "https://[64:ff9b::a00:1]/x", "https://[2002:a00:1::1]/x", "https://198.18.0.1/x",
	} {
		if err := checkWebhook(&Webhook{Name: "n", URL: u}); !errors.Is(err, ErrWebhookInvalid) {
			t.Errorf("%s: error = %v, want ErrWebhookInvalid", u, err)
		}
	}
	for _, u := range []string{"https://hooks.example.com/x", "https://8.8.8.8/x", "https://[2606:4700::1111]:8443/x"} {
		if err := checkWebhook(&Webhook{Name: "n", URL: u}); err != nil {
			t.Errorf("%s: error = %v, want nil", u, err)
		}
	}
}
