package keycheck

import (
	"context"
	"testing"
)

func TestFingerprint(t *testing.T) {
	a := Fingerprint(Master, "secret")
	if len(a) != 32 {
		t.Fatalf("len = %d, want 32", len(a))
	}
	if a != Fingerprint(Master, "secret") {
		t.Fatal("fingerprint is not deterministic")
	}
	if a == Fingerprint(Master, "other") || a == Fingerprint(K8sInternal, "secret") {
		t.Fatal("fingerprint must depend on both the secret and the name")
	}
}

// TestRunSkipsWithoutDatabase 未配置密钥或 DSN 时不连接数据库。
func TestRunSkipsWithoutDatabase(t *testing.T) {
	res, err := Run(context.Background(), "host=127.0.0.1 port=1", false, Key{Name: Master})
	if err != nil || len(res) != 1 || res[0].Status != StatusUnset {
		t.Fatalf("Run() = %v, %v; want one unset result", res, err)
	}
	res, err = Run(context.Background(), "", true, Key{Name: Master, Secret: "k"})
	if err != nil || len(res) != 0 {
		t.Fatalf("Run() = %v, %v; want no results without a DSN", res, err)
	}
}
