package secretbox

import (
	"encoding/hex"
	"errors"
	"testing"
)

// TestContractVector 固定向量（由 core 历史实现生成）：key "contract-master-key"，
// 明文 "smtp-pass-☃"——锁住 sha256 派生 + nonce||ct 布局，防止两端实现漂移。
func TestContractVector(t *testing.T) {
	t.Parallel()
	sealed, _ := hex.DecodeString("ddfb5e98ad8fa78bb60f6d785e2986a3d7e480a0b016f82e6f5b450b7c4669ca8a337dc7626ed3aed2")
	box, err := New("contract-master-key")
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := box.Open(sealed); err != nil || plain != "smtp-pass-☃" {
		t.Fatalf("Open(vector) = %q, %v", plain, err)
	}
	again, err := box.SealWithNonce("smtp-pass-☃", sealed[:12])
	if err != nil || hex.EncodeToString(again) != hex.EncodeToString(sealed) {
		t.Fatalf("SealWithNonce must reproduce vector: %x %v", again, err)
	}
	rt, _ := box.Seal("x")
	if p, err := box.Open(rt); err != nil || p != "x" {
		t.Fatalf("roundtrip = %q, %v", p, err)
	}
	if _, err := box.Open(sealed[:10]); err == nil {
		t.Fatal("truncated ciphertext must fail")
	}
	if _, err := New("other"); err != nil {
		t.Fatal(err)
	}
	wrong, _ := New("other")
	if _, err := wrong.Open(sealed); err == nil {
		t.Fatal("wrong key must fail")
	}
	off, _ := New("")
	if off.Enabled() {
		t.Fatal("empty key must be disabled")
	}
	if _, err := off.Seal("x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("disabled Seal error = %v", err)
	}
	if _, err := off.Open(sealed); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("disabled Open error = %v", err)
	}
}
