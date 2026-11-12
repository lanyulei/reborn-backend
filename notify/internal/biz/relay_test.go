package biz

import (
	"context"
	"testing"
)

type relayerStub struct{ calls, n int }

func (r *relayerStub) RelayOnce(context.Context, int) (int, error) { r.calls++; return r.n, nil }

func TestRelayUsecase(t *testing.T) {
	t.Parallel()
	if uc := NewRelayUsecase(nil, testLogger()); uc.Enabled() {
		t.Fatal("nil relayer must be disabled (pg backbone)")
	} else if n, err := uc.RelayOnce(context.Background(), 10); n != 0 || err != nil {
		t.Fatal("disabled relay must be a no-op")
	}
	stub := &relayerStub{n: 3}
	uc := NewRelayUsecase(stub, testLogger())
	if n, err := uc.RelayOnce(context.Background(), 10); !uc.Enabled() || n != 3 || err != nil || stub.calls != 1 {
		t.Fatalf("relay = %d, %v, calls=%d", n, err, stub.calls)
	}
}
