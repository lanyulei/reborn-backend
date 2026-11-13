package server

import (
	"testing"

	ingressv1 "github.com/lanyulei/reborn/backend/notify/api/ingress/v1"
)

// TestDedupGuarded 重放请求仅当全部事件带 dedup_key 时放行（交给 dedup 门闩）。
func TestDedupGuarded(t *testing.T) {
	t.Parallel()
	keyed := &ingressv1.IngressEvent{DedupKey: "k"}
	bare := &ingressv1.IngressEvent{}
	cases := []struct {
		name string
		req  any
		want bool
	}{
		{"single keyed", &ingressv1.SendEventRequest{Event: keyed}, true},
		{"single bare", &ingressv1.SendEventRequest{Event: bare}, false},
		{"single nil event", &ingressv1.SendEventRequest{}, false},
		{"batch all keyed", &ingressv1.BatchSendRequest{Events: []*ingressv1.IngressEvent{keyed, keyed}}, true},
		{"batch mixed", &ingressv1.BatchSendRequest{Events: []*ingressv1.IngressEvent{keyed, bare}}, false},
		{"batch empty", &ingressv1.BatchSendRequest{}, false},
		{"unknown request", struct{}{}, false},
	}
	for _, c := range cases {
		if got := dedupGuarded(c.req); got != c.want {
			t.Errorf("%s: dedupGuarded = %v, want %v", c.name, got, c.want)
		}
	}
}
