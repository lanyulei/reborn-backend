package operator

import (
	"context"
	"testing"
)

func TestRoundtrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if got := FromContext(ctx); got != "" {
		t.Fatalf("FromContext(empty) = %q", got)
	}
	if got := FromContext(WithID(ctx, "")); got != "" {
		t.Fatalf("empty id must not be injected, got %q", got)
	}
	if got := FromContext(WithID(ctx, "u1")); got != "u1" {
		t.Fatalf("FromContext = %q, want u1", got)
	}
}
