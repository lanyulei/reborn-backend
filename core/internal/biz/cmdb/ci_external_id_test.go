package biz

import (
	"testing"

	"github.com/go-kratos/kratos/v3/errors"
)

// 同步托管资源的外部标识是采集对账的锚点：人工改掉之后下一次同步匹配不上，资源被判「消失」并退役、
// 采集行作为新资源重建。人工路径必须拒绝；同步来源自己写入、以及已解除托管的资源不受限。
func TestRejectExternalIDChange(t *testing.T) {
	t.Parallel()
	syncCI := &CI{ID: "018f3e1a-0400-7000-8000-000000000001", Source: "sync:018f3e1a-0400-7000-8000-000000000002", ExternalID: "i-aaa"}
	manualCI := &CI{ID: syncCI.ID, Source: "manual", ExternalID: "i-aaa"}
	manual := Actor{UserID: "u1"}
	syncActor := Actor{UserID: "u2", Source: "sync:018f3e1a-0400-7000-8000-000000000002"}
	same, changed := "i-aaa", "i-bbb"

	if err := rejectExternalIDChange(manual, syncCI, CIUpdate{}); err != nil {
		t.Fatalf("no change requested: error = %v, want nil", err)
	}
	if err := rejectExternalIDChange(manual, syncCI, CIUpdate{ExternalID: &same}); err != nil {
		t.Fatalf("same value resubmitted: error = %v, want nil", err)
	}
	if err := rejectExternalIDChange(manual, syncCI, CIUpdate{ExternalID: &changed}); !errors.Is(err, ErrCIInvalid) {
		t.Fatalf("manual actor changing the anchor: error = %v, want ErrCIInvalid", err)
	}
	if err := rejectExternalIDChange(syncActor, syncCI, CIUpdate{ExternalID: &changed}); err != nil {
		t.Fatalf("the sync source itself: error = %v, want nil", err)
	}
	if err := rejectExternalIDChange(manual, manualCI, CIUpdate{ExternalID: &changed}); err != nil {
		t.Fatalf("a manual (unclaimed) resource: error = %v, want nil", err)
	}
}
