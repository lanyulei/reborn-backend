//go:build integration

package data

import (
	"context"
	"maps"
	"testing"
	"time"
)

// Names 与「列出全部未删除空间后按 ID 取名」一致：存在的给名称，已软删 / 不存在 / 非法 ID 不出现。
func TestIntegration_SpaceNames(t *testing.T) {
	r := newIT(t)
	ctx := context.Background()
	a, b, gone := r.newSpace(t, "names-a", 1), r.newSpace(t, "names-b", 2), r.newSpace(t, "names-gone", 1)
	if err := r.spaces.SoftDelete(ctx, gone.ID, time.Now()); err != nil {
		t.Fatalf("SoftDelete() error = %v", err)
	}
	all, err := r.spaces.List(ctx, "", "")
	if err != nil {
		t.Fatal(err)
	}
	byList := map[string]string{}
	ids := []string{a.ID, b.ID, gone.ID, mustID(t), "not-a-uuid", ""}
	for _, s := range all {
		for _, id := range ids {
			if s.ID == id {
				byList[id] = s.Name
			}
		}
	}
	got, err := r.spaces.Names(ctx, ids)
	if err != nil {
		t.Fatalf("Names() error = %v", err)
	}
	if len(got) != 2 || !maps.Equal(got, byList) {
		t.Fatalf("Names() = %v, via List = %v", got, byList)
	}
	if empty, err := r.spaces.Names(ctx, nil); err != nil || len(empty) != 0 {
		t.Fatalf("Names(nil) = %v, %v", empty, err)
	}
}
