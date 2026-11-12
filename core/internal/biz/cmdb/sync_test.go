package biz_test

import (
	"context"
	"errors"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"
	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
)

func newSyncUC(enabled bool) (*biz.SyncUsecase, *testfake.SyncStore) {
	store := testfake.NewSyncStore()
	return biz.NewSyncUsecase(store.Sources(), store.Tasks(), store.Runs(), &testfake.FakeSealer{Enabled: enabled}), store
}

func TestSyncUsecase_CreateDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	uc, store := newSyncUC(true)
	actor := biz.Actor{UserID: "u1"}
	src, err := uc.Create(context.Background(), actor, &biz.SyncSource{
		Name: "ali", Provider: cmdbcontract.ProviderAliyun, Config: []byte(`{"region_id":"cn-hangzhou","access_key_id":"LTAI"}`), Enabled: true,
	}, "secret")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if src.IntervalSeconds != biz.DefaultSyncIntervalSeconds || src.LastStatus != cmdbcontract.RunNever || src.NextRunTime == nil || !src.HasCredential {
		t.Fatalf("defaults not applied: %+v", src)
	}
	if string(store.Credential(src.ID)) != "sealed:secret" {
		t.Fatalf("credential must be sealed, got %q", store.Credential(src.ID))
	}
	// 配置里混入凭据键 → 拒绝（DisallowUnknownFields）。
	_, err = uc.Create(context.Background(), actor, &biz.SyncSource{
		Name: "bad", Provider: cmdbcontract.ProviderAliyun, Config: []byte(`{"region_id":"r","access_key_id":"k","access_key_secret":"s"}`),
	}, "secret")
	if !errors.Is(err, biz.ErrSourceInvalid) {
		t.Fatalf("expected ErrSourceInvalid, got %v", err)
	}
	// 名称重复。
	_, err = uc.Create(context.Background(), actor, &biz.SyncSource{
		Name: "ali", Provider: cmdbcontract.ProviderVMware, Config: []byte(`{"url":"https://vc/sdk","username":"a"}`),
	}, "p")
	if !errors.Is(err, biz.ErrSourceNameExists) {
		t.Fatalf("expected ErrSourceNameExists, got %v", err)
	}
}

func TestSyncUsecase_SecretUnavailable(t *testing.T) {
	t.Parallel()
	uc, _ := newSyncUC(false)
	_, err := uc.Create(context.Background(), biz.Actor{}, &biz.SyncSource{
		Name: "ali", Provider: cmdbcontract.ProviderAliyun, Config: []byte(`{"region_id":"r","access_key_id":"k"}`),
	}, "secret")
	if !errors.Is(err, biz.ErrSecretUnavailable) {
		t.Fatalf("expected ErrSecretUnavailable, got %v", err)
	}
}

func TestSyncUsecase_UpdateTriggerRuns(t *testing.T) {
	t.Parallel()
	uc, store := newSyncUC(true)
	ctx, actor := context.Background(), biz.Actor{UserID: "u1"}
	src, err := uc.Create(ctx, actor, &biz.SyncSource{
		Name: "vc", Provider: cmdbcontract.ProviderVMware, Config: []byte(`{"url":"https://vc/sdk","username":"a"}`), Enabled: false,
	}, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if src.NextRunTime != nil {
		t.Fatal("disabled source must not be scheduled")
	}
	upd := biz.SyncSourceUpdate{Name: "vc2", Config: []byte(`{"url":"https://vc2/sdk","username":"b","insecure":true}`), IntervalSeconds: 600, GracePeriodSeconds: 0, Enabled: true}
	// 版本不匹配。
	if _, err := uc.Update(ctx, actor, src.ID, 99, upd, nil); !errors.Is(err, biz.ErrSourceVersionConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
	// 不传凭据 = 保留；启用后计划时间被置上。
	got, err := uc.Update(ctx, actor, src.ID, src.Version, upd, nil)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if got.Version != 2 || got.NextRunTime == nil || string(store.Credential(src.ID)) != "sealed:p1" || got.Name != "vc2" {
		t.Fatalf("update result = %+v cred=%q", got, store.Credential(src.ID))
	}
	// 轮换凭据。
	p2 := "p2"
	if _, err := uc.Update(ctx, actor, src.ID, 2, upd, &p2); err != nil || string(store.Credential(src.ID)) != "sealed:p2" {
		t.Fatalf("rotate credential: err=%v cred=%q", err, store.Credential(src.ID))
	}
	// 配置校验（provider 固定为 vmware）。
	bad := upd
	bad.Config = []byte(`{"region_id":"x"}`)
	if _, err := uc.Update(ctx, actor, src.ID, 3, bad, nil); !errors.Is(err, biz.ErrSourceInvalid) {
		t.Fatalf("expected ErrSourceInvalid, got %v", err)
	}
	// 触发：第一次成功，第二次忙。
	task, err := uc.Trigger(ctx, actor, src.ID)
	if err != nil || task.Trigger != cmdbcontract.TriggerManual || task.Status != cmdbcontract.TaskPending {
		t.Fatalf("Trigger() = %+v err=%v", task, err)
	}
	if _, err := uc.Trigger(ctx, actor, src.ID); !errors.Is(err, biz.ErrSyncBusy) {
		t.Fatalf("expected ErrSyncBusy, got %v", err)
	}
	if s, _ := uc.Get(ctx, src.ID); !s.ActiveTask {
		t.Fatal("ActiveTask must reflect pending task")
	}
	// 数据面完成 → 运行记录可见，可再次触发。
	store.FinishTask(task.ID, &biz.SyncRun{ID: "r1", SourceID: src.ID, TaskID: task.ID, Status: cmdbcontract.RunSucceeded, Discovered: 3, Created: 3})
	runs, total, err := uc.ListRuns(ctx, src.ID, 1, 10)
	if err != nil || total != 1 || runs[0].Created != 3 {
		t.Fatalf("ListRuns() = %+v total=%d err=%v", runs, total, err)
	}
	if _, err := uc.Trigger(ctx, actor, src.ID); err != nil {
		t.Fatalf("re-trigger after finish: %v", err)
	}
	// 删除后不可见、运行记录查询 404。
	if err := uc.Delete(ctx, src.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Get(ctx, src.ID); !errors.Is(err, biz.ErrSourceNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
	if _, _, err := uc.ListRuns(ctx, src.ID, 1, 10); !errors.Is(err, biz.ErrSourceNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}
