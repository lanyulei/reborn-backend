//go:build integration

package data

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	uploader = "018f3e1a-00f0-7000-8000-000000000001"
	stranger = "018f3e1a-00f0-7000-8000-000000000002"
)

var pngData = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)

// newIntegration 构造真实连接池 + 本地磁盘驱动（临时目录）；库名须含 "test"。
func newIntegration(t *testing.T) (*biz.Usecase, *fileRepo, *basedata.TxManager, *basedata.Data, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	if cfg, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	} else if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"（集成测试会 TRUNCATE 业务表）", db)
	}
	root := t.TempDir()
	c := &conf.Data{Database: &kitconf.Database{Source: dsn}, Upload: &conf.Data_Upload{Dir: root}}
	d, cleanup, err := basedata.NewData(c)
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	if _, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE file_object"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	stores, err := NewStores(c, d)
	if err != nil {
		t.Fatalf("NewStores() error = %v", err)
	}
	repo := NewFileRepo(d)
	uc, err := biz.NewUsecase(repo, repo, repo, stores, nil)
	if err != nil {
		t.Fatalf("NewUsecase() error = %v", err)
	}
	return uc, repo, basedata.NewTxManager(d), d, root
}

func TestIntegration_NewStores_UnknownDriver(t *testing.T) {
	_, _, _, d, _ := newIntegration(t)
	c := &conf.Data{Upload: &conf.Data_Upload{Driver: "s3"}}
	if _, err := NewStores(c, d); err == nil {
		t.Fatal("NewStores(unknown driver) must fail at wiring time")
	}
}

func TestIntegration_LocalStore(t *testing.T) {
	store := NewLocalStore(t.TempDir())
	ctx := context.Background()
	key := "2026/11/16/018f3e1a-00f0-7000-8000-0000000000aa"
	if err := store.Put(ctx, key, []byte("hello")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if got, err := store.Get(ctx, key); err != nil || string(got) != "hello" {
		t.Fatalf("Get() = %q, %v", got, err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete(again) must be idempotent: %v", err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrObjectNotFound", err)
	}
	// 纵深防御：不合白名单格式的键一律拒绝（路径穿越 / 绝对路径 / 多余层级）。
	for _, bad := range []string{"../../etc/passwd", "/etc/passwd", "2026/11/16/../../x", "2026/11/16/abc", ""} {
		if err := store.Put(ctx, bad, []byte("x")); err == nil {
			t.Errorf("Put(%q) must be rejected", bad)
		}
		if _, err := store.Get(ctx, bad); err == nil || errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("Get(%q) must be rejected, got %v", bad, err)
		}
		if err := store.Delete(ctx, bad); err == nil {
			t.Errorf("Delete(%q) must be rejected", bad)
		}
	}
}

func TestIntegration_FileLifecycle(t *testing.T) {
	uc, repo, txs, d, root := newIntegration(t)
	ctx := context.Background()

	f, err := uc.Upload(ctx, uploader, "图.png", pngData)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if _, err := os.Stat(root + "/files/" + f.ObjectKey); err != nil {
		t.Fatalf("object file missing on disk: %v", err)
	}
	if _, data, err := uc.Download(ctx, uploader, f.ID); err != nil || !bytes.Equal(data, pngData) {
		t.Fatalf("Download() error = %v", err)
	}
	if _, err := uc.Get(ctx, stranger, f.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Get(stranger) error = %v, want ErrFileNotFound", err)
	}

	// 绑定与业务写同事务：事务回滚则绑定不生效。
	rollback := fmt.Errorf("business write failed")
	err = txs.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := uc.Bind(ctx, tx, uploader, "ticket", "T-1", []string{f.ID}); err != nil {
			return err
		}
		return rollback
	})
	if err != rollback { //nolint:errorlint // 断言原样透传
		t.Fatalf("WithinTx() error = %v, want rollback sentinel", err)
	}
	if got, _ := repo.Get(ctx, f.ID); got.Status != biz.StatusTemp {
		t.Fatalf("status after rollback = %d, want temp", got.Status)
	}
	// 他人不能绑定；本人绑定成功；重复绑定不命中。
	bind := func(user, bizID string, ids ...string) error {
		return txs.WithinTx(ctx, func(tx txrun.Tx) error { return uc.Bind(ctx, tx, user, "ticket", bizID, ids) })
	}
	if err := bind(stranger, "T-1", f.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Bind(stranger) error = %v, want ErrFileNotFound", err)
	}
	if err := bind(uploader, "T-1", strings.ToUpper(f.ID), f.ID); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := bind(uploader, "T-2", f.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Bind(again) error = %v, want ErrFileNotFound", err)
	}
	got, err := repo.Get(ctx, f.ID)
	if err != nil || got.Status != biz.StatusBound || got.BizType != "ticket" || got.BizID != "T-1" {
		t.Fatalf("bound file = %+v, %v", got, err)
	}
	// 已绑定且无检查器：拒绝。
	if _, err := uc.Get(ctx, uploader, f.ID); !errors.Is(err, biz.ErrFileForbidden) {
		t.Fatalf("Get(bound, no checker) error = %v, want ErrFileForbidden", err)
	}

	// 清理：新鲜的临时文件与已绑定文件都不动。
	temp, err := uc.Upload(ctx, uploader, "tmp.png", pngData)
	if err != nil {
		t.Fatalf("Upload(temp) error = %v", err)
	}
	if n, err := uc.Purge(ctx); err != nil || n != 0 {
		t.Fatalf("Purge(nothing expired) = %d, %v", n, err)
	}
	// 过期临时文件 + 超过保留期的软删文件 → 连同磁盘对象物理清理。
	if err := txs.WithinTx(ctx, func(tx txrun.Tx) error { return uc.Release(ctx, tx, "ticket", "T-1") }); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, err := d.Pool.Exec(ctx,
		"UPDATE file_object SET create_time = now() - interval '2 days', delete_time = delete_time - interval '2 days'"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if n, err := uc.Purge(ctx); err != nil || n != 2 {
		t.Fatalf("Purge() = %d, %v; want 2", n, err)
	}
	for _, file := range []*biz.File{f, temp} {
		if _, err := os.Stat(root + "/files/" + file.ObjectKey); !os.IsNotExist(err) {
			t.Fatalf("object %s must be removed from disk, stat err = %v", file.ObjectKey, err)
		}
	}
	var rows int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM file_object").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rows after purge = %d, %v; want 0", rows, err)
	}
}

// 多实例并发清理：每个文件恰被处理一次（SKIP LOCKED），总数不多不少。
func TestIntegration_PurgeConcurrent(t *testing.T) {
	uc, repo, _, d, _ := newIntegration(t)
	ctx := context.Background()
	const total = 60
	for i := range total {
		if _, err := uc.Upload(ctx, uploader, fmt.Sprintf("f%d.png", i), pngData); err != nil {
			t.Fatalf("Upload() error = %v", err)
		}
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE file_object SET create_time = now() - interval '2 days'"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	var (
		mu      sync.Mutex
		removed = map[string]int{}
		wg      sync.WaitGroup
		sum     int
	)
	for range 4 {
		wg.Go(func() {
			for {
				n, err := repo.PurgeExpired(ctx, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), time.Hour, 7,
					func(_ context.Context, f *biz.File) error {
						mu.Lock()
						removed[f.ID]++
						mu.Unlock()
						return nil
					})
				if err != nil {
					t.Errorf("PurgeExpired() error = %v", err)
					return
				}
				mu.Lock()
				sum += n
				mu.Unlock()
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	if sum != total || len(removed) != total {
		t.Fatalf("purged = %d (distinct %d), want %d", sum, len(removed), total)
	}
	for id, n := range removed {
		if n != 1 {
			t.Fatalf("file %s removed %d times, want exactly once", id, n)
		}
	}
}

// 对象删除失败的文件不会堵住清理队列：失败的记下时间、暂不再领取，其余照常清理。
func TestIntegration_PurgePoisonRows(t *testing.T) {
	uc, repo, _, d, _ := newIntegration(t)
	ctx := context.Background()
	var ids []string
	for i := range 6 {
		f, err := uc.Upload(ctx, uploader, fmt.Sprintf("f%d.png", i), pngData)
		if err != nil {
			t.Fatalf("Upload() error = %v", err)
		}
		ids = append(ids, f.ID)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE file_object SET create_time = now() - interval '2 days'"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	// 排在最前的 3 个文件删除对象恒失败；每批只取 3 个。
	poison := map[string]bool{ids[0]: true, ids[1]: true, ids[2]: true}
	remove := func(_ context.Context, f *biz.File) error {
		if poison[f.ID] {
			return fmt.Errorf("driver offline")
		}
		return nil
	}
	old := time.Now().Add(-time.Hour)
	if n, err := repo.PurgeExpired(ctx, old, old, time.Hour, 3, remove); n != 0 || err == nil {
		t.Fatalf("first round = %d, %v; want 0 purged and an error reporting the failures", n, err)
	}
	if n, err := repo.PurgeExpired(ctx, old, old, time.Hour, 3, remove); n != 3 || err != nil {
		t.Fatalf("second round = %d, %v; want the 3 healthy files purged", n, err)
	}
	// 过了重试间隔后坏行重新被领取。
	delete(poison, ids[0])
	if n, _ := repo.PurgeExpired(ctx, old, old, 0, 3, remove); n != 1 {
		t.Fatalf("retry round purged %d, want 1", n)
	}
}

// 上传配额在真实库上生效（单语句判定 + 写入）。
func TestIntegration_UploadQuota(t *testing.T) {
	uc, _, _, d, _ := newIntegration(t)
	ctx := context.Background()
	for i := range biz.MaxTempFiles {
		if _, err := uc.Upload(ctx, uploader, "a.png", pngData); err != nil {
			t.Fatalf("Upload #%d error = %v", i, err)
		}
	}
	if _, err := uc.Upload(ctx, uploader, "a.png", pngData); !errors.Is(err, biz.ErrFileQuotaExceeded) {
		t.Fatalf("Upload(over quota) error = %v, want ErrFileQuotaExceeded", err)
	}
	var rows int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM file_object").Scan(&rows); err != nil || rows != biz.MaxTempFiles {
		t.Fatalf("rows = %d, %v; want %d", rows, err, biz.MaxTempFiles)
	}
	if _, err := uc.Upload(ctx, stranger, "a.png", pngData); err != nil {
		t.Fatalf("Upload(other user) error = %v", err)
	}
}

// 「每个归属只有一份」的文件：写入即替换（旧的软删、对象留待清理），仅在没有时写入不盖已有的；
// 并发替换同一个归属后恰好剩一份，且读到的内容与类型是同一次写入的。
func TestIntegration_Singleton(t *testing.T) {
	uc, _, _, d, root := newIntegration(t)
	ctx := context.Background()
	live := func(bizType, bizID string) (n int) {
		t.Helper()
		if err := d.Pool.QueryRow(ctx,
			"SELECT count(*) FROM file_object WHERE biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL",
			bizType, bizID).Scan(&n); err != nil {
			t.Fatalf("count live: %v", err)
		}
		return n
	}
	if _, _, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || found {
		t.Fatalf("ReadSingleton(empty) = %v, %v", found, err)
	}
	svg := []byte("<svg/>")
	if done, err := uc.PutSingleton(ctx, uploader, "system_logo", "logo", "logo.svg", "image/svg+xml", svg, false); err != nil || !done {
		t.Fatalf("PutSingleton() = %v, %v", done, err)
	}
	if done, err := uc.PutSingleton(ctx, uploader, "system_logo", "logo", "logo.png", "image/png", pngData, true); err != nil || done {
		t.Fatalf("PutSingleton(ifAbsent, exists) = %v, %v; want false", done, err)
	}
	if data, mime, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || !found || mime != "image/svg+xml" || !bytes.Equal(data, svg) {
		t.Fatalf("ReadSingleton() = %q, %q, %v, %v", data, mime, found, err)
	}
	var rows int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM file_object").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows = %d, %v; want 1 (the skipped import must leave nothing)", rows, err)
	}

	// 并发替换（含一半「仅在没有时写入」）：恰好剩一份。
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := append(bytes.Clone(pngData), byte(i))
			if _, err := uc.PutSingleton(ctx, uploader, "user_avatar", stranger, "avatar.png", fmt.Sprintf("image/x-%d", i), body, i%2 == 0); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent PutSingleton() error = %v", err)
	}
	if n := live("user_avatar", stranger); n != 1 {
		t.Fatalf("live avatars = %d, want exactly 1", n)
	}
	data, mime, found, err := uc.ReadSingleton(ctx, "user_avatar", stranger)
	if err != nil || !found || mime != fmt.Sprintf("image/x-%d", data[len(data)-1]) {
		t.Fatalf("ReadSingleton(avatar) = %q, %v, %v; content and type must come from the same write", mime, found, err)
	}
	if n := live("system_logo", "logo"); n != 1 {
		t.Fatalf("live logos = %d, want 1 (other owners untouched)", n)
	}
	// 没有留下临时文件；被替换的过了保留期连同磁盘对象清理，当前的两份不动。
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM file_object WHERE status = 1").Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("temp rows = %d, %v; want 0", rows, err)
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE file_object SET delete_time = delete_time - interval '2 days' WHERE delete_time IS NOT NULL"); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	for {
		n, err := uc.Purge(ctx)
		if err != nil {
			t.Fatalf("Purge() error = %v", err)
		}
		if n == 0 {
			break
		}
	}
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM file_object").Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("rows after purge = %d, %v; want 2", rows, err)
	}
	onDisk := 0
	_ = filepath.WalkDir(root+"/files", func(_ string, e os.DirEntry, _ error) error {
		if e != nil && !e.IsDir() {
			onDisk++
		}
		return nil
	})
	if onDisk != 2 {
		t.Fatalf("objects on disk = %d, want 2", onDisk)
	}
	if _, _, found, err := uc.ReadSingleton(ctx, "user_avatar", stranger); err != nil || !found {
		t.Fatalf("avatar after purge: found %v, %v", found, err)
	}
}
