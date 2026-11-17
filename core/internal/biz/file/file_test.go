package biz_test

import (
	"bytes"
	"context"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

const (
	alice = "018f3e1a-0000-7000-8000-00000000000a"
	bob   = "018f3e1a-0000-7000-8000-00000000000b"
)

var (
	pngData = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)
	pdfData = []byte("%PDF-1.7\n1 0 obj\n<<>>\nendobj\n")
)

func newUsecase(t *testing.T, checkers biz.AccessCheckers) (*biz.Usecase, *testfake.FileStore, *testfake.ObjectStore) {
	t.Helper()
	uc, files, objects, err := testfake.NewFileUsecase(checkers)
	if err != nil {
		t.Fatalf("NewFileUsecase() error = %v", err)
	}
	return uc, files, objects
}

func TestNewUsecase_DefaultDriverMustBeRegistered(t *testing.T) {
	t.Parallel()
	files := testfake.NewFileStore()
	if _, err := biz.NewUsecase(files, files, files, biz.Stores{Default: "s3", Drivers: map[string]storage.ObjectStore{}}, nil); err == nil {
		t.Fatal("NewUsecase() with unregistered default driver must fail")
	}
}

func TestUpload(t *testing.T) {
	t.Parallel()
	uc, _, objects := newUsecase(t, nil)
	ctx := context.Background()

	f, err := uc.Upload(ctx, alice, "报告.pdf", pdfData)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if f.Mime != "application/pdf" || f.Size != int64(len(pdfData)) || f.Status != biz.StatusTemp || len(f.SHA256) != 64 {
		t.Fatalf("Upload() = %+v", f)
	}
	if objects.Len() != 1 {
		t.Fatalf("objects = %d, want 1", objects.Len())
	}

	rejected := map[string]struct {
		name string
		data []byte
		want *errors.Error
	}{
		"html content":          {"a.txt", []byte("<html><script>alert(1)</script></html>"), biz.ErrFileTypeNotAllowed},
		"svg content":           {"a.png", []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"></svg>`), biz.ErrFileTypeNotAllowed},
		"binary with exe ext":   {"a.exe", []byte{0x4d, 0x5a, 0x90, 0x00, 0x03, 0x00, 0x00, 0x00, 0xff, 0xfe, 0x01}, biz.ErrFileTypeNotAllowed},
		"empty":                 {"a.txt", nil, biz.ErrFileTypeNotAllowed},
		"too large":             {"a.txt", bytes.Repeat([]byte("a"), biz.MaxFileSize+1), biz.ErrFileTypeNotAllowed},
		"path in name":          {"../../etc/passwd", pdfData, biz.ErrFileNameInvalid},
		"backslash in name":     {`..\x.pdf`, pdfData, biz.ErrFileNameInvalid},
		"control char in name":  {"a\r\nSet-Cookie: x.pdf", pdfData, biz.ErrFileNameInvalid},
		"blank name":            {"   ", pdfData, biz.ErrFileNameInvalid},
		"dot-dot name":          {"..", pdfData, biz.ErrFileNameInvalid},
		"bidi override in name": {"invoice\u202efdp.exe", pdfData, biz.ErrFileNameInvalid},
	}
	for name, tt := range rejected {
		if _, err := uc.Upload(ctx, alice, tt.name, tt.data); !errors.Is(err, tt.want) {
			t.Errorf("%s: error = %v, want %v", name, err, tt.want)
		}
	}
	if objects.Len() != 1 {
		t.Fatalf("rejected uploads must not leave objects: %d", objects.Len())
	}

	// 嗅探不出的二进制按扩展名二次约束：已知文档扩展名放行。
	legacy := []byte{0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1, 0x00, 0x00, 0xff, 0xfe}
	if _, err := uc.Upload(ctx, alice, "old.XLS", legacy); err != nil {
		t.Fatalf("Upload(legacy office) error = %v", err)
	}
}

func TestAccessControl(t *testing.T) {
	t.Parallel()
	uc, _, _ := newUsecase(t, biz.AccessCheckers{"ticket": testfake.FileChecker{bob + "|T-1": true}})
	ctx := context.Background()
	f, err := uc.Upload(ctx, alice, "a.png", pngData)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}

	// 临时文件：仅上传者；他人看到的是「不存在」而非「无权」。
	if _, data, err := uc.Download(ctx, alice, f.ID); err != nil || !bytes.Equal(data, pngData) {
		t.Fatalf("Download(owner temp) error = %v", err)
	}
	if _, err := uc.Get(ctx, bob, f.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Get(other's temp) error = %v, want ErrFileNotFound", err)
	}

	// 绑定：只能绑定自己的临时文件；重复绑定不命中。
	tx := testfake.Tx{}
	if err := uc.Bind(ctx, tx, bob, "ticket", "T-1", []string{f.ID}); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Bind(other's file) error = %v, want ErrFileNotFound", err)
	}
	if err := uc.Bind(ctx, tx, alice, "ticket", "T-1", []string{f.ID, f.ID}); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := uc.Bind(ctx, tx, alice, "ticket", "T-2", []string{f.ID}); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Bind(already bound) error = %v, want ErrFileNotFound", err)
	}
	if err := uc.Bind(ctx, tx, alice, "ticket", "T-1", nil); err != nil {
		t.Fatalf("Bind(no files) error = %v", err)
	}

	// 已绑定：由业务域检查器判定——上传者本人也不再自动可读。
	if _, err := uc.Get(ctx, bob, f.ID); err != nil {
		t.Fatalf("Get(allowed by checker) error = %v", err)
	}
	if _, err := uc.Get(ctx, alice, f.ID); !errors.Is(err, biz.ErrFileForbidden) {
		t.Fatalf("Get(denied by checker) error = %v, want ErrFileForbidden", err)
	}

	// 非法 ID 不进 SQL。
	if err := uc.Bind(ctx, tx, alice, "ticket", "T-1", []string{"not-a-uuid"}); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Bind(malformed id) error = %v, want ErrFileNotFound", err)
	}

	// 未登记检查器的业务类型：一律拒绝。
	g, _ := uc.Upload(ctx, alice, "b.png", pngData)
	if err := uc.Bind(ctx, tx, alice, "unknown", "X", []string{g.ID}); err != nil {
		t.Fatalf("Bind(unknown biz) error = %v", err)
	}
	if _, err := uc.Get(ctx, alice, g.ID); !errors.Is(err, biz.ErrFileForbidden) {
		t.Fatalf("Get(biz without checker) error = %v, want ErrFileForbidden", err)
	}

	// 释放指定文件：只命中绑定在该对象上的；别的对象的文件、非法 ID 都不受影响。
	h, _ := uc.Upload(ctx, alice, "c.png", pngData)
	if err := uc.Bind(ctx, tx, alice, "ticket", "T-1", []string{h.ID}); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if err := uc.ReleaseFiles(ctx, tx, "ticket", "T-9", []string{h.ID, "not-a-uuid"}); err != nil {
		t.Fatalf("ReleaseFiles(other object) error = %v", err)
	}
	if _, err := uc.Get(ctx, bob, h.ID); err != nil {
		t.Fatalf("file of another object must survive: %v", err)
	}
	if err := uc.ReleaseFiles(ctx, tx, "ticket", "T-1", []string{h.ID}); err != nil {
		t.Fatalf("ReleaseFiles() error = %v", err)
	}
	if _, err := uc.Get(ctx, bob, h.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Get(released file) error = %v, want ErrFileNotFound", err)
	}
	if _, err := uc.Get(ctx, bob, f.ID); err != nil {
		t.Fatalf("sibling file must survive: %v", err)
	}
	// 释放后不可读。
	if err := uc.Release(ctx, tx, "ticket", "T-1"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if _, err := uc.Get(ctx, bob, f.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("Get(released) error = %v, want ErrFileNotFound", err)
	}
}

func TestPurge(t *testing.T) {
	t.Parallel()
	uc, files, objects := newUsecase(t, nil)
	ctx := context.Background()
	tx := testfake.Tx{}
	upload := func(name string) *biz.File {
		t.Helper()
		f, err := uc.Upload(ctx, alice, name, pngData)
		if err != nil {
			t.Fatalf("Upload(%s) error = %v", name, err)
		}
		return f
	}
	freshTemp, oldTemp := upload("fresh.png"), upload("old.png")
	bound, released, releasedOld := upload("bound.png"), upload("released.png"), upload("released-old.png")
	for i, f := range []*biz.File{bound, released, releasedOld} {
		if err := uc.Bind(ctx, tx, alice, "ticket", string(rune('A'+i)), []string{f.ID}); err != nil {
			t.Fatalf("Bind() error = %v", err)
		}
	}
	for _, id := range []string{"B", "C"} {
		if err := uc.Release(ctx, tx, "ticket", id); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	}
	files.Backdate(oldTemp.ID, biz.TempRetention+1)
	files.Backdate(bound.ID, 10*biz.TempRetention) // 已绑定的文件再老也不清理
	files.Backdate(releasedOld.ID, biz.DeletedRetention+1)

	n, err := uc.Purge(ctx)
	if err != nil || n != 2 {
		t.Fatalf("Purge() = %d, %v; want 2", n, err)
	}
	if objects.Len() != 3 {
		t.Fatalf("objects after purge = %d, want 3", objects.Len())
	}
	if _, err := uc.Get(ctx, alice, freshTemp.ID); err != nil {
		t.Fatalf("fresh temp file must survive: %v", err)
	}
	if _, err := uc.Get(ctx, alice, oldTemp.ID); !errors.Is(err, biz.ErrFileNotFound) {
		t.Fatalf("expired temp file must be purged: %v", err)
	}
	if n, err := uc.Purge(ctx); err != nil || n != 0 {
		t.Fatalf("second Purge() = %d, %v; want 0", n, err)
	}
}

// 上传配额：未绑定的临时文件达到上限后拒绝，绑定后腾出额度。
func TestUploadQuota(t *testing.T) {
	t.Parallel()
	uc, _, objects := newUsecase(t, nil)
	ctx := context.Background()
	var last *biz.File
	for i := range biz.MaxTempFiles {
		f, err := uc.Upload(ctx, alice, "a.png", pngData)
		if err != nil {
			t.Fatalf("Upload #%d error = %v", i, err)
		}
		last = f
	}
	if _, err := uc.Upload(ctx, alice, "a.png", pngData); !errors.Is(err, biz.ErrFileQuotaExceeded) {
		t.Fatalf("Upload(over quota) error = %v, want ErrFileQuotaExceeded", err)
	}
	if objects.Len() != biz.MaxTempFiles {
		t.Fatalf("rejected upload must not store an object: %d", objects.Len())
	}
	// 配额按人计；他人不受影响。
	if _, err := uc.Upload(ctx, bob, "a.png", pngData); err != nil {
		t.Fatalf("Upload(other user) error = %v", err)
	}
	if err := uc.Bind(ctx, testfake.Tx{}, alice, "ticket", "T-1", []string{last.ID}); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if _, err := uc.Upload(ctx, alice, "a.png", pngData); err != nil {
		t.Fatalf("Upload(after bind) error = %v", err)
	}
}

// 「每个归属只有一份」的文件（系统 logo、用户头像）：写入即替换，旧的作废；内容类型由调用方
// 给定（不走附件的类型白名单）；仅在没有时写入的模式不会盖掉已有的文件。
func TestSingleton(t *testing.T) {
	t.Parallel()
	uc, files, objects := newUsecase(t, nil)
	ctx := context.Background()

	if _, _, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || found {
		t.Fatalf("ReadSingleton(empty) = found %v, %v", found, err)
	}
	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg"/>`)
	if done, err := uc.PutSingleton(ctx, alice, "system_logo", "logo", "logo.svg", "image/svg+xml", svg, false); err != nil || !done {
		t.Fatalf("PutSingleton() = %v, %v", done, err)
	}
	if data, mime, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || !found || mime != "image/svg+xml" || !bytes.Equal(data, svg) {
		t.Fatalf("ReadSingleton() = %q, %q, %v, %v", data, mime, found, err)
	}
	// 归属之间互不影响。
	if done, err := uc.PutSingleton(ctx, bob, "user_avatar", bob, "avatar.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(avatar) = %v, %v", done, err)
	}
	if _, _, found, _ := uc.ReadSingleton(ctx, "user_avatar", alice); found {
		t.Fatal("another owner's file must not be visible")
	}
	// 仅在没有时写入：已有文件时不动它，也不留下对象。
	before := objects.Len()
	if done, err := uc.PutSingleton(ctx, alice, "system_logo", "logo", "logo.png", "image/png", pngData, true); err != nil || done {
		t.Fatalf("PutSingleton(ifAbsent, exists) = %v, %v; want false", done, err)
	}
	if data, _, _, _ := uc.ReadSingleton(ctx, "system_logo", "logo"); !bytes.Equal(data, svg) || objects.Len() != before {
		t.Fatalf("ifAbsent must leave the existing file alone (objects %d → %d)", before, objects.Len())
	}
	if done, err := uc.PutSingleton(ctx, alice, "user_avatar", alice, "avatar.png", "image/png", pngData, true); err != nil || !done {
		t.Fatalf("PutSingleton(ifAbsent, none) = %v, %v; want true", done, err)
	}
	// 替换：读到新的；旧的作废，过了保留期连同对象清理。
	if done, err := uc.PutSingleton(ctx, alice, "system_logo", "logo", "logo.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(replace) = %v, %v", done, err)
	}
	if data, mime, _, _ := uc.ReadSingleton(ctx, "system_logo", "logo"); mime != "image/png" || !bytes.Equal(data, pngData) {
		t.Fatalf("ReadSingleton(after replace) = %q, %q", data, mime)
	}
	if objects.Len() != 4 {
		t.Fatalf("objects = %d, want 4 (old logo awaits purge)", objects.Len())
	}
	files.BackdateDeleted(biz.DeletedRetention + 1)
	if n, err := uc.Purge(ctx); err != nil || n != 1 || objects.Len() != 3 {
		t.Fatalf("Purge() = %d, %v; objects = %d; want the replaced logo purged", n, err, objects.Len())
	}
	if data, _, found, _ := uc.ReadSingleton(ctx, "system_logo", "logo"); !found || !bytes.Equal(data, pngData) {
		t.Fatal("current logo must survive the purge")
	}
	// 非法入参不落任何东西。
	for name, call := range map[string]func() (bool, error){
		"empty": func() (bool, error) {
			return uc.PutSingleton(ctx, alice, "system_logo", "logo", "l.png", "image/png", nil, false)
		},
		"too large": func() (bool, error) {
			return uc.PutSingleton(ctx, alice, "system_logo", "logo", "l.png", "image/png", bytes.Repeat([]byte("a"), biz.MaxFileSize+1), false)
		},
		"no owner": func() (bool, error) {
			return uc.PutSingleton(ctx, alice, "system_logo", "", "l.png", "image/png", pngData, false)
		},
		"no mime": func() (bool, error) {
			return uc.PutSingleton(ctx, alice, "system_logo", "logo", "l.png", "", pngData, false)
		},
		"path in name": func() (bool, error) {
			return uc.PutSingleton(ctx, alice, "system_logo", "logo", "../l.png", "image/png", pngData, false)
		},
	} {
		if done, err := call(); err == nil || done {
			t.Errorf("%s: = %v, %v; want an error", name, done, err)
		}
	}
	if objects.Len() != 3 {
		t.Fatalf("rejected writes must not leave objects: %d", objects.Len())
	}
}

// 对象写入失败：原有的文件照旧可读，不留下元数据行。
func TestSingleton_StoreFailure(t *testing.T) {
	t.Parallel()
	uc, files, objects := newUsecase(t, nil)
	ctx := context.Background()
	if _, err := uc.PutSingleton(ctx, alice, "system_logo", "logo", "logo.png", "image/png", pngData, false); err != nil {
		t.Fatalf("PutSingleton() error = %v", err)
	}
	objects.FailPuts(true)
	if done, err := uc.PutSingleton(ctx, alice, "system_logo", "logo", "logo.svg", "image/svg+xml", []byte("<svg/>"), false); err == nil || done {
		t.Fatalf("PutSingleton(store down) = %v, %v; want an error", done, err)
	}
	objects.FailPuts(false)
	if data, mime, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || !found || mime != "image/png" || !bytes.Equal(data, pngData) {
		t.Fatalf("ReadSingleton(after failed replace) = %q, %v, %v", mime, found, err)
	}
	if files.Len() != 1 || objects.Len() != 1 {
		t.Fatalf("failed replace must leave nothing behind: rows %d, objects %d", files.Len(), objects.Len())
	}
	// 对象丢失：按没有处理（调用方据此退回旧位置 / 返回未找到）。
	objects.Clear()
	if _, _, found, err := uc.ReadSingleton(ctx, "system_logo", "logo"); err != nil || found {
		t.Fatalf("ReadSingleton(object lost) = found %v, %v; want not found", found, err)
	}
}
