package biz_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 单例文件（头像 / logo）同样占用未绑定临时文件配额：配额已满时换头像被拒绝。对外仍是 FILE_QUOTA_EXCEEDED
// （429），但文案说的是「保存不了这个文件」，而不是让用户去「提交上传」；不写对象；配额按人计，腾出额度后恢复。
func TestPutSingletonCountsTowardUploadQuota(t *testing.T) {
	t.Parallel()
	uc, _, objects := newUsecase(t, nil)
	ctx := context.Background()

	if done, err := uc.PutSingleton(ctx, alice, "user_avatar", alice, "avatar.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(avatar, under quota) = %v, %v", done, err)
	}
	if done, err := uc.PutSingleton(ctx, bob, "system_logo", "logo", "logo.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(logo, under quota) = %v, %v", done, err)
	}

	var last *biz.File
	for i := range biz.MaxTempFiles {
		f, err := uc.Upload(ctx, alice, "a.png", pngData)
		if err != nil {
			t.Fatalf("Upload #%d error = %v", i, err)
		}
		last = f
	}
	done, err := uc.PutSingleton(ctx, alice, "user_avatar", alice, "avatar.png", "image/png", pngData, false)
	if done || !errors.Is(err, biz.ErrFileQuotaExceeded) {
		t.Fatalf("PutSingleton(avatar, over quota) = %v, %v; want false and ErrFileQuotaExceeded", done, err)
	}
	var ke *kerrors.Error
	if !errors.As(err, &ke) || ke.Code != 429 || ke.Reason != "FILE_QUOTA_EXCEEDED" {
		t.Fatalf("over-quota error = %+v, want 429 FILE_QUOTA_EXCEEDED", err)
	}
	if ke.Message == biz.ErrFileQuotaExceeded.Message ||
		!strings.Contains(ke.Message, "cannot save this file") || !strings.Contains(ke.Message, "unused upload limit") {
		t.Fatalf("over-quota message = %q, want the one written for saving avatar / logo", ke.Message)
	}
	// 普通上传超配额的文案不变。
	if _, err := uc.Upload(ctx, alice, "a.png", pngData); !errors.Is(err, biz.ErrFileQuotaExceeded) || err.Error() != biz.ErrFileQuotaExceeded.Error() {
		t.Fatalf("Upload(over quota) error = %v, want the plain ErrFileQuotaExceeded", err)
	}
	if n := objects.Len(); n != 2+biz.MaxTempFiles {
		t.Fatalf("objects = %d, want %d (rejected write must not store an object)", n, 2+biz.MaxTempFiles)
	}

	// 配额按人计：他人不受影响。
	if done, err := uc.PutSingleton(ctx, bob, "user_avatar", bob, "avatar.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(avatar, other user) = %v, %v, want true and no error", done, err)
	}
	// 腾出额度后恢复。
	if err := uc.Bind(ctx, testfake.Tx{}, alice, "ticket", "T-1", []string{last.ID}); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}
	if done, err := uc.PutSingleton(ctx, alice, "user_avatar", alice, "avatar.png", "image/png", pngData, false); err != nil || !done {
		t.Fatalf("PutSingleton(avatar, after freeing quota) = %v, %v", done, err)
	}
}
