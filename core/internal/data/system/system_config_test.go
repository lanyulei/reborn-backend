package data

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// 旧存放位置上的 logo 与头像（改用对象存储之前上传的）：只读，按扩展名探查；没有即未找到，
// 头像的用户 ID 不是 UUID 时不碰磁盘。
func TestLegacyLogoAndAvatarFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	d := &basedata.Data{LogoDir: dir}
	ctx := context.Background()
	repo, avatars := NewSystemConfigRepo(d), NewAvatarStore(d)
	const uid = "11111111-2222-7333-8444-555555555555"

	if _, _, err := repo.ReadLogoFile(ctx); !errors.Is(err, biz.ErrLogoNotFound) {
		t.Fatalf("ReadLogoFile(empty dir) error = %v, want ErrLogoNotFound", err)
	}
	if _, _, err := avatars.ReadAvatarFile(ctx, uid); !errors.Is(err, biz.ErrAvatarNotFound) {
		t.Fatalf("ReadAvatarFile(empty dir) error = %v, want ErrAvatarNotFound", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "avatars"), 0o750); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"logo.svg": "<svg/>", "avatars/" + uid + ".webp": "webp"} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if data, mime, err := repo.ReadLogoFile(ctx); err != nil || string(data) != "<svg/>" || mime != "image/svg+xml" {
		t.Fatalf("ReadLogoFile() = %q, %q, %v", data, mime, err)
	}
	if data, mime, err := avatars.ReadAvatarFile(ctx, uid); err != nil || string(data) != "webp" || mime != "image/webp" {
		t.Fatalf("ReadAvatarFile() = %q, %q, %v", data, mime, err)
	}
	if _, _, err := avatars.ReadAvatarFile(ctx, "../logo"); !errors.Is(err, biz.ErrAvatarNotFound) {
		t.Fatalf("ReadAvatarFile(non-uuid) error = %v, want ErrAvatarNotFound", err)
	}
}
