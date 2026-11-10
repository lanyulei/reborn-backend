package data

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// TestSaveLogoFile_AtomicReplace logo 覆盖写：目标文件原子替换、其他扩展名
// 旧文件清理、无临时文件残留、权限 0600；读取始终拿到完整的当前文件。
func TestSaveLogoFile_AtomicReplace(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "nested", "uploads") // 目录不存在时递归创建
	repo := NewSystemConfigRepo(&basedata.Data{LogoDir: dir})
	ctx := context.Background()

	if _, err := repo.SaveLogoFile(ctx, []byte("png-bytes"), ".png"); err != nil {
		t.Fatalf("SaveLogoFile(png) error = %v", err)
	}
	if _, err := repo.SaveLogoFile(ctx, []byte("<svg/>"), ".svg"); err != nil {
		t.Fatalf("SaveLogoFile(svg) error = %v", err)
	}
	data, mime, err := repo.ReadLogoFile(ctx)
	if err != nil || string(data) != "<svg/>" || mime != "image/svg+xml" {
		t.Fatalf("ReadLogoFile() = %q, %q, %v; want svg", data, mime, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "logo.svg" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("dir entries = %v, want only logo.svg (old png removed, no temp left)", names)
	}
	info, err := os.Stat(filepath.Join(dir, "logo.svg"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("logo.svg mode = %v (%v), want 0600", info.Mode().Perm(), err)
	}
}
