package data

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

var _ biz.AvatarStore = (*avatarStore)(nil)

// avatarStore 头像文件本地磁盘仓储：<上传目录>/avatars/<userID><ext>，与 logo 同一
// 原子落盘策略。userID 经 UUID 白名单校验后才参与路径拼接（防路径穿越）。
type avatarStore struct {
	data *basedata.Data
}

// NewAvatarStore new an avatarStore.
func NewAvatarStore(data *basedata.Data) *avatarStore { return &avatarStore{data: data} }

var (
	avatarIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	avatarFileExts  = []string{".png", ".jpg", ".webp"}
)

func (s *avatarStore) dir() string { return filepath.Join(s.data.LogoDir, "avatars") }

// SaveAvatarFile implements biz.AvatarStore.
func (s *avatarStore) SaveAvatarFile(_ context.Context, userID string, data []byte, ext string) error {
	if !avatarIDPattern.MatchString(userID) {
		return fmt.Errorf("invalid user id for avatar: %q", userID)
	}
	dir := s.dir()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create avatar dir: %w", err)
	}
	if err := writeFileAtomic(dir, filepath.Join(dir, userID+ext), data); err != nil {
		return fmt.Errorf("write avatar file: %w", err)
	}
	for _, e := range avatarFileExts {
		if e == ext {
			continue
		}
		if err := os.Remove(filepath.Join(dir, userID+e)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove old avatar: %w", err)
		}
	}
	return nil
}

// ReadAvatarFile implements biz.AvatarStore.
func (s *avatarStore) ReadAvatarFile(_ context.Context, userID string) ([]byte, string, error) {
	if !avatarIDPattern.MatchString(userID) {
		return nil, "", biz.ErrAvatarNotFound
	}
	for _, ext := range avatarFileExts {
		// #nosec G304 -- 路径由受控目录、UUID 白名单校验的 ID 与固定扩展名拼接
		data, err := os.ReadFile(filepath.Join(s.dir(), userID+ext))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("read avatar file: %w", err)
		}
		return data, extMime[ext], nil
	}
	return nil, "", biz.ErrAvatarNotFound
}
