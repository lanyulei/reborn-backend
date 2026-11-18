package data

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/system"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

var _ biz.AvatarStore = (*avatarStore)(nil)

// avatarStore 头像的旧存放位置（只读）：<上传目录>/avatars/<userID><ext>，改用对象存储之前
// 上传的头像，首次读到时由用例导入对象存储。userID 经 UUID 白名单校验后才参与路径拼接
// （防路径穿越）。
type avatarStore struct {
	data *basedata.Data
}

// NewAvatarStore new an avatarStore.
func NewAvatarStore(data *basedata.Data) *avatarStore { return &avatarStore{data: data} }

var avatarFileExts = []string{".png", ".jpg", ".webp"}

func (s *avatarStore) dir() string { return filepath.Join(s.data.LogoDir, "avatars") }

// ReadAvatarFile implements biz.AvatarStore.
func (s *avatarStore) ReadAvatarFile(_ context.Context, userID string) ([]byte, string, error) {
	if !idgen.IsUUID(userID) {
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
