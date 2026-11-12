package biz

import (
	"context"
	"errors"
	"fmt"
)

// AvatarStore 头像文件仓储（data 层本地磁盘实现；按用户 ID 一人一文件）。
type AvatarStore interface {
	// SaveAvatarFile 覆盖式写入用户头像文件（原子替换，清理其它扩展名的旧文件）。
	SaveAvatarFile(ctx context.Context, userID string, data []byte, ext string) error
	// ReadAvatarFile 读取用户头像；未上传过返回 ErrAvatarNotFound。
	ReadAvatarFile(ctx context.Context, userID string) (data []byte, mime string, err error)
}

// ErrAvatarNotFound 用户未设置头像（未配置语义，service 层映射 404）。
var ErrAvatarNotFound = errors.New("avatar not found")

// AvatarMimeExts 是允许的头像 MIME 类型到扩展名的映射（与 proto 校验白名单一致；
// 不含 SVG：用户上传内容不允许可执行脚本的格式）。
var AvatarMimeExts = map[string]string{
	"image/png":  ".png",
	"image/jpeg": ".jpg",
	"image/webp": ".webp",
}

// AvatarURL 用户头像的服务路径（同 GET 接口；?v= 为内容指纹，内容变化即变化）。
func AvatarURL(userID, version string) string {
	return fmt.Sprintf("/api/v1/user/%s/avatar?v=%s", userID, version)
}

// AvatarUsecase 用户头像：自助上传（落盘 + 回写 avatar 字段）与读取。
type AvatarUsecase struct {
	reader UserReader
	writer UserWriter
	store  AvatarStore
}

// NewAvatarUsecase new an AvatarUsecase.
func NewAvatarUsecase(reader UserReader, writer UserWriter, store AvatarStore) *AvatarUsecase {
	return &AvatarUsecase{reader: reader, writer: writer, store: store}
}

// Upload 保存头像并把带指纹的访问地址写入用户 avatar 字段，返回该地址。
func (uc *AvatarUsecase) Upload(ctx context.Context, userID string, data []byte, mime string) (string, error) {
	ext, ok := AvatarMimeExts[mime]
	if !ok {
		return "", fmt.Errorf("unsupported avatar mime: %s", mime)
	}
	if _, err := uc.reader.GetUser(ctx, userID); err != nil {
		return "", err
	}
	if err := uc.store.SaveAvatarFile(ctx, userID, data, ext); err != nil {
		return "", err
	}
	url := AvatarURL(userID, LogoVersion(data))
	if err := uc.writer.UpdateUser(ctx, userID, UserUpdate{Avatar: &url}); err != nil {
		return "", err
	}
	return url, nil
}

// Get 读取用户头像二进制与 MIME；未设置返回 ErrAvatarNotFound。
func (uc *AvatarUsecase) Get(ctx context.Context, userID string) ([]byte, string, error) {
	return uc.store.ReadAvatarFile(ctx, userID)
}
