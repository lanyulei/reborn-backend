package biz

import (
	"context"
	"errors"
	"fmt"

	v1 "github.com/lanyulei/reborn/backend/core/api/system/v1"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/imagesniff"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// SingletonFiles 「每个归属只有一份」的文件的存取端口（由 file 域实现，内容落在对象存储
// 驱动里）：系统 logo、用户头像经它保存与读取，随 upload.driver 走本地磁盘或 S3。
type SingletonFiles interface {
	// PutSingleton 保存 bizType / bizID 的那一份文件并作废它原有的文件；ifAbsent 为真时仅在
	// 还没有文件时写入。返回是否写入。
	PutSingleton(ctx context.Context, uploader, bizType, bizID, filename, contentType string, data []byte, ifAbsent bool) (bool, error)
	// ReadSingleton 读取 bizType / bizID 的那一份文件；没有时 found 为 false。
	ReadSingleton(ctx context.Context, bizType, bizID string) (data []byte, contentType string, found bool, err error)
}

// 文件域里的归属类型（file_object.biz_type）。
const (
	// FileBizAvatar 用户头像：biz_id 为用户 ID。
	FileBizAvatar = "user_avatar"
	// FileBizLogo 系统 logo：biz_id 恒为 logoBizID。
	FileBizLogo = "system_logo"
	logoBizID   = "logo"
)

// systemUploader 导入旧 logo 文件时记的上传者（全零 UUID）：旧文件没有留下是谁传的。
const systemUploader = "00000000-0000-0000-0000-000000000000"

// AvatarStore 头像的旧存放位置（上传目录下的 avatars/<用户 ID><扩展名>，改用对象存储之前
// 上传的头像）：只读，首次读到时导入对象存储。
type AvatarStore interface {
	// ReadAvatarFile 读取旧位置上的用户头像；没有返回 ErrAvatarNotFound。
	ReadAvatarFile(ctx context.Context, userID string) (data []byte, mime string, err error)
}

// ErrAvatarNotFound 用户未设置头像（未配置语义，service 层映射 404）。
var ErrAvatarNotFound = errors.New("avatar not found")

// ErrImageTypeInvalid 上传的 logo / 头像不是允许的图片格式。
var ErrImageTypeInvalid = kerrors.BadRequest(v1.ErrorReason_IMAGE_TYPE_INVALID.String(), "the file is not an allowed image format")

// imageType 按内容判定上传图片的类型并核对白名单（类型 → 扩展名）：客户端声明的类型只是
// 提示，不作数——声明成 PNG 的 HTML 不会被当成图片存下来，声明错了的真图片按实际类型保存。
func imageType(data []byte, allowed map[string]string) (mime, ext string, err error) {
	mime = imagesniff.Detect(data)
	ext, ok := allowed[mime]
	if !ok {
		return "", "", ErrImageTypeInvalid
	}
	return mime, ext, nil
}

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

// AvatarUsecase 用户头像：自助上传（存入对象存储 + 回写 avatar 字段）与读取。
type AvatarUsecase struct {
	reader UserReader
	writer UserWriter
	files  SingletonFiles
	legacy AvatarStore
}

// NewAvatarUsecase new an AvatarUsecase.
func NewAvatarUsecase(reader UserReader, writer UserWriter, files SingletonFiles, legacy AvatarStore) *AvatarUsecase {
	return &AvatarUsecase{reader: reader, writer: writer, files: files, legacy: legacy}
}

// Upload 保存头像并把带指纹的访问地址写入用户 avatar 字段，返回该地址。类型按内容判定。
func (uc *AvatarUsecase) Upload(ctx context.Context, userID string, data []byte) (string, error) {
	mime, ext, err := imageType(data, AvatarMimeExts)
	if err != nil {
		return "", err
	}
	u, err := uc.reader.GetUser(ctx, userID)
	if err != nil {
		return "", err
	}
	if u.IsMachine() {
		return "", ErrMachineAccount
	}
	if _, err := uc.files.PutSingleton(ctx, userID, FileBizAvatar, userID, "avatar"+ext, mime, data, false); err != nil {
		return "", err
	}
	url := AvatarURL(userID, LogoVersion(data))
	if err := uc.writer.UpdateUser(ctx, userID, UserUpdate{Avatar: &url}); err != nil {
		return "", err
	}
	return url, nil
}

// Get 读取用户头像二进制与 MIME；未设置返回 ErrAvatarNotFound。对象存储里没有时退回旧位置，
// 读到即顺手导入（仅在仍然没有时写入，不会盖掉并发的新上传；导入失败不影响这次读取）。
func (uc *AvatarUsecase) Get(ctx context.Context, userID string) ([]byte, string, error) {
	data, mime, found, err := uc.files.ReadSingleton(ctx, FileBizAvatar, userID)
	if err != nil || found {
		return data, mime, err
	}
	data, mime, err = uc.legacy.ReadAvatarFile(ctx, userID)
	if err != nil {
		return nil, "", err
	}
	if ext, ok := AvatarMimeExts[mime]; ok {
		_, _ = uc.files.PutSingleton(context.WithoutCancel(ctx), userID, FileBizAvatar, userID, "avatar"+ext, mime, data, true)
	}
	return data, mime, nil
}
