// Package service 文件域 proto 接口实现：参数编排、模型转换与响应头设置。
package service

import (
	"context"
	"mime"

	v1 "github.com/lanyulei/reborn/backend/core/api/file/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"

	"github.com/go-kratos/kratos/v3/transport"
	"github.com/google/wire"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is file 域 service providers.
var ProviderSet = wire.NewSet(NewFileService)

// FileService 实现 api/file/v1 生成的 FileServiceServer。
type FileService struct {
	uc *biz.Usecase
}

// NewFileService new a FileService.
func NewFileService(uc *biz.Usecase) *FileService {
	return &FileService{uc: uc}
}

// UploadFile implements proto FileService/UploadFile：上传者取自登录身份。
func (s *FileService) UploadFile(ctx context.Context, req *v1.UploadFileRequest) (*v1.UploadFileResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	f, err := s.uc.Upload(ctx, userID, req.GetFilename(), req.GetData())
	if err != nil {
		return nil, err
	}
	return &v1.UploadFileResponse{File: toProtoFile(f)}, nil
}

// GetFile implements proto FileService/GetFile.
func (s *FileService) GetFile(ctx context.Context, req *v1.GetFileRequest) (*v1.GetFileResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	f, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetFileResponse{File: toProtoFile(f)}, nil
}

// DownloadFile implements proto FileService/DownloadFile：恒以附件方式返回
// （Content-Disposition: attachment + nosniff + CSP sandbox），浏览器不会内联渲染
// 用户上传的内容；响应不进共享缓存（内容受访问控制）。
func (s *FileService) DownloadFile(ctx context.Context, req *v1.DownloadFileRequest) (*httpbody.HttpBody, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	f, data, err := s.uc.Download(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	if tr, ok := transport.FromServerContext(ctx); ok {
		h := tr.ReplyHeader()
		// mime.FormatMediaType 按 RFC 2231 编码非 ASCII 文件名并转义引号，杜绝响应头注入。
		h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": f.Filename}))
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", "sandbox")
		h.Set("Cache-Control", "private, no-store")
	}
	return &httpbody.HttpBody{ContentType: f.Mime, Data: data}, nil
}

// toProtoFile 将领域模型显式转换为 proto 实体（不暴露驱动、对象键与业务归属）。
func toProtoFile(f *biz.File) *v1.File {
	return &v1.File{
		Id:         f.ID,
		Filename:   f.Filename,
		Size:       f.Size,
		Mime:       f.Mime,
		CreateUser: f.CreateUser,
		CreateTime: timestamppb.New(f.CreateTime),
	}
}
