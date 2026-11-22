package biz

import (
	"context"
	"errors"
)

// FileAccess 文档附件（正文内图片、导入源文件）的读取判定：对所属空间有读权限即可读。
// 由 file 域经 AccessCheckers 调用（biz_type = knowledge，biz_id = 文档 ID）。
type FileAccess struct {
	docs   DocumentRepo
	spaces *SpaceUsecase
}

// NewFileAccess new a FileAccess.
func NewFileAccess(docs DocumentRepo, spaces *SpaceUsecase) *FileAccess {
	return &FileAccess{docs: docs, spaces: spaces}
}

// CanReadFile implements file 域 AccessChecker。
func (a *FileAccess) CanReadFile(ctx context.Context, userID, bizID, _ string) (bool, error) {
	doc, err := a.docs.Get(ctx, bizID)
	if errors.Is(err, ErrDocumentNotFound) {
		return false, nil // 文档不存在 / 已删除 = 不可读，不是系统错误。
	}
	if err != nil {
		return false, err
	}
	acc, err := a.spaces.Resolve(ctx, userID)
	if err != nil {
		return false, err
	}
	return acc.Can(doc.SpaceID, LevelRead), nil
}

// DescribeContext 助手页内上下文提供者：目前只支持 knowledge_document。
func (uc *DocumentUsecase) DescribeContext(ctx context.Context, userID, typ, id string) (module, text string, err error) {
	if typ != "knowledge_document" {
		return "", "", ErrInvalidArgument
	}
	return uc.Describe(ctx, userID, id)
}
