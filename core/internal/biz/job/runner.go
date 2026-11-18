package biz

import (
	"context"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// RunnerUsecase 执行节点用例。接入令牌明文只在创建 / 重置时返回一次，库里只存摘要。
type RunnerUsecase struct {
	reader RunnerReader
	writer RunnerWriter
	tokens RunnerTokens
}

// NewRunnerUsecase new a RunnerUsecase.
func NewRunnerUsecase(reader RunnerReader, writer RunnerWriter, tokens RunnerTokens) *RunnerUsecase {
	return &RunnerUsecase{reader: reader, writer: writer, tokens: tokens}
}

// List 分页查询执行节点。
func (uc *RunnerUsecase) List(ctx context.Context, keyword string, page, pageSize int64) ([]*Runner, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.reader.List(ctx, strings.TrimSpace(keyword), page, pageSize)
}

// Zones 已登记的区域名。
func (uc *RunnerUsecase) Zones(ctx context.Context) ([]string, error) {
	return uc.reader.Zones(ctx)
}

func (in *RunnerInput) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	in.Zone = strings.TrimSpace(in.Zone)
	in.Description = strings.TrimSpace(in.Description)
	switch {
	case in.Name == "":
		return detail(ErrRunnerInvalid, "name must not be blank")
	case !jc.ValidZone(in.Zone):
		return detail(ErrRunnerInvalid, "invalid zone name")
	}
	return nil
}

// Create 登记执行节点，返回节点与接入令牌明文。
func (uc *RunnerUsecase) Create(ctx context.Context, userID string, in RunnerInput) (*Runner, string, error) {
	if err := in.normalize(); err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	token, hash, err := jc.NewRunnerToken(id)
	if err != nil {
		return nil, "", err
	}
	r := &Runner{ID: id, Name: in.Name, Zone: in.Zone, Enabled: in.Enabled, Description: in.Description}
	if err := uc.writer.Create(ctx, r, hash, userID); err != nil {
		return nil, "", err
	}
	created, err := uc.reader.Get(ctx, id)
	return created, token, err
}

// Update 编辑执行节点（整体替换）。
func (uc *RunnerUsecase) Update(ctx context.Context, userID, id string, in RunnerInput, version int32) (*Runner, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	r := &Runner{ID: id, Name: in.Name, Zone: in.Zone, Enabled: in.Enabled, Description: in.Description}
	if err := uc.writer.Update(ctx, r, version, userID); err != nil {
		return nil, err
	}
	return uc.reader.Get(ctx, id)
}

// ResetToken 重置接入令牌：旧令牌立即失效，返回新令牌明文。
func (uc *RunnerUsecase) ResetToken(ctx context.Context, userID, id string) (string, error) {
	token, hash, err := jc.NewRunnerToken(id)
	if err != nil {
		return "", err
	}
	if err := uc.tokens.SetToken(ctx, id, hash, userID); err != nil {
		return "", err
	}
	return token, nil
}

// Delete 删除执行节点：令牌随即失效。
func (uc *RunnerUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.writer.Delete(ctx, id, userID)
}
