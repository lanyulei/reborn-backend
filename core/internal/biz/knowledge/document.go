package biz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/secretscan"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 通知事件类型（notify 管道按 event_type 路由）。
const (
	// EventReviewPending 文档提交审核，通知空间负责人。
	EventReviewPending = "knowledge.review_pending"
	// EventReviewDue 文档复审到期（前 7 天与到期后），通知文档负责人。
	EventReviewDue = "knowledge.review_due"
	// NotifyBizID 通知事件的业务标识。
	NotifyBizID = "knowledge"
)

// DocumentUsecase 知识文档用例：草稿 / 发布 / 审核 / 版本 / 归档 / 关联 / 概览。
type DocumentUsecase struct {
	spaces   *SpaceUsecase
	search   *SearchUsecase
	docs     DocumentRepo
	versions VersionRepo
	chunks   ChunkRepo
	links    LinkRepo
	index    IndexRepo
	files    Files
	txm      txrun.Manager
	notify   biznotify.NotificationWriter
	dir      Directory
	now      func() time.Time
}

// NewDocumentUsecase new a DocumentUsecase.
func NewDocumentUsecase(spaces *SpaceUsecase, search *SearchUsecase, docs DocumentRepo, versions VersionRepo, chunks ChunkRepo,
	links LinkRepo, index IndexRepo, files Files, txm txrun.Manager, notify biznotify.NotificationWriter, dir Directory,
) *DocumentUsecase {
	return &DocumentUsecase{
		spaces: spaces, search: search, docs: docs, versions: versions, chunks: chunks, links: links, index: index,
		files: files, txm: txm, notify: notify, dir: dir, now: time.Now,
	}
}

// CreateDocumentInput 新建文档入参。
type CreateDocumentInput struct {
	SpaceID   string
	Title     string
	Slug      string
	Content   string
	Tags      []string
	Summary   string
	OwnerID   string
	Source    string
	SourceRef string
	Publish   bool
}

// Create 新建文档（需空间写权限）；Publish 为 true 时随即发布（空间要求审核时转为待审核）。
func (uc *DocumentUsecase) Create(ctx context.Context, userID string, in CreateDocumentInput) (*Document, error) {
	space, acc, err := uc.spaces.Require(ctx, userID, in.SpaceID, LevelWrite)
	if err != nil {
		return nil, err
	}
	doc, err := uc.create(ctx, userID, space, in, "")
	if err != nil {
		return nil, err
	}
	if in.Publish {
		if _, err := uc.publishOrSubmit(ctx, doc, space, userID, acc.Level(space.ID), ""); err != nil {
			return nil, err
		}
	}
	return uc.decorateOne(ctx, doc, acc)
}

// create 落草稿（不做权限判定）；fileID 非空时在同一事务里把导入源文件绑定到文档。
func (uc *DocumentUsecase) create(ctx context.Context, actor string, space *Space, in CreateDocumentInput, fileID string) (*Document, error) {
	doc, auto, err := uc.newDocument(actor, space, in)
	if err != nil {
		return nil, err
	}
	var bind func(ctx context.Context, tx txrun.Tx) error
	if fileID != "" {
		bind = func(ctx context.Context, tx txrun.Tx) error {
			return uc.files.Bind(ctx, tx, actor, BusinessType, doc.ID, []string{fileID})
		}
	}
	if err := uc.insertDocument(ctx, doc, auto, bind); err != nil {
		return nil, err
	}
	return doc, nil
}

// newDocument 组装草稿（校验正文、标签、标题，生成 ID 与 slug）；返回 slug 是否自动生成。
func (uc *DocumentUsecase) newDocument(actor string, space *Space, in CreateDocumentInput) (*Document, bool, error) {
	if err := checkContent(in.Content); err != nil {
		return nil, false, err
	}
	tags, err := normalizeTags(in.Tags)
	if err != nil {
		return nil, false, err
	}
	title := strings.TrimSpace(in.Title)
	if title == "" {
		return nil, false, ErrInvalidArgument
	}
	owner := in.OwnerID
	if owner == "" {
		owner = actor
	}
	source := in.Source
	if source == "" {
		source = SourceManual
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, false, err
	}
	now := uc.now()
	doc := &Document{
		ID: id, SpaceID: space.ID, Title: title, Status: DocStatusDraft, Content: in.Content, Summary: in.Summary, Tags: tags,
		Source: source, SourceRef: in.SourceRef, OwnerID: owner, CreateUser: actor, UpdateUser: actor, CreateTime: now, UpdateTime: now,
	}
	auto := in.Slug == ""
	doc.Slug = in.Slug
	if auto {
		doc.Slug = slugify(title, id)
	}
	return doc, auto, nil
}

// insertDocument 落库：then 非 nil 时与插入同一事务执行（绑定附件、随即发布）；自动 slug 冲突时追加后缀整体重试一次。
func (uc *DocumentUsecase) insertDocument(ctx context.Context, doc *Document, autoSlug bool, then func(ctx context.Context, tx txrun.Tx) error) error {
	insert := func(ctx context.Context) error {
		if then == nil {
			return uc.docs.Create(ctx, doc)
		}
		return uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
			if err := uc.docs.Create(txrun.NewContext(ctx, tx), doc); err != nil {
				return err
			}
			return then(ctx, tx)
		})
	}
	err := insert(ctx)
	if errors.Is(err, ErrSlugExists) && autoSlug {
		doc.Slug = doc.Slug + "-" + strings.ReplaceAll(doc.ID, "-", "")[:6]
		err = insert(ctx)
	}
	return err
}

// DocumentPatch 保存草稿入参（nil = 不改）。
type DocumentPatch struct {
	Title         *string
	Content       *string
	Tags          *[]string
	Summary       *string
	OwnerID       *string
	ReviewAt      *time.Time
	ClearReviewAt bool
}

// Update 保存草稿：已发布文档的标题 / 正文改动只落草稿字段，其它元数据即时生效；归档文档不可改。
func (uc *DocumentUsecase) Update(ctx context.Context, userID, id string, p DocumentPatch) (*Document, error) {
	doc, _, acc, err := uc.require(ctx, userID, id, LevelWrite)
	if err != nil {
		return nil, err
	}
	if doc.Status == DocStatusArchived {
		return nil, ErrInvalidState
	}
	if p.Content != nil {
		if err := checkContent(*p.Content); err != nil {
			return nil, err
		}
	}
	if p.Title != nil && strings.TrimSpace(*p.Title) == "" {
		return nil, ErrInvalidArgument
	}
	switch doc.Status {
	case DocStatusPublished:
		if p.Title != nil {
			t := strings.TrimSpace(*p.Title)
			if t == doc.Title {
				doc.DraftTitle = nil
			} else {
				doc.DraftTitle = &t
			}
		}
		if p.Content != nil {
			c := *p.Content
			if c == doc.Content {
				doc.DraftContent = nil
			} else {
				doc.DraftContent = &c
			}
		}
	default:
		if p.Title != nil {
			doc.Title = strings.TrimSpace(*p.Title)
		}
		if p.Content != nil {
			doc.Content = *p.Content
		}
	}
	if p.Tags != nil {
		tags, err := normalizeTags(*p.Tags)
		if err != nil {
			return nil, err
		}
		doc.Tags = tags
	}
	if p.Summary != nil {
		doc.Summary = *p.Summary
	}
	if p.OwnerID != nil {
		if err := uc.spaces.checkUsers(ctx, []string{*p.OwnerID}); err != nil {
			return nil, err
		}
		doc.OwnerID = *p.OwnerID
	}
	if p.ClearReviewAt {
		doc.ReviewAt = nil
		doc.ReviewNotifyTime = nil
	} else if p.ReviewAt != nil {
		t := *p.ReviewAt
		doc.ReviewAt = &t
		doc.ReviewNotifyTime = nil
	}
	doc.UpdateUser = userID
	doc.UpdateTime = uc.now()
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		tctx := txrun.NewContext(ctx, tx)
		if err := uc.docs.Update(tctx, doc, doc.Version); err != nil {
			return err
		}
		if p.ClearReviewAt || p.ReviewAt != nil {
			if err := uc.docs.ResetReviewNotified(tctx, doc.ID); err != nil {
				return err
			}
		}
		if p.Tags == nil {
			return nil
		}
		return uc.docs.SetTags(tctx, doc.ID, doc.Tags)
	})
	if err != nil {
		return nil, err
	}
	return uc.decorateOne(ctx, doc, acc)
}

// Publish 发布：空间要求审核且操作者不是管理级别时转为待审核（返回 pending=true）。
func (uc *DocumentUsecase) Publish(ctx context.Context, userID, id, note string) (*Document, bool, error) {
	doc, space, acc, err := uc.require(ctx, userID, id, LevelWrite)
	if err != nil {
		return nil, false, err
	}
	pending, err := uc.publishOrSubmit(ctx, doc, space, userID, acc.Level(space.ID), note)
	if err != nil {
		return nil, false, err
	}
	out, err := uc.decorateOne(ctx, doc, acc)
	return out, pending, err
}

// publishOrSubmit 发布或提交审核。
func (uc *DocumentUsecase) publishOrSubmit(ctx context.Context, doc *Document, space *Space, actor string, level int32, note string) (bool, error) {
	if doc.Status == DocStatusArchived {
		return false, ErrInvalidState
	}
	if doc.Status == DocStatusPublished && !doc.HasDraft() {
		return false, ErrInvalidState
	}
	if space.ReviewRequired && level < LevelManage {
		if doc.ReviewState == ReviewPending {
			return true, nil
		}
		doc.ReviewState = ReviewPending
		doc.ReviewNote = note
		doc.UpdateUser = actor
		doc.UpdateTime = uc.now()
		if err := uc.docs.Update(ctx, doc, doc.Version); err != nil {
			return false, err
		}
		uc.notifyReviewPending(ctx, doc, space, actor)
		return true, nil
	}
	title, content := doc.Title, doc.Content
	if doc.DraftTitle != nil {
		title = *doc.DraftTitle
	}
	if doc.DraftContent != nil {
		content = *doc.DraftContent
	}
	return false, uc.publishContent(ctx, doc, space, actor, title, content, note)
}

// publishContent 以给定标题 / 正文发布新版本：写版本、更新文档、整篇替换分块、排队索引任务，同一事务。
func (uc *DocumentUsecase) publishContent(ctx context.Context, doc *Document, space *Space, actor, title, content, note string) error {
	return uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		return uc.publishContentTx(ctx, tx, doc, space, actor, title, content, note)
	})
}

// publishContentTx 在调用方事务内发布（导入路径与建档 / 绑定附件同一事务）。
func (uc *DocumentUsecase) publishContentTx(ctx context.Context, tx txrun.Tx, doc *Document, space *Space, actor, title, content, note string) error {
	if err := checkContent(content); err != nil {
		return err
	}
	now := uc.now()
	chunks, err := buildChunks(doc.ID, space.ID, doc.Version+1, title, content)
	if err != nil {
		return err
	}
	vid, err := idgen.NewID()
	if err != nil {
		return err
	}
	version := &Version{ID: vid, DocumentID: doc.ID, Version: doc.Version + 1, Title: title, Content: content, Note: note, PublishUser: actor, CreateTime: now}
	tctx := txrun.NewContext(ctx, tx)
	if err := uc.versions.CreateTx(ctx, tx, version); err != nil {
		return err
	}
	prev := doc.Version
	doc.Status, doc.Title, doc.Content = DocStatusPublished, title, content
	doc.DraftTitle, doc.DraftContent = nil, nil
	doc.ReviewState, doc.ReviewNote = ReviewNone, ""
	doc.Version = version.Version
	doc.PublishTime = &now
	doc.IndexStatus = IndexPending
	if len(chunks) == 0 {
		doc.IndexStatus = IndexDone
	}
	reviewChanged := space.ReviewDays > 0
	if reviewChanged {
		at := now.Add(time.Duration(space.ReviewDays) * 24 * time.Hour)
		doc.ReviewAt = &at
		doc.ReviewNotifyTime = nil
	}
	doc.UpdateUser, doc.UpdateTime = actor, now
	if err := uc.docs.Update(tctx, doc, prev); err != nil {
		return err
	}
	if reviewChanged {
		if err := uc.docs.ResetReviewNotified(tctx, doc.ID); err != nil {
			return err
		}
	}
	if err := uc.docs.SetIndexStatus(tctx, doc.ID, doc.IndexStatus); err != nil {
		return err
	}
	if err := uc.chunks.ReplaceTx(ctx, tx, doc.ID, chunks); err != nil {
		return err
	}
	if len(chunks) == 0 {
		return uc.index.DeleteTx(ctx, tx, doc.ID)
	}
	return uc.index.UpsertTx(ctx, tx, doc.ID, doc.Version)
}

// Review 审核待发布文档（需管理级别）：通过即发布，驳回退回。
func (uc *DocumentUsecase) Review(ctx context.Context, userID, id string, approved bool, note string) (*Document, error) {
	doc, space, acc, err := uc.require(ctx, userID, id, LevelManage)
	if err != nil {
		return nil, err
	}
	if doc.ReviewState != ReviewPending {
		return nil, ErrInvalidState
	}
	if approved {
		title, content := doc.Title, doc.Content
		if doc.DraftTitle != nil {
			title = *doc.DraftTitle
		}
		if doc.DraftContent != nil {
			content = *doc.DraftContent
		}
		if err := uc.publishContent(ctx, doc, space, userID, title, content, note); err != nil {
			return nil, err
		}
	} else {
		doc.ReviewState, doc.ReviewNote = ReviewNone, note
		doc.UpdateUser, doc.UpdateTime = userID, uc.now()
		if err := uc.docs.Update(ctx, doc, doc.Version); err != nil {
			return nil, err
		}
	}
	return uc.decorateOne(ctx, doc, acc)
}

// ConfirmReview 确认文档仍然有效：复审时间顺延一个周期（空间无周期则清除）。文档负责人或空间写权限。
func (uc *DocumentUsecase) ConfirmReview(ctx context.Context, userID, id string) (*Document, error) {
	doc, space, acc, err := uc.require(ctx, userID, id, LevelRead)
	if err != nil {
		return nil, err
	}
	if doc.OwnerID != userID && acc.Level(space.ID) < LevelWrite {
		return nil, ErrForbidden
	}
	if doc.Status != DocStatusPublished {
		return nil, ErrInvalidState
	}
	now := uc.now()
	if space.ReviewDays > 0 {
		at := now.Add(time.Duration(space.ReviewDays) * 24 * time.Hour)
		doc.ReviewAt = &at
	} else {
		doc.ReviewAt = nil
	}
	doc.ReviewNotifyTime = nil
	doc.UpdateUser, doc.UpdateTime = userID, now
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		tctx := txrun.NewContext(ctx, tx)
		if err := uc.docs.Update(tctx, doc, doc.Version); err != nil {
			return err
		}
		return uc.docs.ResetReviewNotified(tctx, doc.ID)
	})
	if err != nil {
		return nil, err
	}
	return uc.decorateOne(ctx, doc, acc)
}

// Archive 归档：同事务删索引（文档负责人或空间管理级别）。
func (uc *DocumentUsecase) Archive(ctx context.Context, userID, id string) (*Document, error) {
	doc, _, acc, err := uc.requireOwnerOrManage(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if doc.Status == DocStatusArchived {
		return nil, ErrInvalidState
	}
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		tctx := txrun.NewContext(ctx, tx)
		doc.Status = DocStatusArchived
		doc.ReviewState, doc.ReviewNote = ReviewNone, ""
		doc.IndexStatus = IndexNone
		doc.UpdateUser, doc.UpdateTime = userID, uc.now()
		if err := uc.docs.Update(tctx, doc, doc.Version); err != nil {
			return err
		}
		if err := uc.docs.SetIndexStatus(tctx, doc.ID, doc.IndexStatus); err != nil {
			return err
		}
		if err := uc.chunks.DeleteTx(ctx, tx, doc.ID); err != nil {
			return err
		}
		return uc.index.DeleteTx(ctx, tx, doc.ID)
	})
	if err != nil {
		return nil, err
	}
	return uc.decorateOne(ctx, doc, acc)
}

// Restore 恢复归档文档：发布过的回到已发布并重建索引，否则回到草稿。
func (uc *DocumentUsecase) Restore(ctx context.Context, userID, id string) (*Document, error) {
	doc, space, acc, err := uc.requireOwnerOrManage(ctx, userID, id)
	if err != nil {
		return nil, err
	}
	if doc.Status != DocStatusArchived {
		return nil, ErrInvalidState
	}
	if doc.Version == 0 {
		doc.Status = DocStatusDraft
		doc.UpdateUser, doc.UpdateTime = userID, uc.now()
		if err := uc.docs.Update(ctx, doc, doc.Version); err != nil {
			return nil, err
		}
		return uc.decorateOne(ctx, doc, acc)
	}
	chunks, err := buildChunks(doc.ID, space.ID, doc.Version, doc.Title, doc.Content)
	if err != nil {
		return nil, err
	}
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		tctx := txrun.NewContext(ctx, tx)
		doc.Status = DocStatusPublished
		doc.IndexStatus = IndexPending
		doc.UpdateUser, doc.UpdateTime = userID, uc.now()
		if err := uc.docs.Update(tctx, doc, doc.Version); err != nil {
			return err
		}
		if err := uc.docs.SetIndexStatus(tctx, doc.ID, doc.IndexStatus); err != nil {
			return err
		}
		if err := uc.chunks.ReplaceTx(ctx, tx, doc.ID, chunks); err != nil {
			return err
		}
		return uc.index.UpsertTx(ctx, tx, doc.ID, doc.Version)
	})
	if err != nil {
		return nil, err
	}
	return uc.decorateOne(ctx, doc, acc)
}

// Delete 软删：同事务删索引、释放附件。
func (uc *DocumentUsecase) Delete(ctx context.Context, userID, id string) error {
	doc, _, _, err := uc.requireOwnerOrManage(ctx, userID, id)
	if err != nil {
		return err
	}
	return uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if err := uc.docs.SoftDelete(txrun.NewContext(ctx, tx), doc.ID, uc.now()); err != nil {
			return err
		}
		if err := uc.chunks.DeleteTx(ctx, tx, doc.ID); err != nil {
			return err
		}
		if err := uc.index.DeleteTx(ctx, tx, doc.ID); err != nil {
			return err
		}
		return uc.files.Release(ctx, tx, BusinessType, doc.ID)
	})
}

// Get 文档详情（需读权限）：headingPath 非空只取该节；草稿正文仅写权限可见；从未发布的草稿只有负责人与写权限可见。
func (uc *DocumentUsecase) Get(ctx context.Context, userID, id, headingPath string) (*Document, error) {
	doc, _, acc, err := uc.require(ctx, userID, id, LevelRead)
	if err != nil {
		return nil, err
	}
	if !draftVisible(doc, userID, acc) {
		return nil, ErrDocumentNotFound
	}
	out, err := uc.decorateOne(ctx, doc, acc)
	if err != nil {
		return nil, err
	}
	links, err := uc.links.ListByDocument(ctx, doc.ID)
	if err != nil {
		return nil, err
	}
	out.Links = links
	out.Headings = Headings(doc.Content)
	if headingPath != "" {
		section, ok := Section(doc.Content, headingPath)
		if !ok {
			return nil, ErrSectionNotFound
		}
		out.Content = section
	}
	return out, nil
}

// List 分页列表：只取可见空间；待审核列表只取管理级别的空间；从未发布的草稿只对负责人与写权限空间可见。
func (uc *DocumentUsecase) List(ctx context.Context, userID string, f DocumentFilter, page, pageSize int64) ([]*Document, int64, error) {
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	f.Unrestricted = acc.Unrestricted()
	if !f.Unrestricted {
		minLevel := LevelRead
		if f.ReviewPending {
			minLevel = LevelManage
		}
		f.SpaceIDs = acc.IDs(minLevel)
		if f.SpaceID != "" && !slices.Contains(f.SpaceIDs, f.SpaceID) {
			return []*Document{}, 0, nil
		}
		if len(f.SpaceIDs) == 0 {
			return []*Document{}, 0, nil
		}
		f.HideDrafts, f.DraftOwnerID, f.DraftSpaceIDs = true, userID, acc.IDs(LevelWrite)
	}
	items, total, err := uc.docs.List(ctx, f, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if err := uc.decorate(ctx, items, acc); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListVersions 版本列表（需读权限）。
func (uc *DocumentUsecase) ListVersions(ctx context.Context, userID, id string) ([]*Version, error) {
	if _, _, _, err := uc.require(ctx, userID, id, LevelRead); err != nil {
		return nil, err
	}
	items, err := uc.versions.List(ctx, id)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(items))
	for _, v := range items {
		ids = append(ids, v.PublishUser)
	}
	names, err := uc.userNames(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, v := range items {
		v.PublishUserName = names[v.PublishUser]
	}
	return items, nil
}

// GetVersion 版本内容（需读权限）。
func (uc *DocumentUsecase) GetVersion(ctx context.Context, userID, id string, version int32) (*Version, error) {
	if _, _, _, err := uc.require(ctx, userID, id, LevelRead); err != nil {
		return nil, err
	}
	v, err := uc.versions.Get(ctx, id, version)
	if err != nil {
		return nil, err
	}
	names, err := uc.userNames(ctx, []string{v.PublishUser})
	if err != nil {
		return nil, err
	}
	v.PublishUserName = names[v.PublishUser]
	return v, nil
}

// Rollback 以历史版本正文发布新版本（需写权限；归档文档不可）。
func (uc *DocumentUsecase) Rollback(ctx context.Context, userID, id string, version int32) (*Document, error) {
	doc, space, acc, err := uc.require(ctx, userID, id, LevelWrite)
	if err != nil {
		return nil, err
	}
	if doc.Status == DocStatusArchived {
		return nil, ErrInvalidState
	}
	v, err := uc.versions.Get(ctx, id, version)
	if err != nil {
		return nil, err
	}
	if err := uc.publishContent(ctx, doc, space, userID, v.Title, v.Content, fmt.Sprintf("回滚到 v%d", version)); err != nil {
		return nil, err
	}
	return uc.decorateOne(ctx, doc, acc)
}

// AddLink 添加关联（需写权限）。
func (uc *DocumentUsecase) AddLink(ctx context.Context, userID, id, targetType, targetID, targetName string) (*Link, error) {
	doc, _, _, err := uc.require(ctx, userID, id, LevelWrite)
	if err != nil {
		return nil, err
	}
	if _, ok := linkTargetTypes[targetType]; !ok || strings.TrimSpace(targetID) == "" {
		return nil, ErrInvalidArgument
	}
	existing, err := uc.links.ListByDocument(ctx, doc.ID)
	if err != nil {
		return nil, err
	}
	if len(existing) >= maxLinksPerDocument {
		return nil, ErrInvalidArgument
	}
	lid, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	l := &Link{ID: lid, DocumentID: doc.ID, TargetType: targetType, TargetID: strings.TrimSpace(targetID), TargetName: strings.TrimSpace(targetName), CreateUser: userID, CreateTime: uc.now()}
	if err := uc.links.Create(ctx, l); err != nil {
		return nil, err
	}
	return l, nil
}

// RemoveLink 移除关联（需写权限）。
func (uc *DocumentUsecase) RemoveLink(ctx context.Context, userID, id, linkID string) error {
	if _, _, _, err := uc.require(ctx, userID, id, LevelWrite); err != nil {
		return err
	}
	return uc.links.Delete(ctx, id, linkID)
}

// RelatedDocument 与对象相关的文档。
type RelatedDocument struct {
	DocumentID    string
	Title         string
	SpaceName     string
	Summary       string
	Linked        bool
	LinkID        string
	ReviewOverdue bool
}

// ListRelated 与对象相关的文档：显式关联在前，再按 query 语义检索补足（排除已关联的）。
func (uc *DocumentUsecase) ListRelated(ctx context.Context, userID, targetType, targetID, query string, limit int) ([]*RelatedDocument, error) {
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultTopK
	}
	visible := acc.IDs(LevelRead)
	if !acc.Unrestricted() && len(visible) == 0 {
		return []*RelatedDocument{}, nil
	}
	linked, err := uc.links.ListByTarget(ctx, targetType, targetID, acc.Unrestricted(), visible)
	if err != nil {
		return nil, err
	}
	now := uc.now()
	out := make([]*RelatedDocument, 0, limit)
	seen := map[string]struct{}{}
	for _, l := range linked {
		if len(out) >= limit {
			break
		}
		seen[l.DocumentID] = struct{}{}
		out = append(out, &RelatedDocument{
			DocumentID: l.DocumentID, Title: l.Title, SpaceName: l.SpaceName, Summary: l.Summary, Linked: true, LinkID: l.LinkID,
			ReviewOverdue: l.ReviewAt != nil && l.ReviewAt.Before(now),
		})
	}
	if q := strings.TrimSpace(query); q != "" && len(out) < limit {
		// 权限视图上面已解析过：直接沿用，检索不再解析一遍（同一用户、同一请求，口径相同）。
		res, err := uc.search.searchAs(ctx, acc, userID, SearchSourceRelated, SearchInput{Query: q, TopK: limit}, uc.search.now())
		if err != nil {
			return nil, err
		}
		for _, h := range res.Hits {
			if len(out) >= limit {
				break
			}
			if _, dup := seen[h.DocumentID]; dup {
				continue
			}
			seen[h.DocumentID] = struct{}{}
			out = append(out, &RelatedDocument{DocumentID: h.DocumentID, Title: h.Title, SpaceName: h.SpaceName, Summary: h.Snippet, ReviewOverdue: h.ReviewOverdue})
		}
	}
	return out, nil
}

// Overview 首页概览。
type Overview struct {
	Recent        []*Document
	Mine          []*Document
	PendingReview []*Document
	ReviewDue     []*Document
	SpaceCount    int32
	DocumentCount int64
}

// GetOverview 概览：最近发布、我负责的、待我审核、即将 / 已到复审期。
func (uc *DocumentUsecase) GetOverview(ctx context.Context, userID string) (*Overview, error) {
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := &Overview{Recent: []*Document{}, Mine: []*Document{}, PendingReview: []*Document{}, ReviewDue: []*Document{}}
	readable := acc.IDs(LevelRead)
	if !acc.Unrestricted() {
		out.SpaceCount = clampInt32(len(readable))
		if len(readable) == 0 {
			return out, nil
		}
	} else {
		all, err := uc.spaces.repo.List(ctx, "", "")
		if err != nil {
			return nil, err
		}
		out.SpaceCount = clampInt32(len(all))
	}
	count, err := uc.docs.CountVisible(ctx, acc.Unrestricted(), readable)
	if err != nil {
		return nil, err
	}
	out.DocumentCount = count
	published := DocStatusPublished
	query := func(f DocumentFilter) ([]*Document, error) {
		f.Unrestricted = acc.Unrestricted()
		if f.SpaceIDs == nil && !f.Unrestricted {
			f.SpaceIDs = readable
		}
		items, _, err := uc.docs.List(ctx, f, 1, maxOverviewItems)
		return items, err
	}
	if out.Recent, err = query(DocumentFilter{Status: &published, OrderByPublish: true}); err != nil {
		return nil, err
	}
	if out.Mine, err = query(DocumentFilter{OwnerID: userID}); err != nil {
		return nil, err
	}
	manage := acc.IDs(LevelManage)
	if acc.Unrestricted() || len(manage) > 0 {
		if out.PendingReview, err = query(DocumentFilter{ReviewPending: true, SpaceIDs: manage}); err != nil {
			return nil, err
		}
	}
	due := uc.now().Add(reviewNoticeAhead)
	if out.ReviewDue, err = query(DocumentFilter{OwnerID: userID, Status: &published, ReviewDueBefore: &due}); err != nil {
		return nil, err
	}
	// 四个列表合并后装饰一次：装饰按文档逐个原地补字段、与文档在哪个列表里无关，分四次做只是把
	// 「列全部空间 + 查负责人名」重复四遍。
	if err := uc.decorate(ctx, slices.Concat(out.Recent, out.Mine, out.PendingReview, out.ReviewDue), acc); err != nil {
		return nil, err
	}
	return out, nil
}

// ---------- 导入任务用的内部入口（权限已在创建任务时判定） ----------

// ingestPublishMode 导入任务的发布方式，与手动建档 / 发布（publishOrSubmit）同一口径：要发布且空间要求审核、创建者又不是
// 管理级别时转为提交审核；否则直接发布。
func ingestPublishMode(want bool, space *Space, level int32) (publish, submit bool) {
	if !want {
		return false, false
	}
	if space.ReviewRequired && level < LevelManage {
		return false, true
	}
	return true, false
}

// ingestCreate 导入新建：建草稿、绑定源文件、记录任务 → 文档（then）、（要发布时）发布或提交审核，同一事务——任一步失败整体回滚，
// 重试不会留下半截文档或「文件已绑定」的死局。level 是创建者在空间的级别，决定发布还是提交审核。返回分块数。
func (uc *DocumentUsecase) ingestCreate(ctx context.Context, actor string, space *Space, in CreateDocumentInput, fileID string, level int32,
	then func(ctx context.Context, tx txrun.Tx, docID string) error,
) (*Document, int, error) {
	in.Source, in.SourceRef = SourceImport, fileID
	doc, auto, err := uc.newDocument(actor, space, in)
	if err != nil {
		return nil, 0, err
	}
	publish, submit := ingestPublishMode(in.Publish, space, level)
	if submit {
		doc.ReviewState, doc.ReviewNote = ReviewPending, "导入"
	}
	n := 0
	err = uc.insertDocument(ctx, doc, auto, func(ctx context.Context, tx txrun.Tx) error {
		if err := uc.files.Bind(ctx, tx, actor, BusinessType, doc.ID, []string{fileID}); err != nil {
			return err
		}
		if then != nil {
			if err := then(ctx, tx, doc.ID); err != nil {
				return err
			}
		}
		if !publish {
			return nil
		}
		if err := uc.publishContentTx(ctx, tx, doc, space, actor, doc.Title, doc.Content, "导入"); err != nil {
			return err
		}
		n = len(Split(doc.Title, doc.Content))
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if submit {
		uc.notifyReviewPending(ctx, doc, space, actor)
	}
	return doc, n, nil
}

// ingestReplace 导入覆盖：把解析结果写入既有文档（已发布文档落草稿，publish 时随即发布或提交审核），与绑定源文件同一事务。
func (uc *DocumentUsecase) ingestReplace(ctx context.Context, actor string, space *Space, docID, title, content string, publish bool, fileID string, level int32) (*Document, int, error) {
	doc, err := uc.docs.Get(ctx, docID)
	if err != nil {
		return nil, 0, err
	}
	if doc.SpaceID != space.ID || doc.Status == DocStatusArchived {
		return nil, 0, ErrInvalidState
	}
	if err := checkContent(content); err != nil {
		return nil, 0, err
	}
	publish, submit := ingestPublishMode(publish, space, level)
	now := uc.now()
	n := 0
	err = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		if doc.Status == DocStatusPublished {
			doc.DraftTitle, doc.DraftContent = &title, &content
		} else {
			doc.Title, doc.Content = title, content
		}
		if submit {
			doc.ReviewState, doc.ReviewNote = ReviewPending, "导入"
		}
		doc.UpdateUser, doc.UpdateTime = actor, now
		if err := uc.docs.Update(txrun.NewContext(ctx, tx), doc, doc.Version); err != nil {
			return err
		}
		if err := uc.files.Bind(ctx, tx, actor, BusinessType, doc.ID, []string{fileID}); err != nil {
			return err
		}
		if !publish {
			return nil
		}
		if err := uc.publishContentTx(ctx, tx, doc, space, actor, title, content, "导入"); err != nil {
			return err
		}
		n = len(Split(title, content))
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if submit {
		uc.notifyReviewPending(ctx, doc, space, actor)
	}
	return doc, n, nil
}

// ingestResume 导入任务重领时文档已由上一次建好（任务 → 文档在建档事务里记下）：不再建档 / 绑定，只补没做完的发布
// 或提交审核；已发布的直接返回。返回分块数（未发布为 0）。
func (uc *DocumentUsecase) ingestResume(ctx context.Context, actor string, space *Space, doc *Document, publish bool, level int32) (*Document, int, error) {
	if doc.Status == DocStatusPublished {
		return doc, len(Split(doc.Title, doc.Content)), nil
	}
	publish, submit := ingestPublishMode(publish, space, level)
	switch {
	case publish:
		if err := uc.publishContent(ctx, doc, space, actor, doc.Title, doc.Content, "导入"); err != nil {
			return nil, 0, err
		}
		return doc, len(Split(doc.Title, doc.Content)), nil
	case submit && doc.ReviewState != ReviewPending:
		doc.ReviewState, doc.ReviewNote = ReviewPending, "导入"
		doc.UpdateUser, doc.UpdateTime = actor, uc.now()
		if err := uc.docs.Update(ctx, doc, doc.Version); err != nil {
			return nil, 0, err
		}
		uc.notifyReviewPending(ctx, doc, space, actor)
	}
	return doc, 0, nil
}

// ---------- 助手页内上下文 ----------

// maxContextChars 拼进系统提示的正文上限。
const maxContextChars = 6000

// Describe 为助手页内上下文描述一篇文档（需读权限）：标题、空间、摘要、标签、目录与正文开头。
// 返回所属模块名（预加载工具用）与描述文本。
func (uc *DocumentUsecase) Describe(ctx context.Context, userID, id string) (module, text string, err error) {
	doc, err := uc.Get(ctx, userID, id, "")
	if err != nil {
		return "", "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "用户正在查看知识文档《%s》（文档 ID：%s，所属空间：%s", doc.Title, doc.ID, doc.SpaceName)
	if doc.Version > 0 {
		fmt.Fprintf(&b, "，版本 v%d", doc.Version)
	}
	b.WriteString("）。\n")
	if s := firstNonEmpty(doc.Summary, doc.AISummary); s != "" {
		b.WriteString("摘要：" + s + "\n")
	}
	if len(doc.Tags) > 0 {
		b.WriteString("标签：" + strings.Join(doc.Tags, "、") + "\n")
	}
	if len(doc.Headings) > 0 {
		b.WriteString("目录：\n")
		for i, h := range doc.Headings {
			if i >= 40 {
				b.WriteString("- …\n")
				break
			}
			b.WriteString("- " + h + "\n")
		}
	}
	content := []rune(doc.Content)
	if len(content) > maxContextChars {
		content = content[:maxContextChars]
	}
	b.WriteString("正文（可能被截断；需要某一节的完整内容时用 KnowledgeService_GetDocument 带 heading_path 获取）：\n")
	b.WriteString(string(content))
	return "knowledge", b.String(), nil
}

// ---------- 内部辅助 ----------

// require 取文档并要求对其空间至少 level（不可见按不存在）。
func (uc *DocumentUsecase) require(ctx context.Context, userID, id string, level int32) (*Document, *Space, *Access, error) {
	doc, err := uc.docs.Get(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, nil, nil, err
	}
	lv := acc.Level(doc.SpaceID)
	if lv == LevelNone {
		return nil, nil, nil, ErrDocumentNotFound
	}
	if lv < level {
		return nil, nil, nil, ErrForbidden
	}
	space, err := uc.spaces.repo.Get(ctx, doc.SpaceID)
	if err != nil {
		return nil, nil, nil, err
	}
	space.Level = lv
	return doc, space, acc, nil
}

// requireOwnerOrManage 文档负责人或空间管理级别。
func (uc *DocumentUsecase) requireOwnerOrManage(ctx context.Context, userID, id string) (*Document, *Space, *Access, error) {
	doc, space, acc, err := uc.require(ctx, userID, id, LevelRead)
	if err != nil {
		return nil, nil, nil, err
	}
	if doc.OwnerID != userID && acc.Level(space.ID) < LevelManage {
		return nil, nil, nil, ErrForbidden
	}
	return doc, space, acc, nil
}

// draftVisible 从未发布的草稿只对负责人与空间写权限可见（只读成员看不到别人的半成品，也不暴露其存在）。
func draftVisible(doc *Document, userID string, acc *Access) bool {
	return doc.Status != DocStatusDraft || doc.OwnerID == userID || acc.Level(doc.SpaceID) >= LevelWrite
}

// decorateOne 补展示字段（空间名、负责人名、级别；草稿正文按权限隐藏）。
func (uc *DocumentUsecase) decorateOne(ctx context.Context, doc *Document, acc *Access) (*Document, error) {
	if err := uc.decorate(ctx, []*Document{doc}, acc); err != nil {
		return nil, err
	}
	return doc, nil
}

func (uc *DocumentUsecase) decorate(ctx context.Context, docs []*Document, acc *Access) error {
	if len(docs) == 0 {
		return nil
	}
	// 只取这批文档涉及的空间的名称（去重后按主键查），不再每次把全部空间整行列出来。
	spaceIDs := make([]string, 0, len(docs))
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		spaceIDs = append(spaceIDs, d.SpaceID)
		ids = append(ids, d.OwnerID)
	}
	spaceNames, err := uc.spaces.repo.Names(ctx, slices.Compact(slices.Sorted(slices.Values(spaceIDs))))
	if err != nil {
		return err
	}
	names, err := uc.userNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, d := range docs {
		d.SpaceName = spaceNames[d.SpaceID]
		d.OwnerName = names[d.OwnerID]
		d.Level = acc.Level(d.SpaceID)
		if d.Level < LevelWrite {
			d.DraftContent, d.DraftTitle = nil, nil
		}
	}
	return nil
}

func (uc *DocumentUsecase) userNames(ctx context.Context, ids []string) (map[string]string, error) {
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	if len(ids) == 0 {
		return map[string]string{}, nil
	}
	return uc.dir.UserNames(ctx, ids)
}

// notifyReviewPending 提交审核通知空间负责人（失败只记录，不影响主流程；通知写在独立事务）。
func (uc *DocumentUsecase) notifyReviewPending(ctx context.Context, doc *Document, space *Space, actor string) {
	if space.OwnerID == "" || space.OwnerID == actor {
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"documentId": doc.ID, "title": doc.Title, "space": space.Name, "submitter": actor, "path": "/knowledge/doc/" + doc.ID,
	})
	_ = uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		_, err := uc.notify.NotifyEvent(ctx, tx, biznotify.Event{
			BizID: NotifyBizID, EventType: EventReviewPending, Severity: 1,
			Title: "知识文档待审核：" + doc.Title, Payload: payload, Recipients: []string{space.OwnerID},
			DedupKey: EventReviewPending + ":" + doc.ID + ":" + uc.now().Format("2006-01-02"),
		})
		return err
	})
}

// buildChunks 分块并补齐标识字段。
func buildChunks(docID, spaceID string, version int32, title, content string) ([]*Chunk, error) {
	chunks := Split(title, content)
	for _, c := range chunks {
		id, err := idgen.NewID()
		if err != nil {
			return nil, err
		}
		c.ID, c.DocumentID, c.SpaceID, c.Version = id, docID, spaceID, version
	}
	return chunks, nil
}

// checkContent 正文长度与密钥扫描。
func checkContent(content string) error {
	if len(content) > MaxContentBytes {
		return ErrContentTooLarge
	}
	if findings := secretscan.Scan(content); len(findings) > 0 {
		f := findings[0]
		return ErrSecretDetected.WithMetadata(map[string]string{"line": fmt.Sprint(f.Line), "kind": f.Kind, "count": fmt.Sprint(len(findings))})
	}
	return nil
}

// normalizeTags 去空白、去重、限制数量与长度。
func normalizeTags(tags []string) ([]string, error) {
	out := make([]string, 0, len(tags))
	seen := map[string]struct{}{}
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if runeLen(t) > 32 {
			return nil, ErrInvalidArgument
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	if len(out) > maxTags {
		return nil, ErrInvalidArgument
	}
	return out, nil
}

// slugify 由标题生成 URL 片段：小写字母数字保留、其余折叠为连字符；中文等无可用字符时取 doc-<id 前 8 位>。
func slugify(title, id string) string {
	var b strings.Builder
	lastDash := true
	for _, r := range strings.ToLower(title) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case unicode.IsSpace(r), r == '-', r == '_', r == '.', r == '/':
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	s := strings.Trim(b.String(), "-")
	if len(s) > 64 {
		s = strings.Trim(s[:64], "-")
	}
	if len(s) < 2 {
		return "doc-" + strings.ReplaceAll(id, "-", "")[:8]
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
