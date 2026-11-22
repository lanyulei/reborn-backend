// Package service 知识库域 proto 接口实现：参数编排、模型转换与错误透传。
package service

import (
	"context"
	"math"
	"strings"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/knowledge/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	systemservice "github.com/lanyulei/reborn/backend/core/internal/service/system"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/transport"
	"github.com/google/wire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ProviderSet is knowledge 域 service providers.
var ProviderSet = wire.NewSet(NewSpaceService, NewKnowledgeService, NewIngestService, NewAdminService)

// 编译期断言锁住生成接口。
var (
	_ v1.KnowledgeSpaceServiceHTTPServer  = (*SpaceService)(nil)
	_ v1.KnowledgeServiceHTTPServer       = (*KnowledgeService)(nil)
	_ v1.KnowledgeIngestServiceHTTPServer = (*IngestService)(nil)
	_ v1.KnowledgeAdminServiceHTTPServer  = (*AdminService)(nil)
)

// ---------- 空间 ----------

// SpaceService 实现 KnowledgeSpaceServiceHTTPServer。
type SpaceService struct {
	uc *biz.SpaceUsecase
}

// NewSpaceService new a SpaceService.
func NewSpaceService(uc *biz.SpaceUsecase) *SpaceService { return &SpaceService{uc: uc} }

// ListSpaces implements proto.
func (s *SpaceService) ListSpaces(ctx context.Context, req *v1.ListSpacesRequest) (*v1.ListSpacesResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.List(ctx, userID, req.GetKeyword(), req.GetType())
	if err != nil {
		return nil, err
	}
	out := &v1.ListSpacesResponse{Items: make([]*v1.Space, 0, len(items))}
	for _, it := range items {
		out.Items = append(out.Items, toProtoSpace(it))
	}
	return out, nil
}

// GetSpace implements proto.
func (s *SpaceService) GetSpace(ctx context.Context, req *v1.GetSpaceRequest) (*v1.GetSpaceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sp, err := s.uc.Get(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetSpaceResponse{Space: toProtoSpace(sp)}, nil
}

// CreateSpace implements proto.
func (s *SpaceService) CreateSpace(ctx context.Context, req *v1.CreateSpaceRequest) (*v1.CreateSpaceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	embedding := true
	if req.EmbeddingEnabled != nil {
		embedding = req.GetEmbeddingEnabled()
	}
	sp, err := s.uc.Create(ctx, userID, biz.CreateSpaceInput{
		Code: req.GetCode(), Name: req.GetName(), Type: req.GetType(), Description: req.GetDescription(), Visibility: req.GetVisibility(),
		OwnerID: req.GetOwnerId(), EmbeddingEnabled: embedding, ReviewRequired: req.GetReviewRequired(), ReviewDays: req.GetReviewDays(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateSpaceResponse{Space: toProtoSpace(sp)}, nil
}

// UpdateSpace implements proto.
func (s *SpaceService) UpdateSpace(ctx context.Context, req *v1.UpdateSpaceRequest) (*v1.UpdateSpaceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sp, err := s.uc.Update(ctx, userID, req.GetId(), biz.SpacePatch{
		Name: req.Name, Type: req.Type, Description: req.Description, Visibility: req.Visibility, OwnerID: req.OwnerId,
		EmbeddingEnabled: req.EmbeddingEnabled, ReviewRequired: req.ReviewRequired, ReviewDays: req.ReviewDays,
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateSpaceResponse{Space: toProtoSpace(sp)}, nil
}

// DeleteSpace implements proto.
func (s *SpaceService) DeleteSpace(ctx context.Context, req *v1.DeleteSpaceRequest) (*v1.DeleteSpaceResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.uc.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteSpaceResponse{}, nil
}

// GetSpaceAcl implements proto.
func (s *SpaceService) GetSpaceAcl(ctx context.Context, req *v1.GetSpaceAclRequest) (*v1.GetSpaceAclResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.uc.GetAcl(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetSpaceAclResponse{Items: toProtoAcl(items)}, nil
}

// SetSpaceAcl implements proto.
func (s *SpaceService) SetSpaceAcl(ctx context.Context, req *v1.SetSpaceAclRequest) (*v1.SetSpaceAclResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]*biz.AclEntry, 0, len(req.GetItems()))
	for _, e := range req.GetItems() {
		entries = append(entries, &biz.AclEntry{SubjectType: subjectTypeOf(e.GetSubjectType()), SubjectID: e.GetSubjectId(), Level: e.GetLevel()})
	}
	items, err := s.uc.SetAcl(ctx, userID, req.GetId(), entries)
	if err != nil {
		return nil, err
	}
	return &v1.SetSpaceAclResponse{Items: toProtoAcl(items)}, nil
}

// ---------- 文档与检索 ----------

// KnowledgeService 实现 KnowledgeServiceHTTPServer。
type KnowledgeService struct {
	docs   *biz.DocumentUsecase
	search *biz.SearchUsecase
}

// NewKnowledgeService new a KnowledgeService.
func NewKnowledgeService(docs *biz.DocumentUsecase, search *biz.SearchUsecase) *KnowledgeService {
	return &KnowledgeService{docs: docs, search: search}
}

// Search implements proto：来源按请求头判定（助手回环请求的 X-Request-Id 以 assistant- 开头）。
func (s *KnowledgeService) Search(ctx context.Context, req *v1.SearchRequest) (*v1.SearchResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	res, err := s.search.Search(ctx, userID, searchSource(ctx), biz.SearchInput{
		Query: req.GetQuery(), SpaceIDs: req.GetSpaceIds(), Tags: req.GetTags(), TopK: int(req.GetTopK()),
	})
	if err != nil {
		return nil, err
	}
	out := &v1.SearchResponse{Items: make([]*v1.SearchHit, 0, len(res.Hits)), Degraded: res.Degraded, SearchLogId: res.LogID, DurationMs: res.Duration.Milliseconds()}
	for _, h := range res.Hits {
		out.Items = append(out.Items, &v1.SearchHit{
			DocumentId: h.DocumentID, Title: h.Title, SpaceId: h.SpaceID, SpaceName: h.SpaceName, ChunkSeq: h.Seq, HeadingPath: h.HeadingPath,
			Snippet: h.Snippet, Section: h.Section, Score: h.Score, ReviewOverdue: h.ReviewOverdue, Citation: h.Citation(),
		})
	}
	return out, nil
}

// searchSource 判定检索来源。
func searchSource(ctx context.Context) string {
	if tr, ok := transport.FromServerContext(ctx); ok {
		if strings.HasPrefix(tr.RequestHeader().Get("X-Request-Id"), "assistant-") {
			return biz.SearchSourceAssistant
		}
	}
	return biz.SearchSourceWeb
}

// ListDocuments implements proto.
func (s *KnowledgeService) ListDocuments(ctx context.Context, req *v1.ListDocumentsRequest) (*v1.ListDocumentsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	page, size := paging.Normalize(req.GetPage(), int64(req.GetPageSize()))
	f := biz.DocumentFilter{
		SpaceID: req.GetSpaceId(), Status: req.Status, Keyword: req.GetKeyword(), Tag: req.GetTag(), OwnerID: req.GetOwnerId(),
		ReviewPending: req.GetReviewPending(), ReviewOverdue: req.GetReviewOverdue(),
	}
	items, total, err := s.docs.List(ctx, userID, f, page, size)
	if err != nil {
		return nil, err
	}
	out := &v1.ListDocumentsResponse{Items: make([]*v1.Document, 0, len(items)), Total: total}
	for _, d := range items {
		out.Items = append(out.Items, toProtoDocument(d, false))
	}
	return out, nil
}

// GetDocument implements proto.
func (s *KnowledgeService) GetDocument(ctx context.Context, req *v1.GetDocumentRequest) (*v1.GetDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Get(ctx, userID, req.GetId(), req.GetHeadingPath())
	if err != nil {
		return nil, err
	}
	return &v1.GetDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// CreateDocument implements proto.
func (s *KnowledgeService) CreateDocument(ctx context.Context, req *v1.CreateDocumentRequest) (*v1.CreateDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Create(ctx, userID, biz.CreateDocumentInput{
		SpaceID: req.GetSpaceId(), Title: req.GetTitle(), Slug: req.GetSlug(), Content: req.GetContent(), Tags: req.GetTags(), Summary: req.GetSummary(),
		OwnerID: req.GetOwnerId(), Source: req.GetSource(), SourceRef: req.GetSourceRef(), Publish: req.GetPublish(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// UpdateDocument implements proto.
func (s *KnowledgeService) UpdateDocument(ctx context.Context, req *v1.UpdateDocumentRequest) (*v1.UpdateDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	p := biz.DocumentPatch{Title: req.Title, Content: req.Content, Summary: req.Summary, OwnerID: req.OwnerId, ClearReviewAt: req.GetClearReviewAt()}
	if req.Tags != nil {
		tags := req.GetTags().GetItems()
		if tags == nil {
			tags = []string{}
		}
		p.Tags = &tags
	}
	if req.ReviewAt != nil {
		t := req.GetReviewAt().AsTime()
		p.ReviewAt = &t
	}
	d, err := s.docs.Update(ctx, userID, req.GetId(), p)
	if err != nil {
		return nil, err
	}
	return &v1.UpdateDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// PublishDocument implements proto.
func (s *KnowledgeService) PublishDocument(ctx context.Context, req *v1.PublishDocumentRequest) (*v1.PublishDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, pending, err := s.docs.Publish(ctx, userID, req.GetId(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.PublishDocumentResponse{Document: toProtoDocument(d, true), PendingReview: pending}, nil
}

// ReviewDocument implements proto.
func (s *KnowledgeService) ReviewDocument(ctx context.Context, req *v1.ReviewDocumentRequest) (*v1.ReviewDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Review(ctx, userID, req.GetId(), req.GetApproved(), req.GetNote())
	if err != nil {
		return nil, err
	}
	return &v1.ReviewDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// ConfirmDocumentReview implements proto.
func (s *KnowledgeService) ConfirmDocumentReview(ctx context.Context, req *v1.ConfirmDocumentReviewRequest) (*v1.ConfirmDocumentReviewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.ConfirmReview(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.ConfirmDocumentReviewResponse{Document: toProtoDocument(d, true)}, nil
}

// ArchiveDocument implements proto.
func (s *KnowledgeService) ArchiveDocument(ctx context.Context, req *v1.ArchiveDocumentRequest) (*v1.ArchiveDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Archive(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.ArchiveDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// RestoreDocument implements proto.
func (s *KnowledgeService) RestoreDocument(ctx context.Context, req *v1.RestoreDocumentRequest) (*v1.RestoreDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Restore(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RestoreDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// DeleteDocument implements proto.
func (s *KnowledgeService) DeleteDocument(ctx context.Context, req *v1.DeleteDocumentRequest) (*v1.DeleteDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.docs.Delete(ctx, userID, req.GetId()); err != nil {
		return nil, err
	}
	return &v1.DeleteDocumentResponse{}, nil
}

// ListVersions implements proto.
func (s *KnowledgeService) ListVersions(ctx context.Context, req *v1.ListVersionsRequest) (*v1.ListVersionsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.docs.ListVersions(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	out := &v1.ListVersionsResponse{Items: make([]*v1.Version, 0, len(items))}
	for _, v := range items {
		out.Items = append(out.Items, toProtoVersion(v, false))
	}
	return out, nil
}

// GetVersion implements proto.
func (s *KnowledgeService) GetVersion(ctx context.Context, req *v1.GetVersionRequest) (*v1.GetVersionResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.docs.GetVersion(ctx, userID, req.GetId(), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.GetVersionResponse{Version: toProtoVersion(v, true)}, nil
}

// RollbackDocument implements proto.
func (s *KnowledgeService) RollbackDocument(ctx context.Context, req *v1.RollbackDocumentRequest) (*v1.RollbackDocumentResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	d, err := s.docs.Rollback(ctx, userID, req.GetId(), req.GetVersion())
	if err != nil {
		return nil, err
	}
	return &v1.RollbackDocumentResponse{Document: toProtoDocument(d, true)}, nil
}

// AddLink implements proto.
func (s *KnowledgeService) AddLink(ctx context.Context, req *v1.AddLinkRequest) (*v1.AddLinkResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	l, err := s.docs.AddLink(ctx, userID, req.GetId(), req.GetTargetType(), req.GetTargetId(), req.GetTargetName())
	if err != nil {
		return nil, err
	}
	return &v1.AddLinkResponse{Link: toProtoLink(l)}, nil
}

// RemoveLink implements proto.
func (s *KnowledgeService) RemoveLink(ctx context.Context, req *v1.RemoveLinkRequest) (*v1.RemoveLinkResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.docs.RemoveLink(ctx, userID, req.GetId(), req.GetLinkId()); err != nil {
		return nil, err
	}
	return &v1.RemoveLinkResponse{}, nil
}

// ListRelated implements proto.
func (s *KnowledgeService) ListRelated(ctx context.Context, req *v1.ListRelatedRequest) (*v1.ListRelatedResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	items, err := s.docs.ListRelated(ctx, userID, req.GetTargetType(), req.GetTargetId(), req.GetQuery(), int(req.GetLimit()))
	if err != nil {
		return nil, err
	}
	out := &v1.ListRelatedResponse{Items: make([]*v1.RelatedDocument, 0, len(items))}
	for _, it := range items {
		out.Items = append(out.Items, &v1.RelatedDocument{
			DocumentId: it.DocumentID, Title: it.Title, SpaceName: it.SpaceName, Summary: it.Summary, Linked: it.Linked, LinkId: it.LinkID, ReviewOverdue: it.ReviewOverdue,
		})
	}
	return out, nil
}

// GetOverview implements proto.
func (s *KnowledgeService) GetOverview(ctx context.Context, _ *v1.GetOverviewRequest) (*v1.GetOverviewResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	ov, err := s.docs.GetOverview(ctx, userID)
	if err != nil {
		return nil, err
	}
	conv := func(items []*biz.Document) []*v1.Document {
		out := make([]*v1.Document, 0, len(items))
		for _, d := range items {
			out = append(out, toProtoDocument(d, false))
		}
		return out
	}
	return &v1.GetOverviewResponse{
		Recent: conv(ov.Recent), Mine: conv(ov.Mine), PendingReview: conv(ov.PendingReview), ReviewDue: conv(ov.ReviewDue),
		SpaceCount: ov.SpaceCount, DocumentCount: clampInt32(ov.DocumentCount),
	}, nil
}

// SubmitSearchFeedback implements proto.
func (s *KnowledgeService) SubmitSearchFeedback(ctx context.Context, req *v1.SubmitSearchFeedbackRequest) (*v1.SubmitSearchFeedbackResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.search.Feedback(ctx, userID, req.GetId(), req.GetHelpful()); err != nil {
		return nil, err
	}
	return &v1.SubmitSearchFeedbackResponse{}, nil
}

// ---------- 导入 ----------

// IngestService 实现 KnowledgeIngestServiceHTTPServer。
type IngestService struct {
	uc *biz.IngestUsecase
}

// NewIngestService new an IngestService.
func NewIngestService(uc *biz.IngestUsecase) *IngestService { return &IngestService{uc: uc} }

// CreateIngestJob implements proto.
func (s *IngestService) CreateIngestJob(ctx context.Context, req *v1.CreateIngestJobRequest) (*v1.CreateIngestJobResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	job, err := s.uc.Create(ctx, userID, biz.CreateIngestInput{
		SpaceID: req.GetSpaceId(), FileID: req.GetFileId(), DocumentID: req.GetDocumentId(), Title: req.GetTitle(), Tags: req.GetTags(), Publish: req.GetPublish(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateIngestJobResponse{Job: toProtoIngest(job)}, nil
}

// ListIngestJobs implements proto.
func (s *IngestService) ListIngestJobs(ctx context.Context, req *v1.ListIngestJobsRequest) (*v1.ListIngestJobsResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	page, size := paging.Normalize(req.GetPage(), int64(req.GetPageSize()))
	items, total, err := s.uc.List(ctx, userID, biz.IngestFilter{SpaceID: req.GetSpaceId(), Status: req.Status}, page, size)
	if err != nil {
		return nil, err
	}
	out := &v1.ListIngestJobsResponse{Items: make([]*v1.IngestJob, 0, len(items)), Total: total}
	for _, j := range items {
		out.Items = append(out.Items, toProtoIngest(j))
	}
	return out, nil
}

// RetryIngestJob implements proto.
func (s *IngestService) RetryIngestJob(ctx context.Context, req *v1.RetryIngestJobRequest) (*v1.RetryIngestJobResponse, error) {
	userID, err := systemservice.UserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	job, err := s.uc.Retry(ctx, userID, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RetryIngestJobResponse{Job: toProtoIngest(job)}, nil
}

// ---------- 管理 ----------

// AdminService 实现 KnowledgeAdminServiceHTTPServer（接口授权即门槛，缺省只授予管理员）。
type AdminService struct {
	index *biz.IndexUsecase
	gov   *biz.GovernanceUsecase
}

// NewAdminService new an AdminService.
func NewAdminService(index *biz.IndexUsecase, gov *biz.GovernanceUsecase) *AdminService {
	return &AdminService{index: index, gov: gov}
}

// GetIndexStatus implements proto.
func (s *AdminService) GetIndexStatus(ctx context.Context, _ *v1.GetIndexStatusRequest) (*v1.GetIndexStatusResponse, error) {
	st, err := s.index.Status(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetIndexStatusResponse{
		VectorAvailable: st.VectorAvailable, EmbeddingConfigured: st.EmbeddingConfigured, EmbeddingModel: st.EmbeddingModel, EmbeddingOpen: st.BreakerOpen,
		ChunkCount: st.ChunkCount, EmbeddedCount: st.EmbeddedCount, PendingJobs: st.PendingJobs, FailedJobs: st.FailedJobs, Hint: st.Hint,
	}, nil
}

// RebuildIndex implements proto.
func (s *AdminService) RebuildIndex(ctx context.Context, req *v1.RebuildIndexRequest) (*v1.RebuildIndexResponse, error) {
	n, err := s.index.Rebuild(ctx, req.GetSpaceId(), req.GetDocumentId())
	if err != nil {
		return nil, err
	}
	return &v1.RebuildIndexResponse{Queued: clampInt32(int64(n))}, nil
}

// ListSearchLogs implements proto.
func (s *AdminService) ListSearchLogs(ctx context.Context, req *v1.ListSearchLogsRequest) (*v1.ListSearchLogsResponse, error) {
	page, size := paging.Normalize(req.GetPage(), int64(req.GetPageSize()))
	items, total, err := s.gov.ListSearchLogs(ctx, biz.SearchLogFilter{Source: req.GetSource(), NoResult: req.GetNoResult(), Keyword: req.GetKeyword()}, page, size)
	if err != nil {
		return nil, err
	}
	out := &v1.ListSearchLogsResponse{Items: make([]*v1.SearchLog, 0, len(items)), Total: total}
	for _, l := range items {
		out.Items = append(out.Items, &v1.SearchLog{
			Id: l.ID, UserId: l.UserID, UserName: l.UserName, Source: l.Source, Query: l.Query, HitCount: l.HitCount, Degraded: l.Degraded,
			DurationMs: int64(l.DurationMS), Feedback: l.Feedback, CreateTime: timestamppb.New(l.CreateTime),
		})
	}
	return out, nil
}

// GetStats implements proto.
func (s *AdminService) GetStats(ctx context.Context, req *v1.GetStatsRequest) (*v1.GetStatsResponse, error) {
	st, err := s.gov.Stats(ctx, int(req.GetDays()))
	if err != nil {
		return nil, err
	}
	out := &v1.GetStatsResponse{
		SpaceCount: st.SpaceCount, DocumentCount: st.PublishedCount, DraftCount: st.DraftCount, ChunkCount: st.ChunkCount,
		ReviewOverdueCount: st.ReviewOverdueCount, PendingReviewCount: st.PendingReviewCount, SearchCount: st.SearchCount,
		NoResultCount: st.NoResultCount, HelpfulCount: st.HelpfulCount, UnhelpfulCount: st.UnhelpfulCount,
	}
	for _, q := range st.TopNoResult {
		out.TopQueries = append(out.TopQueries, &v1.QueryCount{Query: q.Query, Count: q.Count})
	}
	return out, nil
}

// ---------- 转换 ----------

func toProtoSpace(s *biz.Space) *v1.Space {
	return &v1.Space{
		Id: s.ID, Code: s.Code, Name: s.Name, Type: s.Type, Description: s.Description, Visibility: s.Visibility, OwnerId: s.OwnerID, OwnerName: s.OwnerName,
		EmbeddingEnabled: s.EmbeddingEnabled, ReviewRequired: s.ReviewRequired, ReviewDays: s.ReviewDays, Level: s.Level, DocumentCount: s.DocumentCount,
		CreateTime: timestamppb.New(s.CreateTime), UpdateTime: timestamppb.New(s.UpdateTime),
	}
}

func toProtoAcl(items []*biz.AclEntry) []*v1.AclEntry {
	out := make([]*v1.AclEntry, 0, len(items))
	for _, e := range items {
		out = append(out, &v1.AclEntry{SubjectType: subjectTypeName(e.SubjectType), SubjectId: e.SubjectID, SubjectName: e.SubjectName, Level: e.Level})
	}
	return out
}

func subjectTypeOf(name string) int32 {
	switch name {
	case "role":
		return biz.SubjectRole
	case "user":
		return biz.SubjectUser
	case "machine":
		return biz.SubjectMachine
	}
	return 0
}

func subjectTypeName(t int32) string {
	switch t {
	case biz.SubjectRole:
		return "role"
	case biz.SubjectUser:
		return "user"
	case biz.SubjectMachine:
		return "machine"
	}
	return ""
}

// toProtoDocument 转换文档；detail 为 false 时不带正文。
func toProtoDocument(d *biz.Document, detail bool) *v1.Document {
	now := time.Now()
	out := &v1.Document{
		Id: d.ID, SpaceId: d.SpaceID, SpaceName: d.SpaceName, Slug: d.Slug, Title: d.Title, Status: d.Status, ReviewState: d.ReviewState,
		HasDraft: d.HasDraft(), Summary: firstNonEmpty(d.Summary, d.AISummary), Tags: d.Tags, Source: d.Source, SourceRef: d.SourceRef,
		OwnerId: d.OwnerID, OwnerName: d.OwnerName, ReviewAt: optTime(d.ReviewAt), ReviewOverdue: d.ReviewOverdue(now), Version: d.Version,
		IndexStatus: d.IndexStatus, Level: d.Level, CreateUser: d.CreateUser, UpdateUser: d.UpdateUser,
		CreateTime: timestamppb.New(d.CreateTime), UpdateTime: timestamppb.New(d.UpdateTime), PublishTime: optTime(d.PublishTime),
	}
	if out.Tags == nil {
		out.Tags = []string{}
	}
	if detail {
		out.Content = d.Content
		if d.DraftContent != nil {
			out.DraftContent = *d.DraftContent
		}
		if d.DraftTitle != nil && out.DraftContent == "" && d.Level >= biz.LevelWrite {
			// 只改了标题的草稿：正文沿用已发布正文，便于编辑器直接打开。
			out.DraftContent = d.Content
		}
		out.Headings = d.Headings
		for _, l := range d.Links {
			out.Links = append(out.Links, toProtoLink(l))
		}
	}
	return out
}

func toProtoVersion(v *biz.Version, withContent bool) *v1.Version {
	out := &v1.Version{Version: v.Version, Title: v.Title, Note: v.Note, PublishUser: v.PublishUser, PublishUserName: v.PublishUserName, CreateTime: timestamppb.New(v.CreateTime)}
	if withContent {
		out.Content = v.Content
	}
	return out
}

func toProtoLink(l *biz.Link) *v1.Link {
	return &v1.Link{Id: l.ID, TargetType: l.TargetType, TargetId: l.TargetID, TargetName: l.TargetName, CreateTime: timestamppb.New(l.CreateTime)}
}

func toProtoIngest(j *biz.IngestJob) *v1.IngestJob {
	out := &v1.IngestJob{
		Id: j.ID, SpaceId: j.SpaceID, SpaceName: j.SpaceName, FileId: j.FileID, Filename: j.Filename, Status: j.Status, Error: j.Error,
		ChunkCount: j.ChunkCount, Attempts: j.Attempts, CreateUser: j.CreateUser, CreateTime: timestamppb.New(j.CreateTime), UpdateTime: timestamppb.New(j.UpdateTime),
		Title: j.Title, Tags: j.Tags, Publish: j.Publish,
	}
	if j.DocumentID != nil {
		out.DocumentId = *j.DocumentID
	}
	return out
}

// clampInt32 把 int64 计数收敛到 proto 的 int32 字段范围。
func clampInt32(n int64) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(n) //nolint:gosec // 已在上方限定范围（计数非负）。
}

func optTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
