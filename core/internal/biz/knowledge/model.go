// Package biz 知识库域业务核心：知识空间（权限单位）、Markdown 文档（发布即版本）、分块索引
// （全文 + 向量）、与运维对象的关联、文件导入与索引后台任务、检索日志。
//
// 检索只覆盖已发布文档；下线类操作（归档 / 删除 / 撤回）同事务删索引，发布后的向量化异步完成。
// embedding 服务与 pgvector 都是可选依赖：缺任一项时检索退化为纯全文，其余功能不受影响。
package biz

import (
	"context"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/knowledge/v1"
	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 空间可见性（knowledge_space.visibility）。
const (
	VisibilityPublic     int32 = 1
	VisibilityRestricted int32 = 2
)

// ACL 主体类型（knowledge_space_acl.subject_type）。
const (
	SubjectRole    int32 = 1
	SubjectUser    int32 = 2
	SubjectMachine int32 = 3
)

// 权限级别：0 无 / 1 读 / 2 写 / 3 管理；高级别包含低级别。
const (
	LevelNone   int32 = 0
	LevelRead   int32 = 1
	LevelWrite  int32 = 2
	LevelManage int32 = 3
)

// 文档状态（knowledge_document.status）。
const (
	DocStatusDraft     int32 = 1
	DocStatusPublished int32 = 2
	DocStatusArchived  int32 = 3
)

// 审核状态（knowledge_document.review_state）。
const (
	ReviewNone    int32 = 0
	ReviewPending int32 = 1
)

// 索引状态（knowledge_document.index_status）。
const (
	IndexNone    int32 = 0
	IndexPending int32 = 1
	IndexDone    int32 = 2
	IndexFailed  int32 = 3
)

// 文档来源（knowledge_document.source）。
const (
	SourceManual     = "manual"
	SourceImport     = "import"
	SourceAssistant  = "assistant"
	SourcePostmortem = "postmortem"
)

// 后台任务状态（导入任务 / 索引任务共用）。
const (
	JobQueued    int32 = 1
	JobRunning   int32 = 2
	JobSucceeded int32 = 3
	JobFailed    int32 = 4
)

// 检索反馈（knowledge_search_log.feedback）。
const (
	FeedbackNone      int32 = 0
	FeedbackHelpful   int32 = 1
	FeedbackUnhelpful int32 = 2
)

// 检索来源（knowledge_search_log.source）。
const (
	SearchSourceWeb       = "web"
	SearchSourceAssistant = "assistant"
	SearchSourceRelated   = "related"
)

// 空间类型。
var spaceTypes = map[string]struct{}{"general": {}, "manual": {}, "standard": {}, "incident": {}, "faq": {}}

// 关联对象类型。
var linkTargetTypes = map[string]struct{}{
	"cmdb_model": {}, "cmdb_ci": {}, "monitor_rule": {}, "incident_playbook": {}, "ticket": {}, "job_template": {}, "workflow_definition": {},
}

const (
	// BusinessType 文档附件（正文内图片、导入源文件）在 file 域的业务类型。
	BusinessType = "knowledge"
	// EmbeddingDimensions 向量维度（与迁移里 halfvec(1024) 一致）。
	EmbeddingDimensions = 1024
	// MaxContentBytes 正文上限（与 proto 校验一致）。
	MaxContentBytes = 512 << 10
	// DefaultTopK / MaxTopK 检索返回条数。
	DefaultTopK = 5
	MaxTopK     = 20
	// candidateLimit 每一路召回的候选数。
	candidateLimit = 50
	// maxSectionChars 命中所在整节返回的字符上限。
	maxSectionChars = 2000
	// maxSnippetChars 命中片段字符上限。
	maxSnippetChars = 240
	// DeletedRetention 软删文档保留期（之后物理清理）。
	DeletedRetention = 30 * 24 * time.Hour
	// reviewNoticeAhead 复审到期前多久开始提醒。
	reviewNoticeAhead = 7 * 24 * time.Hour
	// maxOverviewItems 概览各列表条数。
	maxOverviewItems = 10
	// maxACLEntries 空间 ACL 条数上限。
	maxACLEntries = 200
	// maxLinksPerDocument 单篇文档关联上限。
	maxLinksPerDocument = 50
	// maxTags 标签数上限。
	maxTags = 10
)

// 哨兵错误：reason 来自 api/knowledge/v1 错误码枚举。
var (
	ErrSpaceNotFound      = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_SPACE_NOT_FOUND.String(), "knowledge space not found")
	ErrSpaceCodeExists    = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_SPACE_CODE_EXISTS.String(), "knowledge space code already exists")
	ErrSpaceNotEmpty      = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_SPACE_NOT_EMPTY.String(), "knowledge space still has documents")
	ErrDocumentNotFound   = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_DOCUMENT_NOT_FOUND.String(), "knowledge document not found")
	ErrDocumentConflict   = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_DOCUMENT_CONFLICT.String(), "knowledge document was modified concurrently; reload and retry")
	ErrSectionNotFound    = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_DOCUMENT_NOT_FOUND.String(), "section not found in document")
	ErrSlugExists         = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_SLUG_EXISTS.String(), "document slug already exists in this space")
	ErrForbidden          = kerrors.Forbidden(v1.ErrorReason_KNOWLEDGE_FORBIDDEN.String(), "no permission on this knowledge space")
	ErrInvalidState       = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_INVALID_STATE.String(), "operation not allowed in current document state")
	ErrSecretDetected     = kerrors.BadRequest(v1.ErrorReason_KNOWLEDGE_SECRET_DETECTED.String(), "content contains what looks like a secret; remove it before saving")
	ErrVersionNotFound    = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_VERSION_NOT_FOUND.String(), "document version not found")
	ErrLinkExists         = kerrors.Conflict(v1.ErrorReason_KNOWLEDGE_LINK_EXISTS.String(), "link already exists")
	ErrLinkNotFound       = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_LINK_NOT_FOUND.String(), "link not found")
	ErrIngestNotFound     = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_INGEST_NOT_FOUND.String(), "ingest job not found")
	ErrIngestUnsupported  = kerrors.BadRequest(v1.ErrorReason_KNOWLEDGE_INGEST_UNSUPPORTED.String(), "unsupported file type; supported: md, txt, html, pdf, docx")
	ErrSearchLogNotFound  = kerrors.NotFound(v1.ErrorReason_KNOWLEDGE_SEARCH_LOG_NOT_FOUND.String(), "search log not found")
	ErrContentTooLarge    = kerrors.BadRequest(v1.ErrorReason_KNOWLEDGE_CONTENT_TOO_LARGE.String(), "content exceeds size limit")
	ErrACLInvalid         = kerrors.BadRequest(v1.ErrorReason_KNOWLEDGE_ACL_INVALID.String(), "invalid acl entry")
	ErrInvalidArgument    = kerrors.BadRequest(v1.ErrorReason_KNOWLEDGE_INVALID.String(), "invalid argument")
	ErrEmbeddingDimension = kerrors.InternalServer(v1.ErrorReason_KNOWLEDGE_UNSPECIFIED.String(), "embedding dimension mismatch")
)

// Space 知识空间领域模型。
type Space struct {
	ID               string
	Code             string
	Name             string
	Type             string
	Description      string
	Visibility       int32
	OwnerID          string
	EmbeddingEnabled bool
	ReviewRequired   bool
	ReviewDays       int32
	CreateUser       string
	CreateTime       time.Time
	UpdateTime       time.Time
	// 派生字段（展示用，不落库）。
	Level         int32
	DocumentCount int32
	OwnerName     string
}

// AclEntry 空间访问控制条目。
type AclEntry struct {
	SubjectType int32
	SubjectID   string
	Level       int32
	SubjectName string
}

// Document 知识文档领域模型。
type Document struct {
	ID           string
	SpaceID      string
	Slug         string
	Title        string
	Status       int32
	ReviewState  int32
	ReviewNote   string
	Content      string
	DraftContent *string
	DraftTitle   *string
	// Draft 列表查询里的「有未发布修改」标记（列表不带草稿正文）。
	Draft     bool
	Summary   string
	AISummary string
	Tags      []string
	Source    string
	SourceRef string
	OwnerID   string
	ReviewAt  *time.Time
	// ReviewNotifyTime 最近一次复审提醒时间（巡检去重）。
	ReviewNotifyTime *time.Time
	Version          int32
	IndexStatus      int32
	CreateUser       string
	UpdateUser       string
	CreateTime       time.Time
	UpdateTime       time.Time
	PublishTime      *time.Time
	// 派生字段。
	SpaceName string
	OwnerName string
	Level     int32
	Links     []*Link
	Headings  []string
}

// HasDraft 已发布文档是否有未发布修改。
func (d *Document) HasDraft() bool { return d.Draft || d.DraftContent != nil || d.DraftTitle != nil }

// ReviewOverdue 是否超期未复审（只对已发布文档有意义）。
func (d *Document) ReviewOverdue(now time.Time) bool {
	return d.Status == DocStatusPublished && d.ReviewAt != nil && d.ReviewAt.Before(now)
}

// Version 发布版本。
type Version struct {
	ID          string
	DocumentID  string
	Version     int32
	Title       string
	Content     string
	Note        string
	PublishUser string
	CreateTime  time.Time
	// 派生字段。
	PublishUserName string
}

// Chunk 分块。
type Chunk struct {
	ID          string
	DocumentID  string
	SpaceID     string
	Version     int32
	Seq         int32
	HeadingPath string
	Content     string
	// Tokens 供 to_tsvector('simple', …) 的词元串（zhtext.Document）。
	Tokens         string
	SHA256         string
	EmbeddingModel string
	TokenCount     int32
}

// EmbeddingUpdate 一个分块的向量写入。
type EmbeddingUpdate struct {
	ChunkID string
	Model   string
	Vector  []float32
}

// ChunkHit 一路召回里的一条候选。
type ChunkHit struct {
	ChunkID     string
	DocumentID  string
	SpaceID     string
	Seq         int32
	HeadingPath string
	Content     string
	Score       float64
	Title       string
	SpaceName   string
	ReviewAt    *time.Time
}

// DocHit 标题召回里的一条候选。
type DocHit struct {
	DocumentID string
	Score      float64
}

// SectionKey 定位一节：文档 + 标题路径。
type SectionKey struct {
	DocumentID  string
	HeadingPath string
}

// Link 文档与运维对象的关联。
type Link struct {
	ID         string
	DocumentID string
	TargetType string
	TargetID   string
	TargetName string
	CreateUser string
	CreateTime time.Time
}

// LinkedDocument 按对象反查到的文档。
type LinkedDocument struct {
	LinkID     string
	DocumentID string
	Title      string
	SpaceName  string
	Summary    string
	ReviewAt   *time.Time
}

// IngestJob 文件导入任务。
type IngestJob struct {
	ID         string
	SpaceID    string
	DocumentID *string
	FileID     string
	Filename   string
	Title      string
	Tags       []string
	Publish    bool
	Status     int32
	Error      string
	ChunkCount int32
	Attempts   int32
	CreateUser string
	CreateTime time.Time
	UpdateTime time.Time
	// 派生字段。
	SpaceName string
}

// IndexJob 索引任务。
type IndexJob struct {
	DocumentID  string
	Version     int32
	Status      int32
	Attempts    int32
	Error       string
	NextRunTime time.Time
}

// SearchLog 检索日志。
type SearchLog struct {
	ID         string
	UserID     string
	Source     string
	Query      string
	TopK       int32
	HitCount   int32
	Degraded   bool
	DurationMS int32
	ChunkIDs   []string
	Feedback   int32
	CreateTime time.Time
	// 派生字段。
	UserName string
}

// DocumentFilter 文档列表过滤条件。
type DocumentFilter struct {
	// Unrestricted 为 true 不限空间（超管）；否则只取 SpaceIDs 内的文档。
	Unrestricted bool
	SpaceIDs     []string
	SpaceID      string
	Status       *int32
	Keyword      string
	Tag          string
	OwnerID      string
	// ReviewPending 只取待审核。
	ReviewPending bool
	// ReviewOverdue 只取超期未复审（已发布）。
	ReviewOverdue bool
	// ReviewDueBefore 只取复审时间早于该时刻的已发布文档。
	ReviewDueBefore *time.Time
	// ReviewNoticePending 只取尚未发出本阶段复审提醒的文档：从未提醒过，或已到期且上次提醒早于到期时刻
	//（到期前提醒过一次、到期后还要再提醒一次）。
	ReviewNoticePending bool
	// HideDrafts 隐藏从未发布的草稿，除非文档负责人是 DraftOwnerID 或所属空间在 DraftSpaceIDs（写权限）内。
	HideDrafts    bool
	DraftOwnerID  string
	DraftSpaceIDs []string
	// OrderByPublish 按发布时间倒序（缺省按更新时间倒序）。
	OrderByPublish bool
}

// IngestFilter 导入任务过滤条件。
type IngestFilter struct {
	Unrestricted bool
	SpaceIDs     []string
	SpaceID      string
	Status       *int32
}

// SearchLogFilter 检索日志过滤条件。
type SearchLogFilter struct {
	Source   string
	NoResult bool
	Keyword  string
}

// DocumentStats 文档统计。
type DocumentStats struct {
	SpaceCount         int64
	PublishedCount     int64
	DraftCount         int64
	ReviewOverdueCount int64
	PendingReviewCount int64
}

// SearchStats 检索统计。
type SearchStats struct {
	SearchCount    int64
	NoResultCount  int64
	HelpfulCount   int64
	UnhelpfulCount int64
	TopNoResult    []QueryCount
}

// QueryCount 检索词计数。
type QueryCount struct {
	Query string
	Count int64
}

// SpaceRepo 空间仓储（data 层实现）。
type SpaceRepo interface {
	// Create 落一行；编码冲突返回 ErrSpaceCodeExists。
	Create(ctx context.Context, s *Space) error
	// Update 更新可变字段。
	Update(ctx context.Context, s *Space) error
	// Get 取未删除空间；不存在返回 ErrSpaceNotFound。
	Get(ctx context.Context, id string) (*Space, error)
	// List 全部未删除空间（关键字匹配名称 / 编码，type 精确），按名称排序。
	List(ctx context.Context, keyword, typ string) ([]*Space, error)
	// Names 给定空间中未删除者的名称（空间 ID → 名称）；已删除 / 不存在的不出现在结果里。
	Names(ctx context.Context, ids []string) (map[string]string, error)
	// SoftDelete 软删；仍有未删除文档返回 ErrSpaceNotEmpty，不存在返回 ErrSpaceNotFound。
	SoftDelete(ctx context.Context, id string, at time.Time) error
	// DocumentCounts 各空间已发布文档数。
	DocumentCounts(ctx context.Context) (map[string]int32, error)
	// ListAcl 空间 ACL。
	ListAcl(ctx context.Context, spaceID string) ([]*AclEntry, error)
	// ReplaceAcl 整组替换 ACL。
	ReplaceAcl(ctx context.Context, spaceID string, entries []*AclEntry, userID string) error
	// Levels 解析主体在全部未删除空间上的级别：公开空间至少 1；负责人 3；ACL 里用户条目与
	// roleIDs 内角色条目取最大。级别为 0 的空间不出现在结果里。
	Levels(ctx context.Context, userID string, roleIDs []string) (map[string]int32, error)
}

// DocumentRepo 文档仓储（data 层实现）。
type DocumentRepo interface {
	// Create 落一行；slug 冲突返回 ErrSlugExists。
	Create(ctx context.Context, d *Document) error
	// Update 更新可变字段（状态、正文、草稿、元数据、版本号；不含标签与索引状态，二者各有专用写入）。
	// 乐观锁：只命中 version 仍等于 expectedVersion（调用方读到的版本）的行，否则返回 ErrDocumentConflict。
	// ctx 携带事务时在事务内执行。
	Update(ctx context.Context, d *Document, expectedVersion int32) error
	// SetTags 覆盖用户标签（与 AI 标签写入互不覆盖的专用路径）；ctx 携带事务时在事务内执行。
	SetTags(ctx context.Context, id string, tags []string) error
	// Get 取未删除文档；不存在返回 ErrDocumentNotFound。ctx 携带事务时在事务内执行。
	Get(ctx context.Context, id string) (*Document, error)
	// List 分页列表（不含正文）。
	List(ctx context.Context, f DocumentFilter, page, pageSize int64) ([]*Document, int64, error)
	// SoftDelete 软删。
	SoftDelete(ctx context.Context, id string, at time.Time) error
	// MarkReviewNotified 记录复审提醒时间。
	MarkReviewNotified(ctx context.Context, id string, at time.Time) error
	// SetAISummary 写入 AI 摘要与（标签为空时的）AI 标签。
	SetAISummary(ctx context.Context, id, summary string, tags []string) error
	// SetIndexStatus 更新索引状态。
	SetIndexStatus(ctx context.Context, id string, status int32) error
	// SetIndexStatusIfVersion 围栏更新索引状态：只在文档仍是已发布且版本等于 version 时写入（索引任务回写用，
	// 文档期间又发布了新版本 / 归档时不覆盖新版本排队中的状态）。
	SetIndexStatusIfVersion(ctx context.Context, id string, status, version int32) error
	// ResetReviewNotified 清空复审提醒时间（复审日期变化时调用；通用 Update 不写该列）；ctx 携带事务时在事务内执行。
	ResetReviewNotified(ctx context.Context, id string) error
	// PublishedIDs 某空间全部已发布文档 ID（重建索引用）。
	PublishedIDs(ctx context.Context, spaceID string) ([]string, error)
	// CountVisible 可见空间内已发布文档数（unrestricted 为 true 不限空间）。
	CountVisible(ctx context.Context, unrestricted bool, spaceIDs []string) (int64, error)
	// Stats 全局统计。
	Stats(ctx context.Context, now time.Time) (DocumentStats, error)
	// PurgeDeleted 物理清理软删超过保留期的文档（连同版本、关联）；返回清理条数。
	PurgeDeleted(ctx context.Context, before time.Time, limit int) (int, error)
}

// VersionRepo 版本仓储。
type VersionRepo interface {
	// CreateTx 事务内落一行。
	CreateTx(ctx context.Context, tx txrun.Tx, v *Version) error
	// List 某文档全部版本（不含正文），版本号倒序。
	List(ctx context.Context, documentID string) ([]*Version, error)
	// Get 某版本（含正文）；不存在返回 ErrVersionNotFound。
	Get(ctx context.Context, documentID string, version int32) (*Version, error)
}

// ChunkRepo 分块仓储。
type ChunkRepo interface {
	// VectorAvailable 向量列是否可用（启动期探测 pgvector）。
	VectorAvailable() bool
	// ReplaceTx 事务内整篇替换分块：删旧插新；旧分块里内容摘要与模型都相同的向量复用到新分块上。
	ReplaceTx(ctx context.Context, tx txrun.Tx, documentID string, chunks []*Chunk) error
	// DeleteTx 事务内删除某文档全部分块。
	DeleteTx(ctx context.Context, tx txrun.Tx, documentID string) error
	// ListUnembedded 某文档尚未用 model 向量化的分块（含正文）。
	ListUnembedded(ctx context.Context, documentID, model string) ([]*Chunk, error)
	// SetEmbeddings 批量写入向量。
	SetEmbeddings(ctx context.Context, items []EmbeddingUpdate) error
	// FullText 全文召回：tsquery 为 to_tsquery('simple', …) 的表达式。
	FullText(ctx context.Context, tsquery string, unrestricted bool, spaceIDs, tags []string, limit int) ([]*ChunkHit, error)
	// Vector 向量召回（余弦相似度）。
	Vector(ctx context.Context, vector []float32, unrestricted bool, spaceIDs, tags []string, limit int) ([]*ChunkHit, error)
	// Titles 标题召回（trgm 相似度 / 包含）。
	Titles(ctx context.Context, query string, unrestricted bool, spaceIDs, tags []string, limit int) ([]*DocHit, error)
	// Sections 取若干节的正文（同文档同标题路径的分块按序拼接，各节裁到 maxChars）。
	Sections(ctx context.Context, keys []SectionKey, maxChars int) (map[SectionKey]string, error)
	// Counts 分块总数与已向量化数。
	Counts(ctx context.Context) (total, embedded int64, err error)
}

// LinkRepo 关联仓储。
type LinkRepo interface {
	// Create 落一行；重复返回 ErrLinkExists。
	Create(ctx context.Context, l *Link) error
	// Delete 删除；不存在返回 ErrLinkNotFound。
	Delete(ctx context.Context, documentID, linkID string) error
	// ListByDocument 某文档全部关联。
	ListByDocument(ctx context.Context, documentID string) ([]*Link, error)
	// ListByTarget 按对象反查已发布且可见的文档。
	ListByTarget(ctx context.Context, targetType, targetID string, unrestricted bool, spaceIDs []string) ([]*LinkedDocument, error)
}

// IngestRepo 导入任务仓储。
type IngestRepo interface {
	Create(ctx context.Context, j *IngestJob) error
	// Get 不存在返回 ErrIngestNotFound。
	Get(ctx context.Context, id string) (*IngestJob, error)
	List(ctx context.Context, f IngestFilter, page, pageSize int64) ([]*IngestJob, int64, error)
	// Requeue 失败任务重新排队；不是失败状态不命中（返回 false）。
	Requeue(ctx context.Context, id string) (bool, error)
	// Claim 领取一批可执行任务（排队的，或处理中但租约已过期的），置处理中并续租；SKIP LOCKED。
	Claim(ctx context.Context, lease time.Duration, limit int) ([]*IngestJob, error)
	// Finish 写入结果：终态清空租约；status 为处理中（可重试失败）时保留租约，到期后才会被 Claim 重新领取。
	Finish(ctx context.Context, id string, status int32, documentID *string, chunkCount int32, errMsg string) error
	// SetDocumentTx 在建档事务内把任务指向新建的文档：任务重领时据此续作而不是再建一篇。
	SetDocumentTx(ctx context.Context, tx txrun.Tx, id, documentID string) error
	// PurgeFinished 物理清理已结束（成功 / 失败）且 update_time 早于 now() - olderThan 的任务，至多 limit 条；
	// 返回清理条数。只删终态行：排队 / 处理中（含租约过期等待重领）的任务不碰。
	PurgeFinished(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// IndexRepo 索引任务仓储。
type IndexRepo interface {
	// UpsertTx 事务内排队（每篇文档至多一条：覆盖为最新版本、重置重试）。
	UpsertTx(ctx context.Context, tx txrun.Tx, documentID string, version int32) error
	// Upsert 非事务排队。
	Upsert(ctx context.Context, documentID string, version int32) error
	// DeleteTx 事务内删除。
	DeleteTx(ctx context.Context, tx txrun.Tx, documentID string) error
	// Claim 领取到期任务；SKIP LOCKED。
	Claim(ctx context.Context, lease time.Duration, limit int) ([]*IndexJob, error)
	// Finish 写入结果：成功删除任务行；失败按 nextRun 重排或置终态失败。
	Finish(ctx context.Context, j *IndexJob, status int32, errMsg string, nextRun time.Time) error
	// Counts 排队中与失败的任务数。
	Counts(ctx context.Context) (pending, failed int64, err error)
}

// SearchLogRepo 检索日志仓储。
type SearchLogRepo interface {
	Create(ctx context.Context, l *SearchLog) error
	// SetFeedback 只命中本人的记录；不存在返回 ErrSearchLogNotFound。
	SetFeedback(ctx context.Context, id, userID string, feedback int32) error
	List(ctx context.Context, f SearchLogFilter, page, pageSize int64) ([]*SearchLog, int64, error)
	Stats(ctx context.Context, since time.Time) (SearchStats, error)
	// Purge 清理保留期之外的日志；返回条数。
	Purge(ctx context.Context, before time.Time, limit int) (int, error)
}

// RoleSource 跨域窄接口（system 域 AuthzUsecase 实现）：用户的启用角色；超管 (true, nil)。
type RoleSource interface {
	EnabledRoleIDs(ctx context.Context, userID string) (isAdmin bool, roleIDs []string, err error)
}

// Directory 跨域窄接口（system 域组织目录实现）：用户显示名。
type Directory interface {
	UserNames(ctx context.Context, ids []string) (map[string]string, error)
}

// RoleRef 角色的最小披露视图。
type RoleRef struct {
	ID      string
	Name    string
	Enabled bool
}

// RoleDirectory 跨域窄接口（system 域角色用例适配）：全部角色（ACL 校验与展示）。
type RoleDirectory interface {
	ListRoles(ctx context.Context) ([]RoleRef, error)
}

// Embedder 向量化服务（data 层以 OpenAI 兼容协议实现）。
type Embedder interface {
	// Enabled 是否配置了 api_key。
	Enabled() bool
	// Model 模型名。
	Model() string
	// Embed 批量向量化；返回向量与输入一一对应，维度须为 EmbeddingDimensions。
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Summarizer 跨域窄接口（助手域模型客户端适配）：单次文本生成。
type Summarizer interface {
	Available() bool
	Summarize(ctx context.Context, system, prompt string) (string, error)
}

// Files 跨域窄接口（file 域用例实现）：导入源文件读取与附件绑定 / 释放。
type Files interface {
	// Get 文件元数据（访问控制：临时文件仅上传者本人）。
	Get(ctx context.Context, userID, id string) (*bizfile.File, error)
	// Download 文件内容（权限同 Get）。
	Download(ctx context.Context, userID, id string) (*bizfile.File, []byte, error)
	// Bind 调用方事务内把 uploader 上传的临时文件绑定到业务对象。
	Bind(ctx context.Context, tx txrun.Tx, uploader, bizType, bizID string, fileIDs []string) error
	// Release 调用方事务内软删某业务对象的全部附件。
	Release(ctx context.Context, tx txrun.Tx, bizType, bizID string) error
}
