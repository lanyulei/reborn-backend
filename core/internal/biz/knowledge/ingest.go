package biz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/docparse"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 导入任务参数。
const (
	// ingestLease 领取租约。
	ingestLease = 5 * time.Minute
	// ingestBatch 每轮领取条数。
	ingestBatch = 4
	// parseTimeout 单个文件解析上限。
	parseTimeout = 60 * time.Second
	// ingestMaxAttempts 自动重试上限（超过后留在失败态，可手工重试）。
	ingestMaxAttempts = 3
)

// IngestUsecase 文件导入用例：创建任务、后台解析、重试。
type IngestUsecase struct {
	spaces *SpaceUsecase
	docs   *DocumentUsecase
	jobs   IngestRepo
	files  Files
	logger *slog.Logger
	now    func() time.Time
}

// NewIngestUsecase new an IngestUsecase.
func NewIngestUsecase(spaces *SpaceUsecase, docs *DocumentUsecase, jobs IngestRepo, files Files, logger *slog.Logger) *IngestUsecase {
	return &IngestUsecase{spaces: spaces, docs: docs, jobs: jobs, files: files, logger: logger, now: time.Now}
}

// CreateIngestInput 创建导入任务入参。
type CreateIngestInput struct {
	SpaceID    string
	FileID     string
	DocumentID string
	Title      string
	Tags       []string
	Publish    bool
}

// Create 创建导入任务（需空间写权限）：校验文件归属与类型后排队。
func (uc *IngestUsecase) Create(ctx context.Context, userID string, in CreateIngestInput) (*IngestJob, error) {
	space, _, err := uc.spaces.Require(ctx, userID, in.SpaceID, LevelWrite)
	if err != nil {
		return nil, err
	}
	f, err := uc.files.Get(ctx, userID, in.FileID)
	if err != nil {
		return nil, err
	}
	if docparse.Kind(f.Filename, f.Mime) == "" {
		return nil, ErrIngestUnsupported
	}
	tags, err := normalizeTags(in.Tags)
	if err != nil {
		return nil, err
	}
	var docID *string
	if in.DocumentID != "" {
		doc, err := uc.docs.docs.Get(ctx, in.DocumentID)
		if err != nil {
			return nil, err
		}
		if doc.SpaceID != space.ID || doc.Status == DocStatusArchived {
			return nil, ErrInvalidState
		}
		docID = &doc.ID
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(in.Title)
	if title == "" {
		title = strings.TrimSuffix(f.Filename, extOf(f.Filename))
	}
	now := uc.now()
	job := &IngestJob{
		ID: id, SpaceID: space.ID, DocumentID: docID, FileID: f.ID, Filename: f.Filename, Title: title, Tags: tags, Publish: in.Publish,
		Status: JobQueued, CreateUser: userID, CreateTime: now, UpdateTime: now, SpaceName: space.Name,
	}
	if err := uc.jobs.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

// List 导入任务列表（只看可见空间）。
func (uc *IngestUsecase) List(ctx context.Context, userID string, f IngestFilter, page, pageSize int64) ([]*IngestJob, int64, error) {
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, 0, err
	}
	f.Unrestricted = acc.Unrestricted()
	if !f.Unrestricted {
		f.SpaceIDs = acc.IDs(LevelRead)
		if len(f.SpaceIDs) == 0 {
			return []*IngestJob{}, 0, nil
		}
	}
	items, total, err := uc.jobs.List(ctx, f, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if err := uc.fillSpaceNames(ctx, items); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// Retry 重试失败任务（需空间写权限）。
func (uc *IngestUsecase) Retry(ctx context.Context, userID, id string) (*IngestJob, error) {
	job, err := uc.jobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := uc.spaces.Require(ctx, userID, job.SpaceID, LevelWrite); err != nil {
		return nil, err
	}
	ok, err := uc.jobs.Requeue(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrInvalidState
	}
	job, err = uc.jobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := uc.fillSpaceNames(ctx, []*IngestJob{job}); err != nil {
		return nil, err
	}
	return job, nil
}

// Run 领取并处理一批导入任务（kit/loop 约定）。
func (uc *IngestUsecase) Run(ctx context.Context) (int, error) {
	jobs, err := uc.jobs.Claim(ctx, ingestLease, ingestBatch)
	if err != nil || len(jobs) == 0 {
		return 0, err
	}
	for _, job := range jobs {
		uc.process(ctx, job)
	}
	return len(jobs), nil
}

// process 处理一条任务：下载 → 解析 → 建文档 / 覆盖 → 终态。
func (uc *IngestUsecase) process(ctx context.Context, job *IngestJob) {
	doc, n, err := uc.run(ctx, job)
	if err != nil {
		uc.logger.WarnContext(ctx, "knowledge: ingest failed", "job", job.ID, "attempt", job.Attempts, "error", err)
		status := JobFailed
		// 解析超时不重试：解析库不接受 ctx，超时的 goroutine 仍在后台跑完；重试只会为同一个坏文件再起一个。
		if job.Attempts < ingestMaxAttempts && !errors.Is(err, ErrIngestUnsupported) && !errors.Is(err, ErrSecretDetected) &&
			!errors.Is(err, ErrContentTooLarge) && !errors.Is(err, errParseTimeout) {
			status = JobRunning // 保持处理中并保留租约：租约（ingestLease）到期后才会被重新领取，充当重试退避
		}
		if ferr := uc.jobs.Finish(context.WithoutCancel(ctx), job.ID, status, job.DocumentID, 0, truncateRunes(err.Error(), 1000)); ferr != nil {
			uc.logger.ErrorContext(ctx, "knowledge: finish ingest job", "job", job.ID, "error", ferr)
		}
		return
	}
	if ferr := uc.jobs.Finish(context.WithoutCancel(ctx), job.ID, JobSucceeded, &doc.ID, clampInt32(n), ""); ferr != nil {
		uc.logger.ErrorContext(ctx, "knowledge: finish ingest job", "job", job.ID, "error", ferr)
	}
}

// run 发布方式按创建者当前在空间的级别判定（与手动建档 / 发布同一口径：要求审核的空间里非管理级别转提交审核）。
// 任务指向的文档若是本任务上一次建好的（来源 import 且源文件就是本任务的文件），说明上次建档已提交、只是没来得及
// 写任务终态：续作发布 / 提交审核，不再解析建档。
func (uc *IngestUsecase) run(ctx context.Context, job *IngestJob) (*Document, int, error) {
	space, err := uc.spaces.repo.Get(ctx, job.SpaceID)
	if err != nil {
		return nil, 0, err
	}
	acc, err := uc.spaces.Resolve(ctx, job.CreateUser)
	if err != nil {
		return nil, 0, err
	}
	level := acc.Level(space.ID)
	if job.DocumentID != nil {
		doc, err := uc.docs.docs.Get(ctx, *job.DocumentID)
		if err != nil {
			return nil, 0, err
		}
		if doc.Source == SourceImport && doc.SourceRef == job.FileID {
			return uc.docs.ingestResume(ctx, job.CreateUser, space, doc, job.Publish, level)
		}
	}
	f, data, err := uc.files.Download(ctx, job.CreateUser, job.FileID)
	if err != nil {
		return nil, 0, fmt.Errorf("read source file: %w", err)
	}
	kind := docparse.Kind(f.Filename, f.Mime)
	if kind == "" {
		return nil, 0, ErrIngestUnsupported
	}
	md, err := parseWithTimeout(ctx, kind, data)
	if err != nil {
		return nil, 0, err
	}
	if job.DocumentID != nil {
		return uc.docs.ingestReplace(ctx, job.CreateUser, space, *job.DocumentID, job.Title, md, job.Publish, job.FileID, level)
	}
	return uc.docs.ingestCreate(ctx, job.CreateUser, space, CreateDocumentInput{
		SpaceID: space.ID, Title: job.Title, Content: md, Tags: job.Tags, Publish: job.Publish,
	}, job.FileID, level, func(ctx context.Context, tx txrun.Tx, docID string) error {
		return uc.jobs.SetDocumentTx(ctx, tx, job.ID, docID)
	})
}

// errParseTimeout 解析超时（终态失败，不自动重试）。
var errParseTimeout = errors.New("parse timeout")

// parseWithTimeout 解析在独立 goroutine 执行并受 parseTimeout 约束：解析库本身不接受 ctx，超时后结果被丢弃。
func parseWithTimeout(ctx context.Context, kind string, data []byte) (string, error) {
	type result struct {
		md  string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		md, err := docparse.Parse(kind, data)
		ch <- result{md, err}
	}()
	timer := time.NewTimer(parseTimeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		if errors.Is(r.err, docparse.ErrUnsupported) {
			return "", ErrIngestUnsupported
		}
		return r.md, r.err
	case <-timer.C:
		return "", errParseTimeout
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (uc *IngestUsecase) fillSpaceNames(ctx context.Context, jobs []*IngestJob) error {
	if len(jobs) == 0 {
		return nil
	}
	spaces, err := uc.spaces.repo.List(ctx, "", "")
	if err != nil {
		return err
	}
	names := make(map[string]string, len(spaces))
	for _, s := range spaces {
		names[s.ID] = s.Name
	}
	for _, j := range jobs {
		j.SpaceName = names[j.SpaceID]
	}
	return nil
}

func extOf(name string) string {
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		return name[i:]
	}
	return ""
}
