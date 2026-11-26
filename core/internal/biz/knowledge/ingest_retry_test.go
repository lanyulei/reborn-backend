package biz

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/docparse"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// fakeIngestRepo 内存导入任务仓储：只记录 process 的落账结果（状态与错误）。
type fakeIngestRepo struct {
	finished []fakeIngestFinish
}

type fakeIngestFinish struct {
	status int32
	errMsg string
}

func (f *fakeIngestRepo) Create(context.Context, *IngestJob) error { return nil }

func (f *fakeIngestRepo) Get(context.Context, string) (*IngestJob, error) {
	return nil, ErrIngestNotFound
}

func (f *fakeIngestRepo) List(context.Context, IngestFilter, int64, int64) ([]*IngestJob, int64, error) {
	return nil, 0, nil
}
func (f *fakeIngestRepo) Requeue(context.Context, string) (bool, error) { return false, nil }
func (f *fakeIngestRepo) Claim(context.Context, time.Duration, int) ([]*IngestJob, error) {
	return nil, nil
}

func (f *fakeIngestRepo) Finish(_ context.Context, _ string, status int32, _ *string, _ int32, errMsg string) error {
	f.finished = append(f.finished, fakeIngestFinish{status: status, errMsg: errMsg})
	return nil
}

func (f *fakeIngestRepo) SetDocumentTx(context.Context, txrun.Tx, string, string) error { return nil }

func (f *fakeIngestRepo) PurgeFinished(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

// fakeIngestFiles 导入源文件桩：Download 返回预置内容或错误。
type fakeIngestFiles struct {
	data []byte
	err  error
}

func (f *fakeIngestFiles) Get(context.Context, string, string) (*bizfile.File, error) {
	return &bizfile.File{ID: "f1", Filename: "note.txt", Mime: "text/plain"}, nil
}

func (f *fakeIngestFiles) Download(context.Context, string, string) (*bizfile.File, []byte, error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	return &bizfile.File{ID: "f1", Filename: "note.txt", Mime: "text/plain"}, f.data, nil
}

func (f *fakeIngestFiles) Bind(context.Context, txrun.Tx, string, string, string, []string) error {
	return nil
}

func (f *fakeIngestFiles) Release(context.Context, txrun.Tx, string, string) error { return nil }

// newIngestUsecaseForTest 导入用例：空间与文档仓储复用既有内存实现，文件与任务仓储用桩。
func newIngestUsecaseForTest(files *fakeIngestFiles, docRepo *fakeDocumentRepo) (*IngestUsecase, *fakeIngestRepo) {
	spaces, _ := newSpaceUsecaseForTest()
	docs := NewDocumentUsecase(spaces, nil, docRepo, &fakeVersionRepo{}, fakeChunkRepo{}, fakeLinkRepo{}, fakeIndexRepo{}, files, fakeTxm{}, nil, fakeDirectory{})
	jobs := &fakeIngestRepo{}
	return NewIngestUsecase(spaces, docs, jobs, files, slog.Default()), jobs
}

// 导入任务的确定性失败直接置失败：文件编码不支持（不再重试 3 次、最后报数据库的编码错误），以及建队之后目标
// 文档 / 空间被删除、目标文档已归档。瞬时故障（对象存储不可用等）仍保留租约等待重试。
func TestIngestProcess_FatalErrorsFailImmediately(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gbk := []byte{0xb8, 0xe6, 0xbe, 0xaf, 0xb4, 0xa6, 0xc0, 0xed, 0xb1, 0xca, 0xbc, 0xc7} // 「告警处理笔记」的 GBK 编码
	deletedSpace := "018f3e1a-0000-7000-8000-0000000000e3"

	archived := &Document{ID: "018f3e1a-0000-7000-8000-0000000000d2", SpaceID: tPub, Title: "旧文档", Status: DocStatusArchived, Source: SourceManual}
	cases := []struct {
		name   string
		file   *fakeIngestFiles
		seed   *Document // 预置的目标文档
		docID  *string
		space  string
		fatal  bool
		reason string
	}{
		{name: "gbk text", file: &fakeIngestFiles{data: gbk}, space: tPub, fatal: true, reason: docparse.ErrEncoding.Error()},
		{name: "target document deleted", file: &fakeIngestFiles{data: []byte("正文")}, docID: &archived.ID, space: tPub, fatal: true, reason: "KNOWLEDGE_DOCUMENT_NOT_FOUND"},
		{name: "space deleted", file: &fakeIngestFiles{data: []byte("正文")}, space: deletedSpace, fatal: true, reason: "KNOWLEDGE_SPACE_NOT_FOUND"},
		{name: "target document archived", file: &fakeIngestFiles{data: []byte("正文")}, seed: archived, docID: &archived.ID, space: tPub, fatal: true, reason: "KNOWLEDGE_INVALID_STATE"},
		{name: "object store unavailable", file: &fakeIngestFiles{err: errors.New("object store unavailable")}, space: tPub, fatal: false},
	}
	for _, tc := range cases {
		docRepo := newFakeDocumentRepo()
		if tc.seed != nil {
			docRepo.docs[tc.seed.ID] = tc.seed
		}
		uc, jobs := newIngestUsecaseForTest(tc.file, docRepo)
		// 第 1 次尝试（未到 ingestMaxAttempts）：此前这些失败都被当成可重试而保留租约。
		uc.process(ctx, &IngestJob{ID: "j1", SpaceID: tc.space, DocumentID: tc.docID, FileID: "f1", Title: "告警处理笔记", CreateUser: tOwner, Attempts: 1})
		if len(jobs.finished) != 1 {
			t.Fatalf("%s: Finish called %d times, want 1", tc.name, len(jobs.finished))
		}
		got := jobs.finished[0]
		if tc.fatal && got.status != JobFailed {
			t.Errorf("%s: status = %d, want JobFailed (err = %q)", tc.name, got.status, got.errMsg)
		}
		if !tc.fatal && got.status != JobRunning {
			t.Errorf("%s: status = %d, want JobRunning to keep the lease for retry (err = %q)", tc.name, got.status, got.errMsg)
		}
		if tc.reason != "" && !strings.Contains(got.errMsg, tc.reason) {
			t.Errorf("%s: error = %q, want it to mention %q", tc.name, got.errMsg, tc.reason)
		}
	}
}

// 编码错误写入任务的原因就是给人看的那句话（不是包装过的内部错误），且 UTF-16 的文件能正常导入。
func TestIngestProcess_EncodingErrorIsUserFacing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gbk := []byte{0xd6, 0xd0, 0xce, 0xc4} // 「中文」的 GBK 编码
	uc, jobs := newIngestUsecaseForTest(&fakeIngestFiles{data: gbk}, newFakeDocumentRepo())
	uc.process(ctx, &IngestJob{ID: "j1", SpaceID: tPub, FileID: "f1", Title: "笔记", CreateUser: tOwner, Attempts: 1})
	if len(jobs.finished) != 1 || jobs.finished[0].status != JobFailed {
		t.Fatalf("finished = %+v, want one JobFailed", jobs.finished)
	}
	if got, want := jobs.finished[0].errMsg, "文件不是 UTF-8 编码，请转存为 UTF-8（或带 BOM 的 UTF-16）后重新导入"; got != want {
		t.Fatalf("job error = %q, want %q", got, want)
	}

	// 同一段文字存成带 BOM 的 UTF-16LE：正常建档。
	utf16le := []byte{0xFF, 0xFE}
	for _, u := range utf16.Encode([]rune("中文正文")) {
		utf16le = binary.LittleEndian.AppendUint16(utf16le, u)
	}
	docRepo := newFakeDocumentRepo()
	uc, jobs = newIngestUsecaseForTest(&fakeIngestFiles{data: utf16le}, docRepo)
	uc.process(ctx, &IngestJob{ID: "j2", SpaceID: tPub, FileID: "f1", Title: "笔记", CreateUser: tOwner, Attempts: 1})
	if len(jobs.finished) != 1 || jobs.finished[0].status != JobSucceeded {
		t.Fatalf("finished = %+v, want one JobSucceeded", jobs.finished)
	}
	if len(docRepo.docs) != 1 {
		t.Fatalf("imported documents = %d, want 1", len(docRepo.docs))
	}
	for _, d := range docRepo.docs {
		if d.Content != "中文正文\n" || d.Source != SourceImport || d.SourceRef != "f1" {
			t.Fatalf("imported document = %+v", d)
		}
	}
}
