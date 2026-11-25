// Package biz 文件域业务核心：通用附件的上传、访问控制、绑定与清理。
// 文件内容经 storage.ObjectStore 存取（驱动无关），元数据经 Repo 落库。
package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"
	"unicode"

	v1 "github.com/lanyulei/reborn/backend/core/api/file/v1"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/idgen"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 文件状态（file_object.status）。
const (
	// StatusTemp 临时：已上传、尚未绑定到业务对象。
	StatusTemp int32 = 1
	// StatusBound 已绑定到业务对象。
	StatusBound int32 = 2
)

const (
	// MaxFileSize 单文件上限（与 proto 校验一致）。
	MaxFileSize = 5 << 20
	// TempRetention 临时文件保留期：超过仍未绑定即由后台清理。
	TempRetention = 24 * time.Hour
	// DeletedRetention 软删文件的保留期：留出回滚 / 排障窗口后物理清理。
	DeletedRetention = 24 * time.Hour
	// purgeBatch 后台清理单批条数。
	purgeBatch = 100
	// maxBindFiles 单次绑定的文件数上限。
	maxBindFiles = 50
	// maxListFiles 列出一个业务对象的附件时的条数上限：业务对象可以分多次绑定（如工单提单时
	// 绑一批、处理人之后再补），总数由业务域自己约束，这里只是防御性的上限。
	maxListFiles = 200
	// MaxTempFiles 单个用户同时持有的未绑定临时文件数上限（上传配额：临时文件 24h 后
	// 才清理，不设上限则任何登录用户都能持续占用存储）。
	MaxTempFiles = 100
	// PurgeRetryAfter 对象删除失败的文件隔多久再重试清理。
	PurgeRetryAfter = time.Hour
)

// 哨兵错误：reason 来自 api/file/v1 错误码枚举。
var (
	// ErrFileNotFound 文件不存在 / 已删除 / 不是当前用户可绑定的临时文件。
	ErrFileNotFound = kerrors.NotFound(v1.ErrorReason_FILE_NOT_FOUND.String(), "file not found")
	// ErrFileTypeNotAllowed 文件类型不在允许范围内。
	ErrFileTypeNotAllowed = kerrors.BadRequest(v1.ErrorReason_FILE_TYPE_NOT_ALLOWED.String(), "file type is not allowed")
	// ErrFileForbidden 无权访问文件所属业务对象。
	ErrFileForbidden = kerrors.Forbidden(v1.ErrorReason_FILE_FORBIDDEN.String(), "no permission to access this file")
	// ErrFileNameInvalid 文件名不合法。
	ErrFileNameInvalid = kerrors.BadRequest(v1.ErrorReason_FILE_NAME_INVALID.String(), "invalid file name")
	// ErrFileQuotaExceeded 未绑定的临时文件数达到上限。
	ErrFileQuotaExceeded = kerrors.New(429, v1.ErrorReason_FILE_QUOTA_EXCEEDED.String(), "too many unused uploads; submit or wait for them to expire")
	// errSingletonQuotaExceeded 换头像 / logo 时操作者的未绑定临时文件配额已满（PutSingleton 也先落一条临时文件）。
	// 与 ErrFileQuotaExceeded 是同一 reason 与状态码（对外契约不变，errors.Is 仍匹配得到），仅文案区分：
	// 用户此时并没有在「上传附件」，「先提交或等其过期」的提示对不上他正在做的事。
	errSingletonQuotaExceeded = kerrors.New(429, v1.ErrorReason_FILE_QUOTA_EXCEEDED.String(), "unused upload limit reached, cannot save this file; submit or discard pending uploads, or wait for them to expire")
)

// File 是文件元数据领域模型。
type File struct {
	ID         string
	Driver     string
	ObjectKey  string
	Filename   string
	Size       int64
	Mime       string
	SHA256     string
	Status     int32
	BizType    string
	BizID      string
	CreateUser string
	CreateTime time.Time
}

// Repo 是文件元数据仓储接口，由 data 层实现（依赖倒置）。
type Repo interface {
	// Create 落一行临时文件元数据；上传者未绑定的临时文件数已达 maxTemp 时不写入并返回
	// ErrFileQuotaExceeded（单语句判定 + 写入）。
	Create(ctx context.Context, f *File, maxTemp int) error
	// Get 按 ID 查询未删除文件；不存在返回 ErrFileNotFound。
	Get(ctx context.Context, id string) (*File, error)
	// Discard 物理删除一行尚未绑定的临时文件元数据（上传中途失败的回退；幂等）。
	Discard(ctx context.Context, id string) error
}

// BindRepo 是文件绑定仓储接口（经调用方事务句柄执行，与业务写同事务）。
type BindRepo interface {
	// BindTx 把 uploader 本人上传的临时文件绑定到业务对象；返回实际绑定的行数
	// （条件更新：只命中 status=临时 且上传者匹配的行，重复绑定不命中）。
	BindTx(ctx context.Context, tx txrun.Tx, uploader, bizType, bizID string, fileIDs []string) (int64, error)
	// ReleaseTx 软删某业务对象的全部附件（业务对象删除时调用）。
	ReleaseTx(ctx context.Context, tx txrun.Tx, bizType, bizID string) error
	// ReleaseFilesTx 软删某业务对象的指定附件：只命中确实绑定在该对象上的文件（别的对象的
	// 文件、临时文件都不命中）。
	ReleaseFilesTx(ctx context.Context, tx txrun.Tx, bizType, bizID string, fileIDs []string) error
	// ListByBiz 某业务对象的全部未删除附件，按上传顺序；至多 limit 个。
	ListByBiz(ctx context.Context, bizType, bizID string, limit int) ([]*File, error)
	// PromoteSingleton 把临时文件 id 立为某业务对象「唯一的那一份」：同一事务里（按业务对象
	// 串行）软删它原有的文件、再把 id 绑定上去。ifAbsent 为真时该对象已有文件则什么都不做并
	// 返回 false。id 不是未删除的临时文件返回 ErrFileNotFound。
	PromoteSingleton(ctx context.Context, id, bizType, bizID string, ifAbsent bool) (bool, error)
	// LatestByBiz 某业务对象最新的一个未删除文件（含驱动与对象键）；没有返回 ErrFileNotFound。
	LatestByBiz(ctx context.Context, bizType, bizID string) (*File, error)
}

// PurgeRepo 是后台清理仓储接口。
type PurgeRepo interface {
	// PurgeExpired 领取一批待清理文件（过期临时文件 / 超过保留期的软删文件）并打上领取标记，
	// 事务外对每个文件调用 remove 删除对象，成功的物理删除元数据行，失败的保留领取标记、
	// retryAfter 之内不再领取（坏行不会堵住队列）；返回成功清理的条数。
	// 多实例并发安全：领取使用 SKIP LOCKED + 领取标记，同一行不会被两个实例同时处理。
	PurgeExpired(ctx context.Context, tempBefore, deletedBefore time.Time, retryAfter time.Duration, limit int, remove func(ctx context.Context, f *File) error) (int, error)
}

// AccessChecker 由持有附件的业务域实现：判定用户能否读取某业务对象的附件。
type AccessChecker interface {
	// CanReadFile 报告 userID 是否可读取 bizID 对应业务对象的附件 fileID（业务域可以细到单个
	// 文件：例如它挂在一个对该用户隐藏的表单字段上）。
	CanReadFile(ctx context.Context, userID, bizID, fileID string) (bool, error)
}

// AccessCheckers 业务类型 → 访问检查器（装配期构造，只读）。未登记的业务类型一律拒绝读取。
type AccessCheckers map[string]AccessChecker

// Stores 存储驱动名 → 驱动实例（装配期构造，只读）；Default 为新上传文件使用的驱动。
type Stores struct {
	Default string
	Drivers map[string]storage.ObjectStore
}

// allowedMimes 允许的内容类型白名单（按内容嗅探结果判定，不信任扩展名与客户端声明）。
// 不含 HTML / SVG / XML / 脚本等可在浏览器执行的类型；下载恒以附件方式返回。
var allowedMimes = map[string]struct{}{
	"image/png": {}, "image/jpeg": {}, "image/gif": {}, "image/webp": {}, "image/bmp": {},
	"application/pdf":              {},
	"text/plain":                   {},
	"application/zip":              {}, // 含 docx / xlsx / pptx 等 OOXML（容器即 zip）
	"application/x-gzip":           {},
	"application/x-rar-compressed": {},
	"application/octet-stream":     {}, // 嗅探不出的二进制（如旧版 Office、7z）；按扩展名二次约束
}

// octetStreamExts 嗅探为 application/octet-stream 时允许的扩展名（已知的文档 / 压缩 / 数据格式）。
var octetStreamExts = map[string]struct{}{
	".doc": {}, ".xls": {}, ".ppt": {}, ".7z": {}, ".csv": {}, ".log": {}, ".json": {}, ".md": {},
}

// Usecase 是文件用例。
type Usecase struct {
	repo     Repo
	binds    BindRepo
	purges   PurgeRepo
	stores   Stores
	checkers AccessCheckers
	now      func() time.Time
}

// NewUsecase new a Usecase；stores.Default 必须已登记在 Drivers 中。
func NewUsecase(repo Repo, binds BindRepo, purges PurgeRepo, stores Stores, checkers AccessCheckers) (*Usecase, error) {
	if _, ok := stores.Drivers[stores.Default]; !ok {
		return nil, fmt.Errorf("file: default storage driver %q is not registered", stores.Default)
	}
	return &Usecase{repo: repo, binds: binds, purges: purges, stores: stores, checkers: checkers, now: time.Now}, nil
}

// sanitizeFilename 校验并规整原始文件名：只取基名（丢弃任何目录部分），拒绝空名、
// 控制字符与路径分隔符。文件名只用于展示与下载命名，从不参与存储路径。
func sanitizeFilename(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || len(name) > 255 {
		return "", ErrFileNameInvalid
	}
	for _, r := range name {
		// 控制字符、路径分隔符，以及不可见的格式控制字符（如 U+202E 可把扩展名显示反转，
		// 用于伪装文件类型）。
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '/' || r == '\\' {
			return "", ErrFileNameInvalid
		}
	}
	return name, nil
}

// detectMime 按内容嗅探类型并校验白名单；返回规整后的内容类型（不带参数）。
func detectMime(filename string, data []byte) (string, error) {
	detected, _, err := mime.ParseMediaType(http.DetectContentType(data))
	if err != nil {
		return "", ErrFileTypeNotAllowed
	}
	if _, ok := allowedMimes[detected]; !ok {
		return "", ErrFileTypeNotAllowed
	}
	if detected == "application/octet-stream" {
		if _, ok := octetStreamExts[strings.ToLower(path.Ext(filename))]; !ok {
			return "", ErrFileTypeNotAllowed
		}
	}
	return detected, nil
}

// Upload 保存一个文件：校验文件名与类型 → 落临时元数据（含配额判定）→ 写对象。
// 元数据先写、对象后写：任何时刻存在的对象都有元数据行与之对应，进程在两步之间
// 崩溃只会留下「有行无对象」的临时文件（下载按不存在处理，到期由清理任务删除），
// 不会产生无人认领的孤儿对象。对象写失败时回退元数据行。
func (uc *Usecase) Upload(ctx context.Context, uploader, filename string, data []byte) (*File, error) {
	if len(data) == 0 || len(data) > MaxFileSize {
		return nil, ErrFileTypeNotAllowed
	}
	name, err := sanitizeFilename(filename)
	if err != nil {
		return nil, err
	}
	contentType, err := detectMime(name, data)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := uc.now().UTC()
	sum := sha256.Sum256(data)
	f := &File{
		ID:     id,
		Driver: uc.stores.Default,
		// 对象键完全由服务端生成：按日期分目录 + 文件 ID。
		ObjectKey:  now.Format("2006/01/02") + "/" + id,
		Filename:   name,
		Size:       int64(len(data)),
		Mime:       contentType,
		SHA256:     hex.EncodeToString(sum[:]),
		Status:     StatusTemp,
		CreateUser: uploader,
		CreateTime: now,
	}
	if err := uc.repo.Create(ctx, f, MaxTempFiles); err != nil {
		return nil, err
	}
	if err := uc.stores.Drivers[f.Driver].Put(ctx, f.ObjectKey, data); err != nil {
		// 请求可能已取消：回退用不随请求取消的 ctx；回退失败也无妨（到期清理）。
		_ = uc.repo.Discard(context.WithoutCancel(ctx), f.ID)
		return nil, fmt.Errorf("store file: %w", err)
	}
	return f, nil
}

// Get 查询文件元数据并判定访问权限：临时文件仅上传者本人；已绑定文件由所属业务域
// 的 AccessChecker 判定，未登记检查器的业务类型一律拒绝。
func (uc *Usecase) Get(ctx context.Context, userID, id string) (*File, error) {
	f, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if f.Status == StatusTemp {
		if f.CreateUser != userID {
			return nil, ErrFileNotFound // 不暴露他人临时文件是否存在
		}
		return f, nil
	}
	checker, ok := uc.checkers[f.BizType]
	if !ok {
		return nil, ErrFileForbidden
	}
	allowed, err := checker.CanReadFile(ctx, userID, f.BizID, f.ID)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrFileForbidden
	}
	return f, nil
}

// Download 读取文件内容（权限同 Get）。对象缺失（驱动未登记 / 对象丢失）按不存在处理。
func (uc *Usecase) Download(ctx context.Context, userID, id string) (*File, []byte, error) {
	f, err := uc.Get(ctx, userID, id)
	if err != nil {
		return nil, nil, err
	}
	store, ok := uc.stores.Drivers[f.Driver]
	if !ok {
		return nil, nil, fmt.Errorf("file: storage driver %q is not registered", f.Driver)
	}
	data, err := store.Get(ctx, f.ObjectKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, nil, ErrFileNotFound
		}
		return nil, nil, fmt.Errorf("read file: %w", err)
	}
	return f, data, nil
}

// Bind 在调用方业务事务内把 uploader 本人上传的临时文件绑定到业务对象（跨域窄接口：
// 持有附件的业务域在自己的写事务里调用，业务提交 = 绑定生效）。任一文件不存在、
// 不属于 uploader 或已被绑定，返回 ErrFileNotFound（调用方事务整体回滚）。
func (uc *Usecase) Bind(ctx context.Context, tx txrun.Tx, uploader, bizType, bizID string, fileIDs []string) error {
	if len(fileIDs) == 0 {
		return nil
	}
	if bizType == "" || bizID == "" || len(fileIDs) > maxBindFiles {
		return ErrFileNotFound
	}
	ids := uniqueLower(fileIDs)
	if slices.ContainsFunc(ids, func(id string) bool { return !idgen.IsUUID(id) }) {
		return ErrFileNotFound // 非法 ID 不进 SQL（转 uuid 报错会连带回滚调用方事务并表现为 500）
	}
	n, err := uc.binds.BindTx(ctx, tx, uploader, bizType, bizID, ids)
	if err != nil {
		return err
	}
	if n != int64(len(ids)) {
		return ErrFileNotFound
	}
	return nil
}

// ListByBiz 某业务对象的附件（跨域窄接口：访问控制由持有附件的业务域在调用前完成）。
func (uc *Usecase) ListByBiz(ctx context.Context, bizType, bizID string) ([]*File, error) {
	return uc.binds.ListByBiz(ctx, bizType, bizID, maxListFiles)
}

// Release 在调用方业务事务内软删某业务对象的全部附件（业务对象删除时调用）。
func (uc *Usecase) Release(ctx context.Context, tx txrun.Tx, bizType, bizID string) error {
	return uc.binds.ReleaseTx(ctx, tx, bizType, bizID)
}

// ReleaseFiles 在调用方业务事务内软删某业务对象的指定附件（业务对象不再引用它们时调用）。
// 只作用于确实绑定在该对象上的文件；非法 ID 直接忽略（不会命中任何行）。
func (uc *Usecase) ReleaseFiles(ctx context.Context, tx txrun.Tx, bizType, bizID string, fileIDs []string) error {
	ids := slices.DeleteFunc(uniqueLower(fileIDs), func(id string) bool { return !idgen.IsUUID(id) })
	if len(ids) == 0 || bizType == "" || bizID == "" {
		return nil
	}
	return uc.binds.ReleaseFilesTx(ctx, tx, bizType, bizID, ids)
}

// PutSingleton 保存某业务对象「唯一的那一份」文件（如系统 logo、用户头像）：写入后它原有的
// 文件作废（由清理任务连同对象删除）。内容类型由调用方给定——调用方已按自己的白名单校验，
// 这里不再嗅探。ifAbsent 为真时仅在该对象还没有文件时写入（导入旧数据用，不会盖掉并发的新
// 上传）；返回是否写入。
//
// 步骤与 Upload 同一思路：先落临时元数据、再写对象、最后在一个事务里换上去。任何一步失败
// 或进程中途崩溃，留下的只是一条临时文件，到期由清理任务删除；读方在换上去之前看到的始终
// 是完整的旧文件。
//
// 因为同样先落一条临时文件，它也计入操作者的未绑定临时文件配额：操作者名下积压的未绑定
// 临时文件达到 MaxTempFiles 时，换头像 / logo 会被拒绝（FILE_QUOTA_EXCEEDED）。
func (uc *Usecase) PutSingleton(ctx context.Context, uploader, bizType, bizID, filename, contentType string, data []byte, ifAbsent bool) (bool, error) {
	if bizType == "" || bizID == "" || contentType == "" || len(data) == 0 || len(data) > MaxFileSize {
		return false, ErrFileTypeNotAllowed
	}
	name, err := sanitizeFilename(filename)
	if err != nil {
		return false, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return false, err
	}
	now := uc.now().UTC()
	sum := sha256.Sum256(data)
	f := &File{
		ID: id, Driver: uc.stores.Default, ObjectKey: now.Format("2006/01/02") + "/" + id,
		Filename: name, Size: int64(len(data)), Mime: contentType, SHA256: hex.EncodeToString(sum[:]),
		Status: StatusTemp, CreateUser: uploader, CreateTime: now,
	}
	if err := uc.repo.Create(ctx, f, MaxTempFiles); err != nil {
		if errors.Is(err, ErrFileQuotaExceeded) {
			return false, errSingletonQuotaExceeded
		}
		return false, err
	}
	store := uc.stores.Drivers[f.Driver]
	// 回退用不随请求取消的 ctx；回退失败也无妨（临时文件到期清理）。
	discard := func() {
		bg := context.WithoutCancel(ctx)
		_ = store.Delete(bg, f.ObjectKey)
		_ = uc.repo.Discard(bg, f.ID)
	}
	if err := store.Put(ctx, f.ObjectKey, data); err != nil {
		discard()
		return false, fmt.Errorf("store file: %w", err)
	}
	done, err := uc.binds.PromoteSingleton(ctx, f.ID, bizType, bizID, ifAbsent)
	if err != nil || !done {
		discard()
		return false, err
	}
	return true, nil
}

// ReadSingleton 读取某业务对象「唯一的那一份」文件的内容与内容类型；没有（或对象已丢失）时
// found 为 false。访问控制由调用方负责（logo、头像是公开可读的）。
func (uc *Usecase) ReadSingleton(ctx context.Context, bizType, bizID string) (data []byte, contentType string, found bool, err error) {
	f, err := uc.binds.LatestByBiz(ctx, bizType, bizID)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	store, ok := uc.stores.Drivers[f.Driver]
	if !ok {
		return nil, "", false, fmt.Errorf("file: storage driver %q is not registered", f.Driver)
	}
	data, err = store.Get(ctx, f.ObjectKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, fmt.Errorf("read file: %w", err)
	}
	return data, f.Mime, true, nil
}

// Purge 后台清理一批：过期临时文件与超过保留期的软删文件，连同对象物理删除；
// 返回处理条数（kit/loop 约定：>0 立即进入下一轮）。
func (uc *Usecase) Purge(ctx context.Context) (int, error) {
	now := uc.now()
	return uc.purges.PurgeExpired(ctx, now.Add(-TempRetention), now.Add(-DeletedRetention), PurgeRetryAfter, purgeBatch,
		func(ctx context.Context, f *File) error {
			store, ok := uc.stores.Drivers[f.Driver]
			if !ok {
				return fmt.Errorf("file: storage driver %q is not registered", f.Driver)
			}
			return store.Delete(ctx, f.ObjectKey)
		})
}

// uniqueLower 返回小写、去重的 ID（UUID 文本不区分大小写）。
func uniqueLower(ids []string) []string {
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.ToLower(id)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
