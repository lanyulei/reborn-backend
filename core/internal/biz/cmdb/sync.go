package biz

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// 采集源缺省参数（秒）。
const (
	DefaultSyncIntervalSeconds = 3600
	DefaultSyncGraceSeconds    = 86400
)

// SyncSource 采集源领域模型（凭据只持有密文，且仅在写路径经过 biz）。
type SyncSource struct {
	ID                 string
	Name               string
	Provider           string
	Config             []byte
	HasCredential      bool
	Env                string
	IntervalSeconds    int32
	GracePeriodSeconds int32
	Enabled            bool
	NextRunTime        *time.Time
	LastRunID          string
	LastRunTime        *time.Time
	LastStatus         string
	ActiveTask         bool
	WebhookEnabled     bool
	Version            int64
	CreateUser         string
	UpdateUser         string
	CreateTime         time.Time
	UpdateTime         time.Time
}

// SyncSourceFilter 采集源列表过滤。
type SyncSourceFilter struct {
	Keyword  string
	Provider string
}

// SyncSourceUpdate 采集源更新（整体替换；Credential nil = 保留旧密文）。
type SyncSourceUpdate struct {
	Name               string
	Config             []byte
	Credential         []byte
	Env                string
	IntervalSeconds    int32
	GracePeriodSeconds int32
	Enabled            bool
}

// SyncTask 同步任务（控制面只创建手动任务）。
type SyncTask struct {
	ID         string
	SourceID   string
	Trigger    string
	Status     string
	CreateUser string
	CreateTime time.Time
}

// SyncRun 同步运行记录。
type SyncRun struct {
	ID          string
	SourceID    string
	TaskID      string
	Status      string
	Discovered  int32
	Created     int32
	Updated     int32
	Unchanged   int32
	Disappeared int32
	Retired     int32
	Failed      int32
	Error       string
	Errors      []cmdbcontract.RunError
	StartTime   time.Time
	FinishTime  *time.Time
}

// SyncSourceRepo 采集源仓储接口。
type SyncSourceRepo interface {
	// Create 落库；名称冲突返回 ErrSourceNameExists。
	Create(ctx context.Context, src *SyncSource, credential []byte) error
	Get(ctx context.Context, id string) (*SyncSource, error)
	List(ctx context.Context, filter SyncSourceFilter, page, pageSize int64) ([]*SyncSource, int64, error)
	// Update 乐观锁更新（version 不匹配返回 ErrSourceVersionConflict）；credential nil = 保留。
	// next_run_time 不整行回写：调度器推进它时不升 version，写回调用方读到的值会把刚推进的计划时间
	// 拉回去。仓储按行内旧值与库内时钟调整——更新后启用且为空时置为 now()，周期变化时不晚于
	// now() + 新周期，其余保持调度器维护的值；src.NextRunTime 只是给不带调度器的内存桩的提示。
	Update(ctx context.Context, src *SyncSource, expectedVersion int64, credential []byte) error
	SoftDelete(ctx context.Context, id string, now time.Time) error
	// SetWebhookHash 写回调令牌哈希；nil = 停用。
	SetWebhookHash(ctx context.Context, id string, hash []byte) error
}

// SyncTaskRepo 任务仓储接口（控制面只插入手动任务）。
type SyncTaskRepo interface {
	// Create 插入任务；同源已有未完成任务返回 ErrSyncBusy（partial unique index 兜底）。
	Create(ctx context.Context, task *SyncTask) error
}

// SyncRunRepo 运行记录仓储接口（控制面只读）。
type SyncRunRepo interface {
	ListBySource(ctx context.Context, sourceID string, page, pageSize int64) ([]*SyncRun, int64, error)
	// ClaimConflict 把冲突行绑定到已有资源：资源的来源改为该采集源、外部标识改为冲突行的标识，
	// 版本 +1、写历史并移除冲突行（一事务，与对账互斥）。冲突行不存在返回 ErrSourceInvalid；
	// 资源不存在 / 不是该类型返回 ErrCINotFound；资源已由同步维护返回 ErrCIInvalid；
	// 该外部标识已有在册资源返回 ErrCIDuplicate。
	ClaimConflict(ctx context.Context, claim *ConflictClaim) error
	// ListConflicts 采集源当前未解决的同步冲突（最早出现的在前）。
	ListConflicts(ctx context.Context, sourceID string, page, pageSize int64) ([]*SyncConflict, int64, error)
	// ConflictsCapped 该采集源是否有类型的冲突数达到留存上限（清单不完整）。
	ConflictsCapped(ctx context.Context, sourceID string) (bool, error)
}

// ScopedConflictRepo 冲突清单仓储的可选能力：按调用者可见类型过滤清单与封顶判定。
// 清单带着采集到的资源名称与外部标识，数据范围外的类型不该看得到。生产仓储必须实现
// （编译期断言）；未实现时（内存桩）受限调用者得到空清单，不退回全量。
type ScopedConflictRepo interface {
	// ListConflictsIn 同 ListConflicts，但只取 modelCodes 内类型的冲突行（空集合 = 无结果）。
	ListConflictsIn(ctx context.Context, sourceID string, modelCodes []string, page, pageSize int64) ([]*SyncConflict, int64, error)
	// ConflictsCappedIn 同 ConflictsCapped，但只看 modelCodes 内的类型。
	ConflictsCappedIn(ctx context.Context, sourceID string, modelCodes []string) (bool, error)
}

// ConflictClaim 认领参数。
type ConflictClaim struct {
	SourceID   string
	ModelCode  string
	ExternalID string
	CIID       string
	Actor      Actor
	At         time.Time
	// HistoryID 历史记录 ID（仓储在事务内按实际的前后值写入）。
	HistoryID string
}

// SyncConflict 一条未解决的同步冲突（数据面对账时写入，控制面只读）。
type SyncConflict struct {
	ModelCode  string
	ModelName  string
	ExternalID string
	Name       string
	Reason     string
	FirstSeen  time.Time
	LastSeen   time.Time
}

// CredentialSealer 凭据加密器（data 层以 kit/secretbox 实现；只写不读）。
type CredentialSealer interface {
	// Seal 加密明文；未配置主密钥返回 ErrSecretUnavailable。
	Seal(plaintext string) ([]byte, error)
}

// SyncUsecase 同步控制面用例：采集源 CRUD、手动触发、运行记录。
type SyncUsecase struct {
	sources SyncSourceRepo
	tasks   SyncTaskRepo
	runs    SyncRunRepo
	sealer  CredentialSealer
}

// NewSyncUsecase new a SyncUsecase.
func NewSyncUsecase(sources SyncSourceRepo, tasks SyncTaskRepo, runs SyncRunRepo, sealer CredentialSealer) *SyncUsecase {
	return &SyncUsecase{sources: sources, tasks: tasks, runs: runs, sealer: sealer}
}

// validateConfig 按提供方解码校验配置，返回规范化后的 JSON（拒绝未知键，避免凭据误入配置）。
func validateConfig(provider string, raw []byte) ([]byte, error) {
	cfg, err := cmdbcontract.DecodeConfig(provider, raw)
	if err != nil {
		if errors.Is(err, cmdbcontract.ErrConfigInvalid) {
			return nil, detail(ErrSourceInvalid, "%v", err)
		}
		return nil, err
	}
	return json.Marshal(cfg)
}

// Create 创建采集源：配置按提供方校验，凭据加密后落库，首个计划时间 = 现在（启用时立即可被调度）。
func (uc *SyncUsecase) Create(ctx context.Context, actor Actor, src *SyncSource, credential string) (*SyncSource, error) {
	cfg, err := validateConfig(src.Provider, src.Config)
	if err != nil {
		return nil, err
	}
	sealed, err := uc.sealer.Seal(credential)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	src.ID, src.Config, src.HasCredential = id, cfg, true
	if src.IntervalSeconds <= 0 {
		src.IntervalSeconds = DefaultSyncIntervalSeconds
	}
	if src.GracePeriodSeconds < 0 {
		src.GracePeriodSeconds = DefaultSyncGraceSeconds
	}
	src.LastStatus, src.Version = cmdbcontract.RunNever, 1
	src.CreateUser, src.UpdateUser, src.CreateTime, src.UpdateTime = actor.UserID, actor.UserID, now, now
	if src.Enabled {
		next := now
		src.NextRunTime = &next
	}
	if err := uc.sources.Create(ctx, src, sealed); err != nil {
		return nil, err
	}
	return uc.sources.Get(ctx, id)
}

// Get 查询采集源。
func (uc *SyncUsecase) Get(ctx context.Context, id string) (*SyncSource, error) {
	return uc.sources.Get(ctx, id)
}

// List 分页查询采集源。
func (uc *SyncUsecase) List(ctx context.Context, filter SyncSourceFilter, page, pageSize int64) ([]*SyncSource, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.sources.List(ctx, filter, page, pageSize)
}

// Update 乐观锁整体更新；provider 不可改；凭据传新值即轮换。启用且无计划时间时置为现在。
// next_run_time 的落库值由仓储在 UPDATE 内按行内旧值决定（见 SyncSourceRepo.Update）：调度器并发
// 推进它时不升 version，乐观锁发现不了，写回请求开始时读到的值会把它拉回去（当前任务一结束立刻
// 再跑一次）；缩短周期也要等到旧计划时间才生效。
func (uc *SyncUsecase) Update(ctx context.Context, actor Actor, id string, expectedVersion int64, upd SyncSourceUpdate, credential *string) (*SyncSource, error) {
	cur, err := uc.sources.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.Version != expectedVersion {
		return nil, ErrSourceVersionConflict
	}
	cfg, err := validateConfig(cur.Provider, upd.Config)
	if err != nil {
		return nil, err
	}
	var sealed []byte
	if credential != nil {
		if sealed, err = uc.sealer.Seal(*credential); err != nil {
			return nil, err
		}
	}
	now := time.Now()
	cur.Name, cur.Config, cur.Env = upd.Name, cfg, upd.Env
	cur.IntervalSeconds, cur.GracePeriodSeconds, cur.Enabled = upd.IntervalSeconds, upd.GracePeriodSeconds, upd.Enabled
	cur.UpdateUser, cur.UpdateTime = actor.UserID, now
	if cur.Enabled && cur.NextRunTime == nil {
		next := now
		cur.NextRunTime = &next
	}
	if err := uc.sources.Update(ctx, cur, expectedVersion, sealed); err != nil {
		return nil, err
	}
	return uc.sources.Get(ctx, id)
}

// Delete 软删采集源（已同步的资源保留，来源字面量不变，后续按周期判定陈旧）。
func (uc *SyncUsecase) Delete(ctx context.Context, id string) error {
	if _, err := uc.sources.Get(ctx, id); err != nil {
		return err
	}
	return uc.sources.SoftDelete(ctx, id, time.Now())
}

// Trigger 手动触发：插入 pending 任务，由数据面领取；同源已有未完成任务返回 ErrSyncBusy。
func (uc *SyncUsecase) Trigger(ctx context.Context, actor Actor, sourceID string) (*SyncTask, error) {
	src, err := uc.sources.Get(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	if !src.HasCredential {
		return nil, detail(ErrSourceInvalid, "credential is not configured")
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	task := &SyncTask{ID: id, SourceID: sourceID, Trigger: cmdbcontract.TriggerManual, Status: cmdbcontract.TaskPending, CreateUser: actor.UserID, CreateTime: time.Now()}
	if err := uc.tasks.Create(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

// RotateWebhook 生成并保存回调令牌（明文只返回一次；库中只存 SHA-256）。
func (uc *SyncUsecase) RotateWebhook(ctx context.Context, id string) (token, path, header string, err error) {
	if _, err := uc.sources.Get(ctx, id); err != nil {
		return "", "", "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", "", err
	}
	token = hex.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	if err := uc.sources.SetWebhookHash(ctx, id, sum[:]); err != nil {
		return "", "", "", err
	}
	return token, cmdbcontract.WebhookPath(id), cmdbcontract.WebhookTokenHeader, nil
}

// DisableWebhook 停用回调。
func (uc *SyncUsecase) DisableWebhook(ctx context.Context, id string) error {
	if _, err := uc.sources.Get(ctx, id); err != nil {
		return err
	}
	return uc.sources.SetWebhookHash(ctx, id, nil)
}

// ListRuns 分页查询运行记录。
func (uc *SyncUsecase) ListRuns(ctx context.Context, sourceID string, page, pageSize int64) ([]*SyncRun, int64, error) {
	if _, err := uc.sources.Get(ctx, sourceID); err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.runs.ListBySource(ctx, sourceID, page, pageSize)
}

// ListConflicts 分页查询采集源当前未解决的同步冲突。受限调用者（scope）只看得到自己可见类型的
// 冲突；采集源发现的资源会写进它的 env，调用者看不到该环境时整份清单不可见（见 conflictScope）。
func (uc *SyncUsecase) ListConflicts(ctx context.Context, scope Scope, sourceID string, page, pageSize int64) ([]*SyncConflict, int64, error) {
	src, err := uc.sources.Get(ctx, sourceID)
	if err != nil {
		return nil, 0, err
	}
	page, pageSize = paging.Normalize(page, pageSize)
	modelCodes, visible := conflictScope(scope, src.Env)
	if !visible {
		return nil, 0, nil
	}
	if modelCodes == nil {
		return uc.runs.ListConflicts(ctx, sourceID, page, pageSize)
	}
	r, ok := uc.runs.(ScopedConflictRepo)
	if !ok {
		return nil, 0, nil
	}
	return r.ListConflictsIn(ctx, sourceID, modelCodes, page, pageSize)
}

// conflictScope 调用者在某采集源冲突清单上的可见类型集合：nil = 类型不限；ok = false 表示清单
// 对其不可见——冲突行是采集到却没能写入的资源，采集源发现 的资源都落在它的 env 里。
func conflictScope(scope Scope, sourceEnv string) (modelCodes []string, ok bool) {
	if scope.Unrestricted {
		return nil, true
	}
	if !scope.AllowsEnv(sourceEnv) {
		return nil, false
	}
	if scope.ModelUnrestricted {
		return nil, true
	}
	return scopeKeyList(scope.Models), true
}

// ClaimConflict 认领同步冲突：把冲突行绑定到已有的人工资源。
func (uc *SyncUsecase) ClaimConflict(ctx context.Context, actor Actor, sourceID, modelCode, externalID, ciID string) error {
	if _, err := uc.sources.Get(ctx, sourceID); err != nil {
		return err
	}
	id, err := idgen.NewID()
	if err != nil {
		return err
	}
	return uc.runs.ClaimConflict(ctx, &ConflictClaim{
		SourceID: sourceID, ModelCode: modelCode, ExternalID: externalID, CIID: ciID,
		Actor: actor, At: time.Now(), HistoryID: id,
	})
}

// ConflictsCapped 冲突清单是否不完整（调用者可见类型里是否有达到留存上限的；口径同 ListConflicts）。
func (uc *SyncUsecase) ConflictsCapped(ctx context.Context, scope Scope, sourceID string) (bool, error) {
	src, err := uc.sources.Get(ctx, sourceID)
	if err != nil {
		return false, err
	}
	modelCodes, visible := conflictScope(scope, src.Env)
	if !visible {
		return false, nil
	}
	if modelCodes == nil {
		return uc.runs.ConflictsCapped(ctx, sourceID)
	}
	r, ok := uc.runs.(ScopedConflictRepo)
	if !ok {
		return false, nil
	}
	return r.ConflictsCappedIn(ctx, sourceID, modelCodes)
}
