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
