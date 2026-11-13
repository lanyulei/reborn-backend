package biz

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"

	notifyv1 "github.com/lanyulei/reborn/backend/core/api/notify/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/paging"

	"github.com/go-kratos/kratos/v3/errors"
)

// producerTokenPrefix 令牌明文前缀（便于密钥扫描器识别）；后接 base64url
// 编码的 32 字节 crypto/rand 随机数。
const producerTokenPrefix = "npt_"

// ProducerSpec 生产者配置载荷：名称 / 事件白名单 / 过期时间，创建与整组
// 替换更新共用同一校验。白名单空 = 不限事件类型（token scope 仍限定 biz）。
type ProducerSpec struct {
	Name           string
	EventWhitelist []string
	ExpireTime     *time.Time // nil = 永不过期
	QPSLimit       int32      // 每秒接入事件上限，0 = 不限（管道 ingress 按生产者计数）
	DailyLimit     int32      // 每日（UTC）接入事件上限，0 = 不限
}

// Producer 生产者领域模型（DO）：一行 = 一个事件生产者的接入凭据。
// TokenHash 是 SHA-256(token 明文)（即 ingress HMAC 密钥）；明文只在用例层签发时
// 产生一次，不进入任何实体返回路径。data 层落库前用主密钥封存，读回的是库存形态
// （可能已封存），用例层不得依赖其内容。与 proto 实体、data 层 PO 显式转换。
type Producer struct {
	ID    string
	BizID string
	ProducerSpec
	TokenHash  []byte
	Enabled    bool
	CreateTime time.Time
	UpdateTime time.Time
}

// ProducerFilter 生产者列表过滤条件；指针为 nil 表示不过滤。
type ProducerFilter struct {
	BizID   string // 空 = 不过滤（受限用户由 service 层强制携带）
	Keyword string
	Enabled *bool
}

// ProducerUpdate 生产者更新：Spec 非 nil 时整组替换配置字段；Enabled 独立
// 启停；两者至少其一。
type ProducerUpdate struct {
	Spec    *ProducerSpec
	Enabled *bool
}

// 生产者哨兵错误：reason 与 api/notify/v1 的 ErrorReason 枚举对齐。
var (
	// ErrProducerNotFound 目标生产者不存在。
	ErrProducerNotFound = errors.NotFound(notifyv1.ErrorReason_NOTIFY_PRODUCER_NOT_FOUND.String(), "producer not found")
	// ErrProducerNameExists 同 biz 内生产者名已被占用（未删除行）。
	ErrProducerNameExists = errors.Conflict(notifyv1.ErrorReason_NOTIFY_PRODUCER_NAME_EXISTS.String(), "producer name already exists")
	// ErrProducerInvalid 生产者不合法：名称为空、白名单条目非事件模式或
	// 超出上限、过期时间不在未来。
	ErrProducerInvalid = errors.BadRequest(notifyv1.ErrorReason_NOTIFY_PRODUCER_INVALID.String(), "invalid producer")
)

// ProducerRepo 生产者仓储接口，由 data 层实现（依赖倒置）。生产者属管道
// ingress 鉴权缓存集：每次写操作（含 token 轮换）同事务 bump
// notify_cfg_version，Disable/Rotate ≤10s 生效。
type ProducerRepo interface {
	// CreateProducer 创建生产者；同 biz 未删除行内重名返回 ErrProducerNameExists。
	CreateProducer(ctx context.Context, producer *Producer) error
	// UpdateProducer 按 ID 更新未删除生产者（Spec 整组替换 / Enabled 启停）；
	// 不存在返回 ErrProducerNotFound。
	UpdateProducer(ctx context.Context, id string, upd ProducerUpdate) error
	// UpdateProducerToken 按 ID 轮换令牌哈希并返回权威行；不存在返回
	// ErrProducerNotFound。
	UpdateProducerToken(ctx context.Context, id string, tokenHash []byte) (*Producer, error)
	// DeleteProducer 软删生产者（token 随之作废，名称可复用）；不存在返回
	// ErrProducerNotFound。
	DeleteProducer(ctx context.Context, id string) error
	// GetProducer 按 ID 查询未删除生产者；不存在返回 ErrProducerNotFound。
	GetProducer(ctx context.Context, id string) (*Producer, error)
	// ListProducers 分页查询未删除生产者；page 从 1 起，pageSize 上限 100，
	// 按创建时间倒序。
	ListProducers(ctx context.Context, filter ProducerFilter, page, pageSize int64) ([]*Producer, int64, error)
}

// ProducerUsecase 生产者用例：承载配置校验与 token 签发（明文仅签发时
// 返回一次，库存 SHA-256）。
type ProducerUsecase struct {
	repo ProducerRepo
}

// NewProducerUsecase new a ProducerUsecase.
func NewProducerUsecase(repo ProducerRepo) *ProducerUsecase {
	return &ProducerUsecase{repo: repo}
}

// generateProducerToken 签发新令牌：返回明文（仅调用方响应一次）与
// SHA-256 哈希（落库）。
func generateProducerToken() (token string, tokenHash []byte, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate producer token: %w", err)
	}
	token = producerTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(token))
	return token, sum[:], nil
}

// validateProducerSpec 校验生产者配置：名称非空 ≤64 字符（按 rune 计，与
// proto max_len / varchar(64) 同口径）、白名单 ≤32 项且每项为合法事件模式
// （与路由同式，入口双保险）。过期时间另由 validateExpireTime 校验：创建
// 恒校验；更新仅在过期时间变化时校验，已过期生产者改名/改白名单不被
// 「过期时间须在未来」卡住。
func validateProducerSpec(spec *ProducerSpec) error {
	if n := utf8.RuneCountInString(spec.Name); n == 0 || n > 64 {
		return fmt.Errorf("%w: name must be 1-64 chars", ErrProducerInvalid)
	}
	if len(spec.EventWhitelist) > 32 {
		return fmt.Errorf("%w: event_whitelist has %d entries, want <= 32",
			ErrProducerInvalid, len(spec.EventWhitelist))
	}
	for _, pattern := range spec.EventWhitelist {
		if !eventPatternPattern.MatchString(pattern) {
			return fmt.Errorf("%w: bad event_whitelist entry %q", ErrProducerInvalid, pattern)
		}
	}
	if spec.QPSLimit < 0 || spec.DailyLimit < 0 {
		return fmt.Errorf("%w: quota limits must be >= 0", ErrProducerInvalid)
	}
	return nil
}

// validateExpireTime 过期时间（若设置）须在未来。
func validateExpireTime(expire *time.Time, now time.Time) error {
	if expire != nil && !expire.After(now) {
		return fmt.Errorf("%w: expire_time %v not in the future", ErrProducerInvalid, expire)
	}
	return nil
}

// sameExpireTime 报告两个可空过期时间是否等价。
func sameExpireTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

// Create 创建生产者：校验配置、生成 ID 与 token（哈希落库、明文返回一次）；
// BizID 缺省 default。
func (uc *ProducerUsecase) Create(ctx context.Context, producer *Producer) (*Producer, string, error) {
	if producer.BizID == "" {
		producer.BizID = DefaultBizID
	}
	now := time.Now().UTC()
	if err := validateProducerSpec(&producer.ProducerSpec); err != nil {
		return nil, "", err
	}
	if err := validateExpireTime(producer.ExpireTime, now); err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	token, tokenHash, err := generateProducerToken()
	if err != nil {
		return nil, "", err
	}
	producer.ID = id
	producer.TokenHash = tokenHash
	producer.CreateTime, producer.UpdateTime = now, now
	if err := uc.repo.CreateProducer(ctx, producer); err != nil {
		return nil, "", err
	}
	return producer, token, nil
}

// Update 更新生产者：Spec 整组替换（先确认存在），Enabled 独立启停；
// 不存在返回 ErrProducerNotFound。
func (uc *ProducerUsecase) Update(ctx context.Context, id string, upd ProducerUpdate) error {
	if upd.Spec != nil {
		existing, err := uc.repo.GetProducer(ctx, id)
		if err != nil {
			return err
		}
		if err := validateProducerSpec(upd.Spec); err != nil {
			return err
		}
		if !sameExpireTime(existing.ExpireTime, upd.Spec.ExpireTime) {
			if err := validateExpireTime(upd.Spec.ExpireTime, time.Now().UTC()); err != nil {
				return err
			}
		}
	}
	return uc.repo.UpdateProducer(ctx, id, upd)
}

// RotateToken 轮换令牌：签发新 token（明文仅本次返回），旧哈希立即替换；
// 返回轮换后的权威行。
func (uc *ProducerUsecase) RotateToken(ctx context.Context, id string) (*Producer, string, error) {
	token, tokenHash, err := generateProducerToken()
	if err != nil {
		return nil, "", err
	}
	producer, err := uc.repo.UpdateProducerToken(ctx, id, tokenHash)
	if err != nil {
		return nil, "", err
	}
	return producer, token, nil
}

// Delete 软删生产者；不存在返回 ErrProducerNotFound。
func (uc *ProducerUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteProducer(ctx, id)
}

// Get 查询生产者详情（不含 token 与哈希的返回路径由 service 层保证）。
func (uc *ProducerUsecase) Get(ctx context.Context, id string) (*Producer, error) {
	return uc.repo.GetProducer(ctx, id)
}

// List 分页查询生产者；分页参数先规整再委托仓储。
func (uc *ProducerUsecase) List(ctx context.Context, filter ProducerFilter, page, pageSize int64) ([]*Producer, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.repo.ListProducers(ctx, filter, page, pageSize)
}
