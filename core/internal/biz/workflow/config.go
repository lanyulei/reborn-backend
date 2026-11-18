package biz

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	v1 "github.com/lanyulei/reborn/backend/core/api/workflow/v1"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/netguard"

	"github.com/go-kratos/kratos/v3/errors"
)

// 哨兵错误。
var (
	ErrDelegationNotFound = errors.NotFound(v1.ErrorReason_WORKFLOW_DELEGATION_NOT_FOUND.String(), "workflow delegation not found")
	ErrDelegationInvalid  = errors.BadRequest(v1.ErrorReason_WORKFLOW_DELEGATION_INVALID.String(), "invalid workflow delegation")
	ErrCalendarInvalid    = errors.BadRequest(v1.ErrorReason_WORKFLOW_CALENDAR_INVALID.String(), "invalid working calendar")
	ErrWebhookNotFound    = errors.NotFound(v1.ErrorReason_WORKFLOW_WEBHOOK_NOT_FOUND.String(), "workflow webhook not found")
	ErrWebhookNameExists  = errors.Conflict(v1.ErrorReason_WORKFLOW_WEBHOOK_NAME_EXISTS.String(), "workflow webhook name already exists")
	ErrWebhookInvalid     = errors.BadRequest(v1.ErrorReason_WORKFLOW_WEBHOOK_INVALID.String(), "invalid workflow webhook")
	ErrWebhookInUse       = errors.Conflict(v1.ErrorReason_WORKFLOW_WEBHOOK_IN_USE.String(), "workflow webhook is still referenced by a published workflow")
	ErrSecretUnavailable  = errors.Conflict(v1.ErrorReason_WORKFLOW_SECRET_UNAVAILABLE.String(), "master key is not configured; cannot store a signing secret")
)

func invalid(base *errors.Error, message string) error {
	return errors.New(int(base.Code), base.Reason, message)
}

// ---- 工作日历 ----

// calendarTTL 已编译日历的进程内缓存时长：日历改动后各实例至多这么久生效（只影响
// 此后新算出的时限）。
const calendarTTL = 30 * time.Second

// CalendarStore 工作日历配置的存取（全局唯一一份）。读取可能发生在命令事务内，实现须复用事务连接。
type CalendarStore interface {
	GetCalendar(ctx context.Context) (CalendarConfig, error)
	SaveCalendar(ctx context.Context, c CalendarConfig) error
}

// CalendarUsecase 工作日历用例。
type CalendarUsecase struct {
	store CalendarStore

	mu      sync.Mutex
	cached  *Calendar
	expires time.Time
}

// NewCalendarUsecase new a CalendarUsecase.
func NewCalendarUsecase(store CalendarStore) *CalendarUsecase {
	return &CalendarUsecase{store: store}
}

// Get 当前的日历配置。
func (uc *CalendarUsecase) Get(ctx context.Context) (CalendarConfig, error) {
	return uc.store.GetCalendar(ctx)
}

// Save 校验并保存日历配置。
func (uc *CalendarUsecase) Save(ctx context.Context, c CalendarConfig) error {
	cal, err := NewCalendar(c)
	if err != nil {
		return invalid(ErrCalendarInvalid, err.Error())
	}
	if err := uc.store.SaveCalendar(ctx, c); err != nil {
		return err
	}
	uc.mu.Lock()
	uc.cached, uc.expires = cal, time.Now().Add(calendarTTL)
	uc.mu.Unlock()
	return nil
}

// Due 自 from 起经过 d 之后的时刻（按当前的工作日历；d.WorkTime 为 false 时按自然时间）。
// 跨域窄接口：业务域据此推算自己的时限（如工单 SLA）。
func (uc *CalendarUsecase) Due(ctx context.Context, from time.Time, d Duration) (time.Time, error) {
	cal, err := uc.Current(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return cal.Add(from, d), nil
}

// Current 当前生效的日历（带短时缓存）。库内配置已不合法时（不应发生）返回 nil——
// 时限退回按自然时间计，不让流程命令失败。
func (uc *CalendarUsecase) Current(ctx context.Context) (*Calendar, error) {
	uc.mu.Lock()
	if uc.cached != nil && time.Now().Before(uc.expires) {
		defer uc.mu.Unlock()
		return uc.cached, nil
	}
	uc.mu.Unlock()
	c, err := uc.store.GetCalendar(ctx)
	if err != nil {
		return nil, err
	}
	cal, err := NewCalendar(c)
	if err != nil {
		return nil, nil
	}
	uc.mu.Lock()
	uc.cached, uc.expires = cal, time.Now().Add(calendarTTL)
	uc.mu.Unlock()
	return cal, nil
}

// ---- 代理设置 ----

const (
	// maxDelegations 单个用户的代理设置条数上限（含已过期未删除的）。
	maxDelegations = 20
	// maxDelegationSpan 单条代理设置的最长时间跨度。
	maxDelegationSpan = 366 * 24 * time.Hour
)

// Delegation 一条代理设置：UserID 在 [StartTime, EndTime) 内把新到达的待办交给 DelegateID。
type Delegation struct {
	ID         string
	UserID     string
	DelegateID string
	StartTime  time.Time
	EndTime    time.Time
	Reason     string
	CreateTime time.Time
}

// DelegationRepo 代理设置仓储接口。
type DelegationRepo interface {
	// CreateDelegation 创建；同一用户的时间段与已有设置重叠、或条数达到 limit 返回 ErrDelegationInvalid。
	CreateDelegation(ctx context.Context, d *Delegation, limit int) error
	// DeleteDelegation 软删用户自己的一条设置；不存在返回 ErrDelegationNotFound。
	DeleteDelegation(ctx context.Context, userID, id string) error
	// ListDelegations 用户的全部未删除设置，开始时间倒序。
	ListDelegations(ctx context.Context, userID string) ([]*Delegation, error)
}

// DelegationUsecase 代理设置用例（用户自助维护自己的代理人）。
type DelegationUsecase struct {
	repo  DelegationRepo
	dir   Directory
	users Users
}

// NewDelegationUsecase new a DelegationUsecase.
func NewDelegationUsecase(repo DelegationRepo, dir Directory, users Users) *DelegationUsecase {
	return &DelegationUsecase{repo: repo, dir: dir, users: users}
}

// Create 新增一条代理设置：代理人须为可用用户且不是自己；时间段须在将来结束、跨度有上限，
// 且不与本人已有的设置重叠。
func (uc *DelegationUsecase) Create(ctx context.Context, d *Delegation) (*Delegation, error) {
	d.DelegateID, d.Reason = strings.ToLower(d.DelegateID), clean(d.Reason)
	now := time.Now().UTC()
	switch {
	case d.DelegateID == d.UserID:
		return nil, invalid(ErrDelegationInvalid, "you cannot delegate to yourself")
	case !d.EndTime.After(d.StartTime) || !d.EndTime.After(now):
		return nil, invalid(ErrDelegationInvalid, "the delegation period must end in the future and after it starts")
	case d.EndTime.Sub(d.StartTime) > maxDelegationSpan:
		return nil, invalid(ErrDelegationInvalid, "the delegation period must not exceed one year")
	}
	active, err := uc.dir.ActiveUserIDs(ctx, []string{d.DelegateID})
	if err != nil {
		return nil, err
	}
	if len(active) != 1 {
		return nil, invalid(ErrDelegationInvalid, "the delegate must be an active user")
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	d.ID, d.StartTime, d.EndTime, d.CreateTime = id, d.StartTime.UTC(), d.EndTime.UTC(), now
	if err := uc.repo.CreateDelegation(ctx, d, maxDelegations); err != nil {
		return nil, err
	}
	return d, nil
}

// Delete 删除自己的一条代理设置。
func (uc *DelegationUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.repo.DeleteDelegation(ctx, userID, id)
}

// List 自己的代理设置与代理人的显示名。
func (uc *DelegationUsecase) List(ctx context.Context, userID string) ([]*Delegation, map[string]string, error) {
	items, err := uc.repo.ListDelegations(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, len(items))
	for _, d := range items {
		ids = append(ids, d.DelegateID)
	}
	names, err := uc.users.UserNames(ctx, uniqueStrings(ids))
	return items, names, err
}

// ---- Webhook 端点 ----

// maxWebhooks 端点数量上限（列表不分页）。
const maxWebhooks = 200

// Webhook 自动任务可调用的端点。Secret 只在创建 / 更新入参与 Resolve 的结果里有值，
// 查询接口不返回。
type Webhook struct {
	ID          string
	Name        string
	URL         string
	Description string
	Status      int32
	HasSecret   bool
	Secret      string
	UpdateTime  time.Time
}

// Webhook 端点状态。
const (
	WebhookDisabled int32 = 0
	WebhookEnabled  int32 = 1
)

// WebhookRepo 端点仓储接口。
type WebhookRepo interface {
	// CreateWebhook 创建；重名返回 ErrWebhookNameExists，数量达到 limit 返回 ErrWebhookInvalid，
	// 带密钥但未配置主密钥返回 ErrSecretUnavailable。
	CreateWebhook(ctx context.Context, w *Webhook, limit int) error
	// UpdateWebhook 整组更新；setSecret 为 false 时保留原密钥（w.Secret 为空串且 setSecret 为 true 则清除）。
	UpdateWebhook(ctx context.Context, w *Webhook, setSecret bool) error
	// DeleteWebhook 软删；仍被已发布的流程引用返回 ErrWebhookInUse。
	DeleteWebhook(ctx context.Context, id string) error
	// ListWebhooks 全部未删除的端点（不含密钥），名称升序。
	ListWebhooks(ctx context.Context) ([]*Webhook, error)
}

// WebhookResolver 执行期按 ID 取端点（含解密后的密钥）；不存在或已停用返回 ErrWebhookNotFound。
type WebhookResolver interface {
	Resolve(ctx context.Context, id string) (*Webhook, error)
}

// WebhookChecker 发布校验期报告给定端点各自是否可用（存在且启用）。
type WebhookChecker interface {
	UsableWebhooks(ctx context.Context, ids []string) (map[string]bool, error)
}

// WebhookUsecase 端点管理用例。
type WebhookUsecase struct {
	repo WebhookRepo
}

// NewWebhookUsecase new a WebhookUsecase.
func NewWebhookUsecase(repo WebhookRepo) *WebhookUsecase {
	return &WebhookUsecase{repo: repo}
}

// BlockedIP 报告地址是否不允许作为 Webhook 的目标（规则在 kit/netguard，与通知渠道共用）。
func BlockedIP(ip net.IP) bool { return netguard.BlockedIP(ip) }

// checkWebhook 归一化并校验端点：地址须为 https、带主机名、不含账号口令，且不是指向
// localhost 或回环 / 内网的 IP 字面量（域名解析结果在每次调用时再校验，免疫 DNS 重绑定）。
func checkWebhook(w *Webhook) error {
	w.Name, w.Description, w.URL = clean(strings.TrimSpace(w.Name)), clean(strings.TrimSpace(w.Description)), strings.TrimSpace(w.URL)
	if w.Name == "" {
		return invalid(ErrWebhookInvalid, "webhook name must not be blank")
	}
	u, err := url.Parse(w.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return invalid(ErrWebhookInvalid, "webhook url must be an https url without credentials")
	}
	if netguard.UnsafeHost(u.Hostname()) {
		return invalid(ErrWebhookInvalid, "webhook url must not point to a private or local address")
	}
	if strings.ContainsAny(w.Secret, "\x00\r\n") {
		return invalid(ErrWebhookInvalid, "webhook secret contains invalid characters")
	}
	return nil
}

// Create 创建端点。
func (uc *WebhookUsecase) Create(ctx context.Context, w *Webhook) (*Webhook, error) {
	if err := checkWebhook(w); err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	w.ID, w.HasSecret, w.UpdateTime = id, w.Secret != "", time.Now().UTC()
	if err := uc.repo.CreateWebhook(ctx, w, maxWebhooks); err != nil {
		return nil, err
	}
	w.Secret = ""
	return w, nil
}

// Update 整组更新端点；secret 为 nil 时保留原密钥，指向空串则清除。
func (uc *WebhookUsecase) Update(ctx context.Context, w *Webhook, secret *string) error {
	if secret != nil {
		w.Secret = *secret
	}
	if err := checkWebhook(w); err != nil {
		return err
	}
	return uc.repo.UpdateWebhook(ctx, w, secret != nil)
}

// Delete 删除端点。
func (uc *WebhookUsecase) Delete(ctx context.Context, id string) error {
	return uc.repo.DeleteWebhook(ctx, id)
}

// List 全部端点（不含密钥）。
func (uc *WebhookUsecase) List(ctx context.Context) ([]*Webhook, error) {
	return uc.repo.ListWebhooks(ctx)
}
