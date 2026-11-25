package biz

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 公共地址参数。
const (
	// shareTokenBytes 令牌熵（256 bit，base64url 后 43 字符），不可枚举。
	shareTokenBytes = 32
	// maxPublicRange 公共日历单次查询跨度上限。
	maxPublicRange = 62 * 24 * time.Hour
	// 匿名访问限速：每个来源 IP 每分钟的请求数与无效令牌次数（纯内存、单实例口径）。
	publicRatePerMinute    = 120
	publicFailurePerMinute = 20
	publicLimiterEntries   = 10000
)

// ShareUsecase 公共值班日历地址用例。
type ShareUsecase struct {
	store     Store
	groups    GroupRepo
	schedules ScheduleRepo
	shifts    ShiftRepo
	shares    ShareRepo
	users     UserDirectory
	cipher    TokenCipher
	limiter   *publicLimiter
	now       func() time.Time
}

// NewShareUsecase new a ShareUsecase.
func NewShareUsecase(store Store, groups GroupRepo, schedules ScheduleRepo, shifts ShiftRepo, shares ShareRepo, users UserDirectory, cipher TokenCipher) *ShareUsecase {
	return &ShareUsecase{
		store: store, groups: groups, schedules: schedules, shifts: shifts, shares: shares, users: users, cipher: cipher,
		// 过期判定用未截断的时间：截断到秒会让地址最多晚 1 秒才失效。
		limiter: newPublicLimiter(time.Now), now: time.Now,
	}
}

// newToken 生成令牌明文、哈希（匿名访问查找用）与密文（再次查看用）。
func (uc *ShareUsecase) newToken() (token string, hash, sealed []byte, err error) {
	raw := make([]byte, shareTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, nil, fmt.Errorf("generate share token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	if sealed, err = uc.cipher.Seal(token); err != nil {
		return "", nil, nil, err
	}
	return token, hashToken(token), sealed, nil
}

func hashToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

// scopeGroupID 解析范围所属分组（个人范围返回空串）；范围对象不存在返回对应的 NotFound。
// 个人范围恒为操作者本人。
func (uc *ShareUsecase) scopeGroupID(ctx context.Context, actor Actor, scopeType int32, scopeID string) (resolvedID, groupID string, err error) {
	switch scopeType {
	case ScopeUser:
		return actor.UserID, "", nil
	case ScopeGroup:
		if scopeID == "" {
			return "", "", detail(ErrShareLinkInvalid, "scope id is required")
		}
		if _, err := uc.groups.Get(ctx, scopeID, ""); err != nil {
			return "", "", err
		}
		return scopeID, scopeID, nil
	case ScopeSchedule:
		if scopeID == "" {
			return "", "", detail(ErrShareLinkInvalid, "scope id is required")
		}
		s, err := uc.schedules.Get(ctx, scopeID)
		if err != nil {
			return "", "", err
		}
		return scopeID, s.GroupID, nil
	}
	return "", "", detail(ErrShareLinkInvalid, "unknown scope type")
}

// List 查询某个范围下的公共地址（不含令牌）。个人范围只能看自己的。
func (uc *ShareUsecase) List(ctx context.Context, actor Actor, scopeType int32, scopeID string) ([]*ShareLink, error) {
	scopeID, _, err := uc.scopeGroupID(ctx, actor, scopeType, scopeID)
	if err != nil {
		return nil, err
	}
	return uc.shares.List(ctx, scopeType, scopeID)
}

// ShareInput 新建公共地址入参。
type ShareInput struct {
	ScopeType  int32
	ScopeID    string
	Name       string
	ShowPhone  bool
	ExpireTime *time.Time
}

// Create 新建公共地址，返回令牌明文。分组/值班表范围须为分组管理员；
// 个人范围只能为本人创建。
func (uc *ShareUsecase) Create(ctx context.Context, actor Actor, in ShareInput) (*ShareLink, string, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, "", detail(ErrShareLinkInvalid, "name is required")
	}
	now := uc.now()
	if in.ExpireTime != nil && !in.ExpireTime.After(now) {
		return nil, "", detail(ErrShareLinkInvalid, "expire time must be in the future")
	}
	scopeID, groupID, err := uc.scopeGroupID(ctx, actor, in.ScopeType, in.ScopeID)
	if err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", fmt.Errorf("generate share link id: %w", err)
	}
	token, hash, sealed, err := uc.newToken()
	if err != nil {
		return nil, "", err
	}
	link := &ShareLink{
		ID: id, ScopeType: in.ScopeType, ScopeID: scopeID, Name: name, TokenHash: hash, TokenSealed: sealed,
		ShowPhone: in.ShowPhone, ExpireTime: in.ExpireTime, CreateTime: now,
	}
	err = uc.store.WithinTx(ctx, func(tx Tx) error {
		if groupID != "" {
			if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
				return err
			}
		}
		n, err := tx.CountShareLinks(ctx, in.ScopeType, scopeID)
		if err != nil {
			return err
		}
		if n >= maxShareLinks {
			return detail(ErrShareLinkInvalid, "at most %d share links per scope", maxShareLinks)
		}
		if err := tx.InsertShareLink(ctx, link, actor.UserID); err != nil {
			return err
		}
		if groupID == "" {
			return nil
		}
		return writeLog(ctx, tx, actor, groupID, scheduleScope(link), ActionShareCreate, "创建公共地址「"+name+"」",
			map[string]any{"scope_type": in.ScopeType, "show_phone": in.ShowPhone})
	})
	if err != nil {
		return nil, "", err
	}
	return link, token, nil
}

func scheduleScope(link *ShareLink) string {
	if link.ScopeType == ScopeSchedule {
		return link.ScopeID
	}
	return ""
}

// owner 读取地址并解析其管理归属：个人范围须为本人（或超级管理员），否则按不存在处理；
// 分组/值班表范围返回所属分组 ID（调用方再校验分组管理员身份）。
func (uc *ShareUsecase) owner(ctx context.Context, actor Actor, id string) (link *ShareLink, groupID string, err error) {
	if link, err = uc.shares.Get(ctx, id); err != nil {
		return nil, "", err
	}
	switch link.ScopeType {
	case ScopeUser:
		if link.ScopeID != actor.UserID && !actor.IsAdmin {
			// 不暴露他人地址是否存在。
			return nil, "", ErrShareLinkNotFound
		}
	case ScopeGroup:
		groupID = link.ScopeID
	case ScopeSchedule:
		s, err := uc.schedules.Get(ctx, link.ScopeID)
		if err != nil {
			return nil, "", err
		}
		groupID = s.GroupID
	}
	return link, groupID, nil
}

// Token 再次查看令牌明文：权限同管理操作（个人范围须为本人，分组/值班表范围须为
// 分组管理员或超级管理员）。
func (uc *ShareUsecase) Token(ctx context.Context, actor Actor, id string) (string, error) {
	_, groupID, err := uc.owner(ctx, actor, id)
	if err != nil {
		return "", err
	}
	if groupID != "" {
		g, err := uc.groups.Get(ctx, groupID, actor.UserID)
		if err != nil {
			return "", err
		}
		if !CanManage(actor, g) {
			return "", ErrForbidden
		}
	}
	sealed, err := uc.shares.GetSealedToken(ctx, id)
	if err != nil {
		return "", err
	}
	if len(sealed) == 0 {
		// 密文列引入之前创建的地址：库里只有哈希，无法还原。
		return "", detail(ErrShareLinkInvalid, "this address was created before viewing was supported; reset it to get a new address")
	}
	return uc.cipher.Open(sealed)
}

// manage 在事务内对已有地址执行管理操作（归属与权限见 owner）。
func (uc *ShareUsecase) manage(ctx context.Context, actor Actor, id, action, verb string, fn func(tx Tx, link *ShareLink) error) error {
	link, groupID, err := uc.owner(ctx, actor, id)
	if err != nil {
		return err
	}
	return uc.store.WithinTx(ctx, func(tx Tx) error {
		if groupID != "" {
			if _, err := lockManaged(ctx, tx, actor, groupID); err != nil {
				return err
			}
		}
		if err := fn(tx, link); err != nil {
			return err
		}
		if groupID == "" {
			return nil
		}
		return writeLog(ctx, tx, actor, groupID, scheduleScope(link), action, verb+"公共地址「"+link.Name+"」", nil)
	})
}

// Reset 重置令牌：旧地址立即失效，返回新令牌明文。
func (uc *ShareUsecase) Reset(ctx context.Context, actor Actor, id string) (string, error) {
	token, hash, sealed, err := uc.newToken()
	if err != nil {
		return "", err
	}
	err = uc.manage(ctx, actor, id, ActionShareReset, "重置", func(tx Tx, link *ShareLink) error {
		return tx.UpdateShareLinkToken(ctx, link.ID, hash, sealed, actor.UserID)
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

// Delete 作废公共地址。
func (uc *ShareUsecase) Delete(ctx context.Context, actor Actor, id string) error {
	return uc.manage(ctx, actor, id, ActionShareDelete, "作废", func(tx Tx, link *ShareLink) error {
		return tx.DeleteShareLink(ctx, link.ID, actor.UserID)
	})
}

// shareTouchTimeout 公共日历访问时间落账的上限（拿连接 + 一条按主键的 UPDATE，正常是毫秒级）。
const shareTouchTimeout = 2 * time.Second

// PublicCalendar 匿名可见的日历内容。
type PublicCalendar struct {
	Title string
	// Description 分组说明（仅分组范围）。
	Description string
	ScopeType   int32
	Shifts      []*Shift
}

// Public 凭令牌读取 [from, to) 的班次。无效、过期、作废、范围对象已删除统一返回
// ErrShareLinkNotFound（不区分原因，防探测）；来源 IP 超速返回 ErrPublicThrottled。
func (uc *ShareUsecase) Public(ctx context.Context, token, ip string, from, to time.Time) (*PublicCalendar, error) {
	if !from.Before(to) || to.Sub(from) > maxPublicRange {
		return nil, detail(ErrTimeRangeInvalid, "time range must be positive and within 62 days")
	}
	return uc.public(ctx, token, ip, from, to)
}

func (uc *ShareUsecase) public(ctx context.Context, token, ip string, from, to time.Time) (*PublicCalendar, error) {
	if !uc.limiter.allow(ip) {
		return nil, ErrPublicThrottled
	}
	cal, err := uc.resolve(ctx, token, from, to)
	if err != nil {
		if errIsNotFound(err) {
			uc.limiter.fail(ip)
			return nil, ErrShareLinkNotFound
		}
		return nil, err
	}
	return cal, nil
}

// errIsNotFound 报告错误是否为"对象不存在"类（地址、分组、值班表）。
func errIsNotFound(err error) bool {
	return ErrShareLinkNotFound.Is(err) || ErrGroupNotFound.Is(err) || ErrScheduleNotFound.Is(err)
}

func (uc *ShareUsecase) resolve(ctx context.Context, token string, from, to time.Time) (*PublicCalendar, error) {
	link, err := uc.shares.FindByHash(ctx, hashToken(token))
	if err != nil {
		return nil, err
	}
	if link.ExpireTime != nil && !link.ExpireTime.After(uc.now()) {
		return nil, ErrShareLinkNotFound
	}
	cal := &PublicCalendar{ScopeType: link.ScopeType}
	var scope ShiftScope
	switch link.ScopeType {
	case ScopeGroup:
		g, err := uc.groups.Get(ctx, link.ScopeID, "")
		if err != nil {
			return nil, err
		}
		cal.Title = g.Name
		cal.Description = g.Description
		scope.GroupID = link.ScopeID
	case ScopeSchedule:
		s, err := uc.schedules.Get(ctx, link.ScopeID)
		if err != nil {
			return nil, err
		}
		// 值班表范围：标题带上所属分组，访问者能看出是哪个分组的哪张值班表。
		cal.Title = s.Name
		if s.GroupName != "" {
			cal.Title = s.GroupName + " / " + s.Name
		}
		scope.ScheduleID = link.ScopeID
	default:
		found, err := uc.users.Lookup(ctx, []string{link.ScopeID})
		if err != nil {
			return nil, err
		}
		u := found[link.ScopeID]
		if u == nil || !u.Available {
			return nil, ErrShareLinkNotFound
		}
		cal.Title = u.DisplayName() + " 的值班"
		scope.UserID = link.ScopeID
	}
	shifts, err := uc.shifts.ListRangeScoped(ctx, from, to, scope)
	if err != nil {
		return nil, err
	}
	cal.Shifts = shifts
	if !link.ShowPhone {
		for _, sh := range cal.Shifts {
			sh.UserPhone = ""
		}
	}
	// 访问时间仅用于管理页展示，写失败不影响读取。落账在响应前同步执行：不随请求取消（WithoutCancel），
	// 但必须有上限——连接池排队时不能让这条可有可无的写把匿名接口拖住。
	touchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shareTouchTimeout)
	defer cancel()
	_ = uc.shares.Touch(touchCtx, link.ID)
	return cal, nil
}

// publicLimiter 匿名访问限速器：按来源 IP 的一分钟固定窗口计数（请求数与无效令牌次数
// 分别计），有界 map + 惰性过期。令牌有 256 bit 熵，限速主要用于抑制滥用与探测流量。
type publicLimiter struct {
	mu      sync.Mutex
	entries map[string]*publicWindow
	now     func() time.Time
}

type publicWindow struct {
	start    time.Time
	requests int
	failures int
}

func newPublicLimiter(now func() time.Time) *publicLimiter {
	return &publicLimiter{entries: make(map[string]*publicWindow, 64), now: now}
}

// window 取（或新开）ip 的当前窗口；调用方须持锁。
func (l *publicLimiter) window(ip string) *publicWindow {
	now := l.now()
	w, ok := l.entries[ip]
	if ok && now.Sub(w.start) < time.Minute {
		return w
	}
	if !ok && len(l.entries) >= publicLimiterEntries {
		for k, v := range l.entries {
			if now.Sub(v.start) >= time.Minute {
				delete(l.entries, k)
			}
		}
		if len(l.entries) >= publicLimiterEntries {
			clear(l.entries) // 仍然满：整体重置，宁可放过也不让内存无界增长
		}
	}
	w = &publicWindow{start: now}
	l.entries[ip] = w
	return w
}

// allow 记一次请求并报告是否放行。
func (l *publicLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	w := l.window(ip)
	w.requests++
	return w.requests <= publicRatePerMinute && w.failures < publicFailurePerMinute
}

// fail 记一次无效令牌。
func (l *publicLimiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.window(ip).failures++
}
