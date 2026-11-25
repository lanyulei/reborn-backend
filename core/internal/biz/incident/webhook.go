package biz

import (
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// Webhook 订阅（功能设计 §8.5）：Agent / 外部系统感知故障的出站钩子。领域事件与业务写同事务入箱，
// 后台循环以租约领取投递；仅 https、不跟随跳转、解析后地址做内网校验（data 层 WebhookSender）。

// 投递参数。
const (
	outboxBatch = 20
	// outboxLease 一批出站箱行的租约。整批是串行投递的，租约必须覆盖「批大小 × 单次投递上限」：租约先于整批
	// 到期时，多副本下还没轮到的行会被别的副本重新领取并重复投递（尝试次数也被多算，提前丢弃）。余量留给领取
	// 与落账。代价：实例在一批中途崩溃时，这些行要等租约到期才会被重领。
	outboxLease       = time.Duration(outboxBatch)*WebhookSendMax + time.Minute
	outboxBackoffBase = 30 * time.Second
	outboxBackoffMax  = 30 * time.Minute
	outboxMaxAttempts = 8
	outboxRetention   = 30 * 24 * time.Hour
	maxWebhooks       = 50
)

// knownHookEvents 可订阅的领域事件。
var knownHookEvents = []string{"opened", "severity", "status", "alert", "proposal", "update", "merged", "finding", "comment", "agent_lease", "commander"}

// webhookCache 启用的 Webhook 订阅的短缓存（每次写事务都要判断是否入箱）。
type webhookCache struct {
	repo    WebhookRepo
	mu      sync.Mutex
	items   []*Webhook
	expires time.Time
	now     func() time.Time
}

func newWebhookCache(repo WebhookRepo) *webhookCache {
	return &webhookCache{repo: repo, now: time.Now}
}

func (c *webhookCache) enabled(ctx context.Context) ([]*Webhook, error) {
	c.mu.Lock()
	if c.items != nil && c.now().Before(c.expires) {
		items := c.items
		c.mu.Unlock()
		return items, nil
	}
	c.mu.Unlock()
	items, err := c.repo.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []*Webhook{}
	}
	c.mu.Lock()
	c.items, c.expires = items, c.now().Add(webhookCacheTTL)
	c.mu.Unlock()
	return items, nil
}

func (c *webhookCache) invalidate() {
	c.mu.Lock()
	c.items = nil
	c.mu.Unlock()
}

// WebhookUsecase Webhook 订阅的维护与出站投递。
type WebhookUsecase struct {
	d     *Deps
	cache *webhookCache
	now   func() time.Time
}

// NewWebhookUsecase new a WebhookUsecase.
func NewWebhookUsecase(d *Deps, inc *IncidentUsecase) *WebhookUsecase {
	return &WebhookUsecase{d: d, cache: inc.hooks, now: time.Now}
}

// WebhookSpec 可编辑内容。
type WebhookSpec struct {
	Name    string
	URL     string
	Secret  string
	Events  []string
	Enabled bool
}

func validateWebhook(s *WebhookSpec) error {
	s.Name, s.URL = cleanText(s.Name), strings.TrimSpace(s.URL)
	if s.Name == "" || len([]rune(s.Name)) > 128 {
		return detail(ErrWebhookInvalid, "name must be 1-128 characters")
	}
	u, err := url.Parse(s.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return detail(ErrWebhookInvalid, "url must be an https address without credentials")
	}
	for _, e := range s.Events {
		if !slices.Contains(knownHookEvents, e) {
			return detail(ErrWebhookInvalid, "unknown event %q", e)
		}
	}
	s.Events = uniq(s.Events)
	return nil
}

// List 全部订阅（含出站箱计数）。
func (uc *WebhookUsecase) List(ctx context.Context) ([]*Webhook, error) {
	items, err := uc.d.Webhooks.List(ctx)
	if err != nil {
		return nil, err
	}
	pending, failed, err := uc.d.Outbox.Counts(ctx)
	if err != nil {
		return nil, err
	}
	for _, w := range items {
		w.Pending, w.Failed = pending[w.ID], failed[w.ID]
	}
	return items, nil
}

// Create 新建订阅（密钥必填，AES-256-GCM 落库）。
func (uc *WebhookUsecase) Create(ctx context.Context, userID string, s WebhookSpec) (*Webhook, error) {
	if err := validateWebhook(&s); err != nil {
		return nil, err
	}
	if s.Secret == "" || len(s.Secret) < 16 {
		return nil, detail(ErrWebhookInvalid, "secret must be at least 16 characters")
	}
	items, err := uc.d.Webhooks.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(items) >= maxWebhooks {
		return nil, detail(ErrWebhookInvalid, "at most %d webhooks", maxWebhooks)
	}
	sealed, err := uc.d.Cipher.Seal(s.Secret)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	w := &Webhook{ID: id, Name: s.Name, URL: s.URL, Secret: sealed, Events: s.Events, Enabled: s.Enabled}
	if err := uc.d.Webhooks.Create(ctx, w, userID); err != nil {
		return nil, err
	}
	uc.cache.invalidate()
	return uc.d.Webhooks.Get(ctx, id)
}

// Update 编辑订阅；Secret 为空保留原密钥。
func (uc *WebhookUsecase) Update(ctx context.Context, userID, id string, s WebhookSpec, version int32) (*Webhook, error) {
	if err := validateWebhook(&s); err != nil {
		return nil, err
	}
	w := &Webhook{ID: id, Name: s.Name, URL: s.URL, Events: s.Events, Enabled: s.Enabled}
	if s.Secret != "" {
		if len(s.Secret) < 16 {
			return nil, detail(ErrWebhookInvalid, "secret must be at least 16 characters")
		}
		sealed, err := uc.d.Cipher.Seal(s.Secret)
		if err != nil {
			return nil, err
		}
		w.Secret = sealed
	}
	if err := uc.d.Webhooks.Update(ctx, w, version, userID); err != nil {
		return nil, err
	}
	uc.cache.invalidate()
	return uc.d.Webhooks.Get(ctx, id)
}

// Delete 删除订阅。
func (uc *WebhookUsecase) Delete(ctx context.Context, userID, id string) error {
	w, err := uc.d.Webhooks.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := uc.d.Webhooks.Delete(ctx, id, w.Version, userID); err != nil {
		return err
	}
	uc.cache.invalidate()
	return nil
}

// Run 投递一批出站箱行（后台循环 incident-webhook）：租约领取、失败退避、次数用尽丢弃。
func (uc *WebhookUsecase) Run(ctx context.Context) (int, error) {
	items, err := uc.d.Outbox.Claim(ctx, outboxBatch, outboxLease)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	webhooks := map[string]*Webhook{}
	for _, it := range items {
		if _, ok := webhooks[it.WebhookID]; ok {
			continue
		}
		w, err := uc.d.Webhooks.Get(ctx, it.WebhookID)
		if err != nil {
			if isNotFound(err) {
				webhooks[it.WebhookID] = nil
				continue
			}
			return 0, err
		}
		webhooks[it.WebhookID] = w
	}
	done := 0
	for _, it := range items {
		w := webhooks[it.WebhookID]
		if w == nil || !w.Enabled {
			if err := uc.d.Outbox.Finish(ctx, it, OutboxDropped, "订阅已删除或停用"); err != nil {
				return done, err
			}
			done++
			continue
		}
		secret := ""
		if len(w.Secret) > 0 {
			if secret, err = uc.d.Cipher.Open(w.Secret); err != nil {
				uc.d.Logger.ErrorContext(ctx, "incident webhook: open secret", "webhook", w.ID, "error", err)
				if err := uc.d.Outbox.Finish(ctx, it, OutboxDropped, "密钥无法解密"); err != nil {
					return done, err
				}
				done++
				continue
			}
		}
		retryable, serr := uc.d.Sender.Send(ctx, WebhookDelivery{URL: w.URL, Secret: secret, EventID: it.ID, Body: it.Payload})
		switch {
		case serr == nil:
			err = uc.d.Outbox.Finish(ctx, it, OutboxDone, "")
		case !retryable || it.Attempts >= outboxMaxAttempts:
			uc.d.Logger.WarnContext(ctx, "incident webhook dropped", "webhook", w.ID, "outbox", it.ID, "attempts", it.Attempts, "error", serr)
			err = uc.d.Outbox.Finish(ctx, it, OutboxDropped, truncate(serr.Error(), 500))
		default:
			err = uc.d.Outbox.Retry(ctx, it, backoff(it.Attempts, outboxBackoffBase, outboxBackoffMax), truncate(serr.Error(), 500))
		}
		if err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// Purge 清理已终结的出站箱行。
func (uc *WebhookUsecase) Purge(ctx context.Context) (int, error) {
	return uc.d.Outbox.Purge(ctx, uc.now().Add(-outboxRetention), 1000)
}

// backoff 第 attempts 次失败后的等待：base × 2^(attempts-1)，封顶 maxWait。
func backoff(attempts int32, base, maxWait time.Duration) time.Duration {
	d := base
	for i := int32(1); i < attempts && d < maxWait; i++ {
		d *= 2
	}
	return min(d, maxWait)
}

// jsonRaw 便捷编码（失败回退空对象）。
func jsonRaw(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return raw
}
