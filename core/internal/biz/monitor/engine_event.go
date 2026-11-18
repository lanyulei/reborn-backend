package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 平台自监控事件的派发：评估引擎检测到数据源持续不可用 / 规则持续评估失败（及其恢复）时写
// monitor_engine_event，这里把它派发为 monitor.engine.* 通知事件。

const (
	// enginePagePath 引擎状态页（数据源类事件的详情链接）；规则类事件链接到规则详情。
	enginePagePath = "/monitor/engine"
	rulePathPrefix = "/monitor/rule/"
	// engineDedupPrefix / engineGroupPrefix 通知事件的去重键与聚合键前缀。
	engineDedupPrefix = "monitor:engine:"
	engineGroupPrefix = "monitor:engine:"
	// 丢弃原因（记入 last_error）。
	dropEngineUnknownType = "未知的自监控事件类型"
	dropEngineBadDetail   = "事件内容无法解析"
)

// engineEventMeta 各类自监控事件的通知级别与标题前缀。故障类按「严重」，恢复类按「提示」。
var engineEventMeta = map[string]struct {
	severity int32
	title    string
}{
	mc.EventEngineDatasourceDown: {mc.SeverityCritical, "数据源持续不可用"},
	mc.EventEngineDatasourceUp:   {mc.SeverityInfo, "数据源已恢复"},
	mc.EventEngineRuleFailing:    {mc.SeverityCritical, "告警规则持续评估失败"},
	mc.EventEngineRuleRecovered:  {mc.SeverityInfo, "告警规则评估已恢复"},
}

// EngineEvent 自监控事件发件箱的一行（monitor_engine_event，引擎写入、core 派发）。Detail 是
// monitorcontract.EngineEventPayload 的 JSON。
type EngineEvent struct {
	ID       string
	Type     string
	ObjectID string
	DedupKey string
	Detail   []byte
	Attempts int32
}

// EngineEventRepo 自监控事件发件箱仓储。core 只允许改 status / attempts / last_error / done_time。
// 语义同 OutboxRepo：行锁 SKIP LOCKED 领取、失败退避、次数用尽丢弃。
type EngineEventRepo interface {
	// ClaimTx 在事务内领取一条可派发的待派发行（最早入箱的在前）；没有返回 nil。
	ClaimTx(ctx context.Context, tx txrun.Tx, exclude []string) (*EngineEvent, error)
	// FinishTx 在事务内把行置为已派发 / 已丢弃（reason 记入 last_error）。
	FinishTx(ctx context.Context, tx txrun.Tx, id string, status int32, reason string) error
	// Fail 独立语句记一次失败；失败次数达到 monitorcontract.OutboxMaxAttempts 时置为已丢弃并返回 true。
	Fail(ctx context.Context, id, reason string) (dropped bool, err error)
}

// EngineEventUsecase 把自监控事件派发为通知事件（后台循环 monitor-engine-notify）。多实例安全：每行在
// 自己的事务里处理（行锁领取、同事务写通知事件并置为已派发），notify 的去重键是第二道保险。
type EngineEventUsecase struct {
	txm      TxManager
	repo     EngineEventRepo
	audience EngineAudience
	events   EventWriter
	log      *slog.Logger
	siteURL  string
}

// NewEngineEventUsecase new an EngineEventUsecase.
func NewEngineEventUsecase(txm TxManager, repo EngineEventRepo, audience EngineAudience, events EventWriter, logger *slog.Logger,
	c *conf.Monitor,
) (*EngineEventUsecase, error) {
	siteURL, err := normalizeSiteURL(c.GetSiteUrl())
	if err != nil {
		return nil, err
	}
	return &EngineEventUsecase{txm: txm, repo: repo, audience: audience, events: events, log: logger, siteURL: siteURL}, nil
}

// Run 处理一批自监控事件，返回处理（派发或丢弃）的行数。error 的含义同 DispatchUsecase.Run。
func (uc *EngineEventUsecase) Run(ctx context.Context) (int, error) {
	// 收件人一轮只解析一次（按配置的角色取可用用户）；没有待派发的行时不解析。
	var recipients [][]string
	return drain(ctx, uc.txm, "monitor engine event dispatch", rowHandler[EngineEvent]{
		claim: uc.repo.ClaimTx,
		id:    func(ev *EngineEvent) string { return ev.ID },
		process: func(ctx context.Context, tx txrun.Tx, ev *EngineEvent) error {
			if recipients == nil {
				users, err := uc.audience.UserIDs(ctx)
				if err != nil {
					return fmt.Errorf("resolve engine event audience: %w", err)
				}
				// 没有配置收件角色：不带收件人写一条，交由通知路由的固定收件人与订阅者。
				recipients = [][]string{nil}
				if users = uniq(users); len(users) > 0 {
					recipients = slices.Collect(slices.Chunk(users, maxEventRecipients))
				}
			}
			return uc.process(ctx, tx, ev, recipients)
		},
		fail: uc.fail,
	})
}

func (uc *EngineEventUsecase) fail(ctx context.Context, ev *EngineEvent, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	dropped, err := uc.repo.Fail(ctx, ev.ID, truncate(cause.Error(), 500))
	if err != nil {
		return fmt.Errorf("dispatch engine event %s: %w (record failure: %w)", ev.ID, cause, err)
	}
	if dropped {
		uc.log.ErrorContext(ctx, "monitor engine event dropped after too many failures",
			"event_id", ev.ID, "type", ev.Type, "object_id", ev.ObjectID, "attempts", mc.OutboxMaxAttempts, "error", cause)
		return nil
	}
	uc.log.WarnContext(ctx, "monitor engine event failed, will retry after backoff",
		"event_id", ev.ID, "type", ev.Type, "object_id", ev.ObjectID, "attempts", ev.Attempts+1, "error", cause)
	return nil
}

// process 在事务内派发一行；recipients 为按 maxEventRecipients 分好片的收件人（单个 nil 片 = 不指定）。
func (uc *EngineEventUsecase) process(ctx context.Context, tx txrun.Tx, ev *EngineEvent, recipients [][]string) error {
	drop := func(reason string) error {
		uc.log.WarnContext(ctx, "monitor engine event dropped", "event_id", ev.ID, "type", ev.Type, "object_id", ev.ObjectID, "reason", reason)
		return uc.repo.FinishTx(ctx, tx, ev.ID, mc.OutboxDropped, reason)
	}
	meta, ok := engineEventMeta[ev.Type]
	if !ok {
		return drop(dropEngineUnknownType)
	}
	var p mc.EngineEventPayload
	if err := json.Unmarshal(ev.Detail, &p); err != nil {
		return drop(dropEngineBadDetail)
	}
	if p.ObjectID == "" {
		p.ObjectID = ev.ObjectID
	}
	if p.ObjectType == "" {
		p.ObjectType = mc.EngineObjectDatasource
		if ev.Type == mc.EventEngineRuleFailing || ev.Type == mc.EventEngineRuleRecovered {
			p.ObjectType = mc.EngineObjectRule
		}
	}
	p.URL = uc.siteURL + enginePagePath
	if p.ObjectType == mc.EngineObjectRule {
		p.URL = uc.siteURL + rulePathPrefix + p.ObjectID
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", ev.Type, err)
	}
	title := meta.title
	if p.ObjectName != "" {
		title += "：" + p.ObjectName
	}
	title = truncateBytes(truncate(title, maxTitleRunes), maxTitleBytes)
	// 去重键：引擎的 dedup_key 可达 160 字节，超过 notify 的 128 字节上限，取其摘要（稳定、同一次故障同键）。
	sum := sha256.Sum256([]byte(ev.DedupKey))
	dedup := engineDedupPrefix + hex.EncodeToString(sum[:16])
	for i, chunk := range recipients {
		key := dedup
		if i > 0 {
			key += ":" + strconv.Itoa(i)
		}
		if _, err := uc.events.NotifyEvent(ctx, tx, biznotify.Event{
			BizID: mc.NotifyBizID, EventType: ev.Type, Severity: meta.severity, Title: title, Payload: raw,
			GroupKey: engineGroupPrefix + strings.ReplaceAll(ev.ObjectID, "-", ""), DedupKey: key, Recipients: chunk,
		}); err != nil {
			return err
		}
	}
	return uc.repo.FinishTx(ctx, tx, ev.ID, mc.OutboxDone, "")
}
