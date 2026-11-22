package biz

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 治理巡检参数。
const (
	// reviewPatrolBatch 每轮处理的文档数。
	reviewPatrolBatch = 200
	// purgeBatch 每轮物理清理条数。
	purgeBatch = 200
)

// GovernanceUsecase 治理与统计：复审巡检通知、日志 / 软删文档清理、统计、检索日志查询。
type GovernanceUsecase struct {
	docs   DocumentRepo
	spaces SpaceRepo
	logs   SearchLogRepo
	chunks ChunkRepo
	txm    txrun.Manager
	notify biznotify.NotificationWriter
	dir    Directory
	cfg    Config
	logger *slog.Logger
	now    func() time.Time
}

// NewGovernanceUsecase new a GovernanceUsecase.
func NewGovernanceUsecase(docs DocumentRepo, spaces SpaceRepo, logs SearchLogRepo, chunks ChunkRepo, txm txrun.Manager,
	notify biznotify.NotificationWriter, dir Directory, cfg Config, logger *slog.Logger,
) *GovernanceUsecase {
	return &GovernanceUsecase{docs: docs, spaces: spaces, logs: logs, chunks: chunks, txm: txm, notify: notify, dir: dir, cfg: cfg, logger: logger, now: time.Now}
}

// ReviewPatrol 复审巡检：到期前 7 天提醒一次、到期后再提醒一次（按 review_notify_time 去重）。
// 「尚未提醒」的判定下推到查询里，否则已提醒过的文档会占满每轮的一页，让后面的文档永远轮不到。
func (uc *GovernanceUsecase) ReviewPatrol(ctx context.Context) (int, error) {
	now := uc.now()
	due := now.Add(reviewNoticeAhead)
	published := DocStatusPublished
	items, _, err := uc.docs.List(ctx, DocumentFilter{Unrestricted: true, Status: &published, ReviewDueBefore: &due, ReviewNoticePending: true}, 1, reviewPatrolBatch)
	if err != nil {
		return 0, err
	}
	spaces, err := uc.spaces.List(ctx, "", "")
	if err != nil {
		return 0, err
	}
	spaceNames := make(map[string]string, len(spaces))
	for _, s := range spaces {
		spaceNames[s.ID] = s.Name
	}
	n := 0
	for _, d := range items {
		if d.ReviewAt == nil {
			continue
		}
		overdue := !d.ReviewAt.After(now)
		switch {
		case d.ReviewNotifyTime == nil:
		case overdue && d.ReviewNotifyTime.Before(*d.ReviewAt):
		default:
			continue
		}
		if err := uc.notifyReviewDue(ctx, d, spaceNames[d.SpaceID], overdue); err != nil {
			uc.logger.WarnContext(ctx, "knowledge: review due notice", "document", d.ID, "error", err)
			continue
		}
		if err := uc.docs.MarkReviewNotified(ctx, d.ID, now); err != nil {
			return n, err
		}
		n++
	}
	// 返回本批实际提醒的条数：>0 时循环立即进入下一轮，把到期的文档一次巡检完。
	return n, nil
}

func (uc *GovernanceUsecase) notifyReviewDue(ctx context.Context, d *Document, spaceName string, overdue bool) error {
	title := "知识文档即将到期复审：" + d.Title
	if overdue {
		title = "知识文档已超期未复审：" + d.Title
	}
	payload, _ := json.Marshal(map[string]any{
		"documentId": d.ID, "title": d.Title, "space": spaceName, "reviewAt": d.ReviewAt.UTC().Format(time.RFC3339),
		"overdue": overdue, "path": "/knowledge/doc/" + d.ID,
	})
	return uc.txm.WithinTx(ctx, func(tx txrun.Tx) error {
		_, err := uc.notify.NotifyEvent(ctx, tx, biznotify.Event{
			BizID: NotifyBizID, EventType: EventReviewDue, Severity: 1, Title: title, Payload: payload,
			Recipients: []string{d.OwnerID}, DedupKey: EventReviewDue + ":" + d.ID + ":" + d.ReviewAt.Format("2006-01-02") + ":" + boolStr(overdue),
		})
		return err
	})
}

// Purge 清理：保留期外的检索日志、软删超过保留期的文档。
func (uc *GovernanceUsecase) Purge(ctx context.Context) (int, error) {
	now := uc.now()
	n, err := uc.logs.Purge(ctx, now.Add(-uc.cfg.LogRetention), purgeBatch)
	if err != nil {
		return 0, err
	}
	m, err := uc.docs.PurgeDeleted(ctx, now.Add(-DeletedRetention), purgeBatch)
	if err != nil {
		return n, err
	}
	return n + m, nil
}

// Stats 统计视图。
type Stats struct {
	DocumentStats
	ChunkCount int64
	SearchStats
}

// Stats 全局统计：文档 / 分块 / 最近 days 天的检索与反馈。
func (uc *GovernanceUsecase) Stats(ctx context.Context, days int) (*Stats, error) {
	if days <= 0 {
		days = 30
	}
	now := uc.now()
	ds, err := uc.docs.Stats(ctx, now)
	if err != nil {
		return nil, err
	}
	total, _, err := uc.chunks.Counts(ctx)
	if err != nil {
		return nil, err
	}
	ss, err := uc.logs.Stats(ctx, now.Add(-time.Duration(days)*24*time.Hour))
	if err != nil {
		return nil, err
	}
	return &Stats{DocumentStats: ds, ChunkCount: total, SearchStats: ss}, nil
}

// ListSearchLogs 检索日志（管理员），带用户显示名。
func (uc *GovernanceUsecase) ListSearchLogs(ctx context.Context, f SearchLogFilter, page, pageSize int64) ([]*SearchLog, int64, error) {
	items, total, err := uc.logs.List(ctx, f, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	ids := make([]string, 0, len(items))
	for _, l := range items {
		ids = append(ids, l.UserID)
	}
	if len(ids) > 0 {
		names, err := uc.dir.UserNames(ctx, ids)
		if err != nil {
			return nil, 0, err
		}
		for _, l := range items {
			l.UserName = names[l.UserID]
		}
	}
	return items, total, nil
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
