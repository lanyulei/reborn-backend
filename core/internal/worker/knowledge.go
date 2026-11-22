package worker

import (
	"log/slog"
	"time"

	bizknowledge "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// 知识库后台循环周期。导入 / 索引任务以 SKIP LOCKED 领取并带租约，多实例无需选主；巡检与清理是幂等的
// 批处理，多实例重复执行无害（通知有 dedup 门闩）。
const (
	knowledgeIngestInterval = 5 * time.Second
	knowledgeIndexInterval  = 3 * time.Second
	knowledgeReviewInterval = 6 * time.Hour
	knowledgePurgeInterval  = time.Hour
)

// KnowledgeLoops 知识库域后台循环。
type KnowledgeLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *KnowledgeLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewKnowledgeLoops 构造知识库域后台循环：文件导入、分块向量化、复审巡检、日志与软删文档清理。
func NewKnowledgeLoops(ingest *bizknowledge.IngestUsecase, index *bizknowledge.IndexUsecase, gov *bizknowledge.GovernanceUsecase,
	logger *slog.Logger, rec loop.Recorder,
) *KnowledgeLoops {
	return &KnowledgeLoops{loops: []*loop.Loop{
		loop.New("knowledge-ingest", knowledgeIngestInterval, logger, rec, ingest.Run),
		loop.New("knowledge-index", knowledgeIndexInterval, logger, rec, index.Run),
		loop.New("knowledge-review-patrol", knowledgeReviewInterval, logger, rec, gov.ReviewPatrol),
		loop.New("knowledge-purge", knowledgePurgeInterval, logger, rec, gov.Purge),
	}}
}
