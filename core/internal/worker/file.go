package worker

import (
	"log/slog"
	"time"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// filePurgeInterval 附件清理轮询周期。任务自身并发安全（SKIP LOCKED 领取），
// 多实例部署无需选主；积压时按批连续处理直到取空。
const filePurgeInterval = 10 * time.Minute

// FileLoops 文件域后台循环。
type FileLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *FileLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewFileLoops 构造文件域后台循环：清理过期临时文件与超过保留期的软删文件。
func NewFileLoops(uc *bizfile.Usecase, logger *slog.Logger, rec loop.Recorder) *FileLoops {
	return &FileLoops{loops: []*loop.Loop{
		loop.New("file-purge", filePurgeInterval, logger, rec, uc.Purge),
	}}
}
