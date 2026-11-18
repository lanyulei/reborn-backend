package worker

import (
	"log/slog"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/loop"
)

// inspectionInterval 巡检循环的检查间隔：每轮至多执行一个集群的一次巡检（手动触发的等待时间上限）。
const inspectionInterval = 15 * time.Second

// GovLoops 治理类后台循环：巡检。每个实例都跑，各自只处理本实例负责的集群（领取语句 SKIP LOCKED）。
type GovLoops struct {
	Loops []*loop.Loop
}

// NewGovLoops 构造治理类循环。
func NewGovLoops(inspect *biz.InspectionUsecase, rec loop.Recorder, logger *slog.Logger) *GovLoops {
	return &GovLoops{Loops: []*loop.Loop{
		guarded("inspection", inspectionInterval, logger, rec, inspect.Run),
	}}
}
