package worker

import (
	"log/slog"
	"time"

	bizk8s "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	"github.com/lanyulei/reborn/backend/kit/loop"

	"github.com/go-kratos/kratos/v3/transport"
)

// k8sNotifyInterval 容器平台通知派发的轮询周期。派发自身并发安全（发件箱行锁 SKIP LOCKED 领取，
// 与通知事件同事务提交），多实例各跑各的，不需要选主。
const k8sNotifyInterval = 2 * time.Second

// K8sLoops 容器平台域后台循环。
type K8sLoops struct {
	loops []*loop.Loop
}

// Servers 以 transport.Server 形式返回各循环，交给 kratos.App 托管。
func (l *K8sLoops) Servers() []transport.Server {
	out := make([]transport.Server, 0, len(l.loops))
	for _, lp := range l.loops {
		out = append(out, lp)
	}
	return out
}

// NewK8sLoops 构造容器平台域后台循环：把数据面写入的通知发件箱派发为通知事件。
func NewK8sLoops(uc *bizk8s.DispatchUsecase, changes *bizk8s.ChangeWorker, logger *slog.Logger, rec loop.Recorder) *K8sLoops {
	return &K8sLoops{loops: []*loop.Loop{
		loop.New("k8s-notify", k8sNotifyInterval, logger, rec, uc.Run),
		// 变更审批：执行已批准的权限申请、作废过期的变更单、把执行结果写回工单并通知发起人。
		loop.New("k8s-change", 3*time.Second, logger, rec, changes.Run),
	}}
}
