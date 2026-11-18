package biz

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 平台自监控（P2/P3 设计 §3.9）：实例心跳、数据源持续不可用与规则持续评估失败的事件。
//
// 「同一次故障只写一行」靠 monitor_engine_event.dedup_key 的唯一索引：键 = 事件类型 + 对象 + 故障的
// 起始时刻（数据源行的 health_since / 任务行的 error_since，微秒）。起始时刻落在库里，换哪个实例检测、
// 检测多少遍都是同一个键。恢复事件与故障事件用同一个起始时刻配对，并且只在故障事件确实发过时才写；
// 两者的写入分别与「状态翻转」在同一行上加锁串行（数据源行 / 任务行），不会出现发了故障却漏了恢复、
// 或没发故障却发了恢复。

// engineTimeLayout 自监控事件内容里的时间格式（与 core 通知正文的时间格式一致：带偏移，这里恒为 UTC）。
const engineTimeLayout = "2006-01-02 15:04:05 -07:00"

// engineEventKey 自监控事件的去重键：类型 + 对象 ID + 故障起始时刻（微秒：timestamptz 的精度）。
func engineEventKey(typ string, since time.Time, ids ...string) string {
	return typ + ":" + strings.Join(ids, ":") + ":" + strconv.FormatInt(since.UnixMicro(), 10)
}

// humanDuration 把时长写成至多两个相邻单位的中文短语（与 core 通知正文里的写法一致：「3 小时 20 分钟」）。
func humanDuration(d time.Duration) string {
	secs := max(int64(d.Seconds()), 0)
	units := [...]struct {
		size int64
		name string
	}{{86400, "天"}, {3600, "小时"}, {60, "分钟"}, {1, "秒"}}
	parts := make([]string, 0, 2)
	for _, u := range units {
		if n := secs / u.size; n > 0 && len(parts) < 2 {
			parts = append(parts, strconv.FormatInt(n, 10)+" "+u.name)
			secs -= n * u.size
		} else if len(parts) > 0 {
			break
		}
	}
	if len(parts) == 0 {
		return "0 秒"
	}
	return strings.Join(parts, " ")
}

func outagePayload(objectType, id, name string, since, now time.Time) monitorcontract.EngineEventPayload {
	return monitorcontract.EngineEventPayload{
		ObjectType: objectType, ObjectID: id, ObjectName: name,
		Since: since.UTC().Format(engineTimeLayout), Duration: humanDuration(now.Sub(since)),
	}
}

// datasourceDownEvent 数据源持续不可用。
func datasourceDownEvent(newID func() (string, error), o *DatasourceOutage) (*EngineEventInsert, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	detail := outagePayload(monitorcontract.EngineObjectDatasource, o.DatasourceID, o.Name, o.Since, o.Now)
	detail.Error = o.Error
	return &EngineEventInsert{
		ID: id, Type: monitorcontract.EventEngineDatasourceDown, ObjectID: o.DatasourceID, Detail: detail,
		DedupKey: engineEventKey(monitorcontract.EventEngineDatasourceDown, o.Since, o.DatasourceID),
	}, nil
}

// datasourceRecoveredEvent 数据源恢复：只在同一次故障的 down 事件存在时写入。
func datasourceRecoveredEvent(newID func() (string, error), r HealthRecovery) (*EngineEventInsert, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	return &EngineEventInsert{
		ID: id, Type: monitorcontract.EventEngineDatasourceUp, ObjectID: r.DatasourceID,
		Detail:   outagePayload(monitorcontract.EngineObjectDatasource, r.DatasourceID, r.Name, r.Since, r.Now),
		DedupKey: engineEventKey(monitorcontract.EventEngineDatasourceUp, r.Since, r.DatasourceID),
		Requires: engineEventKey(monitorcontract.EventEngineDatasourceDown, r.Since, r.DatasourceID),
	}, nil
}

// ruleFailingEvent 规则持续评估失败。
func ruleFailingEvent(newID func() (string, error), f *RuleFailure) (*EngineEventInsert, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	detail := outagePayload(monitorcontract.EngineObjectRule, f.RuleID, f.RuleName, f.Since, f.Now)
	detail.DatasourceName, detail.Error, detail.Failures = f.DatasourceName, f.Error, f.Failures
	return &EngineEventInsert{
		ID: id, Type: monitorcontract.EventEngineRuleFailing, ObjectID: f.RuleID, Detail: detail,
		DedupKey: engineEventKey(monitorcontract.EventEngineRuleFailing, f.Since, f.RuleID, f.DatasourceID),
	}, nil
}

// ruleRecoveredEvent 规则的评估恢复：只在同一次故障的 rule_failing 事件存在时写入。
func ruleRecoveredEvent(newID func() (string, error), task *Task, ruleName, datasourceName string, now time.Time) (*EngineEventInsert, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	detail := outagePayload(monitorcontract.EngineObjectRule, task.RuleID, ruleName, task.ErrorSince, now)
	detail.DatasourceName, detail.Failures = datasourceName, task.ErrorCount
	return &EngineEventInsert{
		ID: id, Type: monitorcontract.EventEngineRuleRecovered, ObjectID: task.RuleID, Detail: detail,
		DedupKey: engineEventKey(monitorcontract.EventEngineRuleRecovered, task.ErrorSince, task.RuleID, task.DatasourceID),
		Requires: engineEventKey(monitorcontract.EventEngineRuleFailing, task.ErrorSince, task.RuleID, task.DatasourceID),
	}, nil
}

// WatchOptions 自监控检测参数。
type WatchOptions struct {
	// DatasourceDownAfter 探活连续失败多久算「持续不可用」。
	DatasourceDownAfter time.Duration
	// RuleFailingAfter / RuleFailingMinFailures 评估任务连续查询错误多久、且至少多少次算「持续评估失败」。
	RuleFailingAfter       time.Duration
	RuleFailingMinFailures int
	// InstanceStaleAfter 心跳行多久没有更新即清理。
	InstanceStaleAfter time.Duration
}

// watchBatch 每轮每类至多上报的对象数（余下的下一轮接着报）。
const watchBatch = 200

// WatchUsecase 平台自监控的检测（选主单实例执行）：数据源持续不可用、规则持续评估失败 → 写
// monitor_engine_event；清理过期的心跳行。恢复事件不在这里：它们随状态翻转写入（探活写回 / 评估写回）。
type WatchUsecase struct {
	repo  WatchRepo
	obs   Observer
	log   *slog.Logger
	opts  WatchOptions
	newID func() (string, error)
}

// NewWatchUsecase new a WatchUsecase.
func NewWatchUsecase(repo WatchRepo, obs Observer, log *slog.Logger) *WatchUsecase {
	return &WatchUsecase{
		repo: repo, obs: obs, log: log, newID: idgen.NewID,
		opts: WatchOptions{
			DatasourceDownAfter: 5 * time.Minute, RuleFailingAfter: 10 * time.Minute, RuleFailingMinFailures: 3,
			InstanceStaleAfter: time.Hour,
		},
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *WatchUsecase) WithOptions(o WatchOptions) *WatchUsecase {
	if o.DatasourceDownAfter > 0 {
		uc.opts.DatasourceDownAfter = o.DatasourceDownAfter
	}
	if o.RuleFailingAfter > 0 {
		uc.opts.RuleFailingAfter = o.RuleFailingAfter
	}
	if o.RuleFailingMinFailures > 0 {
		uc.opts.RuleFailingMinFailures = o.RuleFailingMinFailures
	}
	if o.InstanceStaleAfter > 0 {
		uc.opts.InstanceStaleAfter = o.InstanceStaleAfter
	}
	return uc
}

// RunOnce 一轮检测，返回写入的事件数。单项失败不影响其余项，最终返回首个错误供循环退避。
func (uc *WatchUsecase) RunOnce(ctx context.Context) (int, error) {
	var (
		first error
		total int
	)
	keep := func(what string, err error) {
		if err != nil && first == nil {
			first = fmt.Errorf("%s: %w", what, err)
		}
	}
	n, err := uc.reportDatasources(ctx)
	total += n
	keep("report datasources down", err)
	n, err = uc.reportRules(ctx)
	total += n
	keep("report failing rules", err)
	pruned, err := uc.repo.PruneInstances(ctx, uc.opts.InstanceStaleAfter)
	keep("prune stale engine instances", err)
	if pruned > 0 {
		uc.log.InfoContext(ctx, "stale engine instances pruned", "instances", pruned)
	}
	return total, first
}

func (uc *WatchUsecase) reportDatasources(ctx context.Context) (int, error) {
	outages, err := uc.repo.DatasourceOutages(ctx, uc.opts.DatasourceDownAfter, watchBatch)
	if err != nil {
		return 0, err
	}
	written := 0
	for i := range outages {
		o := &outages[i]
		ev, err := datasourceDownEvent(uc.newID, o)
		if err != nil {
			return written, err
		}
		ok, err := uc.repo.ReportDatasourceDown(ctx, o.Since, ev)
		if err != nil {
			return written, err
		}
		if ok {
			written++
			uc.log.WarnContext(ctx, "datasource has been down for a while: engine event queued", "datasource_id", o.DatasourceID,
				"datasource_name", o.Name, "since", o.Since, "error", o.Error)
		}
	}
	if written > 0 {
		uc.obs.AddEngineEvents(ctx, monitorcontract.EventEngineDatasourceDown, written)
	}
	return written, nil
}

func (uc *WatchUsecase) reportRules(ctx context.Context) (int, error) {
	failures, err := uc.repo.RuleFailures(ctx, uc.opts.RuleFailingAfter, uc.opts.RuleFailingMinFailures, watchBatch)
	if err != nil {
		return 0, err
	}
	written := 0
	for i := range failures {
		f := &failures[i]
		ev, err := ruleFailingEvent(uc.newID, f)
		if err != nil {
			return written, err
		}
		ok, err := uc.repo.ReportRuleFailing(ctx, f.DatasourceID, f.Since, ev)
		if err != nil {
			return written, err
		}
		if ok {
			written++
			uc.log.WarnContext(ctx, "rule evaluation has been failing for a while: engine event queued", "rule_id", f.RuleID,
				"rule_name", f.RuleName, "datasource_id", f.DatasourceID, "since", f.Since, "failures", f.Failures)
		}
	}
	if written > 0 {
		uc.obs.AddEngineEvents(ctx, monitorcontract.EventEngineRuleFailing, written)
	}
	return written, nil
}

// InstanceInfo 本实例的身份：ID = 主机名 + 启动时生成的随机后缀（也是评估任务属主令牌的前缀），
// Roles 是启用的角色（逗号分隔），Version 是程序版本。
type InstanceInfo struct {
	ID      string
	Roles   string
	Version string
}

// InstanceStats 本实例的评估统计（心跳据此上报）。进程内计数，不承担任何正确性；并发安全。
type InstanceStats struct {
	evals, fails atomic.Int64
	// 自上次心跳以来：完成的评估数与最大评估延迟（纳秒）。
	windowEvals atomic.Int64
	windowLag   atomic.Int64
	// leaderAt 最近一次以主身份执行任务物化 / 探活的时刻（UnixNano）。
	leaderAt atomic.Int64
}

// NewInstanceStats new an InstanceStats.
func NewInstanceStats() *InstanceStats { return &InstanceStats{} }

// observe 记一次完成的评估（评估任务或记录规则）。被顺延、被围栏拒绝、规则待清理的不算。
func (s *InstanceStats) observe(result string, lag time.Duration) {
	switch result {
	case ResultBusy, ResultSkipped, ResultLeaseLost:
		return
	case ResultError, ResultFailed, ResultWriteError, ResultIncomplete:
		s.fails.Add(1)
	}
	s.evals.Add(1)
	s.windowEvals.Add(1)
	for {
		cur := s.windowLag.Load()
		if int64(lag) <= cur || s.windowLag.CompareAndSwap(cur, int64(lag)) {
			return
		}
	}
}

// MarkLeader 记下本实例此刻正以主身份执行（worker 在选主循环的回调里调用）。
func (s *InstanceStats) MarkLeader() { s.leaderAt.Store(time.Now().UnixNano()) }

// HeartbeatOptions 心跳参数。
type HeartbeatOptions struct {
	// LeaderTTL 距最近一次以主身份执行不超过这么久即视为持有选主锁。
	LeaderTTL time.Duration
}

// HeartbeatUsecase 实例心跳：每个实例（不分角色）按周期 upsert monitor_engine_instance 的一行。
// 行以实例 ID 为主键，各实例只写自己的行；过期的行由自监控检测（选主）清理。
type HeartbeatUsecase struct {
	repo  InstanceRepo
	info  InstanceInfo
	stats *InstanceStats
	opts  HeartbeatOptions
}

// heartbeatRemoveTimeout 停机时删除心跳行的时限。
const heartbeatRemoveTimeout = 2 * time.Second

// NewHeartbeatUsecase new a HeartbeatUsecase；cleanup 在正常停机时删除本实例的心跳行（状态页不必等它过期；
// 进程被强杀时由自监控检测按过期清理）。
func NewHeartbeatUsecase(repo InstanceRepo, info InstanceInfo, stats *InstanceStats, log *slog.Logger) (*HeartbeatUsecase, func()) {
	uc := &HeartbeatUsecase{repo: repo, info: info, stats: stats, opts: HeartbeatOptions{LeaderTTL: 30 * time.Second}}
	return uc, func() {
		ctx, cancel := context.WithTimeout(context.Background(), heartbeatRemoveTimeout)
		defer cancel()
		if err := repo.Remove(ctx, info.ID); err != nil {
			log.Warn("remove engine heartbeat failed, the row will be pruned when stale", "error", err)
		}
	}
}

// WithOptions 注入参数（非法值保持缺省）。
func (uc *HeartbeatUsecase) WithOptions(o HeartbeatOptions) *HeartbeatUsecase {
	if o.LeaderTTL > 0 {
		uc.opts.LeaderTTL = o.LeaderTTL
	}
	return uc
}

// RunOnce 写一次心跳。窗口计数只在写成功后才清零：一次失败的心跳不丢数据。
func (uc *HeartbeatUsecase) RunOnce(ctx context.Context) error {
	evals, lag := uc.stats.windowEvals.Load(), uc.stats.windowLag.Load()
	beat := &InstanceBeat{
		Instance: uc.info.ID, Roles: uc.info.Roles, Version: uc.info.Version,
		Leader:    time.Now().UnixNano()-uc.stats.leaderAt.Load() < int64(uc.opts.LeaderTTL),
		TaskCount: int32(min(evals, 1<<30)), // #nosec G115 -- 已夹紧
		EvalLag:   time.Duration(lag), EvalCount: uc.stats.evals.Load(), FailCount: uc.stats.fails.Load(),
	}
	if err := uc.repo.Beat(ctx, beat); err != nil {
		return fmt.Errorf("write engine heartbeat: %w", err)
	}
	uc.stats.windowEvals.Add(-evals)
	uc.stats.windowLag.CompareAndSwap(lag, 0)
	return nil
}
