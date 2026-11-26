// Package biz 承载监控告警评估引擎的业务核心：评估任务 / 判定状态 / 告警事件的领域模型、
// 仓储接口，以及规划器（任务物化、数据源探活、维护）与评估器（查询、状态机）用例。
// 本包禁止 import 传输层与任何存储驱动（depguard 构建期强制）。
//
// 与 core 的契约（backend/docs/monitor-tech-design.md §2、§4，第二、三期见 monitor-tech-design-p2p3.md 与
// monitor-foundation-notes.md「P2/P3 契约」）：
//   - 引擎只读写 monitor_* 表，不读写其他域的表，不 import core；
//   - 配置表（数据源、分组、规则、屏蔽规则、配置版本）由 core 写、引擎只读，
//     数据源的健康状态三列例外，由引擎写；
//   - 运行表（评估任务、判定状态、告警事件、判定记录）由引擎写、core 只读；
//   - 引擎不解析收件人、不写 notify_event：需要通知时只向 monitor_notify_outbox 写一行，
//     由 core 的 monitor-notify 循环派发；自动建单 / 触发流程 / AI 摘要只向 monitor_alert_action 写一行，
//     平台自监控事件只向 monitor_engine_event 写一行，同样由 core 执行；
//   - 认领、工单、AI 摘要等列由 core 写在告警行上，引擎只读；手动关闭经 monitor_alert_cmd 传达，由引擎执行；
//   - 事件流水线里与 I/O 无关的行为（改写、富化、丢弃、范围、抑制、升级、模板）一律调 kit/monitorpipeline，
//     与 core 的模拟触发同一份实现；
//   - jsonb 结构、状态与枚举字面量、指纹与匹配语义一律取自 kit/monitorcontract，
//     禁止在本模块复制字面量；
//   - 所有落库时间与到期判定使用数据库 now()，不使用应用时钟。
//
// 结构：状态机是纯函数（state.go 的 Advance：规则 + 上一轮状态 + 本轮样本 + 屏蔽 + 时刻 →
// 全部落库变更），I/O 全在边缘——查询在事务之前完成，data 层在单事务内把 Advance 的结果落库。
//
// 多实例：评估任务靠 FOR UPDATE SKIP LOCKED + 租约分片并以 lease_owner 围栏写回；
// 规划器与维护经 PG advisory lock 选主，语句自身幂等。
package biz

import (
	"context"
	"errors"
	"time"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/monitorpipeline"

	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(
	NewConfigCache, NewInhibitCache, NewInstanceStats, NewEvaluatorUsecase, NewPlannerUsecase, NewMaintenanceUsecase,
	NewCommandUsecase, NewRecordUsecase, NewWatchUsecase, NewHeartbeatUsecase,
)

// Pinger 数据库连通性探测（健康检查）。
type Pinger interface {
	Ping() error
}

// ErrLeaseLost 任务租约已不属于本实例（过期后被他人重领，或任务已被规划器删除）：
// 本轮的全部写入随事务回滚。
var ErrLeaseLost = errors.New("eval task lease lost")

// ErrRuleNotFound 规则行不存在。
var ErrRuleNotFound = errors.New("rule not found")

// SecretOpener 解密数据源凭据（data.SecretBox 实现）。主密钥未配置时返回错误，
// 错误信息不含任何凭据。
type SecretOpener interface {
	Open(ciphertext []byte) (string, error)
}

// Observer 评估指标钩子（server.Metrics 实现）。
type Observer interface {
	// ObserveEval 一次任务评估：result 取 Result* 常量；lag = 领取时刻 − 应评估时刻。
	ObserveEval(ctx context.Context, result string, duration, lag time.Duration)
	// AddOutbox 本轮写入发件箱的行数（按通知类型）。
	AddOutbox(ctx context.Context, kind int32, n int)
	// AddMutesTruncated 一次屏蔽规则加载因超过 MaxMutes 被截断。
	AddMutesTruncated(ctx context.Context)
	// AddTruncated 一次缓存加载因超过上限被截断；what 取 Truncated* 常量。
	AddTruncated(ctx context.Context, what string)
	// AddInhibitCycles 一次抑制快照里因抑制关系成环而被钉为「未被抑制」的告警数。
	AddInhibitCycles(ctx context.Context, n int)
	// ObserveRecord 一次记录规则执行：result 取 Result* 常量；series = 写回的序列数。
	ObserveRecord(ctx context.Context, result string, duration time.Duration, series int)
	// AddCommand 处理了一条事件指令：result 取 Command* 常量。
	AddCommand(ctx context.Context, result string)
	// AddEngineEvents 写入了 n 条自监控事件（按事件类型）。
	AddEngineEvents(ctx context.Context, typ string, n int)
}

// 评估结果标签（Observer.ObserveEval 的 result）。
const (
	ResultOK        = "ok"
	ResultFiring    = "firing"
	ResultNoData    = "nodata"
	ResultError     = "query_error"
	ResultInactive  = "inactive"
	ResultSkipped   = "skipped"    // 规则或数据源已不可评估，等规划器清理任务
	ResultBusy      = "busy"       // 本实例对该数据源的并发槽排不上：任务归还稍后重试，不记查询错误
	ResultLeaseLost = "lease_lost" // 写回被围栏拒绝
	ResultFailed    = "failed"     // 引擎自身故障（数据库等）
	// 以下仅记录规则使用。
	ResultWriteError = "write_error" // 查询成功，写回失败
	ResultIncomplete = "incomplete"  // 结果不完整（截断 / 部分结果），整轮不写
)

// 事件指令的处理结果（Observer.AddCommand 的 result）。abandoned 是指令数，failed 是失败的执行次数：
// 放弃的那一次执行同样是失败的，两个计数各记一笔。
const (
	CommandDone      = "done"      // 已执行
	CommandRejected  = "rejected"  // 未执行：事件已不在告警中
	CommandFailed    = "failed"    // 引擎自身故障，下一轮重试
	CommandAbandoned = "abandoned" // 未执行：连续失败已满上限，引擎放弃（时间线写一条说明）
)

// ErrCommandGone 指令已不是待处理状态（被别的实例处理了）。
var ErrCommandGone = errors.New("alert command is no longer pending")

// ConfigRepo 配置表只读访问（数据源、屏蔽规则、配置版本）。
type ConfigRepo interface {
	// Versions 读 monitor_cfg_version。
	Versions(ctx context.Context) (CfgVersions, error)
	// ListDatasources 全部未删除的数据源（含停用的）。
	ListDatasources(ctx context.Context) ([]*Datasource, error)
	// ListMutes 启用且未过期的屏蔽规则（带作用分组的物化路径），按 ID 升序至多 limit 条。
	ListMutes(ctx context.Context, limit int) ([]*Mute, error)
	// ListInhibits 启用的抑制规则（带作用分组的物化路径；作用分组已删除的不返回），按 ID 升序至多 limit 条。
	ListInhibits(ctx context.Context, limit int) ([]monitorpipeline.InhibitRule, error)
	// ListEscalations 启用的升级策略，按 ID 升序至多 limit 条。
	ListEscalations(ctx context.Context, limit int) ([]*Escalation, error)
	// ListCIs 资源索引的行，至多 limit 条。since 为零值：全部有效的行，按资源 ID 升序；否则 sync_time 晚于
	// since 的行（含无效的），按 sync_time 升序。
	ListCIs(ctx context.Context, since time.Time, limit int) ([]CIRow, error)
	// LoadCalendar 工作日历快照；尚未物化时返回零值。
	LoadCalendar(ctx context.Context) (monitorcontract.CalendarSnapshot, error)
}

// RuleRepo 规则读取。
type RuleRepo interface {
	// LoadRule 读规则行与数据库当前时刻（本轮评估的统一时钟）；不存在返回 ErrRuleNotFound。
	LoadRule(ctx context.Context, ruleID string) (*Rule, time.Time, error)
}

// TaskRepo 评估任务的领取与租约。
type TaskRepo interface {
	// Claim 领取至多 limit 个到期任务：租约 = max(lease, 3 × min(数据源超时, maxTimeout) + margin)，数据库时钟
	// （三段各至多一个查询超时：等并发槽、判定查询、恢复取值查询）。
	Claim(ctx context.Context, token string, limit int, lease, maxTimeout, margin time.Duration) ([]*Task, error)
	// Release 归还任务（属主围栏）：清租约，next_eval_time 至少推迟 delay。Task.Postponed 为真的任务在
	// 任务行上留下「被顺延过」的标记，下一次领取时带回（Task.Postponed），有一轮评估完成才清除。
	Release(ctx context.Context, token string, tasks []*Task, delay time.Duration) error
}

// RoundRepo 一轮评估的落库。
type RoundRepo interface {
	// Snapshot 无锁读取任务的判定状态（恢复取值预判用；租约期内只有属主会改这些行）。
	Snapshot(ctx context.Context, task *Task) ([]SeriesState, error)
	// Commit 单事务：围栏锁任务行 → 锁定并读取判定状态 → 调 decide → 落库状态 / 告警 / 发件箱 /
	// 时间线 / 事件动作 / 判定记录并写回任务。decide 必须是纯计算（不得做任何 I/O）。租约已失返回 ErrLeaseLost。
	// escalation 为真时另用一条语句读出这些告警各级升级通知的进度（SeriesState.Escalation）。
	Commit(ctx context.Context, task *Task, escalation bool, decide func(prev []SeriesState) (*Round, error)) error
	// Record 单事务：只写判定记录并写回任务，不读写判定状态（查询错误 / 不在生效时间）。
	Record(ctx context.Context, task *Task, round *Round) error
}

// PlannerRepo 规划器的写入口（语句自身幂等）。
type PlannerRepo interface {
	// Materialize 补齐缺失任务（首次评估时刻带抖动）；把规则在上次评估之后被修改过的任务提前到立即评估；
	// 删除至多 limit 个不再成立的任务，并同事务删除其判定状态、关闭其活跃告警（时间线记一条关闭）。
	Materialize(ctx context.Context, limit int) (MaterializeResult, error)
	// MaterializeRecords 记录规则任务的物化：补齐（首次执行时刻带抖动）、规则被修改后提前、删除不再成立的
	// （至多 limit 个）。
	MaterializeRecords(ctx context.Context, limit int) (MaterializeResult, error)
	// WriteHealth 批量写数据源健康状态（health 三列 + health_since）。recovered 为本次从异常转为正常的
	// 数据源各准备一条恢复事件：只在对应的故障事件发过时才写入，与状态翻转同一事务。返回写入的恢复事件数。
	WriteHealth(ctx context.Context, results []HealthResult, recovered func(HealthRecovery) (*EngineEventInsert, error)) (int, error)
}

// WatchRepo 平台自监控的检测（选主单实例执行；写入以 dedup_key 幂等，多实例同时执行也只写一行）。
type WatchRepo interface {
	// DatasourceOutages 探活连续失败已满 after（且最近一次探活就在这段时间之内）、还没有为这次故障发过事件的
	// 启用数据源，至多 limit 个。
	DatasourceOutages(ctx context.Context, after time.Duration, limit int) ([]DatasourceOutage, error)
	// ReportDatasourceDown 写一条数据源故障事件：只在该数据源此刻仍处于同一次故障（health_since = since）时写入，
	// 并与探活的状态翻转在数据源行上串行。返回是否写入。
	ReportDatasourceDown(ctx context.Context, since time.Time, ev *EngineEventInsert) (bool, error)
	// RuleFailures 连续查询错误已满 after 且不少于 minFailures 次、其数据源同样长时间保持可用、且还没有为
	// 这次故障发过事件的评估任务，至多 limit 个。
	RuleFailures(ctx context.Context, after time.Duration, minFailures, limit int) ([]RuleFailure, error)
	// ReportRuleFailing 写一条规则持续评估失败事件：只在该任务此刻仍处于同一次故障（error_since = since）时
	// 写入，并与评估的任务写回在任务行上串行。返回是否写入。
	ReportRuleFailing(ctx context.Context, datasourceID string, since time.Time, ev *EngineEventInsert) (bool, error)
	// PruneInstances 删除 last_seen 早于 now() − stale 的心跳行，返回删除数。
	PruneInstances(ctx context.Context, stale time.Duration) (int, error)
}

// InstanceRepo 实例心跳。
type InstanceRepo interface {
	// Beat upsert 本实例的心跳行（last_seen 取数据库时钟）。
	Beat(ctx context.Context, beat *InstanceBeat) error
	// Remove 删除本实例的心跳行（正常停机时）。
	Remove(ctx context.Context, instance string) error
}

// CommandRepo 事件指令（monitor_alert_cmd）的消费。
type CommandRepo interface {
	// PendingCommands 待处理的、本版本会执行的指令（先下达的在前），至多 limit 条。
	PendingCommands(ctx context.Context, limit int) ([]AlertCommand, error)
	// CloseAlert 单事务执行一条手动关闭指令：锁指令行（已不是待处理返回 ErrCommandGone）→ 锁事件所属的评估
	// 任务行（与评估的提交串行）→ 事件仍在告警中则置为已关闭、删除它的判定状态、时间线记一条关闭、已关联
	// 工单的入队一条 ticket_note，指令置已执行；否则指令置未执行。返回是否关闭了事件。newID 生成新行的主键。
	CloseAlert(ctx context.Context, cmd AlertCommand, newID func() (string, error)) (bool, error)
}

// CommandFailureRepo 事件指令的失败记录与放弃（与 CommandRepo 是同一个仓储实现，拆开是为了让每个接口
// 都不超过三个方法）。
type CommandFailureRepo interface {
	// RecordCommandFailure 记一次执行失败，指令仍是待处理才记（否则返回 ErrCommandGone，什么都不写）。
	// first_failed_at 是当前这一段连续失败的起点：上一次失败距今超过 gap 就算中断过（引擎宕机、指令长期
	// 没被领取），从这一次重新计时；否则保持不变。返回这一段连续失败是否已满 giveUp。两个时刻与判定都用
	// 数据库时钟。
	RecordCommandFailure(ctx context.Context, id string, gap, giveUp time.Duration) (bool, error)
	// AbandonCommand 放弃一条连续失败已满 giveUp 的指令，单事务：锁指令行（FOR UPDATE SKIP LOCKED）→ 仍是
	// 待处理且 first_failed_at 距今确已满 giveUp 才继续 → 指令置未执行、result = why、时间线追加一条
	// action_failed（说明为 why，系统产生、没有操作人）。围栏未命中（已不是待处理、正被别的实例处理、失败
	// 时段其实未满 giveUp）返回 false 且不写任何东西，所以多实例同时放弃也只会写一条事件。newID 生成事件的主键。
	AbandonCommand(ctx context.Context, cmd AlertCommand, why string, giveUp time.Duration, newID func() (string, error)) (bool, error)
}

// RecordRepo 记录规则任务的读取与写回（领取 / 归还与评估任务共用同一套租约，见 TaskRepo）。
type RecordRepo interface {
	// Claim / Release 同 TaskRepo，对象是 monitor_record_task：Task.RuleID 是记录规则 ID，
	// Task.DatasourceID 是它的数据源（规则行已不存在时为空）。
	TaskRepo
	// LoadRecordRule 读记录规则行与数据库当前时刻；不存在返回 ErrRuleNotFound。
	LoadRecordRule(ctx context.Context, id string) (*RecordRule, time.Time, error)
	// Finish 写回一次执行的结果并归还任务（属主围栏）：租约已失返回 ErrLeaseLost。
	Finish(ctx context.Context, task *Task, res *RecordResult) error
}

// PartitionRepo 判定记录分区维护（数据库时钟，UTC 日界）。
type PartitionRepo interface {
	// EnsurePartitions 确保当天至未来 ahead 天的分区存在（独立建表 → 迁出 DEFAULT 重叠行 → ATTACH）。
	EnsurePartitions(ctx context.Context, ahead int) error
	// DropExpiredPartitions 删除上界不晚于 now() − retention 的日分区，返回删除数。
	DropExpiredPartitions(ctx context.Context, retention time.Duration) (int, error)
	// PurgeDefaultPartition 删除 DEFAULT 分区中超过保留期的行（至多 limit 行）。
	PurgeDefaultPartition(ctx context.Context, retention time.Duration, limit int) (int, error)
}

// RetentionRepo 过期数据清理（分批）。
type RetentionRepo interface {
	// PurgeAlerts 删除 resolve_time 早于 now() − retention 的非活跃告警，连同其通知记录、发件箱行、时间线、
	// 事件动作、事件指令与升级进度（至多 limit 个告警）。
	PurgeAlerts(ctx context.Context, retention time.Duration, limit int) (int, error)
	// PurgeOutbox 删除 done_time 早于 now() − retention 的已派发 / 已丢弃发件箱行（至多 limit 行）。
	PurgeOutbox(ctx context.Context, retention time.Duration, limit int) (int, error)
	// PurgeEngineEvents 删除 done_time 早于 now() − retention 的已派发 / 已丢弃自监控事件（至多 limit 行）。
	PurgeEngineEvents(ctx context.Context, retention time.Duration, limit int) (int, error)
	// PurgeCommands 删除 done_time 早于 now() − retention 的已处理事件指令（至多 limit 行）。
	PurgeCommands(ctx context.Context, retention time.Duration, limit int) (int, error)
}
