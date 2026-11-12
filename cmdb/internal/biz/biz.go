// Package biz 承载 CMDB 数据面的业务核心：采集源 / 任务 / 运行记录领域模型、
// 采集器接口、调度器与执行器用例。本包禁止 import 传输层与任何存储驱动
// （depguard 构建期强制）。
//
// 与 core 的契约：采集源与手动任务由 core 写、本服务读；暂存区、运行记录、任务状态
// 与资源对账结果由本服务写、core 只读。字面量取自 kit/cmdbcontract。
package biz

import (
	"context"
	"time"

	"github.com/lanyulei/reborn/backend/kit/cmdbcontract"

	"github.com/google/wire"
)

// ProviderSet is biz providers.
var ProviderSet = wire.NewSet(NewSchedulerUsecase, NewWorkerUsecase, NewMaintenanceUsecase, NewWebhookUsecase)

// Source 采集源（cmdb_sync_source 的数据面视图；凭据仅在执行任务时解密并驻留调用栈）。
type Source struct {
	ID                 string
	Name               string
	Provider           string
	Config             []byte
	Credential         []byte
	Env                string
	IntervalSeconds    int32
	GracePeriodSeconds int32
	Enabled            bool
}

// fieldMap 采集源配置中的字段映射（采集器输出键 → 模型字段编码）。
func (s *Source) fieldMap() map[string]string {
	return cmdbcontract.FieldMapOf(s.Provider, s.Config)
}

// Task 已领取的同步任务（含属主令牌，所有后续写以其围栏）。
type Task struct {
	ID       string
	SourceID string
	Trigger  string
	Attempts int32
	Token    string
	RunID    string
}

// Counts 三态对账统计。
type Counts struct {
	Discovered  int32
	Created     int32
	Updated     int32
	Unchanged   int32
	Disappeared int32
	Retired     int32
	Failed      int32
}

func (c *Counts) add(o Counts) {
	c.Created += o.Created
	c.Updated += o.Updated
	c.Unchanged += o.Unchanged
	c.Disappeared += o.Disappeared
	c.Retired += o.Retired
	c.Failed += o.Failed
}

// Resource 采集器输出的归一化资源。
type Resource struct {
	ModelCode  string
	ExternalID string
	Name       string
	Attrs      map[string]any
	Relations  []cmdbcontract.StagingRelation
}

// Sink 采集结果接收器（执行器实现：按类型字段过滤属性、分批写暂存）。
type Sink interface {
	Put(ctx context.Context, r *Resource) error
}

// Collector 采集器接口：每个提供方一个实现（data/collector/<provider>），注册即可用。
type Collector interface {
	Provider() string
	// Discover 全量拉取并逐条写入 sink；返回错误即整次运行失败（不进入对账，避免误判消失）。
	Discover(ctx context.Context, src *Source, credential string, sink Sink) error
}

// ModelField 类型字段元数据（对账所需子集）。
type ModelField struct {
	Code   string
	Owner  string
	Unique bool
}

// MetaRepo 类型元数据只读仓储。
type MetaRepo interface {
	// Fields 返回类型未废弃字段；类型不存在或停用返回空切片。
	Fields(ctx context.Context, modelCode string) ([]ModelField, error)
}

// SchedulerRepo 调度器仓储（选主单实例执行，SQL 自身幂等）。
type SchedulerRepo interface {
	// EnqueueDue 为到期且启用的采集源插入 schedule 任务并推进 next_run_time；已有未完成任务的源跳过。返回入队数。
	EnqueueDue(ctx context.Context, now time.Time) (int, error)
	// MarkStale 把 last_synced_at 早于 now - interval×multiplier 的同步资源置 stale。返回行数。
	MarkStale(ctx context.Context, multiplier int32) (int, error)
	// PurgeStaging 清理无对应 running 运行的遗留暂存行（运行异常中止）。
	PurgeStaging(ctx context.Context, olderThan time.Duration) (int, error)
}

// TaskRepo 任务仓储：领取 / 续约 / 完成，全部为围栏条件更新。
type TaskRepo interface {
	// Claim 领取一个 pending 或租约过期的 running 任务（FOR UPDATE SKIP LOCKED），同事务创建 running 运行记录；
	// 无任务返回 (nil, nil)。超过最大尝试次数的任务被直接置 failed（不返回）。
	Claim(ctx context.Context, token string, lease time.Duration) (*Task, error)
	// Renew 续约；属主不符或任务已非 running 返回 ErrLeaseLost。
	Renew(ctx context.Context, task *Task, lease time.Duration) error
	// Finish 以属主围栏写任务终态、运行统计与采集源 last_*；属主不符返回 ErrLeaseLost。
	Finish(ctx context.Context, task *Task, status string, counts Counts, runErr string, samples []cmdbcontract.RunError) error
}

// SourceRepo 采集源只读仓储。
type SourceRepo interface {
	Get(ctx context.Context, id string) (*Source, error)
}

// StagingRepo 暂存区仓储。
type StagingRepo interface {
	// Append 批量写入（同运行内重复 external_id 以后者为准）。
	Append(ctx context.Context, runID string, rows []*Resource) error
	// Models 返回本次运行涉及的类型编码（按首次出现顺序）。
	Models(ctx context.Context, runID string) ([]string, error)
	// Count 返回本次运行暂存总行数。
	Count(ctx context.Context, runID string) (int32, error)
	// Clear 清理本次运行的暂存行。
	Clear(ctx context.Context, runID string) error
}

// ReconcileRepo 对账仓储：集合 SQL，一类型一事务。
type ReconcileRepo interface {
	// ReconcileModel 对单一类型执行 touch / update / insert / disappear / retire，并写历史；
	// manualKeys 为人工归属字段（同步不得覆盖），uniqueCodes 为唯一字段（拼唯一键）。
	ReconcileModel(ctx context.Context, p ReconcileParams) (Counts, []cmdbcontract.RunError, error)
	// ReconcileRelations 按暂存声明同步关系：同源同类型多余关系删除、缺失关系插入（一事务）。
	ReconcileRelations(ctx context.Context, runID, source string) error
}

// ReconcileParams 单类型对账参数。
type ReconcileParams struct {
	RunID       string
	ModelCode   string
	Source      string
	Env         string
	Grace       time.Duration
	ManualKeys  []string
	UniqueCodes []string
	// SkipDisappear 为真时不做消失/退役判定（本次采集为空集时的保护）。
	SkipDisappear bool
}

// CredentialOpener 凭据解密器（data 层以 kit/secretbox 实现；只读）。
type CredentialOpener interface {
	Open(ciphertext []byte) (string, error)
}

// Pinger 数据库连通性探测（健康检查）。
type Pinger interface {
	Ping() error
}
