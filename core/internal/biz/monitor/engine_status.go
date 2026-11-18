package biz

import (
	"context"
	"slices"
	"strings"
	"time"
)

// 引擎状态的口径。
const (
	// EngineAliveWindow 实例在这段时间内有心跳即视为存活。
	EngineAliveWindow = 60 * time.Second
	// EngineStaleWindow 超过这么久没有心跳的实例不再返回。
	EngineStaleWindow = 24 * time.Hour
	// engineMaxIssues 返回的问题数据源 / 规则条数上限。
	engineMaxIssues = 50
	// EngineFailingThreshold 连续失败达到这个次数的规则才算「持续评估失败」。
	EngineFailingThreshold = 3
	// engineBacklogPercent 逾期任务占比超过这个百分比即视为明显积压。
	engineBacklogPercent = 5

	engineRoleEvaluator = "evaluator"
)

// EngineInstance 评估引擎的一个实例（monitor_engine_instance，引擎写）。
type EngineInstance struct {
	Instance  string
	Roles     []string
	Version   string
	Leader    bool
	Alive     bool
	StartTime time.Time
	LastSeen  time.Time
	TaskCount int32
	EvalLagMS int64
	EvalCount int64
	FailCount int64
}

// EngineSummary 任务与发件箱的汇总（全局计数，不含任何对象的内容）。
type EngineSummary struct {
	TaskTotal       int32
	TaskOverdue     int32
	TaskError       int32
	MaxLagMS        int64
	RecordTaskTotal int32
	RecordTaskError int32
	NotifyPending   int32
}

// EngineDatasourceIssue 一个当前不健康的数据源。
type EngineDatasourceIssue struct {
	ID        string
	Name      string
	Error     string
	Since     *time.Time
	RuleCount int32
}

// EngineRuleIssue 一条持续评估失败的规则（在某个数据源上）。
type EngineRuleIssue struct {
	RuleID         string
	RuleName       string
	GroupID        string
	GroupName      string
	DatasourceID   string
	DatasourceName string
	Error          string
	Since          *time.Time
	Failures       int32
}

// EngineStatus 评估引擎状态。
type EngineStatus struct {
	Instances   []*EngineInstance
	Healthy     bool
	Summary     EngineSummary
	Datasources []*EngineDatasourceIssue
	Rules       []*EngineRuleIssue
	CISyncTime  *time.Time
	CICount     int32
}

// EngineReader 引擎运行状态读仓储（运行表，引擎写；core 只读）。
type EngineReader interface {
	// Instances 最近 stale 内有心跳的实例（至多 200 个），存活的在前、按实例标识排序；alive 内有心跳的
	// 标为存活（以数据库时钟判定）。已不存活的实例 Leader 恒为 false。
	Instances(ctx context.Context, alive, stale time.Duration) ([]*EngineInstance, error)
	// Summary 评估任务（只计启用且未删除的规则）、记录规则任务与通知发件箱的汇总。
	Summary(ctx context.Context) (*EngineSummary, error)
}

// EngineIssues 引擎当前的问题对象读仓储。
type EngineIssues interface {
	// Datasources 启用且探活失败的数据源，按开始时间升序，至多 limit 个。
	Datasources(ctx context.Context, limit int) ([]*EngineDatasourceIssue, error)
	// Rules 连续失败不少于 threshold 次的规则（只在 groups 范围内），按开始时间升序，至多 limit 条。
	Rules(ctx context.Context, groups GroupFilter, threshold, limit int) ([]*EngineRuleIssue, error)
}

// EngineUsecase 平台自监控：评估引擎的运行状态（只读）。实例、任务汇总与数据源是全局信息（数据源本就是
// 全局资源）；持续失败的规则只返回调用方数据范围内的。
type EngineUsecase struct {
	scopes *ScopeUsecase
	reader EngineReader
	issues EngineIssues
	cis    CIIndexReader
}

// NewEngineUsecase new an EngineUsecase.
func NewEngineUsecase(scopes *ScopeUsecase, reader EngineReader, issues EngineIssues, cis CIIndexReader) *EngineUsecase {
	return &EngineUsecase{scopes: scopes, reader: reader, issues: issues, cis: cis}
}

// engineHealthy 引擎整体是否正常：至少一个存活的评估实例、有存活的选主实例，且逾期任务不超过任务总数的
// engineBacklogPercent%（逾期 = 超过应评估时刻两个评估周期仍未开始）。
func engineHealthy(instances []*EngineInstance, s *EngineSummary) bool {
	evaluator := slices.ContainsFunc(instances, func(i *EngineInstance) bool {
		return i.Alive && slices.Contains(i.Roles, engineRoleEvaluator)
	})
	leader := slices.ContainsFunc(instances, func(i *EngineInstance) bool { return i.Alive && i.Leader })
	return evaluator && leader && int64(s.TaskOverdue)*100 <= int64(s.TaskTotal)*engineBacklogPercent
}

// SplitEngineRoles 把逗号分隔的角色串拆成列表（去空白、去空项）。
func SplitEngineRoles(roles string) []string {
	out := make([]string, 0, 2)
	for r := range strings.SplitSeq(roles, ",") {
		if r = strings.TrimSpace(r); r != "" {
			out = append(out, r)
		}
	}
	return out
}

// Status 查询评估引擎状态。
func (uc *EngineUsecase) Status(ctx context.Context, userID string) (*EngineStatus, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := &EngineStatus{Rules: []*EngineRuleIssue{}}
	if out.Instances, err = uc.reader.Instances(ctx, EngineAliveWindow, EngineStaleWindow); err != nil {
		return nil, err
	}
	summary, err := uc.reader.Summary(ctx)
	if err != nil {
		return nil, err
	}
	out.Summary, out.Healthy = *summary, engineHealthy(out.Instances, summary)
	if out.Datasources, err = uc.issues.Datasources(ctx, engineMaxIssues); err != nil {
		return nil, err
	}
	if groups := scope.Filter(""); !groups.Empty() {
		if out.Rules, err = uc.issues.Rules(ctx, groups, EngineFailingThreshold, engineMaxIssues); err != nil {
			return nil, err
		}
		for _, r := range out.Rules {
			r.GroupName = scope.GroupName(r.GroupID)
		}
	}
	if out.CICount, out.CISyncTime, err = uc.cis.Summary(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
