package biz

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// Finding 一条巡检发现项。
type Finding struct {
	Check     string
	Severity  int32
	Namespace string
	Kind      string
	Name      string
	Message   string
}

// InspectionResult 一次巡检的结果。Findings 每个检查项至多 MaxFindingsPerCheck 条；Summary 的计数是总数。
type InspectionResult struct {
	Findings []Finding
	Summary  k8scontract.InspectionSummary
}

// FindingSet 巡检过程中的发现项收集器：按检查项计数，每项只保留前 MaxFindingsPerCheck 条。
// 同一对象同一检查项只记一次（多副本的 Pod 归并到其工作负载）。
type FindingSet struct {
	seen     map[string]struct{}
	count    map[string]int
	severity map[string]int32
	findings []Finding
}

// NewFindingSet new a FindingSet.
func NewFindingSet() *FindingSet {
	return &FindingSet{seen: map[string]struct{}{}, count: map[string]int{}, severity: map[string]int32{}}
}

// Add 记录一条发现项。
func (s *FindingSet) Add(f Finding) {
	key := f.Check + "|" + f.Namespace + "|" + f.Kind + "|" + f.Name
	if _, dup := s.seen[key]; dup {
		return
	}
	s.count[f.Check]++
	if f.Severity > s.severity[f.Check] {
		s.severity[f.Check] = f.Severity
	}
	if s.count[f.Check] > k8scontract.MaxFindingsPerCheck {
		// 超出上限的不再去重登记：seen 的大小由此有界（每项至多 MaxFindingsPerCheck 个键），
		// 代价是超出部分的计数可能含重复。
		return
	}
	s.seen[key] = struct{}{}
	f.Message = Truncate(f.Message, 512)
	s.findings = append(s.findings, f)
}

// Result 汇总为巡检结果（检查项按级别降序、名称升序）。
func (s *FindingSet) Result(sum k8scontract.InspectionSummary) InspectionResult {
	sum.Checks = sum.Checks[:0]
	sum.Critical, sum.Warning, sum.Info = 0, 0, 0
	for check, n := range s.count {
		sev := s.severity[check]
		sum.Checks = append(sum.Checks, k8scontract.InspectionCheck{Check: check, Severity: sev, Count: n})
		switch sev {
		case k8scontract.FindingCritical:
			sum.Critical += n
		case k8scontract.FindingWarning:
			sum.Warning += n
		default:
			sum.Info += n
		}
	}
	sort.Slice(sum.Checks, func(i, j int) bool {
		if sum.Checks[i].Severity != sum.Checks[j].Severity {
			return sum.Checks[i].Severity > sum.Checks[j].Severity
		}
		return sum.Checks[i].Check < sum.Checks[j].Check
	})
	return InspectionResult{Findings: s.findings, Summary: sum}
}

// Inspector 对一个集群执行全部检查（data/kube 实现，平台身份、分页读取）。
type Inspector interface {
	Inspect(ctx context.Context, clusterID string) (InspectionResult, error)
}

// InspectionRun 一次巡检运行（k8s_inspection_run）。
type InspectionRun struct {
	ID        string
	ClusterID string
	Trigger   int32
}

// InspectionRepo 巡检运行的领取与结果写入。
type InspectionRepo interface {
	// Claim 领取这些集群里最早一条待执行的运行（置为执行中）；没有则为它们中到期的集群（最近一次运行
	// 早于 interval，interval ≤ 0 时不建）建一条定时运行并领取。都没有返回 nil。
	Claim(ctx context.Context, clusterIDs []string, interval time.Duration) (*InspectionRun, error)
	// Finish 写入结果（errMsg 非空 = 失败），并清理该集群超出 keep 条的旧运行。
	Finish(ctx context.Context, run *InspectionRun, clusterName string, res *InspectionResult, score int, errMsg string, keep int) error
}

// InspectionUsecase 巡检循环：领取 → 执行 → 落库 → 通知。每轮至多执行一个集群（串行，
// 单集群内受该集群的客户端限速约束）。
type InspectionUsecase struct {
	registry  *Registry
	prober    *Prober
	inspector Inspector
	repo      InspectionRepo
	outbox    OutboxRepo
	log       *slog.Logger
	interval  time.Duration
	timeout   time.Duration
}

// NewInspectionUsecase new an InspectionUsecase（定时间隔缺省 24 小时）。
func NewInspectionUsecase(registry *Registry, prober *Prober, inspector Inspector, repo InspectionRepo, outbox OutboxRepo, log *slog.Logger) *InspectionUsecase {
	return &InspectionUsecase{
		registry: registry, prober: prober, inspector: inspector, repo: repo, outbox: outbox, log: log,
		interval: k8scontract.InspectInterval, timeout: 15 * time.Minute,
	}
}

// WithInterval 设置定时巡检的间隔；≤ 0 = 关闭定时巡检（手动触发仍可用）。
func (uc *InspectionUsecase) WithInterval(d time.Duration, disabled bool) {
	switch {
	case disabled:
		uc.interval = 0
	case d > 0:
		uc.interval = d
	}
}

// Run 执行至多一次巡检（循环调用；恒返回 0，按固定间隔再来）。
func (uc *InspectionUsecase) Run(ctx context.Context) (int, error) {
	var ids []string
	names := map[string]*Cluster{}
	for _, c := range uc.registry.OwnedClusters() {
		if uc.prober.Online(c.ID) {
			ids = append(ids, c.ID)
			names[c.ID] = c
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	run, err := uc.repo.Claim(ctx, ids, uc.interval)
	if err != nil || run == nil {
		return 0, err
	}
	c := names[run.ClusterID]
	ictx, cancel := context.WithTimeout(ctx, uc.timeout)
	res, ierr := uc.inspector.Inspect(ictx, run.ClusterID)
	cancel()
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer fcancel()
	if ierr != nil {
		uc.log.Warn("inspection failed", "cluster", c.Name, "error", ierr)
		return 0, uc.repo.Finish(fctx, run, c.Name, nil, 0, truncate(AuditErrorText(ierr), 512), k8scontract.InspectKeepRuns)
	}
	score := k8scontract.InspectionScore(res.Summary.Critical, res.Summary.Warning, res.Summary.Info)
	if err := uc.repo.Finish(fctx, run, c.Name, &res, score, "", k8scontract.InspectKeepRuns); err != nil {
		return 0, err
	}
	now := time.Now()
	ev := OutboxEvent{
		ClusterID: c.ID, EventType: k8scontract.NotifyInspectionReport, Severity: 1,
		DedupKey: "k8s:inspection:" + run.ID, GroupKey: "k8s:inspection:" + c.ID,
		Payload: k8scontract.InspectionNotifyPayload{
			ClusterID: c.ID, ClusterName: c.Name, RunID: run.ID, Score: score,
			Critical: res.Summary.Critical, Warning: res.Summary.Warning, Info: res.Summary.Info,
			Time: now.Format("2006-01-02 15:04:05"),
			URL:  fmt.Sprintf("/k8s/inspection?cluster=%s&run=%s", c.ID, run.ID),
		},
	}
	if res.Summary.Critical > 0 {
		ev.Severity = 3
	} else if res.Summary.Warning > 0 {
		ev.Severity = 2
	}
	if err := uc.outbox.Enqueue(fctx, ev); err != nil {
		uc.log.Error("enqueue inspection report failed", "cluster", c.Name, "error", err)
	}
	return 0, nil
}
