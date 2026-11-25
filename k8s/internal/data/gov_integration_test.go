//go:build integration

package data

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// runRow 一条巡检运行（k8s_inspection_run）的断言视图；可空列用指针。
type runRow struct {
	Status, Trigger, Score      int32
	Summary, Error, ClusterName string
	Start, Finish               *time.Time
	Created                     time.Time
}

func (e *env) run(id string) runRow {
	e.t.Helper()
	var r runRow
	e.scan(`SELECT status, trigger, score, summary::text, error, cluster_name, start_time, finish_time, create_time
		FROM k8s_inspection_run WHERE id = $1`, []any{id},
		&r.Status, &r.Trigger, &r.Score, &r.Summary, &r.Error, &r.ClusterName, &r.Start, &r.Finish, &r.Created)
	return r
}

// sameTime 两个可空时间是否相同（都为空，或指向同一时刻）——两次查询读出的指针不能直接比。
func sameTime(a, b *time.Time) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && a.Equal(*b))
}

// same 两条运行视图是否相同（按值比较）。
func (r runRow) same(o runRow) bool {
	return r.Status == o.Status && r.Trigger == o.Trigger && r.Score == o.Score && r.Summary == o.Summary && r.Error == o.Error &&
		r.ClusterName == o.ClusterName && sameTime(r.Start, o.Start) && sameTime(r.Finish, o.Finish) && r.Created.Equal(o.Created)
}

// pendingRun core 写入的一条待执行（手动触发）运行，返回 ID。
func (e *env) pendingRun(clusterID string) string {
	e.t.Helper()
	id := e.id()
	e.exec(`INSERT INTO k8s_inspection_run (id, cluster_id, cluster_name, trigger, status, create_user) VALUES ($1, $2, '', $3, $4, $5)`,
		id, clusterID, k8scontract.InspectManual, k8scontract.InspectPending, e.id())
	return id
}

// findings 某次运行落库的发现项（按检查项、对象）。
type finding struct {
	Check, Namespace, Kind, Name, Message string
	Severity                              int32
}

func (e *env) findings(runID string) []finding {
	e.t.Helper()
	rows, err := e.admin.Query(ctxb(), `SELECT check_id, severity, namespace, kind, name, message FROM k8s_inspection_finding
		WHERE run_id = $1 ORDER BY check_id, namespace, kind, name`, runID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []finding
	for rows.Next() {
		var f finding
		if err := rows.Scan(&f.Check, &f.Severity, &f.Namespace, &f.Kind, &f.Name, &f.Message); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// 巡检运行的领取：待执行的手动运行优先；到期的集群建定时运行；执行中断（超过 30 分钟）的可被重新领取。
func TestInspectionClaim(t *testing.T) {
	e := newEnv(t)
	repo := NewInspectionRepo(e.d)
	ctx := ctxb()
	c1, c2, c3 := e.cluster(clusterSpec{}), e.cluster(clusterSpec{}), e.cluster(clusterSpec{})
	disabled := e.cluster(clusterSpec{Disabled: true})
	claim := func(interval time.Duration, clusters ...string) *biz.InspectionRun {
		t.Helper()
		run, err := repo.Claim(ctx, clusters, interval)
		if err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return run
	}

	// 没有待执行的运行、interval ≤ 0：不建行。
	if run := claim(0, c1); run != nil || e.count("SELECT count(*) FROM k8s_inspection_run") != 0 {
		t.Fatalf("Claim(…, 0) = %+v; nothing may be created", run)
	}
	// 到期的集群（从未巡检）：建一条定时运行并领取，每次领取至多一个集群。
	seen := map[string]bool{}
	for range 3 {
		run := claim(k8scontract.InspectInterval, c1, c2, c3)
		if run == nil || !idgen.IsUUID(run.ID) || run.Trigger != k8scontract.InspectScheduled || seen[run.ClusterID] {
			t.Fatalf("scheduled Claim() = %+v (seen %v)", run, seen)
		}
		seen[run.ClusterID] = true
		if r := e.run(run.ID); r.Status != k8scontract.InspectRunning || r.Trigger != k8scontract.InspectScheduled || r.Start == nil ||
			r.Finish != nil || r.ClusterName != "" || e.count("SELECT count(*) FROM k8s_cluster WHERE id = $1 AND enabled", run.ClusterID) != 1 {
			t.Fatalf("claimed run = %+v", r)
		}
	}
	// 刚巡检过的集群未到间隔：不再建行。
	if run := claim(k8scontract.InspectInterval, c1, c2, c3); run != nil || e.count("SELECT count(*) FROM k8s_inspection_run") != 3 {
		t.Fatalf("Claim() before the interval elapsed = %+v", run)
	}

	// 手动触发（core 写入的待执行行）优先被领取，触发方式原样带回。
	manual := e.pendingRun(c1)
	run := claim(0, c1)
	if run == nil || run.ID != manual || run.Trigger != k8scontract.InspectManual || run.ClusterID != c1 {
		t.Fatalf("Claim() of a manual run = %+v, want %s", run, manual)
	}
	// 执行中且未超时的运行不再被领取；别的集群的待执行运行不在范围内。
	other := e.pendingRun(c2)
	if run = claim(0, c1); run != nil {
		t.Fatalf("a running inspection was claimed again: %+v", run)
	}
	if run = claim(0, c2); run == nil || run.ID != other {
		t.Fatalf("Claim() of another cluster's run = %+v, want %s", run, other)
	}
	// 执行中断（实例崩溃）：执行中超过 30 分钟的运行可被重新领取。
	e.exec(`UPDATE k8s_inspection_run SET start_time = now() - interval '31 minutes' WHERE id = $1`, manual)
	if run = claim(0, c1); run == nil || run.ID != manual {
		t.Fatalf("Claim() of a stale running inspection = %+v, want %s", run, manual)
	}
	// 到期判定看最近一次运行（任何状态）：早于 interval 即到期。
	e.exec(`UPDATE k8s_inspection_run SET create_time = now() - interval '25 hours' WHERE cluster_id = $1`, c1)
	if run = claim(k8scontract.InspectInterval, c1); run == nil || run.ID == manual || run.Trigger != k8scontract.InspectScheduled {
		t.Fatalf("Claim() of a due cluster = %+v; want a new scheduled run", run)
	}
	// 停用的集群不建定时运行。
	if run = claim(k8scontract.InspectInterval, disabled); run != nil {
		t.Fatalf("a disabled cluster got a scheduled run: %+v", run)
	}

	// 多实例并发领取：每条待执行运行只被一个实例领到（FOR UPDATE SKIP LOCKED）。
	clusters := make([]string, 8)
	pending := make([]string, 0, len(clusters))
	for i := range clusters {
		clusters[i] = e.cluster(clusterSpec{})
		pending = append(pending, e.pendingRun(clusters[i]))
	}
	var (
		mu  sync.Mutex
		got []string
		wg  sync.WaitGroup
	)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				run, err := repo.Claim(ctx, clusters, 0)
				if err != nil {
					t.Errorf("Claim() error = %v", err)
					return
				}
				if run == nil {
					return
				}
				mu.Lock()
				got = append(got, run.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(got) != len(pending) || !sameSet(got, pending) {
		t.Fatalf("claimed %d runs %v across instances, want each of %v exactly once", len(got), got, pending)
	}
	if e.count("SELECT count(*) FROM k8s_inspection_run WHERE status = $1 AND id = ANY($2::uuid[])", k8scontract.InspectRunning, pending) != len(pending) {
		t.Fatal("every pending run must end up running")
	}
}

// 巡检结果的写入：发现项批量落库并按列宽截断、摘要落 jsonb；失败只记原因；整个写入是一个事务；
// 每个集群只保留最近 keep 次运行（发现项随运行级联删除）。
func TestInspectionFinish(t *testing.T) {
	e := newEnv(t)
	repo := NewInspectionRepo(e.d)
	ctx := ctxb()
	c := e.cluster(clusterSpec{})
	keep := k8scontract.InspectKeepRuns
	long := strings.Repeat("x", 800)
	res := &biz.InspectionResult{
		Findings: []biz.Finding{
			{Check: k8scontract.CheckNoLimits, Severity: k8scontract.FindingCritical, Namespace: "default", Kind: "Pod", Name: "web-1", Message: "容器未设置资源限制"},
			{Check: k8scontract.CheckPodRestarts, Severity: k8scontract.FindingWarning, Namespace: long, Kind: long, Name: long, Message: long},
			{Check: k8scontract.CheckImageLatest, Severity: k8scontract.FindingInfo, Namespace: "", Kind: "Pod", Name: "", Message: ""},
		},
		Summary: k8scontract.InspectionSummary{
			Checks: []k8scontract.InspectionCheck{{Check: "no_limits", Severity: k8scontract.FindingCritical, Count: 7}},
			Pods:   20, Workloads: 5, Nodes: 3, Certs: 1, Critical: 7, Warning: 1, Info: 1, Partial: true,
		},
	}

	claim := func() *biz.InspectionRun {
		t.Helper()
		id := e.pendingRun(c)
		run, err := repo.Claim(ctx, []string{c}, 0)
		if err != nil || run == nil || run.ID != id {
			t.Fatalf("Claim() = %+v, %v; want the pending run %s", run, err, id)
		}
		return run
	}
	run := claim()
	before := e.dbNow()
	if err := repo.Finish(ctx, run, "c-prod", res, 88, "", keep); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	r := e.run(run.ID)
	if r.Status != k8scontract.InspectDone || r.Score != 88 || r.ClusterName != "c-prod" || r.Error != "" ||
		r.Finish == nil || r.Finish.Before(before) || r.Finish.After(e.dbNow()) {
		t.Fatalf("run after Finish: %+v", r)
	}
	if !jsonEqual(t, []byte(r.Summary), `{"checks":[{"check":"no_limits","severity":3,"count":7}],"pods":20,"workloads":5,"nodes":3,"certs":1,"critical":7,"warning":1,"info":1,"partial":true}`) {
		t.Fatalf("summary = %s", r.Summary)
	}
	got := e.findings(run.ID)
	if len(got) != 3 || got[0].Check != k8scontract.CheckImageLatest || got[0].Severity != k8scontract.FindingInfo ||
		got[0].Namespace != "" || got[0].Kind != "Pod" {
		t.Fatalf("findings = %+v", got)
	}
	if f := got[1]; f.Check != k8scontract.CheckNoLimits || f.Severity != k8scontract.FindingCritical || f.Namespace != "default" ||
		f.Kind != "Pod" || f.Name != "web-1" || f.Message != "容器未设置资源限制" {
		t.Fatalf("findings = %+v", got)
	}
	// 超过列宽的发现项截断后落库：namespace 63 / kind 128 / name 253 / message 512。
	if f := got[2]; f.Check != k8scontract.CheckPodRestarts || f.Severity != k8scontract.FindingWarning || len(f.Namespace) != 63 ||
		len(f.Kind) != 128 || len(f.Name) != 253 || len(f.Message) != 512 {
		t.Fatalf("overlong finding was not clipped: namespace %d, kind %d, name %d, message %d bytes",
			len(f.Namespace), len(f.Kind), len(f.Name), len(f.Message))
	}

	// 失败：只记状态与原因，摘要为空对象，没有发现项。
	failed := claim()
	if err := repo.Finish(ctx, failed, "c-prod", nil, 0, strings.Repeat("boom ", 200), keep); err != nil {
		t.Fatalf("Finish() of a failed inspection error = %v", err)
	}
	if r := e.run(failed.ID); r.Status != k8scontract.InspectFailed || len(r.Error) != 512 || r.Score != 0 ||
		!jsonEqual(t, []byte(r.Summary), `{}`) || r.Finish == nil || len(e.findings(failed.ID)) != 0 {
		t.Fatalf("failed run = %+v", r)
	}

	// 一个事务：发现项写不进去（运行不存在）时，运行行也不被更新，不留孤儿发现项。
	ghost := &biz.InspectionRun{ID: e.id(), ClusterID: c}
	snapshot := e.run(run.ID)
	if err := repo.Finish(ctx, ghost, "c-prod", res, 50, "", keep); pgCode(err) != "23503" {
		t.Fatalf("Finish() for a missing run error = %v, want 23503", err)
	}
	if !e.run(run.ID).same(snapshot) || e.count("SELECT count(*) FROM k8s_inspection_finding") != 3 {
		t.Fatal("a failed Finish() must leave nothing behind")
	}

	// 只保留最近 keep 次：更早的运行连同发现项被清，保留下来的都是最新的。
	// 此前本集群已有 2 次运行（run、failed），这里再补 keep + 1 次更早的，随后领取并完成第 keep + 4 次。
	const pruned = 4
	old := make([]string, 0, keep+1)
	for i := range keep + 1 {
		id := e.id()
		e.exec(`INSERT INTO k8s_inspection_run (id, cluster_id, trigger, status, score, create_time)
			VALUES ($1, $2, $3, $4, $5, now() - make_interval(secs => $6))`, id, c, k8scontract.InspectScheduled, k8scontract.InspectDone, i+1000, float64(keep+1-i)*60)
		e.exec(`INSERT INTO k8s_inspection_finding (id, run_id, check_id, severity) VALUES ($1, $2, 'no_limits', 2)`, e.id(), id)
		old = append(old, id)
	}
	if n := e.count("SELECT count(*) FROM k8s_inspection_run WHERE cluster_id = $1", c); n != keep+3 {
		t.Fatalf("precondition: %d runs, want %d", n, keep+3)
	}
	latest := claim()
	if err := repo.Finish(ctx, latest, "c-prod", res, 90, "", keep); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if n := e.count("SELECT count(*) FROM k8s_inspection_run WHERE cluster_id = $1", c); n != keep {
		t.Fatalf("%d runs kept, want %d", n, keep)
	}
	// 最旧的 pruned 次连同发现项被清，其余原样保留。
	if e.count("SELECT count(*) FROM k8s_inspection_run WHERE id = ANY($1::uuid[])", old[:pruned]) != 0 ||
		e.count("SELECT count(*) FROM k8s_inspection_finding WHERE run_id = ANY($1::uuid[])", old[:pruned]) != 0 {
		t.Fatal("pruning must drop the oldest runs with their findings")
	}
	kept := append(slices.Clone(old[pruned:]), run.ID, failed.ID, latest.ID)
	// 发现项：保留下来的旧运行各 1 条，加上 run 与 latest 各 3 条（failed 没有）。
	if e.count("SELECT count(*) FROM k8s_inspection_run WHERE id = ANY($1::uuid[])", kept) != len(kept) ||
		e.count("SELECT count(*) FROM k8s_inspection_finding WHERE run_id = ANY($1::uuid[])", kept) != len(old[pruned:])+6 {
		t.Fatalf("runs within the kept window were lost: %v", kept)
	}
	if e.count("SELECT count(*) FROM k8s_inspection_run WHERE cluster_id = $1 AND id = $2", c, latest.ID) != 1 ||
		e.count("SELECT count(*) FROM k8s_inspection_finding WHERE run_id = $1", latest.ID) != 3 {
		t.Fatal("the run just finished must be kept with its findings")
	}
	// 边界（现状）：keep ≤ 0 时连刚完成的这一次也一并清掉（调用方恒传 InspectKeepRuns）。
	last := claim()
	if err := repo.Finish(ctx, last, "c-prod", res, 90, "", 0); err != nil {
		t.Fatalf("Finish(…, keep 0) error = %v", err)
	}
	if e.count("SELECT count(*) FROM k8s_inspection_run WHERE cluster_id = $1", c) != 0 {
		t.Fatal("keep = 0 must remove every run of the cluster (current behaviour)")
	}
}

// changeSpec 建变更单的可调字段（core 的写入）；零值 = 一张已批准、未过期、没有租约的变更单。
type changeSpec struct {
	Cluster     string
	Kind        int32         // 缺省对象变更
	Status      int32         // 缺省已批准
	ApprovedAgo time.Duration // 批准于多久之前；< 0 = 尚未批准
	// ExpiresIn 失效时间距今（nil = 一天后）。
	ExpiresIn *time.Duration
	// Lease 租约到期距今（负数 = 已过期）；nil = 没有租约。
	Lease *time.Duration
	Owner string
	// Started 已经开始向集群提交（result.started）。
	Started             bool
	NoExec, NoRequester bool
}

func (e *env) change(spec changeSpec) string {
	e.t.Helper()
	id, user := e.id(), e.id()
	if spec.Kind == 0 {
		spec.Kind = k8scontract.ChangeKindOp
	}
	if spec.Status == 0 {
		spec.Status = k8scontract.ChangeApproved
	}
	var expires, lease, approved any
	if spec.ExpiresIn != nil {
		expires = spec.ExpiresIn.Seconds()
	}
	if spec.Lease != nil {
		lease = spec.Lease.Seconds()
	}
	if spec.ApprovedAgo >= 0 {
		approved = spec.ApprovedAgo.Seconds()
	}
	var exec, requester any = seal(e.t, `{"op":"apply"}`), `{"userId":"` + user + `","username":"ops","roleIds":["11111111-1111-7111-8111-111111111111"],"isAdmin":false}`
	if spec.NoExec {
		exec = nil
	}
	if spec.NoRequester {
		requester = nil
	}
	result := "{}"
	if spec.Started {
		result = `{"started":true}`
	}
	e.exec(`INSERT INTO k8s_change (id, kind, cluster_id, cluster_name, action, requester_id, requester, exec, client_ip, status, result,
		approve_time, expire_time, lease_owner, lease_until, attempts)
		VALUES ($1, $2, $3, 'c', 'update', $4, $5::jsonb, $6, '10.0.0.9', $7, $8::jsonb,
		CASE WHEN $9::float8 IS NULL THEN NULL ELSE now() - make_interval(secs => $9) END,
		CASE WHEN $10::float8 IS NULL THEN now() + interval '1 day' ELSE now() + make_interval(secs => $10) END,
		$11, CASE WHEN $12::float8 IS NULL THEN NULL ELSE now() + make_interval(secs => $12) END, 0)`,
		id, spec.Kind, spec.Cluster, user, requester, exec, spec.Status, result, approved, expires, spec.Owner, lease)
	return id
}

// changeRow 一张变更单执行状态相关列的断言视图。
type changeRow struct {
	Status, Attempts       int32
	Message, Result, Owner string
	LeaseUntil, Finish     *time.Time
}

func (e *env) changeRow(id string) changeRow {
	e.t.Helper()
	var r changeRow
	e.scan(`SELECT status, attempts, message, result::text, lease_owner, lease_until, finish_time FROM k8s_change WHERE id = $1`, []any{id},
		&r.Status, &r.Attempts, &r.Message, &r.Result, &r.Owner, &r.LeaseUntil, &r.Finish)
	return r
}

// same 两张变更单视图是否相同（按值比较）。
func (r changeRow) same(o changeRow) bool {
	return r.Status == o.Status && r.Attempts == o.Attempts && r.Message == o.Message && r.Result == o.Result && r.Owner == o.Owner &&
		sameTime(r.LeaseUntil, o.LeaseUntil) && sameTime(r.Finish, o.Finish)
}

// 变更单的领取：最早批准的一张，只领可执行的（未过期、有执行计划与发起人快照、本实例负责的集群）；
// 租约过期后尚未开始提交的可重领，已开始提交的记失败不重跑。
func TestChangeClaim(t *testing.T) {
	e := newEnv(t)
	repo := NewChangeRepo(e.d)
	ctx := ctxb()
	const lease = time.Minute
	c1, c2 := e.cluster(clusterSpec{}), e.cluster(clusterSpec{})
	claim := func(clusters ...string) *biz.Change {
		t.Helper()
		ch, err := repo.Claim(ctx, "i1", clusters, lease)
		if err != nil {
			t.Fatalf("Claim() error = %v", err)
		}
		return ch
	}
	newest := e.change(changeSpec{Cluster: c1})
	oldest := e.change(changeSpec{Cluster: c1, ApprovedAgo: time.Hour})
	skip := map[string]int32{
		e.change(changeSpec{Cluster: c1, ExpiresIn: ptr(-time.Minute)}):                                 k8scontract.ChangeApproved,
		e.change(changeSpec{Cluster: c1, Kind: k8scontract.ChangeKindPermission}):                       k8scontract.ChangeApproved,
		e.change(changeSpec{Cluster: c1, NoExec: true}):                                                 k8scontract.ChangeApproved,
		e.change(changeSpec{Cluster: c1, NoRequester: true}):                                            k8scontract.ChangeApproved,
		e.change(changeSpec{Cluster: c1, Status: k8scontract.ChangePending, ApprovedAgo: -time.Second}): k8scontract.ChangePending,
	}

	// 领取：最早批准的一张；写租约、置为执行中、尝试次数 +1；带回执行计划与发起人快照。
	ch := claim(c1)
	if ch == nil || ch.ID != oldest || ch.ClusterID != c1 || ch.Kind != k8scontract.ChangeKindOp || len(ch.Exec) == 0 ||
		ch.Requester.UserID == "" || ch.Requester.Username != "ops" || len(ch.Requester.RoleIDs) != 1 || ch.Requester.IsAdmin || ch.ClientIP != "10.0.0.9" {
		t.Fatalf("Claim() = %+v", ch)
	}
	if r := e.changeRow(oldest); r.Status != k8scontract.ChangeExecuting || r.Owner != "i1" || r.LeaseUntil == nil ||
		r.LeaseUntil.Before(e.dbNow().Add(50*time.Second)) || r.Attempts != 1 {
		t.Fatalf("change after Claim: %+v", r)
	}
	// 不可领取的都不领：过期、申请权限（kind 3）、没有执行计划 / 发起人快照、未批准。
	if ch = claim(c1); ch == nil || ch.ID != newest {
		t.Fatalf("Claim() = %+v, want %s", ch, newest)
	}
	if ch = claim(c1); ch != nil {
		t.Fatalf("nothing is claimable, got %+v", ch)
	}
	for id, status := range skip {
		if r := e.changeRow(id); r.Status != status || r.Owner != "" || r.Attempts != 0 {
			t.Fatalf("unclaimable change %s was touched: %+v", id[:8], r)
		}
	}
	// 租约过期且尚未开始向集群提交：可被重新领取，尝试次数累加。
	e.exec(`UPDATE k8s_change SET lease_until = now() - interval '1 second' WHERE id = $1`, newest)
	if ch = claim(c1); ch == nil || ch.ID != newest || e.changeRow(newest).Attempts != 2 {
		t.Fatalf("re-claim of an expired lease = %+v, attempts = %d", ch, e.changeRow(newest).Attempts)
	}
	// 已开始向集群提交后执行器失联：不重跑，直接记为失败；别的集群的同类变更单不在本次处理范围。
	stuck := e.change(changeSpec{Cluster: c2, Status: k8scontract.ChangeExecuting, Lease: ptr(-time.Minute), Owner: "i9", Started: true})
	other := e.change(changeSpec{Cluster: c2})
	if err := repo.MarkStarted(ctx, "i1", newest); err != nil {
		t.Fatalf("MarkStarted() error = %v", err)
	}
	e.exec(`UPDATE k8s_change SET lease_until = now() - interval '1 second' WHERE id = $1`, newest)
	if ch = claim(c1); ch != nil {
		t.Fatalf("a started change was re-claimed: %+v", ch)
	}
	if r := e.changeRow(newest); r.Status != k8scontract.ChangeFailed || r.Message != interruptedMessage || r.Finish == nil ||
		r.Owner != "" || r.LeaseUntil != nil {
		t.Fatalf("an interrupted change must be failed, not re-queued: %+v", r)
	}
	if r := e.changeRow(stuck); r.Status != k8scontract.ChangeExecuting {
		t.Fatalf("Claim() touched a change of another cluster: %+v", r)
	}
	if ch = claim(c2); ch == nil || ch.ID != other || e.changeRow(stuck).Status != k8scontract.ChangeFailed {
		t.Fatalf("Claim(c2) = %+v; want %s and the stuck change failed", ch, other)
	}

	// 多实例并发领取：每张变更单只被一个实例领到（FOR UPDATE SKIP LOCKED）。
	clusters := []string{c1, c2}
	queued := make([]string, 0, 10)
	for range 10 {
		queued = append(queued, e.change(changeSpec{Cluster: clusters[len(queued)%2]}))
	}
	var (
		mu  sync.Mutex
		got []string
		wg  sync.WaitGroup
	)
	for n := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ch, err := repo.Claim(ctx, fmt.Sprintf("i-%d", n), clusters, lease)
				if err != nil {
					t.Errorf("Claim() error = %v", err)
					return
				}
				if ch == nil {
					return
				}
				mu.Lock()
				got = append(got, ch.ID)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(got) != len(queued) || !sameSet(got, queued) || e.count("SELECT count(*) FROM k8s_change WHERE id = ANY($1::uuid[]) AND attempts <> 1", queued) != 0 {
		t.Fatalf("claimed %d changes %v across instances, want each of %d exactly once", len(got), got, len(queued))
	}
}

// 变更单执行状态的回写：MarkStarted / Finish / Release 都以「执行中 + 归本实例」围栏。
func TestChangeSettle(t *testing.T) {
	e := newEnv(t)
	repo := NewChangeRepo(e.d)
	ctx := ctxb()
	const lease = time.Minute
	c := e.cluster(clusterSpec{})
	claim := func() string {
		t.Helper()
		ch, err := repo.Claim(ctx, "i1", []string{c}, lease)
		if err != nil || ch == nil {
			t.Fatalf("Claim() = %+v, %v", ch, err)
		}
		return ch.ID
	}
	e.change(changeSpec{Cluster: c})
	id := claim()

	// MarkStarted：别的实例写不进去（返回错误，调用方因此不得提交），结果不变。
	if err := repo.MarkStarted(ctx, "i2", id); err == nil {
		t.Fatal("MarkStarted() by another instance must fail")
	}
	if r := e.changeRow(id); !jsonEqual(t, []byte(r.Result), `{}`) || r.Owner != "i1" {
		t.Fatalf("a fenced MarkStarted changed the row: %+v", r)
	}
	if err := repo.MarkStarted(ctx, "i1", id); err != nil {
		t.Fatalf("MarkStarted() error = %v", err)
	}
	if r := e.changeRow(id); !jsonEqual(t, []byte(r.Result), `{"started":true}`) {
		t.Fatalf("result after MarkStarted = %s", r.Result)
	}
	// Finish：终态、结果（保留 started）、说明、结束时间；租约清空。
	result := k8scontract.ChangeResult{
		Applied:  []k8scontract.Ref{{Version: "v1", Resource: "deployments", Namespace: "default", Name: "web"}},
		Attempts: 2, Started: true,
	}
	before := e.dbNow()
	if err := repo.Finish(ctx, "i1", id, k8scontract.ChangeSucceeded, "ok", result); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	r := e.changeRow(id)
	if r.Status != k8scontract.ChangeSucceeded || r.Message != "ok" || r.Finish == nil || r.Finish.Before(before) ||
		r.Owner != "" || r.LeaseUntil != nil ||
		!jsonEqual(t, []byte(r.Result), `{"applied":[{"group":"","version":"v1","resource":"deployments","namespace":"default","name":"web"}],"attempts":2,"started":true}`) {
		t.Fatalf("change after Finish: %+v", r)
	}
	// 终态之后再写不生效（本实例迟到的那次、别的实例），也不报错。
	if err := repo.Finish(ctx, "i1", id, k8scontract.ChangeFailed, "late", k8scontract.ChangeResult{}); err != nil {
		t.Fatalf("late Finish() error = %v", err)
	}
	if err := repo.Release(ctx, "i1", id, "late"); err != nil {
		t.Fatalf("late Release() error = %v", err)
	}
	if err := repo.MarkStarted(ctx, "i1", id); err == nil {
		t.Fatal("MarkStarted() on a finished change must fail")
	}
	if after := e.changeRow(id); !after.same(r) {
		t.Fatalf("a settled change was rewritten: %+v", after)
	}

	// Release：集群暂不可达——放回「已批准」并写明原因，租约清空；只在执行中且归本实例时生效。
	e.change(changeSpec{Cluster: c})
	id = claim()
	if err := repo.Release(ctx, "i2", id, "nope"); err != nil {
		t.Fatalf("Release() by another instance error = %v", err)
	}
	if r = e.changeRow(id); r.Status != k8scontract.ChangeExecuting || r.Owner != "i1" {
		t.Fatalf("a fenced Release changed the row: %+v", r)
	}
	if err := repo.Release(ctx, "i1", id, "cluster unreachable"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if r = e.changeRow(id); r.Status != k8scontract.ChangeApproved || r.Message != "cluster unreachable" || r.Owner != "" ||
		r.LeaseUntil != nil || r.Attempts != 1 {
		t.Fatalf("change after Release: %+v", r)
	}
	// 放回之后可再次领取。
	if claim() != id {
		t.Fatal("a released change must be claimable again")
	}
}

// eventRow 一条 Warning 事件（k8s_event）的断言视图。
type eventRow struct {
	ClusterName, Namespace, Kind, Name, Reason, Message, Source string
	Count                                                       int64
	First, Last                                                 time.Time
	Notified                                                    *time.Time
}

func (e *env) event(id string) eventRow {
	e.t.Helper()
	var r eventRow
	e.scan(`SELECT cluster_name, namespace, kind, name, reason, message, source, count, first_time, last_time, notify_time
		FROM k8s_event WHERE id = $1`, []any{id}, &r.ClusterName, &r.Namespace, &r.Kind, &r.Name, &r.Reason, &r.Message, &r.Source,
		&r.Count, &r.First, &r.Last, &r.Notified)
	return r
}

// Warning 事件落库：同一集群同一对象同一原因只有一行；次数的累加 / 取大、时间的区间只增不减、列按宽度截断。
func TestEventUpsert(t *testing.T) {
	e := newEnv(t)
	repo := NewInsightRepo(e.d)
	ctx := ctxb()
	c := e.cluster(clusterSpec{})
	base := time.Now().Add(-2 * time.Hour).Truncate(time.Microsecond)
	upsert := func(rec biz.EventRecord, first bool) (string, int64) {
		t.Helper()
		id, count, err := repo.Upsert(ctx, rec, first)
		if err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
		return id, count
	}
	rec := biz.EventRecord{
		ClusterID: c, ClusterName: "c", Namespace: "default", Kind: "Pod", Name: "web-1", Reason: "OOMKilling",
		Message: "Memory cgroup out of memory", Source: "kubelet", Absolute: 5, Delta: 5, FirstTime: base, LastTime: base.Add(time.Minute),
	}

	id, count := upsert(rec, true)
	if !idgen.IsUUID(id) || count != 5 {
		t.Fatalf("first Upsert() = %s, %d", id, count)
	}
	if r := e.event(id); r.ClusterName != "c" || r.Namespace != "default" || r.Kind != "Pod" || r.Name != "web-1" || r.Reason != "OOMKilling" ||
		r.Message != "Memory cgroup out of memory" || r.Source != "kubelet" || r.Count != 5 || !r.First.Equal(base) || !r.Last.Equal(base.Add(time.Minute)) || r.Notified != nil {
		t.Fatalf("event after the first Upsert: %+v", r)
	}
	// 再次上报：同一行，次数累加增量，最近发生时间与说明取新的，首次发生时间不变。
	rec.Absolute, rec.Delta, rec.LastTime, rec.Message = 9, 4, base.Add(2*time.Minute), "Memory cgroup out of memory (again)"
	if id2, n := upsert(rec, false); id2 != id || n != 9 {
		t.Fatalf("second Upsert() = %s, %d; want %s, 9", id2, n, id)
	}
	if r := e.event(id); r.Count != 9 || !r.Last.Equal(rec.LastTime) || r.Message != "Memory cgroup out of memory (again)" || !r.First.Equal(base) {
		t.Fatalf("event after the second Upsert: %+v", r)
	}
	// 本实例对该集群的第一轮采集（first = true）：次数取大，不重复累加（实例重启后重放的事件不重复计数）。
	rec.Absolute = 3
	if _, n := upsert(rec, true); n != 9 {
		t.Fatalf("first Upsert() with a smaller absolute count = %d, want 9 (greatest)", n)
	}
	// 更旧的事件：最近发生时间与说明不回退；次数仍累加增量。
	rec.LastTime, rec.Message, rec.Delta = base.Add(30*time.Second), "older", 1
	if _, n := upsert(rec, false); n != 10 {
		t.Fatalf("Upsert() of an older event = %d, want 10", n)
	}
	if r := e.event(id); !r.Last.Equal(base.Add(2*time.Minute)) || r.Message != "Memory cgroup out of memory (again)" {
		t.Fatalf("an older event moved the row backwards: %+v", r)
	}
	if e.count("SELECT count(*) FROM k8s_event") != 1 {
		t.Fatal("one cluster / object / reason must have exactly one row")
	}
	// 集群改名：冗余的集群名跟着更新。
	rec.ClusterName = "renamed"
	upsert(rec, false)
	if r := e.event(id); r.ClusterName != "renamed" {
		t.Fatalf("cluster_name after a rename = %q", r.ClusterName)
	}
	// 边界：上报次数为 0 时至少记 1。
	zero := rec
	zero.Name, zero.Absolute, zero.Delta = "web-2", 0, 0
	if _, n := upsert(zero, true); n != 1 {
		t.Fatalf("Upsert() with a zero count = %d, want 1", n)
	}
	// 列截断：namespace 63 / kind 128 / name 253 / reason 128；message 按字符（不是字节）截到 1024。
	long, wide := strings.Repeat("x", 800), strings.Repeat("警", 2000)
	rec.Namespace, rec.Kind, rec.Name, rec.Reason, rec.Message = long, long, long, long, wide
	wideID, _ := upsert(rec, true)
	if r := e.event(wideID); len(r.Namespace) != 63 || len(r.Kind) != 128 || len(r.Name) != 253 || len(r.Reason) != 128 ||
		len([]rune(r.Message)) != 1024 || r.Message != strings.Repeat("警", 1024) {
		t.Fatalf("overlong event was not clipped to its widths: ns %d kind %d name %d reason %d message %d runes",
			len(r.Namespace), len(r.Kind), len(r.Name), len(r.Reason), len([]rune(r.Message)))
	}
}

// 事件通知门闩：同一事务里置通知时间并写发件箱；窗口内只通知一次；超长的键截断。
func TestClaimNotify(t *testing.T) {
	e := newEnv(t)
	repo := NewInsightRepo(e.d)
	ctx := ctxb()
	c := e.cluster(clusterSpec{})
	base := time.Now().Add(-time.Hour)
	id, _, err := repo.Upsert(ctx, biz.EventRecord{
		ClusterID: c, Namespace: "default", Kind: "Pod", Name: "web-1", Reason: "OOMKilling",
		Absolute: 3, Delta: 3, FirstTime: base, LastTime: base,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	const window = 10 * time.Minute
	build := func(dedup string) func(time.Time) biz.OutboxEvent {
		return func(at time.Time) biz.OutboxEvent {
			return biz.OutboxEvent{
				ClusterID: c, EventType: "k8s.event.oomkilling", Severity: 3, DedupKey: dedup,
				GroupKey: "k8s:event:" + c + ":default:Pod:web-1:OOMKilling", Payload: map[string]any{"id": id, "time": at.Unix()},
			}
		}
	}
	outbox := func(key string) (int, string) {
		e.t.Helper()
		var n int
		var payload string
		if key == "" {
			e.scan("SELECT count(*), '' FROM k8s_notify_outbox", nil, &n, &payload)
			return n, payload
		}
		e.scan("SELECT count(*), min(payload::text) FROM k8s_notify_outbox WHERE dedup_key = $1", []any{key}, &n, &payload)
		return n, payload
	}

	// 窗口内第一次：置通知时间、写发件箱、返回 true。
	claimed, err := repo.ClaimNotify(ctx, id, window, build("k8s:event:one"))
	if err != nil || !claimed {
		t.Fatalf("ClaimNotify() = %v, %v; want claimed", claimed, err)
	}
	r := e.event(id)
	if n, payload := outbox("k8s:event:one"); r.Notified == nil || r.Notified.Before(e.dbNow().Add(-time.Minute)) || n != 1 || !jsonEqual(t, []byte(payload), fmt.Sprintf(`{"id":%q,"time":%d}`, id, r.Notified.Unix())) {
		t.Fatalf("outbox = %d rows %s; notify_time = %v", n, payload, r.Notified)
	}
	// 窗口内再调：不再通知，也不重复写发件箱。
	if claimed, err = repo.ClaimNotify(ctx, id, window, build("k8s:event:two")); err != nil || claimed {
		t.Fatalf("ClaimNotify() inside the window = %v, %v; want not claimed", claimed, err)
	}
	if n, _ := outbox(""); n != 1 || !e.event(id).Notified.Equal(*r.Notified) {
		t.Fatalf("a second claim inside the window wrote %d outbox rows or moved notify_time", n)
	}
	// 窗口过后可以再通知。
	e.exec(`UPDATE k8s_event SET notify_time = now() - interval '1 hour' WHERE id = $1`, id)
	if claimed, err = repo.ClaimNotify(ctx, id, window, build("k8s:event:two")); err != nil || !claimed {
		t.Fatalf("ClaimNotify() after the window = %v, %v; want claimed", claimed, err)
	}
	if n, _ := outbox(""); n != 2 {
		t.Fatalf("%d outbox rows, want 2", n)
	}
	// 边界（现状）：发件箱里已有同一 dedup_key 的行时，门闩照样置上并返回 true，发件箱不重复。
	e.exec(`UPDATE k8s_event SET notify_time = now() - interval '1 hour' WHERE id = $1`, id)
	if claimed, err = repo.ClaimNotify(ctx, id, window, build("k8s:event:one")); err != nil || !claimed {
		t.Fatalf("ClaimNotify() with a known dedup key = %v, %v; want claimed (gate) with no new outbox row", claimed, err)
	}
	if n, _ := outbox(""); n != 2 {
		t.Fatalf("%d outbox rows after a deduplicated notify, want 2", n)
	}
	// 超长的 dedup_key / group_key 截断到列宽（255）。
	e.exec(`UPDATE k8s_event SET notify_time = now() - interval '1 hour' WHERE id = $1`, id)
	if claimed, err = repo.ClaimNotify(ctx, id, window, build(strings.Repeat("k", 400))); err != nil || !claimed {
		t.Fatalf("ClaimNotify() with an overlong dedup key = %v, %v", claimed, err)
	}
	if n, _ := outbox(strings.Repeat("k", 255)); n != 1 {
		t.Fatalf("%d outbox rows for the clipped dedup key, want 1", n)
	}
	// 不存在的事件行：不通知，不报错。
	if claimed, err = repo.ClaimNotify(ctx, e.id(), window, build("k8s:event:none")); err != nil || claimed {
		t.Fatalf("ClaimNotify() of an unknown event = %v, %v; want not claimed", claimed, err)
	}
}

// 过期 Warning 事件的清理：最近发生时间早于保留期的删，其余留；幂等。
func TestPurgeEvents(t *testing.T) {
	e := newEnv(t)
	repo := NewInsightRepo(e.d)
	ctx := ctxb()
	c := e.cluster(clusterSpec{})
	var old, older, recent string
	for i, age := range []time.Duration{8 * 24 * time.Hour, 3 * 24 * time.Hour, time.Hour} {
		at := time.Now().Add(-age)
		id, _, err := repo.Upsert(ctx, biz.EventRecord{
			ClusterID: c, Namespace: "default", Kind: "Pod", Name: fmt.Sprintf("web-%d", i),
			Reason: "BackOff", Absolute: 1, Delta: 1, FirstTime: at, LastTime: at,
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		switch i {
		case 0:
			old = id
		case 1:
			older = id
		default:
			recent = id
		}
	}
	n, err := repo.PurgeEvents(ctx, 7*24*time.Hour)
	if err != nil || n != 1 || e.count("SELECT count(*) FROM k8s_event WHERE id = $1", old) != 0 ||
		e.count("SELECT count(*) FROM k8s_event WHERE id = ANY($1::uuid[])", []string{older, recent}) != 2 {
		t.Fatalf("PurgeEvents() = %d, %v; only the event older than the retention must go", n, err)
	}
	if n, err = repo.PurgeEvents(ctx, 7*24*time.Hour); err != nil || n != 0 {
		t.Fatalf("second PurgeEvents() = %d, %v; want 0", n, err)
	}
	// retention = 0：全部删（都早于现在）。
	if n, err = repo.PurgeEvents(ctx, 0); err != nil || n != 2 || e.count("SELECT count(*) FROM k8s_event") != 0 {
		t.Fatalf("PurgeEvents(0) = %d, %v; want 2", n, err)
	}
}

// 集群总览摘要与事件上报原因：摘要以归属围栏落 jsonb；原因只取配置了的启用集群。
func TestSummaryAndReasons(t *testing.T) {
	e := newEnv(t)
	repo := NewInsightRepo(e.d)
	ctx := ctxb()
	c, idle := e.cluster(clusterSpec{}), e.cluster(clusterSpec{})
	if _, err := NewRegistryRepo(e.d).Claim(ctx, "i1", time.Minute, 10); err != nil {
		t.Fatal(err)
	}
	summary := k8scontract.ClusterSummary{
		NodeTotal: 3, NodeReady: 2, NodeCordoned: 1, CPUAllocatable: 8000, CPURequested: 4100,
		Pods: map[string]int{"Running": 10, "Pending": 1}, PodTotal: 11, WorkloadTotal: 5, WorkloadAbnormal: 1, DurationMS: 12,
	}
	if err := repo.SaveSummary(ctx, "i1", c, summary); err != nil {
		t.Fatalf("SaveSummary() error = %v", err)
	}
	r := e.runtime(c)
	if r.SummaryAt == nil || r.SummaryAt.Before(e.dbNow().Add(-time.Minute)) ||
		!jsonEqual(t, []byte(r.Summary), `{"nodeTotal":3,"nodeReady":2,"nodeCordoned":1,"cpuAllocatable":8000,"cpuRequested":4100,"memoryAllocatable":0,"memoryRequested":0,"podCapacity":0,"pods":{"Pending":1,"Running":10},"podTotal":11,"workloadTotal":5,"workloadAbnormal":1,"abnormalWorkloads":null,"restartPods":null,"abnormalNodes":null,"durationMs":12}`) {
		t.Fatalf("summary = %s at %v", r.Summary, r.SummaryAt)
	}
	// 围栏：不归本实例的写不进去，不报错。
	other := summary
	other.NodeTotal = 99
	if err := repo.SaveSummary(ctx, "i2", c, other); err != nil || e.runtime(c).Summary != r.Summary {
		t.Fatalf("a fenced SaveSummary changed the summary: %v", err)
	}
	if err := repo.SaveSummary(ctx, "i1", idle, other); err != nil || e.runtime(c).Summary != r.Summary {
		t.Fatalf("SaveSummary() of a cluster without a runtime row error = %v", err)
	}

	// Reasons：只返回配置了上报原因的启用且未删除的集群；没有配置的不在结果里。
	plain := e.cluster(clusterSpec{})
	two := e.cluster(clusterSpec{Reasons: []string{"OOMKilling", "BackOff"}})
	e.cluster(clusterSpec{Reasons: []string{"OOMKilling"}, Disabled: true})
	e.cluster(clusterSpec{Reasons: []string{"OOMKilling"}, Deleted: true})
	reasons, err := repo.Reasons(ctx)
	if err != nil || len(reasons) != 1 || !slices.Equal(reasons[two], []string{"OOMKilling", "BackOff"}) {
		t.Fatalf("Reasons() = %v, %v", reasons, err)
	}
	if _, ok := reasons[plain]; ok {
		t.Fatal("a cluster without configured reasons must not be in the result")
	}
}
