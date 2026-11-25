package biz

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// 基准（AGENTS §6）：ListIncidentRuns 的 N+1——HEAD 的「每条运行各查 4 次」对当前的批量补显示名。
// 不连库：仓储桩每次调用固定耗时（模拟一次数据库往返），上报每个操作的仓储调用次数。
// 只比较，不断言性能阈值。

// benchRepoDelay 仓储桩每次调用的固定耗时（一次数据库往返的量级）。
const benchRepoDelay = 100 * time.Microsecond

// stallCalls 记仓储桩的调用次数，并给每次调用加固定耗时。用自旋而不是 Sleep：100µs 级别的睡眠
// 实际时长受调度器影响，噪声会盖过被测的差异。
type stallCalls struct {
	delay time.Duration
	n     int
}

func (c *stallCalls) hit() {
	c.n++
	start := time.Now()
	for time.Since(start) < c.delay {
	}
}

// 各依赖的包装：只拦 ListIncidentRuns 走到的方法并计数、加耗时；其余方法仍是内层桩。
type stallProfiles struct {
	ProfileRepo
	calls *stallCalls
}

type stallAccounts struct {
	Accounts
	calls *stallCalls
}

type stallIncidents struct {
	Incidents
	calls *stallCalls
}

type stallRuns struct {
	RunRepo
	calls *stallCalls
}

type stallTasks struct {
	TaskRepo
	calls *stallCalls
}

func (s stallProfiles) ListByIDs(ctx context.Context, ids []string) ([]*Profile, error) {
	s.calls.hit()
	return s.ProfileRepo.ListByIDs(ctx, ids)
}

func (s stallAccounts) Names(ctx context.Context, ids []string) (map[string]string, error) {
	s.calls.hit()
	return s.Accounts.Names(ctx, ids)
}

func (s stallIncidents) Snapshot(ctx context.Context, id string) (*IncidentSnapshot, error) {
	s.calls.hit()
	return s.Incidents.Snapshot(ctx, id)
}

func (s stallIncidents) MemberAccess(ctx context.Context, userID, incidentID string) (bool, bool, bool, error) {
	s.calls.hit()
	return s.Incidents.MemberAccess(ctx, userID, incidentID)
}

func (s stallIncidents) CanRead(ctx context.Context, userID string) (bool, error) {
	s.calls.hit()
	return s.Incidents.CanRead(ctx, userID)
}

func (s stallRuns) ListByIncident(ctx context.Context, incidentID string, limit int) ([]*Run, error) {
	s.calls.hit()
	return s.RunRepo.ListByIncident(ctx, incidentID, limit)
}

func (s stallTasks) ActiveByIncident(ctx context.Context, incidentID, kind string) (*Task, error) {
	s.calls.hit()
	return s.TaskRepo.ActiveByIncident(ctx, incidentID, kind)
}

// benchDeps 计数 + 固定耗时的依赖：桩的数据与等价性测试相同（3 个 Profile、部分用户已不存在）。
func benchDeps(d *viewDeps, calls *stallCalls) *Deps {
	return &Deps{
		Profiles:  stallProfiles{ProfileRepo: viewProfiles{d: d}, calls: calls},
		Accounts:  stallAccounts{Accounts: viewAccounts{d: d}, calls: calls},
		Incidents: stallIncidents{Incidents: viewIncidents{d: d}, calls: calls},
		Runs:      stallRuns{RunRepo: viewRuns{d: d}, calls: calls},
		Tasks:     stallTasks{TaskRepo: viewTasks{}, calls: calls},
	}
}

// listIncidentRunsLegacy HEAD:core/internal/biz/agent/task.go 的 ListIncidentRuns，循环里的
// uc.runView 换成同文件的 legacyRunView（HEAD 的 runView 逐字拷贝）：每条运行各查一次 Profile 名、
// 用户名、故障快照、成员身份。其余逐字保留。
func listIncidentRunsLegacy(ctx context.Context, uc *TaskUsecase, userID, incidentID string) ([]*RunView, *TaskView, error) {
	if err := uc.requireReader(ctx, userID); err != nil {
		return nil, nil, err
	}
	runs, err := uc.d.Runs.ListByIncident(ctx, incidentID, maxIncidentRuns)
	if err != nil {
		return nil, nil, err
	}
	views := make([]*RunView, 0, len(runs))
	for _, r := range runs {
		v, err := legacyRunView(ctx, uc, userID, r)
		if err != nil {
			return nil, nil, err
		}
		views = append(views, v)
	}
	active, err := uc.d.Tasks.ActiveByIncident(ctx, incidentID, KindDiagnose)
	if err != nil {
		return nil, nil, err
	}
	var tv *TaskView
	if active != nil {
		if tv, err = uc.view(ctx, active); err != nil {
			return nil, nil, err
		}
	}
	return views, tv, nil
}

// BenchmarkListIncidentRuns 一个 op = 故障详情「AI 诊断」页签一次加载（一条故障的全部运行）。
// n 的上界取 maxIncidentRuns（接口一次至多返回这么多条运行）。
func BenchmarkListIncidentRuns(b *testing.B) {
	ctx := context.Background()
	const user = "u-1"
	for _, n := range []int{10, maxIncidentRuns} {
		deps := &viewDeps{
			runs:     sampleRuns(n),
			profiles: map[string]string{"profile-0": "诊断", "profile-1": "复盘"}, // profile-2 已删除
			users:    map[string]string{"agent-0": "Agent A", "user-0": "张三"}, // 其余用户已不存在
			member:   true,
		}
		calls := &stallCalls{delay: benchRepoDelay}
		uc := NewTaskUsecase(benchDeps(deps, calls))

		// 开始计时之前核对：两种做法的视图逐字段相同。
		plain := &stallCalls{}
		plainUC := NewTaskUsecase(benchDeps(deps, plain))
		want, _, err := listIncidentRunsLegacy(ctx, plainUC, user, "inc-1")
		if err != nil {
			b.Fatal(err)
		}
		got, _, err := plainUC.ListIncidentRuns(ctx, user, "inc-1")
		if err != nil || len(got) != n || len(want) != n {
			b.Fatalf("runs = %d / %d, err = %v; want %d", len(got), len(want), err, n)
		}
		for i := range want {
			if !reflect.DeepEqual(got[i], want[i]) {
				b.Fatalf("view %d = %+v, want %+v", i, got[i], want[i])
			}
		}

		b.Run(fmt.Sprintf("runs=%d", n), func(b *testing.B) {
			for _, v := range []struct {
				name string
				run  func() error
			}{
				{"legacy=per-run-lookups", func() error {
					_, _, err := listIncidentRunsLegacy(ctx, uc, user, "inc-1")
					return err
				}},
				{"current=batched-lookups", func() error {
					_, _, err := uc.ListIncidentRuns(ctx, user, "inc-1")
					return err
				}},
			} {
				b.Run(v.name, func(b *testing.B) {
					calls.n = 0
					b.ResetTimer()
					for range b.N {
						if err := v.run(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(calls.n)/float64(b.N), "repo-calls/op")
				})
			}
		})
	}
}
