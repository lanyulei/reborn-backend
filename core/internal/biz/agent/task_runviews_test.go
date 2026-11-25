package biz

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// viewDeps 运行列表补显示名用到的依赖桩，各记调用次数。
type viewDeps struct {
	runs        []*Run
	profiles    map[string]string
	users       map[string]string
	snapshotErr error
	memberErr   error
	member      bool

	profileCalls, nameCalls, snapshotCalls, memberCalls int
}

type (
	viewProfiles struct {
		ProfileRepo
		d *viewDeps
	}
	viewRuns struct {
		RunRepo
		d *viewDeps
	}
	viewTasks struct {
		TaskRepo
		d *viewDeps
	}
	viewAccounts struct {
		Accounts
		d *viewDeps
	}
	viewIncidents struct {
		Incidents
		d *viewDeps
	}
)

func (v viewProfiles) ListByIDs(_ context.Context, ids []string) ([]*Profile, error) {
	v.d.profileCalls++
	var out []*Profile
	for _, id := range ids {
		if name, ok := v.d.profiles[id]; ok {
			out = append(out, &Profile{ID: id, Spec: ProfileSpec{Name: name}})
		}
	}
	return out, nil
}

func (v viewRuns) ListByIncident(context.Context, string, int) ([]*Run, error) { return v.d.runs, nil }

func (viewTasks) ActiveByIncident(context.Context, string, string) (*Task, error) { return nil, nil }

func (v viewAccounts) Names(_ context.Context, ids []string) (map[string]string, error) {
	v.d.nameCalls++
	out := map[string]string{}
	for _, id := range ids {
		if name, ok := v.d.users[id]; ok {
			out[id] = name
		}
	}
	return out, nil
}

func (v viewIncidents) Snapshot(_ context.Context, id string) (*IncidentSnapshot, error) {
	v.d.snapshotCalls++
	if v.d.snapshotErr != nil {
		return nil, v.d.snapshotErr
	}
	return &IncidentSnapshot{ID: id, Number: "INC-" + id}, nil
}

func (v viewIncidents) MemberAccess(context.Context, string, string) (bool, bool, bool, error) {
	v.d.memberCalls++
	return false, false, v.d.member, v.d.memberErr
}

func (viewIncidents) CanRead(context.Context, string) (bool, error) { return true, nil }

func (d *viewDeps) usecase() *TaskUsecase {
	return NewTaskUsecase(&Deps{
		Profiles: viewProfiles{d: d}, Runs: viewRuns{d: d}, Tasks: viewTasks{d: d}, Accounts: viewAccounts{d: d}, Incidents: viewIncidents{d: d},
	})
}

// legacyRunView 改动前 runView 的做法：每条运行各查一次 Profile 名、用户名、故障快照、成员身份。
func legacyRunView(ctx context.Context, uc *TaskUsecase, userID string, run *Run) (*RunView, error) {
	profiles, err := uc.profileNames(ctx, []string{run.ProfileID})
	if err != nil {
		return nil, err
	}
	names, err := uc.d.Accounts.Names(ctx, uniq([]string{run.AgentUserID, run.FeedbackUser}))
	if err != nil {
		return nil, err
	}
	v := &RunView{Run: run, ProfileName: profiles[run.ProfileID], AgentUserName: names[run.AgentUserID], FeedbackUserName: names[run.FeedbackUser]}
	if inc, err := uc.d.Incidents.Snapshot(ctx, run.IncidentID); err == nil {
		v.IncidentNumber = inc.Number
	}
	if userID != "" && run.Status != RunRunning {
		admin, commander, responder, err := uc.d.Incidents.MemberAccess(ctx, userID, run.IncidentID)
		if err == nil {
			v.CanRate = admin || commander || responder
		}
	}
	return v, nil
}

func sampleRuns(n int) []*Run {
	statuses := []string{RunSucceeded, RunRunning, RunFailed, RunCancelled}
	runs := make([]*Run, n)
	for i := range runs {
		runs[i] = &Run{
			ID: fmt.Sprintf("run-%d", i), IncidentID: "inc-1", ProfileID: fmt.Sprintf("profile-%d", i%3),
			AgentUserID: fmt.Sprintf("agent-%d", i%2), Status: statuses[i%len(statuses)],
		}
		if i%4 == 0 {
			runs[i].FeedbackUser = fmt.Sprintf("user-%d", i%5)
		}
	}
	return runs
}

// ListIncidentRuns 的 N+1（core-agent 优化项 A）：批量补显示名之后，每条运行的视图与逐条补的结果逐字段相同，
// 而查询次数从「每条 4 次」降为整批各 1 次。覆盖：已删除的 Profile / 用户（名字留空）、进行中的运行（不可评价）、
// 故障快照或成员身份取不到（编号留空 / 不可评价，不报错）、匿名调用者。
func TestListIncidentRunsBatchesLookupsWithoutChangingViews(t *testing.T) {
	cases := []struct {
		name   string
		userID string
		mod    func(d *viewDeps)
	}{
		{"member", "u-1", func(d *viewDeps) { d.member = true }},
		{"not a member", "u-1", func(*viewDeps) {}},
		{"no caller", "", func(d *viewDeps) { d.member = true }},
		{"incident gone", "u-1", func(d *viewDeps) { d.member, d.snapshotErr = true, errors.New("not found") }},
		{"membership lookup fails", "u-1", func(d *viewDeps) { d.member, d.memberErr = true, errors.New("db down") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newDeps := func() *viewDeps {
				d := &viewDeps{
					runs:     sampleRuns(12),
					profiles: map[string]string{"profile-0": "诊断", "profile-1": "复盘"}, // profile-2 已删除
					users:    map[string]string{"agent-0": "Agent A", "user-0": "张三"}, // 其余用户已不存在
				}
				tc.mod(d)
				return d
			}
			base := newDeps()
			baseUC := base.usecase()
			want := make([]*RunView, 0, len(base.runs))
			for _, r := range base.runs {
				v, err := legacyRunView(context.Background(), baseUC, tc.userID, r)
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, v)
			}

			d := newDeps()
			got, active, err := d.usecase().ListIncidentRuns(context.Background(), tc.userID, "inc-1")
			if err != nil || active != nil {
				t.Fatalf("ListIncidentRuns() active = %v, err = %v", active, err)
			}
			if len(got) != len(want) {
				t.Fatalf("views = %d, want %d", len(got), len(want))
			}
			for i := range want {
				// 两边的 *Run 是各自的样本：比内容。
				if !reflect.DeepEqual(got[i], want[i]) {
					t.Fatalf("view %d = %+v, want %+v", i, got[i], want[i])
				}
			}
			wantMember := 1
			if tc.userID == "" {
				wantMember = 0
			}
			if d.profileCalls != 1 || d.nameCalls != 1 || d.snapshotCalls != 1 || d.memberCalls != wantMember {
				t.Fatalf("lookups = profiles %d, names %d, snapshots %d, membership %d; want 1, 1, 1, %d",
					d.profileCalls, d.nameCalls, d.snapshotCalls, d.memberCalls, wantMember)
			}
			if base.snapshotCalls != len(base.runs) {
				t.Fatalf("baseline made %d snapshot lookups: the test no longer exercises the N+1", base.snapshotCalls)
			}
		})
	}
}

// 没有运行时不做任何查询，返回空列表（非 nil）；全是进行中的运行时不查成员身份。
func TestListIncidentRunsEdgeCases(t *testing.T) {
	d := &viewDeps{}
	got, _, err := d.usecase().ListIncidentRuns(context.Background(), "u-1", "inc-1")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("ListIncidentRuns() with no runs = %v, %v", got, err)
	}
	if d.profileCalls+d.nameCalls+d.snapshotCalls+d.memberCalls != 0 {
		t.Fatal("no lookups are expected when there are no runs")
	}
	d = &viewDeps{runs: []*Run{{ID: "r1", IncidentID: "inc-1", Status: RunRunning}, {ID: "r2", IncidentID: "inc-1", Status: RunRunning}}, member: true}
	got, _, err = d.usecase().ListIncidentRuns(context.Background(), "u-1", "inc-1")
	if err != nil || len(got) != 2 || got[0].CanRate || got[1].CanRate || d.memberCalls != 0 {
		t.Fatalf("running runs: views = %+v, membership lookups = %d, err = %v", got, d.memberCalls, err)
	}
}
