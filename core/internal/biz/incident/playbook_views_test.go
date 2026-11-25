package biz

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// viewProposals 预案执行次数桩：两个方法同一份数据；记下各自被调用的次数。
type viewProposals struct {
	ProposalRepo
	execs       map[string]int
	single, any int
}

func (p *viewProposals) CountPlaybookExecs(_ context.Context, id string, _ time.Time) (int, error) {
	p.single++
	return p.execs[id], nil
}

func (p *viewProposals) CountPlaybookExecsMany(_ context.Context, ids []string, _ time.Time) (map[string]int, error) {
	p.any++
	out := map[string]int{}
	for _, id := range ids {
		if n := p.execs[id]; n > 0 {
			out[id] = n
		}
	}
	return out, nil
}

// viewBusinesses 业务桩：与数据库一样不区分 ID 大小写、返回小写规范形式；listErr 模拟批量读取失败。
type viewBusinesses struct {
	BusinessRepo
	byID      map[string]*Business
	listErr   error
	gets, all int
}

func (b *viewBusinesses) Get(_ context.Context, id string) (*Business, error) {
	b.gets++
	if it := b.byID[strings.ToLower(id)]; it != nil {
		return it, nil
	}
	return nil, ErrBusinessNotFound
}

func (b *viewBusinesses) ListByIDs(_ context.Context, ids []string) ([]*Business, error) {
	b.all++
	if b.listErr != nil {
		return nil, b.listErr
	}
	var out []*Business
	for _, id := range ids {
		if it := b.byID[strings.ToLower(id)]; it != nil {
			out = append(out, it)
		}
	}
	return out, nil
}

// viewJobs 作业模板桩：可见性随代理执行人而不同；calls 记下每次查询。
type viewJobs struct {
	Jobs
	calls []string
}

func (j *viewJobs) Template(_ context.Context, userID, id string) (*JobTemplate, error) {
	j.calls = append(j.calls, userID+"/"+id)
	switch {
	case id == "gone":
		return nil, nil
	case id == "off":
		return &JobTemplate{ID: id, Name: "停用的模板"}, nil
	case id == "private" && userID != "agent-1":
		return nil, ErrForbidden
	}
	return &JobTemplate{ID: id, Name: "作业 " + id, Enabled: true}, nil
}

// viewFlows 流程定义桩。
type viewFlows struct {
	Workflows
	calls []string
}

func (f *viewFlows) Startable(_ context.Context, key string) (string, bool, error) {
	f.calls = append(f.calls, key)
	if key == "stopped" {
		return "", false, nil
	}
	return "流程 " + key, true, nil
}

// legacyPlaybookViews 改写前的 views：逐个预案、逐个步骤查询，作对照。
func legacyPlaybookViews(ctx context.Context, uc *PlaybookUsecase, items []*Playbook) ([]*PlaybookView, error) {
	var users, businesses []string
	for _, p := range items {
		users = append(users, p.Spec.AgentUserID, p.UpdateUser)
		businesses = append(businesses, p.Spec.Match.BusinessIDs...)
	}
	userNames, err := uc.d.Dir.UserNames(ctx, uniq(users))
	if err != nil {
		return nil, err
	}
	businessNames := map[string]string{}
	for _, id := range uniq(businesses) {
		if b, err := uc.d.Businesses.Get(ctx, id); err == nil {
			businessNames[id] = b.Name
		}
	}
	since := time.Now().Add(-30 * 24 * time.Hour)
	out := make([]*PlaybookView, 0, len(items))
	for _, p := range items {
		n, err := uc.d.Proposals.CountPlaybookExecs(ctx, p.ID, since)
		if err != nil {
			return nil, err
		}
		v := &PlaybookView{Playbook: p, AgentUserName: userNames[p.Spec.AgentUserID], UpdateUserName: userNames[p.UpdateUser], BusinessNames: businessNames, ExecCount: i32(n)}
		for _, st := range p.Spec.Steps {
			name, err := uc.targetName(ctx, p.Spec.AgentUserID, st.Kind, st.Ref)
			if err != nil {
				return nil, err
			}
			v.StepNames = append(v.StepNames, name)
		}
		out = append(out, v)
	}
	return out, nil
}

func playbookFixture() []*Playbook {
	step := func(kind, ref string) PlaybookStep { return PlaybookStep{Name: ref, Kind: kind, Ref: ref} }
	return []*Playbook{
		{ID: "p1", UpdateUser: "u1", Spec: PlaybookSpec{
			AgentUserID: "agent-1", Match: PlaybookMatch{BusinessIDs: []string{"b1", "B2", "gone"}},
			Steps: []PlaybookStep{step("job", "restart"), step("workflow", "rollback"), step("job", "private"), step("job", "restart")},
		}},
		{ID: "p2", UpdateUser: "u2", Spec: PlaybookSpec{
			AgentUserID: "agent-2", Match: PlaybookMatch{BusinessIDs: []string{"b1"}},
			Steps: []PlaybookStep{step("job", "restart"), step("job", "private"), step("workflow", "rollback"), step("workflow", "stopped"), step("job", "gone"), step("job", "off")},
		}},
		{ID: "p3", Spec: PlaybookSpec{AgentUserID: "agent-1"}}, // 没有步骤、没执行过
	}
}

func newPlaybookViewUsecase() (*PlaybookUsecase, *viewProposals, *viewBusinesses, *viewJobs, *viewFlows) {
	proposals := &viewProposals{execs: map[string]int{"p1": 4, "p2": 1}}
	businesses := &viewBusinesses{byID: map[string]*Business{"b1": {ID: "b1", Name: "支付"}, "b2": {ID: "b2", Name: "订单"}}}
	jobs, flows := &viewJobs{}, &viewFlows{}
	d := &Deps{
		Dir:       fakeDirectory{names: map[string]string{"agent-1": "机器人一号", "agent-2": "机器人二号", "u1": "张三"}},
		Proposals: proposals, Businesses: businesses, Jobs: jobs, Flows: flows, Logger: testLogger(),
	}
	return &PlaybookUsecase{d: d}, proposals, businesses, jobs, flows
}

// 预案列表：执行次数与业务名称各一条批量语句、同一个作业模板 / 流程定义只解析一次，结果与逐个查询相同。
func TestPlaybookViewsBatchLookupsWithSameResult(t *testing.T) {
	t.Parallel()
	items := playbookFixture()
	ref, _, _, _, _ := newPlaybookViewUsecase()
	want, err := legacyPlaybookViews(context.Background(), ref, items)
	if err != nil {
		t.Fatalf("legacy views error = %v", err)
	}

	uc, proposals, businesses, jobs, flows := newPlaybookViewUsecase()
	got, err := uc.views(context.Background(), items)
	if err != nil {
		t.Fatalf("views() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		for i := range want {
			t.Logf("playbook %s: got steps %q execs %d businesses %v; want steps %q execs %d businesses %v", want[i].ID,
				got[i].StepNames, got[i].ExecCount, got[i].BusinessNames, want[i].StepNames, want[i].ExecCount, want[i].BusinessNames)
		}
		t.Fatal("views() differs from the per-playbook assembly")
	}
	if proposals.single != 0 || proposals.any != 1 || businesses.gets != 0 || businesses.all != 1 {
		t.Fatalf("exec counts: %d single / %d batch; businesses: %d single / %d batch; want one batch each",
			proposals.single, proposals.any, businesses.gets, businesses.all)
	}
	// 作业模板按（代理执行人, 模板）各查一次，流程定义按标识各查一次。
	slices.Sort(jobs.calls)
	if !slices.Equal(jobs.calls, []string{"agent-1/private", "agent-1/restart", "agent-2/gone", "agent-2/off", "agent-2/private", "agent-2/restart"}) {
		t.Fatalf("job template lookups = %v, want each (agent, template) once", jobs.calls)
	}
	slices.Sort(flows.calls)
	if !slices.Equal(flows.calls, []string{"rollback", "stopped"}) {
		t.Fatalf("workflow lookups = %v, want each definition once", flows.calls)
	}
}

// 批量读业务失败：退回逐个读取，名称与此前一样能读到几个算几个。
func TestPlaybookViewsFallsBackWhenBusinessBatchFails(t *testing.T) {
	t.Parallel()
	items := playbookFixture()
	uc, _, businesses, _, _ := newPlaybookViewUsecase()
	businesses.listErr = errors.New("boom")

	got, err := uc.views(context.Background(), items)
	if err != nil {
		t.Fatalf("views() error = %v", err)
	}
	want := map[string]string{"b1": "支付", "B2": "订单"}
	if !reflect.DeepEqual(got[0].BusinessNames, want) || businesses.gets != 3 {
		t.Fatalf("business names = %v after %d single reads, want %v via the per-ID fallback", got[0].BusinessNames, businesses.gets, want)
	}
}

// 步骤类型非法：与此前一样报错。
func TestPlaybookViewsRejectsUnknownStepKind(t *testing.T) {
	t.Parallel()
	uc, _, _, _, _ := newPlaybookViewUsecase()
	items := []*Playbook{{ID: "p1", Spec: PlaybookSpec{Steps: []PlaybookStep{{Kind: "shell", Ref: "x"}}}}}
	if _, err := uc.views(context.Background(), items); !ErrPlaybookInvalid.Is(err) {
		t.Fatalf("views() error = %v, want ErrPlaybookInvalid", err)
	}
}
