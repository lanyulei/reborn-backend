package biz

import (
	"context"
	"testing"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

type goneTemplates struct{ fakeTemplates }

func (goneTemplates) Get(context.Context, string) (*Template, error) { return nil, ErrTemplateNotFound }

type memSchedules struct{ saved *Schedule }

func (m *memSchedules) List(context.Context, ScheduleFilter, int64, int64) ([]*Schedule, int64, error) {
	return nil, 0, nil
}
func (m *memSchedules) Get(context.Context, string) (*Schedule, error) { return m.saved, nil }
func (m *memSchedules) Write(_ context.Context, _ WriteOp, s *Schedule, _ int32, _ string) error {
	m.saved = s
	return nil
}

// 停用必须总能成功：保存人没有执行权限、模板已被删除、模板改过后参数对不上，都不妨碍把它停下来。
func TestDisableAlwaysWorks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tpl := &Template{ID: "t", Name: "x", Enabled: true, Version: 7, Params: []jc.Param{{Code: "v", Name: "v", Type: jc.ParamInt}}}
	deny := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: denyRuns{}})
	off := func() *Schedule {
		return &Schedule{ID: "s", Name: "s", TemplateID: "t", Cron: "* * * * *", Timezone: "UTC", Misfire: 1, TemplateVersion: 5, Params: map[string]any{"v": "not-int"}}
	}
	for name, templates := range map[string]TemplateReader{"stale params": fakeTemplates{tpl}, "template deleted": goneTemplates{}} {
		repo := &memSchedules{}
		uc := NewScheduleUsecase(repo, nil, templates, deny, noNames{})
		if _, err := uc.Update(ctx, "u", off(), 1); err != nil {
			t.Fatalf("%s: disable schedule: %v", name, err)
		}
		// 停用不构成对新模板内容的确认：钉住的版本不变。
		if repo.saved.Enabled || repo.saved.NextTime != nil || repo.saved.TemplateVersion != 5 {
			t.Fatalf("%s: saved = %+v", name, repo.saved)
		}
	}
	trig := &TriggerUsecase{execs: deny}
	for name, templates := range map[string]TemplateReader{"stale params": fakeTemplates{tpl}, "template deleted": goneTemplates{}} {
		trig.templates = templates
		tr := &Trigger{ID: "t", Name: "t", Type: jc.TriggerTypeAPI, TemplateID: "t", TemplateVersion: 5, Params: map[string]any{"v": "not-int"}}
		if err := trig.normalize(ctx, tr); err != nil || tr.TemplateVersion != 5 {
			t.Fatalf("%s: disable trigger: %v (pin %d)", name, err, tr.TemplateVersion)
		}
	}
	// 启用仍然严格。
	allow := NewExecutionUsecase(nil, nil, nil, goneTemplates{}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	on := off()
	on.Enabled = true
	if _, err := NewScheduleUsecase(&memSchedules{}, nil, goneTemplates{}, allow, noNames{}).Update(ctx, "u", on, 1); !errorsIs(err, ErrScheduleInvalid) {
		t.Fatalf("enable with a deleted template: %v", err)
	}
}

// 列表上的启停开关回传的是旧的模板版本：启用被拒（没人看过新内容），未提供版本取当前版本。
func TestConfirmTemplate(t *testing.T) {
	t.Parallel()
	tpl := &Template{Version: 7}
	for _, c := range []struct {
		seen    int32
		enabled bool
		want    int32
		ok      bool
	}{{0, true, 7, true}, {7, true, 7, true}, {5, true, 0, false}, {5, false, 5, true}, {0, false, 7, true}} {
		got, err := confirmTemplate(ErrScheduleInvalid, tpl, c.seen, c.enabled)
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("seen %d enabled %v: got %d, %v", c.seen, c.enabled, got, err)
		}
		if err != nil && !errorsIs(err, ErrScheduleInvalid) {
			t.Errorf("wrong error: %v", err)
		}
	}
	// 手动执行同样由后端把关版本。
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{&Template{ID: "t", Enabled: true, Version: 7}}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	if _, err := execs.Submit(context.Background(), "u", SubmitInput{TemplateID: "t", TemplateVersion: 6}); !errorsIs(err, ErrTemplateInvalid) {
		t.Fatalf("manual submit with a stale version: %v", err)
	}
}

type oneFile struct{ data []byte }

func (f oneFile) Read(context.Context, string, string) (*UploadedFile, error) {
	return &UploadedFile{Name: "x.sh", Data: f.data}, nil
}

// 文件里塞一个非法 UTF-8 字节不能绕过高危规则检查。
func TestFileRiskTextSurvivesInvalidBytes(t *testing.T) {
	t.Parallel()
	uc := NewExecutionUsecase(nil, nil, nil, nil, nil, nil, nil, nil, noNames{}, ExecutionDeps{Files: oneFile{[]byte("# \xff\nrm -rf /\n")}})
	in := &AdhocInput{Kind: jc.KindFile, FileID: "f", Adhoc: jc.Adhoc{Dest: "/tmp"}}
	_, text, _, err := uc.prepareAdhoc(context.Background(), "u", in, true)
	if err != nil {
		t.Fatal(err)
	}
	rules := []*RiskRule{{Name: "rm", Pattern: `rm\s+-rf\s+/`, Action: jc.RiskBlock, Enabled: true}}
	if hits := matchRisk(rules, text); len(hits) != 1 {
		t.Fatalf("risk text %q not matched: %v", text, hits)
	}
}

type oneApprover struct {
	Approvals
	user string
}

func (a oneApprover) Involved(_ context.Context, _, userID string) (bool, error) {
	return userID == a.user, nil
}

// 审批人能看到自己正在审批的执行；无关的人仍然看不到。
func TestApproverSeesExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &fakeExecutions{exec: &Execution{ID: "e", Creator: "alice", ApprovalInstanceID: "i"}}
	uc := NewExecutionUsecase(f, f, nil, nil, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: denyRuns{}, Approvals: oneApprover{user: "carol"}})
	if err := uc.RequireVisible(ctx, "carol", "e"); err != nil {
		t.Fatalf("approver blocked: %v", err)
	}
	if err := uc.RequireVisible(ctx, "bob", "e"); !errorsIs(err, ErrExecutionNotFound) {
		t.Fatalf("bystander allowed: %v", err)
	}
}
