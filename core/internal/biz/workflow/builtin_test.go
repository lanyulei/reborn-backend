package biz

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// adminsStub 系统管理员查询桩。
type adminsStub []string

func (a adminsStub) AdminUserIDs(context.Context) ([]string, error) { return slices.Clone(a), nil }

// 「系统管理员」审批人规则：取当前全部可用的超管；不排除发起人（只有一名管理员的环境里，他要能审批
// 自己提交的申请）；与其它规则取并集；没有接入查询时解析为空，按缺省拦住。
func TestEngine_SuperAdminRule(t *testing.T) {
	t.Parallel()
	const adminRule = `{"kind":"superAdmin"}`
	taskOf := func(rules string) string {
		return `{"id":"a","type":"userTask","name":"审批","assignee":{"rules":[` + rules + `]}}`
	}
	dir := &dirStub{leaders: map[string][]string{"d-team": {"lead"}}, inactive: map[string]bool{"off": true}}
	pendingOf := func(t *testing.T, rules string, initiator string, admins Admins) []string {
		t.Helper()
		s := &State{Instance: Instance{ID: "inst", Initiator: initiator, InitiatorDept: "d-team", Status: InstanceRunning}}
		if err := NewExecution(mustGraph(t, linearGraph(taskOf(rules))), s, dir, time.Now()).WithAdmins(admins).Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		if len(s.Tasks) != 1 {
			t.Fatalf("tasks = %d, want 1", len(s.Tasks))
		}
		return pendingUsers(s.Tasks[0])
	}

	// 可用的超管都是审批人；发起人本人也是超管时不被排除；停用的不算。
	if got := pendingOf(t, adminRule, "boss", adminsStub{"boss", "off", "root"}); !slices.Equal(got, []string{"boss", "root"}) {
		t.Fatalf("assignees = %v, want [boss root]", got)
	}
	// 没有可用的超管：缺省拦住，不会跳过。
	s := &State{Instance: Instance{ID: "inst", Initiator: "u1", Status: InstanceRunning}}
	err := NewExecution(mustGraph(t, linearGraph(taskOf(adminRule))), s, dir, time.Now()).WithAdmins(nil).Start(context.Background())
	wantNoAssignee(t, err, `"审批"`, "must not be skipped")
	// 与部门负责人规则取并集：有负责人时负责人和超管都能审，没有负责人时还有超管。
	union := `{"kind":"deptLeader","level":1},` + adminRule
	if got := pendingOf(t, union, "u1", adminsStub{"root"}); !slices.Equal(got, []string{"lead", "root"}) {
		t.Fatalf("assignees = %v, want [lead root]", got)
	}

	// 模拟运行同样把超管列为处理人。
	res, err := Simulate(context.Background(), mustGraph(t, linearGraph(taskOf(adminRule))), Instance{Initiator: "u1"}, dir, nil, adminsStub{"root"})
	if err != nil || len(res.Steps) != 3 || !slices.Equal(res.Steps[1].Assignees, []string{"root"}) || res.Status != InstanceCompleted {
		t.Fatalf("Simulate() = %+v, %v; want root approving and the flow completed", res.Steps, err)
	}
}

// createRecorder 记录创建请求的仓储桩。
type createRecorder struct{ DefinitionWriter }

func (r *createRecorder) CreateDefinition(_ context.Context, d *Definition) error { return nil }

// 流程标识的内置前缀保留给系统：用户自建流程不得以它开头（内置流程按标识幂等预置时才不会撞上用户的数据）。
func TestDefinition_BuiltinKeyPrefixReserved(t *testing.T) {
	t.Parallel()
	uc := NewDefinitionUsecase(nil, &createRecorder{}, nil, nil, nil, nil)
	ctx := context.Background()
	for _, key := range []string{"builtin_approval_admin", "builtin_", "builtin_x"} {
		if _, err := uc.Create(ctx, &Definition{Key: key, Name: "占用内置前缀"}); !errors.Is(err, ErrDefinitionInvalid) {
			t.Errorf("Create(%q) error = %v, want ErrDefinitionInvalid", key, err)
		}
	}
	for _, key := range []string{"builtinx", "my_builtin_flow", "approval_admin"} {
		if _, err := uc.Create(ctx, &Definition{Key: key, Name: "自建流程"}); err != nil {
			t.Errorf("Create(%q) error = %v, want nil", key, err)
		}
	}
}
