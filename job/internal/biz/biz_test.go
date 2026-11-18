package biz

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

func TestLeaseOptionIsClamped(t *testing.T) {
	uc := NewRunnerUsecase(nil, nil, nil, nil, nil, nil, nil, nil)
	if uc.lease != DefaultLease {
		t.Fatalf("default lease = %s", uc.lease)
	}
	for in, want := range map[time.Duration]time.Duration{
		0: DefaultLease, -time.Second: DefaultLease, time.Second: MinLease, MinLease: MinLease, 5 * time.Minute: 5 * time.Minute,
	} {
		uc.WithOptions(RunnerOptions{Lease: in})
		if uc.lease != want {
			t.Errorf("lease %s → %s, want %s", in, uc.lease, want)
		}
	}
}

func TestNewLeaseToken(t *testing.T) {
	a, err := newLeaseToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := newLeaseToken()
	// 64 字符：job_execution.lease_token 的列宽。
	if len(a) != 64 || a == b {
		t.Fatalf("tokens %q %q", a, b)
	}
}

func TestKnownResultsAndHostUpdates(t *testing.T) {
	known := []Host{{Address: "10.0.0.1"}, {Address: "10.0.0.2"}, {Address: "2001:db8::1"}}
	in := []jobcontract.HostResult{
		{Address: "10.0.0.2", OK: 1, Failed: 1},
		{Address: "10.9.9.9", OK: 9},                 // 不属于本次执行
		{Address: "10.0.0.1", OK: 2, Changed: -5},    // 负数计数归零
		{Address: "", OK: 1},                         // 空地址
		{Address: "10.0.0.2", OK: 4, Unreachable: 0}, // 同一地址后者覆盖前者，位置不变
	}
	got := knownResults(in, known)
	want := []jobcontract.HostResult{{Address: "10.0.0.2", OK: 4}, {Address: "10.0.0.1", OK: 2, Changed: -5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("knownResults = %+v, want %+v", got, want)
	}
	updates := hostUpdates(got)
	wantUpdates := []HostUpdate{
		{Address: "10.0.0.2", Status: jobcontract.HostOK, OK: 4},
		{Address: "10.0.0.1", Status: jobcontract.HostOK, OK: 2},
	}
	if !reflect.DeepEqual(updates, wantUpdates) {
		t.Fatalf("hostUpdates = %+v, want %+v", updates, wantUpdates)
	}
	// 节点谎报不存在的主机成功，不能把失败的执行变成成功：终态只按本次执行的主机算。
	lied := knownResults([]jobcontract.HostResult{{Address: "10.9.9.9", OK: 1}}, known)
	if status := jobcontract.FinalStatus(jobcontract.OutcomeExited, 2, lied); status != jobcontract.StatusFailed {
		t.Fatalf("status = %d, want failed", status)
	}
}

func TestResultOf(t *testing.T) {
	cases := map[string]error{
		ResultOK:           nil,
		ResultUnauthorized: ErrUnauthorized,
		ResultDisabled:     fmt.Errorf("claim: %w", ErrRunnerDisabled),
		ResultLeaseLost:    ErrLeaseLost,
		ResultInvalid:      fmt.Errorf("%w: too many chunks", ErrInvalidRequest),
		ResultError:        errors.New("connection refused"),
	}
	for want, err := range cases {
		if got := resultOf(err); got != want {
			t.Errorf("resultOf(%v) = %q, want %q", err, got, want)
		}
	}
	// 执行不存在按租约失效处理；其它仓储错误带上操作名透传。
	if err := progressError("report", ErrNotFound); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("not found → %v", err)
	}
	inner := errors.New("deadlock detected")
	if err := progressError("finish", inner); !errors.Is(err, inner) || errors.Is(err, ErrLeaseLost) {
		t.Fatalf("repo error → %v", err)
	}
}

func TestValidateSpec(t *testing.T) {
	valid := func() *jobcontract.Spec {
		return &jobcontract.Spec{
			ProjectID: "018f0000-0000-7000-8000-0000000000d1", CredentialID: "018f0000-0000-7000-8000-0000000000c1",
			RepoURL: "git@git.example.com:ops/infra.git", Ref: "v1.2.3", Playbook: "site.yaml",
			ExtraVars: map[string]any{"version": "1"},
		}
	}
	spec := valid()
	if reason := validateSpec(spec); reason != "" {
		t.Fatalf("valid spec rejected: %s", reason)
	}
	// 未设置的选项被补为缺省值（任务里下发的是补齐后的选项）。
	if spec.Options.Forks != jobcontract.DefaultForks || spec.Options.TimeoutS != jobcontract.DefaultTimeoutS || spec.Options.Port != jobcontract.DefaultPort {
		t.Fatalf("options not normalized: %+v", spec.Options)
	}
	for name, mutate := range map[string]func(s *jobcontract.Spec){
		"repo":        func(s *jobcontract.Spec) { s.RepoURL = "/srv/git/repo" },
		"ref":         func(s *jobcontract.Spec) { s.Ref = "a..b" },
		"playbook":    func(s *jobcontract.Spec) { s.Playbook = "site.txt" },
		"credential":  func(s *jobcontract.Spec) { s.CredentialID = "1; DROP TABLE job_credential" },
		"project":     func(s *jobcontract.Spec) { s.ProjectID = "" },
		"var name":    func(s *jobcontract.Spec) { s.ExtraVars["inventory_hostname"] = "x" },
		"timeout":     func(s *jobcontract.Spec) { s.Options.TimeoutS = 5 },
		"many params": func(s *jobcontract.Spec) { s.ExtraVars = manyVars(jobcontract.MaxParams + 1) },
	} {
		s := valid()
		mutate(s)
		if validateSpec(s) == "" {
			t.Errorf("%s: spec accepted", name)
		}
	}
}

func manyVars(n int) map[string]any {
	out := make(map[string]any, n)
	for i := range n {
		out[fmt.Sprintf("v%d", i)] = i
	}
	return out
}
