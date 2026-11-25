package biz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// hostSourceStub 目标主机来源：err 非空时 Resolve 直接返回它（模拟负责人失去数据范围、地址字段失效、
// 数据库故障等）；否则返回 set。
type hostSourceStub struct {
	err error
	set ResourceSet
}

func (hostSourceStub) Check(context.Context, string, jc.Target) error { return nil }

func (s hostSourceStub) Resolve(context.Context, string, jc.Target, int) (ResourceSet, error) {
	return s.set, s.err
}

func (hostSourceStub) ModelNames(context.Context, string, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// recordingFires 触发记录：记下每次登记的记录，供断言跳过的原因。
type recordingFires struct{ fires []*TriggerFire }

func (f *recordingFires) List(context.Context, FireFilter, int64, int64) ([]*TriggerFire, int64, error) {
	return nil, 0, nil
}

func (f *recordingFires) Begin(_ context.Context, fire *TriggerFire, _ int32) (*TriggerFire, bool, error) {
	f.fires = append(f.fires, fire)
	return fire, true, nil
}

func (f *recordingFires) Complete(context.Context, *TriggerFire) error { return nil }

// 配置了 host_label 的告警触发器：解析目标主机被业务规则拒绝（负责人被停用后失去数据范围、地址字段失效等）
// 时，触发记录写明被拒的原因——不能一律写成误导性的「主机不在目标之内」。数据库等故障仍按故障处理
// （本轮报错、不登记，下一轮重试）；主机确实不在目标之内时原因不变。
func TestAlertHostRejectedRecordsReason(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	tpl := &Template{ID: "t", Name: "x", Enabled: true, Target: jc.Target{ModelCode: "host", Mode: jc.TargetModeFilter, AddressField: "ip"}}
	trg := &Trigger{
		ID: "g1", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u",
		Alert: jc.AlertMatch{RuleIDs: []string{"r1"}, HostLabel: "instance"}, UpdateTime: now.Add(-time.Hour),
	}
	event := &AlertEvent{
		ID: "018f3e1a-0000-7000-8000-000000000001", RuleID: "r1", RuleName: "cpu", Severity: 3,
		Labels: map[string]string{"instance": "10.0.0.1:9100"}, FirstFireTime: now.Add(-time.Minute),
	}
	sensitive := TargetInvalid("address field %q is sensitive and cannot be used as the host address", "ip")
	cases := []struct {
		name   string
		source hostSourceStub
		// wantErr：数据库等故障——不登记触发记录，本轮报错。
		wantErr     bool
		wantReason  string
		wantSkipped bool
	}{
		{
			name: "owner lost data scope", source: hostSourceStub{err: ErrTargetForbidden}, wantSkipped: true,
			wantReason: "target hosts are outside your CMDB data scope",
		},
		{name: "address field unusable", source: hostSourceStub{err: sensitive}, wantSkipped: true, wantReason: Message(sensitive)},
		{name: "database down", source: hostSourceStub{err: errors.New("db down")}, wantErr: true},
		{name: "host not among targets", source: hostSourceStub{set: ResourceSet{Items: []Resource{
			{ID: "ci-9", Name: "web-9", Env: "prod", Address: "10.0.0.9"},
		}}}, wantSkipped: true, wantReason: "告警标签 instance 的主机不在作业模板的目标主机之内"},
	}
	for _, tc := range cases {
		execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
		fires := &recordingFires{}
		uc := NewTriggerUsecase(
			TriggerDeps{Secrets: &alertSecrets{triggers: []*Trigger{trg}}, Fires: fires, Alerts: &alertFeedFake{events: []*AlertEvent{event}}},
			fakeTemplates{tpl}, NewTargetUsecase(tc.source), execs, noNames{},
		)
		uc.now = func() time.Time { return now }

		_, err := uc.RunAlerts(context.Background())

		switch {
		case tc.wantErr:
			if err == nil || len(fires.fires) != 0 {
				t.Errorf("%s: err = %v, fires = %d; want the failure reported and nothing recorded", tc.name, err, len(fires.fires))
			}
			continue
		case err != nil || len(fires.fires) != 1:
			t.Fatalf("%s: RunAlerts() = %v, fires = %d; want one record", tc.name, err, len(fires.fires))
		}
		f := fires.fires[0]
		if f.Result != jc.FireSkipped || f.ExecutionID != "" || f.Reason != tc.wantReason || !strings.Contains(f.Summary, "10.0.0.1:9100") {
			t.Errorf("%s: fire = %+v, want skipped with reason %q", tc.name, f, tc.wantReason)
		}
		// 已登记为跳过：同一事件不再重复判断。
		if _, err := uc.RunAlerts(context.Background()); err != nil || len(fires.fires) != 1 {
			t.Errorf("%s: second round = %v, fires = %d; want no new record", tc.name, err, len(fires.fires))
		}
	}
}
