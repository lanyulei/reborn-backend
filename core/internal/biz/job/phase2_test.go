package biz

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

func TestMatchRisk(t *testing.T) {
	t.Parallel()
	rules := []*RiskRule{
		{ID: "w", Name: "warn", Pattern: `chmod\s+-R`, Action: jc.RiskWarn, Enabled: true},
		{ID: "b", Name: "block", Pattern: `rm\s+-rf\s+/(\s|$)`, Action: jc.RiskBlock, Enabled: true, Description: "d"},
		{ID: "off", Name: "disabled", Pattern: `reboot`, Action: jc.RiskBlock},
		{ID: "bad", Name: "broken", Pattern: `([`, Action: jc.RiskBlock, Enabled: true},
	}
	hits := matchRisk(rules, "set -e\nchmod -R 777 /data\nrm -rf /\nreboot\n")
	if len(hits) != 2 || hits[0].RuleID != "b" || hits[1].RuleID != "w" || hits[0].Match != "rm -rf /" || hits[0].Description != "d" {
		t.Fatalf("matchRisk = %+v", hits)
	}
	if got := matchRisk(rules, "rm -rf /tmp/build"); len(got) != 0 {
		t.Fatalf("safe command hit: %+v", got)
	}
	long := matchRisk([]*RiskRule{{ID: "l", Name: "l", Pattern: `x+`, Action: 3, Enabled: true}}, strings.Repeat("x", 500))
	if len([]rune(long[0].Match)) > riskMatchLen+1 {
		t.Fatalf("match snippet not truncated: %d runes", len([]rune(long[0].Match)))
	}
	for name, r := range map[string]*RiskRule{
		"empty match": {Name: "n", Pattern: `a*`, Action: 1}, "bad regex": {Name: "n", Pattern: `(`, Action: 1},
		"bad action": {Name: "n", Pattern: `a`, Action: 9}, "blank": {Name: " ", Pattern: `a`, Action: 1},
	} {
		if err := normalizeRule(r); !errorsIs(err, ErrRiskRuleInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestApprovalReasons(t *testing.T) {
	t.Parallel()
	policy := &Policy{ProtectedEnvs: []string{"prod", "dr"}}
	hosts := []Host{{Env: "test"}, {Env: "prod"}, {Env: "prod"}, {Env: ""}}
	approve := []jc.RiskHit{{Name: "重启", Action: jc.RiskApprove}, {Name: "提醒", Action: jc.RiskWarn}}
	cases := []struct {
		name     string
		kind     int32
		tpl      bool
		check    bool
		adhocAll bool
		hits     []jc.RiskHit
		want     []string
	}{
		{"protected env", jc.KindPlaybook, false, false, false, nil, []string{"目标主机含受保护环境（prod）"}},
		{"check mode skips approval", jc.KindPlaybook, true, true, false, nil, nil},
		{"template + env", jc.KindPlaybook, true, false, false, nil, []string{reasonTemplate, "目标主机含受保护环境（prod）"}},
		{"adhoc risk + policy + env", jc.KindCommand, false, false, true, approve, []string{reasonAdhoc, "命中高危命令规则（重启）", "目标主机含受保护环境（prod）"}},
		{"adhoc ignores check flag", jc.KindScript, false, true, false, approve, []string{"命中高危命令规则（重启）", "目标主机含受保护环境（prod）"}},
	}
	for _, c := range cases {
		p := *policy
		p.AdhocApproval = c.adhocAll
		got := approvalReasons(&p, c.kind, c.tpl, c.check, hosts, c.hits)
		if strings.Join(got, "|") != strings.Join(c.want, "|") {
			t.Errorf("%s: reasons = %v, want %v", c.name, got, c.want)
		}
	}
	if got := approvalReasons(&Policy{}, jc.KindPlaybook, false, false, hosts, nil); got != nil {
		t.Errorf("no policy, no reasons expected: %v", got)
	}
	if (&Policy{MaxHosts: 20}).HostLimit() != 20 || (&Policy{}).HostLimit() != jc.MaxHosts || hostCap(&Policy{MaxHosts: 50}, 100) != 50*jc.MaxSlices {
		t.Error("host limits are wrong")
	}
}

func TestCronParsing(t *testing.T) {
	t.Parallel()
	sched, loc, err := parseCron("30 3 * * 1-5", "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-11-20 是周五：之后的下一次是周一 03:30（上海时间）。
	from := time.Date(2026, 11, 20, 4, 0, 0, 0, loc)
	next, err := nextAfter(sched, loc, from)
	if err != nil || !next.Equal(time.Date(2026, 11, 23, 3, 30, 0, 0, loc)) {
		t.Fatalf("next = %v, %v", next, err)
	}
	for name, c := range map[string][2]string{
		"six fields": {"0 30 3 * * *", "UTC"}, "descriptor": {"@every 1s", "UTC"}, "garbage": {"hello", "UTC"},
		"bad tz": {"* * * * *", "Mars/Olympus"}, "empty tz": {"* * * * *", ""}, "local tz": {"* * * * *", "Local"},
	} {
		if _, _, err := parseCron(c[0], c[1]); !errorsIs(err, ErrScheduleInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	never, loc2, err := parseCron("0 0 30 2 *", "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nextAfter(never, loc2, from); !errorsIs(err, ErrScheduleInvalid) {
		t.Errorf("Feb 30 must never fire: %v", err)
	}
}

type fakeQueue struct {
	due  []*Schedule
	now  time.Time
	runs []ScheduleRun
}

func (q *fakeQueue) Due(context.Context, int) ([]*Schedule, time.Time, error) {
	return q.due, q.now, nil
}

func (q *fakeQueue) Advance(_ context.Context, _ string, run ScheduleRun) (bool, error) {
	q.runs = append(q.runs, run)
	return true, nil
}

// 定时触发：准点的提交执行；错过的按策略跳过；模板被拒（停用）记为失败并照常推进。
func TestScheduleRun(t *testing.T) {
	t.Parallel()
	loc := time.UTC
	now := time.Date(2026, 11, 18, 10, 0, 30, 0, loc)
	onTime := time.Date(2026, 11, 18, 10, 0, 0, 0, loc)
	stale := time.Date(2026, 11, 18, 7, 0, 0, 0, loc)
	tpl := &Template{ID: "t", Name: "x", Enabled: false}
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	q := &fakeQueue{now: now, due: []*Schedule{
		{ID: "018f3e1a-0000-7000-8000-000000000001", TemplateID: "t", Cron: "0 * * * *", Timezone: "UTC", Misfire: jc.MisfireSkip, NextTime: &onTime, Owner: "u"},
		{ID: "018f3e1a-0000-7000-8000-000000000002", TemplateID: "t", Cron: "0 * * * *", Timezone: "UTC", Misfire: jc.MisfireSkip, NextTime: &stale, Owner: "u"},
		{ID: "018f3e1a-0000-7000-8000-000000000003", TemplateID: "t", Cron: "0 * * * *", Timezone: "UTC", Misfire: jc.MisfireRunOnce, NextTime: &stale, Owner: "u"},
		{ID: "018f3e1a-0000-7000-8000-000000000004", TemplateID: "t", Cron: "not a cron", Timezone: "UTC", Misfire: jc.MisfireSkip, NextTime: &onTime, Owner: "u"},
	}}
	uc := NewScheduleUsecase(nil, q, fakeTemplates{tpl}, execs, noNames{})
	n, err := uc.Run(context.Background())
	if err != nil || n != 4 || len(q.runs) != 4 {
		t.Fatalf("Run() = %d, %v (%d runs)", n, err, len(q.runs))
	}
	nextHour := time.Date(2026, 11, 18, 11, 0, 0, 0, loc)
	// 准点：尝试提交（模板停用 → 被拒，记为失败），推进到下一个整点。
	if r := q.runs[0]; !r.Fired.Equal(onTime) || !r.Next.Equal(nextHour) || !strings.Contains(r.Error, "disabled") || r.ExecutionID != "" {
		t.Errorf("on-time run = %+v", r)
	}
	// 错过 + 跳过：不提交，直接推进。
	if r := q.runs[1]; r.Error != "错过触发时间，已跳过" || !r.Next.Equal(nextHour) {
		t.Errorf("missed/skip run = %+v", r)
	}
	// 错过 + 补跑：提交一次（同样被拒），只推进到下一个整点——不会把错过的三次都补上。
	if r := q.runs[2]; !strings.Contains(r.Error, "disabled") || !r.Next.Equal(nextHour) {
		t.Errorf("missed/run-once run = %+v", r)
	}
	// 坏表达式：记原因，一天后再看。
	if r := q.runs[3]; r.Error == "" || !r.Next.Equal(now.Add(24*time.Hour)) {
		t.Errorf("broken cron run = %+v", r)
	}
}

type denyRuns struct{}

func (denyRuns) CanRun(context.Context, string) (bool, error) { return false, nil }

func (denyRuns) CanViewAll(context.Context, string) (bool, error) { return false, nil }

// 没有「执行作业」权限的人：不能保存定时作业 / 触发器；已有的定时作业在负责人失去权限后触发即被拒。
func TestOwnerNeedsRunPermission(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tpl := &Template{ID: "t", Name: "x", Enabled: true}
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: denyRuns{}})
	if _, err := execs.Submit(ctx, "u", SubmitInput{TemplateID: "t", Trigger: jc.TriggerSchedule}); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("scheduled submit without run permission: %v", err)
	}
	if _, err := execs.Submit(ctx, "u", SubmitInput{TemplateID: "t", Trigger: jc.TriggerAlert}); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("alert submit without run permission: %v", err)
	}
	sched := NewScheduleUsecase(nil, nil, fakeTemplates{tpl}, execs, noNames{})
	if _, err := sched.Create(ctx, "u", &Schedule{Name: "s", TemplateID: "t", Cron: "* * * * *", Timezone: "UTC", Misfire: 1}); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("create schedule without run permission: %v", err)
	}
	if _, err := sched.Update(ctx, "u", &Schedule{ID: "s", Enabled: true}, 1); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("update schedule without run permission: %v", err)
	}
	trig := &TriggerUsecase{execs: execs}
	if _, _, err := trig.Create(ctx, "u", &Trigger{Name: "t", Type: jc.TriggerTypeAPI}); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("create trigger without run permission: %v", err)
	}
	if _, err := trig.Update(ctx, "u", &Trigger{ID: "t", Enabled: true}, 1); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("update trigger without run permission: %v", err)
	}
	if _, err := trig.ResetToken(ctx, "u", "t"); !errorsIs(err, ErrRunForbidden) {
		t.Fatalf("reset trigger token without run permission: %v", err)
	}
	if !Rejected(ErrRunForbidden) {
		t.Fatal("a forbidden owner must be recorded as a failed firing, not retried forever")
	}
}

type resumeFires struct {
	existing  *TriggerFire
	completed *TriggerFire
}

func (f *resumeFires) List(context.Context, FireFilter, int64, int64) ([]*TriggerFire, int64, error) {
	return nil, 0, nil
}

func (f *resumeFires) Begin(context.Context, *TriggerFire, int32) (*TriggerFire, bool, error) {
	return f.existing, false, nil
}

func (f *resumeFires) Complete(_ context.Context, fire *TriggerFire) error {
	f.completed = fire
	return nil
}

// 登记之后、提交之前中断的触发记录：再次遇到同一事件时接着做完（这里提交被拒 → 落为失败），
// 而已经做完的记录原样返回、不再提交。
func TestFireResumesInterruptedRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tpl := &Template{ID: "t", Name: "x", Enabled: false} // 停用：提交会被业务规则拒绝
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	stuck := &TriggerFire{ID: "018f3e1a-0000-7000-8000-0000000000f1", TriggerID: "g", Ref: "a1", Result: jc.FireSubmitted}
	fires := &resumeFires{existing: stuck}
	uc := &TriggerUsecase{fires: fires, execs: execs}
	trg := &Trigger{ID: "g", Type: jc.TriggerTypeAlert, TemplateID: "t", Owner: "u"}
	out, err := uc.fire(ctx, trg, &TriggerFire{TriggerID: "g", Ref: "a1"}, nil, nil)
	if err != nil || out != stuck || fires.completed != stuck || stuck.Result != jc.FireFailed || !strings.Contains(stuck.Reason, "disabled") {
		t.Fatalf("interrupted fire not resumed: out=%+v completed=%+v err=%v", out, fires.completed, err)
	}
	done := &TriggerFire{ID: "f2", TriggerID: "g", Ref: "a2", Result: jc.FireSubmitted, ExecutionID: "e1"}
	fires.existing, fires.completed = done, nil
	if out, err := uc.fire(ctx, trg, &TriggerFire{TriggerID: "g", Ref: "a2"}, nil, nil); err != nil || out != done || fires.completed != nil {
		t.Fatalf("finished fire must be returned untouched: %+v, %v", out, err)
	}
}

func TestAlertMatching(t *testing.T) {
	t.Parallel()
	a := &AlertEvent{RuleID: "r1", Severity: 3, Labels: map[string]string{"job": "node", "instance": "10.0.0.5:9100"}}
	cases := []struct {
		name string
		m    jc.AlertMatch
		want bool
	}{
		{"rule", jc.AlertMatch{RuleIDs: []string{"r0", "r1"}}, true},
		{"other rule", jc.AlertMatch{RuleIDs: []string{"r0"}}, false},
		{"severity", jc.AlertMatch{RuleIDs: []string{"r1"}, Severities: []int32{4}}, false},
		{"labels", jc.AlertMatch{Labels: map[string]string{"job": "node"}}, true},
		{"label mismatch", jc.AlertMatch{Labels: map[string]string{"job": "node", "dc": "bj"}}, false},
	}
	for _, c := range cases {
		if got := alertMatches(c.m, a); got != c.want {
			t.Errorf("%s: %v", c.name, got)
		}
	}
	for in, want := range map[string]string{
		"10.0.0.5:9100": "10.0.0.5", "10.0.0.5": "10.0.0.5", "Web-1.example.com:9100": "web-1.example.com",
		"[2001:db8::1]:9100": "2001:db8::1", "2001:db8::1": "2001:db8::1",
	} {
		if got, ok := alertHost(in); !ok || got != want {
			t.Errorf("alertHost(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", "a b:9100", "-oProxyCommand=x"} {
		if _, ok := alertHost(bad); ok {
			t.Errorf("alertHost(%q) accepted", bad)
		}
	}
}

type fakeSecrets struct{ token TriggerToken }

func (f fakeSecrets) SetToken(context.Context, string, TriggerToken, string) error { return nil }
func (f fakeSecrets) Token(_ context.Context, id string) (TriggerToken, error) {
	if id != triggerID {
		return TriggerToken{}, ErrTriggerNotFound
	}
	return f.token, nil
}
func (f fakeSecrets) EnabledAlertTriggers(context.Context) ([]*Trigger, error) { return nil, nil }

type plainOpener struct{}

func (plainOpener) Open(sealed []byte) (string, error) {
	return strings.TrimPrefix(string(sealed), "sealed:"), nil
}

const triggerID = "018f3e1a-0000-7000-8000-0000000000aa"

// 开放接口认证：Bearer 令牌与流程 Webhook 签名；时间戳偏差、签名被篡改、不存在的触发器一律 401。
func TestTriggerAuthenticate(t *testing.T) {
	t.Parallel()
	token, hash, err := jc.NewTriggerToken()
	if err != nil || !strings.HasPrefix(token, jc.TriggerTokenPrefix) {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	uc := &TriggerUsecase{secrets: fakeSecrets{TriggerToken{Hash: hash, Sealed: []byte("sealed:" + token)}}, opener: plainOpener{}, now: func() time.Time { return now }}
	sign := func(ts int64, body string, key string) FireAuth {
		mac := hmac.New(sha256.New, []byte(key))
		mac.Write([]byte(strconv.FormatInt(ts, 10) + "." + body))
		return FireAuth{Timestamp: strconv.FormatInt(ts, 10), Signature: "sha256=" + hex.EncodeToString(mac.Sum(nil)), Body: []byte(body)}
	}
	ctx := context.Background()
	body := `{"jobId":"j1"}`
	good := []FireAuth{{Bearer: token}, sign(now.Unix(), body, token), sign(now.Unix()-200, body, token)}
	for i, a := range good {
		if err := uc.authenticate(ctx, triggerID, a); err != nil {
			t.Errorf("good auth %d rejected: %v", i, err)
		}
	}
	tampered := sign(now.Unix(), body, token)
	tampered.Body = []byte(`{"jobId":"j2"}`)
	bad := map[string]FireAuth{
		"wrong bearer": {Bearer: token + "x"}, "empty": {}, "stale timestamp": sign(now.Unix()-jc.SignatureSkew-5, body, token),
		"future timestamp": sign(now.Unix()+jc.SignatureSkew+5, body, token), "wrong key": sign(now.Unix(), body, "other"),
		"tampered body": tampered, "garbage timestamp": {Timestamp: "abc", Signature: "sha256=00"},
	}
	for name, a := range bad {
		if err := uc.authenticate(ctx, triggerID, a); !errorsIs(err, ErrTriggerUnauthorized) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if err := uc.authenticate(ctx, "018f3e1a-0000-7000-8000-0000000000bb", FireAuth{Bearer: token}); !errorsIs(err, ErrTriggerUnauthorized) {
		t.Errorf("unknown trigger must look like a bad token: %v", err)
	}
	// 没有密文（主密钥未配置）时只支持 Bearer。
	uc.secrets = fakeSecrets{TriggerToken{Hash: hash}}
	if err := uc.authenticate(ctx, triggerID, sign(now.Unix(), body, token)); !errorsIs(err, ErrTriggerUnauthorized) {
		t.Errorf("signature accepted without a sealed token: %v", err)
	}
	if err := uc.authenticate(ctx, triggerID, FireAuth{Bearer: token}); err != nil {
		t.Errorf("bearer must still work: %v", err)
	}
}

func TestNoticeAndPartialParams(t *testing.T) {
	t.Parallel()
	start := time.Unix(100, 0)
	uc := NewNotifyUsecase(nil, nil, nil, "https://ops.example.com/")
	n := uc.notice(&FinishedExecution{
		ID: "018f3e1a-0000-7000-8000-000000000001", Name: "发布", Status: jc.StatusPartial, Trigger: jc.TriggerSchedule,
		Creator: "u1", HostTotal: 3, HostOK: 2, HostFailed: 1, StartTime: &start, FinishTime: time.Unix(165, 0),
	})
	if n.Severity != 2 || n.Title != "作业部分失败：发布" || n.Payload["duration"] != "1m5s" || n.Payload["trigger_text"] != "定时作业" ||
		n.Payload["url"] != "https://ops.example.com/job/execution/018f3e1a-0000-7000-8000-000000000001" || n.Recipients[0] != "u1" ||
		n.DedupKey != "job:exec:018f3e1a000070008000000000000001:5:165" {
		t.Fatalf("notice = %+v", n)
	}
	if noticeSeverity(jc.StatusSuccess) != 1 || noticeSeverity(jc.StatusUnknown) != 3 || noticeSeverity(jc.StatusRejected) != 2 {
		t.Error("severities are wrong")
	}
	defs := []jc.Param{{Code: "version", Name: "v", Type: jc.ParamString, Required: true}, {Code: "n", Name: "n", Type: jc.ParamInt}}
	if got, err := checkPartialParams(defs, map[string]any{"n": float64(3)}); err != nil || got["n"] != int64(3) {
		t.Fatalf("partial params = %v, %v", got, err)
	}
	for name, v := range map[string]map[string]any{"wrong type": {"n": "x"}, "undefined": {"ansible_user": "root"}} {
		if _, err := checkPartialParams(defs, v); !errorsIs(err, ErrParamsInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

type testAllow struct{}

func (testAllow) CanRun(context.Context, string) (bool, error) { return true, nil }

func (testAllow) CanViewAll(context.Context, string) (bool, error) { return true, nil }

// 审查发现的几处加固：模板版本钉住、参数里的 NUL、Cron 时区前缀。
func TestHardening(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tpl := &Template{ID: "t", Name: "x", Enabled: true, Version: 5}
	execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	// 负责人确认的是 v4，模板已被改到 v5：拒绝，且属于「业务拒绝」（记为失败而不是无限重试）。
	_, err := execs.Submit(ctx, "u", SubmitInput{TemplateID: "t", TemplateVersion: 4, Trigger: jc.TriggerSchedule})
	if !errorsIs(err, ErrTemplateInvalid) || !strings.Contains(err.Error(), "v4") || !Rejected(err) {
		t.Fatalf("stale template version: %v", err)
	}
	defs := []jc.Param{{Code: "v", Name: "v", Type: jc.ParamString}}
	for _, fn := range []func([]jc.Param, map[string]any) (map[string]any, error){resolveParams, checkPartialParams} {
		if _, err := fn(defs, map[string]any{"v": "a\x00b"}); !errorsIs(err, ErrParamsInvalid) {
			t.Errorf("NUL in a param value accepted: %v", err)
		}
	}
	for _, expr := range []string{"CRON_TZ=UTC 0 3 * * *", "TZ=Asia/Tokyo 0 3 * * *"} {
		if _, _, err := parseCron(expr, "Asia/Shanghai"); !errorsIs(err, ErrScheduleInvalid) {
			t.Errorf("%q accepted", expr)
		}
	}
	if _, err := execs.Stats(ctx, 7, "Local"); !errorsIs(err, ErrScheduleInvalid) {
		t.Errorf("stats timezone Local accepted: %v", err)
	}
}

// 只有「执行」按钮权限的人只能看自己提交的执行；别人的按不存在处理。
func TestExecutionVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &fakeExecutions{exec: &Execution{ID: "e", Creator: "alice"}}
	own := NewExecutionUsecase(f, f, nil, nil, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: denyRuns{}})
	if err := own.RequireVisible(ctx, "alice", "e"); err != nil {
		t.Fatalf("creator cannot see own execution: %v", err)
	}
	if err := own.RequireVisible(ctx, "bob", "e"); !errorsIs(err, ErrExecutionNotFound) {
		t.Fatalf("someone else's execution visible: %v", err)
	}
	all := NewExecutionUsecase(f, f, nil, nil, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
	if err := all.RequireVisible(ctx, "bob", "e"); err != nil {
		t.Fatalf("view-all holder blocked: %v", err)
	}
}
