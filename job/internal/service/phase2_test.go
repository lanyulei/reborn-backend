package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// adhocExecution 建一条排队中的快速执行（没有项目、没有模板），返回 ID。
func (f *fixture) adhocExecution(kind int32, adhoc *jobcontract.Adhoc, mutate func(spec *jobcontract.Spec)) string {
	f.t.Helper()
	spec := jobcontract.Spec{
		Kind: kind, Adhoc: adhoc, CredentialID: credID, CredentialName: "ops-key",
		Options: jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, BatchSize: 1, MaxFailPercent: 30},
	}
	if mutate != nil {
		mutate(&spec)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		f.t.Fatal(err)
	}
	id := newID(f.t)
	f.store.AddExecution(biztest.Execution{
		ID: id, Spec: raw,
		Hosts: []biztest.HostRow{{Host: biz.Host{Address: "10.0.0.1", Port: 22, CIID: newID(f.t), Name: "web-1", Env: "prod"}}},
	})
	return id
}

func fileAdhoc(content []byte) *jobcontract.Adhoc {
	sum := sha256.Sum256(content)
	return &jobcontract.Adhoc{FileName: "app.conf", FileSize: int64(len(content)), FileSHA256: hex.EncodeToString(sum[:]), Dest: "/etc/app", Mode: "0644"}
}

func TestClaimQuickExecution(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	// 快速执行没有项目：项目表是空的也照常派发。
	f.store.RemoveProject(projID)
	content := []byte("key = value\n")
	cases := []struct {
		kind    int32
		adhoc   *jobcontract.Adhoc
		payload []byte
	}{
		{jobcontract.KindCommand, &jobcontract.Adhoc{Command: "uptime && echo {{ x }}"}, nil},
		{jobcontract.KindScript, &jobcontract.Adhoc{Script: "#!/bin/sh\nid\n", Interpreter: "sh", ScriptArgs: "-v", ScriptName: "巡检"}, nil},
		{jobcontract.KindFile, fileAdhoc(content), content},
	}
	for _, tc := range cases {
		id := f.adhocExecution(tc.kind, tc.adhoc, nil)
		if tc.payload != nil {
			f.store.SetPayload(id, tc.payload)
		}
		task := f.claim(token)
		if task == nil || task.ExecutionID != id || task.Kind != tc.kind {
			t.Fatalf("kind %d: task %+v", tc.kind, task)
		}
		if task.Adhoc == nil || *task.Adhoc != *tc.adhoc || string(task.FileContent) != string(tc.payload) {
			t.Fatalf("kind %d: adhoc %+v content %q", tc.kind, task.Adhoc, task.FileContent)
		}
		if task.RepoURL != "" || task.Ref != "" || task.Playbook != "" || task.Git != (jobcontract.GitAuth{}) || len(task.ExtraVars) != 0 {
			t.Fatalf("kind %d: repository fields must stay empty: %+v", tc.kind, task)
		}
		if task.Credential.Secret != "PRIVATE-KEY-MATERIAL" || len(task.Hosts) != 1 || task.Options.BatchSize != 1 || task.Options.MaxFailPercent != 30 {
			t.Fatalf("kind %d: credential / hosts / options: %+v", tc.kind, task)
		}
		if e := f.store.Get(id); e.Status != jobcontract.StatusRunning {
			t.Fatalf("kind %d: status %d", tc.kind, e.Status)
		}
	}
	// 一期的记录没有 kind 字段：按作业模板处理（此时项目已删除 → 失败）。
	legacy := f.execution("", nil)
	if task := f.claim(token); task != nil {
		t.Fatalf("unexpected task %+v", task)
	}
	if e := f.store.Get(legacy); e.Status != jobcontract.StatusFailed || !strings.Contains(e.Error, "项目已被删除") {
		t.Fatalf("legacy playbook execution: %+v", e)
	}
}

func TestClaimFailsBadQuickExecution(t *testing.T) {
	content := []byte("key = value\n")
	cases := map[string]struct {
		kind    int32
		adhoc   *jobcontract.Adhoc
		payload []byte
		mutate  func(spec *jobcontract.Spec)
		setup   func(f *fixture)
		want    string
	}{
		"no adhoc":         {kind: jobcontract.KindCommand, want: "缺少快速执行的内容"},
		"empty command":    {kind: jobcontract.KindCommand, adhoc: &jobcontract.Adhoc{Command: " "}, want: "命令"},
		"bad interpreter":  {kind: jobcontract.KindScript, adhoc: &jobcontract.Adhoc{Script: "id", Interpreter: "perl"}, want: "脚本"},
		"bad dest":         {kind: jobcontract.KindFile, adhoc: &jobcontract.Adhoc{FileName: "a", Dest: "/tmp/../etc", Mode: "0644"}, want: "目标目录"},
		"setuid mode":      {kind: jobcontract.KindFile, adhoc: &jobcontract.Adhoc{FileName: "a", Dest: "/tmp", Mode: "4755"}, want: "权限"},
		"unknown kind":     {kind: 7, adhoc: &jobcontract.Adhoc{Command: "id"}, want: "作业类型"},
		"extra vars":       {kind: jobcontract.KindCommand, adhoc: &jobcontract.Adhoc{Command: "id"}, mutate: func(s *jobcontract.Spec) { s.ExtraVars = map[string]any{"v": 1} }, want: "执行参数"},
		"bad batch":        {kind: jobcontract.KindCommand, adhoc: &jobcontract.Adhoc{Command: "id"}, mutate: func(s *jobcontract.Spec) { s.Options.MaxFailPercent = 200 }, want: "执行选项"},
		"payload missing":  {kind: jobcontract.KindFile, adhoc: fileAdhoc(content), want: "文件内容不存在"},
		"payload size":     {kind: jobcontract.KindFile, adhoc: fileAdhoc(content), payload: content[:5], want: "不一致"},
		"payload checksum": {kind: jobcontract.KindFile, adhoc: fileAdhoc(content), payload: []byte("key = VALUE\n"), want: "不一致"},
		"credential gone":  {kind: jobcontract.KindCommand, adhoc: &jobcontract.Adhoc{Command: "id"}, setup: func(f *fixture) { f.store.RemoveCredential(credID) }, want: "凭据已被删除"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			_, token := f.runner("", true)
			id := f.adhocExecution(tc.kind, tc.adhoc, tc.mutate)
			if tc.payload != nil {
				f.store.SetPayload(id, tc.payload)
			}
			if tc.setup != nil {
				tc.setup(f)
			}
			if task := f.claim(token); task != nil {
				t.Fatalf("dispatched %+v", task)
			}
			e := f.store.Get(id)
			if e.Status != jobcontract.StatusFailed || !e.Finished || !strings.Contains(e.Error, tc.want) {
				t.Fatalf("status %d error %q, want failed with %q", e.Status, e.Error, tc.want)
			}
		})
	}
}

// 4MiB 的文件：领取应答（base64 后约 5.6MiB）完整送达。
func TestClaimLargeFilePayload(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	content := make([]byte, jobcontract.MaxFileBytes)
	for i := range content {
		content[i] = byte(i * 7)
	}
	id := f.adhocExecution(jobcontract.KindFile, fileAdhoc(content), nil)
	f.store.SetPayload(id, content)
	code, body := f.post(jobcontract.RunnerPathClaim, token, nil)
	if code != http.StatusOK || len(body) < 5<<20 {
		t.Fatalf("claim: status %d, %d bytes", code, len(body))
	}
	var out jobcontract.ClaimResponse
	if err := json.Unmarshal(body, &out); err != nil || out.Task == nil || string(out.Task.FileContent) != string(content) {
		t.Fatalf("payload did not survive the round trip: %v", err)
	}
	// 文件内容不进引擎日志。
	if strings.Contains(f.logs.String(), out.Task.Adhoc.FileSHA256) || f.logs.Len() > 64<<10 {
		t.Fatalf("engine log grew to %d bytes", f.logs.Len())
	}
}

// 待审批（9）与审批未通过（10）的执行：引擎不领取、不清扫、不接受上报。
func TestPendingAndRejectedAreUntouched(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	pending := f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) { e.Status = jobcontract.StatusPending })
	rejected := f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) {
		e.Status, e.Finished = jobcontract.StatusRejected, true
	})
	queued := f.execution("", nil)

	if task := f.claim(token); task == nil || task.ExecutionID != queued {
		t.Fatalf("claimed %+v, want the queued execution", task)
	}
	if task := f.claim(token); task != nil {
		t.Fatalf("claimed %s: pending / rejected executions must never be dispatched", task.ExecutionID)
	}
	f.store.Advance(24 * time.Hour)
	if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("sweep = %d, %v; only the running execution may expire", n, err)
	}
	for id, want := range map[string]int32{pending: jobcontract.StatusPending, rejected: jobcontract.StatusRejected} {
		e := f.store.Get(id)
		if e.Status != want || e.RunnerID != "" || e.LeaseToken != "" || e.Error != "" {
			t.Fatalf("execution %s changed: %+v", id, e)
		}
		if code, _ := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: "x"}); code != http.StatusConflict {
			t.Fatalf("report on status %d: %d, want 409", want, code)
		}
		if code, _ := f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{
			ExecutionID: id, LeaseToken: "x", Outcome: jobcontract.OutcomeExited,
		}); code != http.StatusConflict {
			t.Fatalf("finish on status %d: %d, want 409", want, code)
		}
	}
}

// 互斥位：结果未知期间同一模板有了一条待审批（9）的执行，同样占着位——不恢复。
func TestReviveBlockedByPendingSuccessor(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	tpl := newID(t)
	id := f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) { e.TemplateID, e.Exclusive = tpl, true })
	task := f.claim(token)
	f.store.Advance(2 * time.Minute)
	if n, _ := f.house.SweepLeases(context.Background()); n != 1 {
		t.Fatal("sweep")
	}
	f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) {
		e.TemplateID, e.Exclusive, e.Status = tpl, true, jobcontract.StatusPending
	})
	code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(1)})
	if code != http.StatusOK || resp.AckSeq != 1 || resp.Cancel {
		t.Fatalf("report: %d %+v", code, resp)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusUnknown || e.Error != biz.LeaseExpiredReason || len(e.Logs) != 1 {
		t.Fatalf("must stay unknown: %+v", e)
	}
}
