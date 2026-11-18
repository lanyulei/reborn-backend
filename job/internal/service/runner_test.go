package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

const (
	credID = "018f0000-0000-7000-8000-0000000000c1"
	projID = "018f0000-0000-7000-8000-0000000000d1"
)

// fixture 一套引擎测试环境：内存仓储 + 用例 + HTTP 服务。
type fixture struct {
	t      *testing.T
	store  *biztest.Store
	opener *biztest.Opener
	obs    *biztest.Observer
	uc     *biz.RunnerUsecase
	house  *biz.HousekeepUsecase
	srv    *httptest.Server
	logs   *bytes.Buffer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, store: biztest.NewStore(), opener: &biztest.Opener{}, obs: biztest.NewObserver(), logs: &bytes.Buffer{}}
	logger := slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.uc = biz.NewRunnerUsecase(f.store, f.store, f.store, f.store, f.store, f.opener, f.obs, logger)
	f.house = biz.NewHousekeepUsecase(f.store, f.obs, logger)
	mux := http.NewServeMux()
	for path, h := range NewRunnerService(f.uc, logger).Routes() {
		mux.HandleFunc(path, h)
	}
	f.srv = httptest.NewServer(httpx.LimitRequestBody(mux))
	t.Cleanup(f.srv.Close)
	f.store.AddCredential(credID, biz.Credential{
		Type: jobcontract.CredentialSSHKey, Username: "ops", Secret: biztest.Seal("PRIVATE-KEY-MATERIAL"),
		BecomeMethod: jobcontract.BecomeSudo, BecomeUser: "root", BecomeSecret: biztest.Seal("become-pass"),
	})
	f.store.AddProject(projID, biz.Project{AuthType: jobcontract.GitAuthToken, Username: "deploy", Secret: biztest.Seal("git-token-123")})
	return f
}

func newID(t *testing.T) string {
	t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// runner 登记一个节点并返回它的接入令牌。
func (f *fixture) runner(zone string, enabled bool) (id, token string) {
	f.t.Helper()
	id = newID(f.t)
	token, hash, err := jobcontract.NewRunnerToken(id)
	if err != nil {
		f.t.Fatal(err)
	}
	f.store.AddRunner(biz.Runner{ID: id, Zone: zone, Enabled: enabled, TokenHash: hash})
	return id, token
}

func defaultSpec() jobcontract.Spec {
	return jobcontract.Spec{
		ProjectID: projID, ProjectName: "infra", RepoURL: "https://git.example.com/ops/infra.git", Ref: "main",
		Playbook: "site.yml", CredentialID: credID, CredentialName: "ops-key",
		ExtraVars: map[string]any{"version": "1.2.3", "replicas": 3},
		Options:   jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, Become: true},
	}
}

// execution 建一条排队中的执行（两台主机），返回 ID。
func (f *fixture) execution(zone string, mutate func(spec *jobcontract.Spec, e *biztest.Execution)) string {
	f.t.Helper()
	spec := defaultSpec()
	e := biztest.Execution{
		ID: newID(f.t), TemplateID: newID(f.t), RunnerZone: zone,
		Hosts: []biztest.HostRow{
			{Host: biz.Host{Address: "10.0.0.1", Port: 22, CIID: newID(f.t), Name: "web-1", Env: "prod"}},
			{Host: biz.Host{Address: "10.0.0.2", Port: 2222, CIID: newID(f.t), Name: "web-2", Env: "prod"}},
		},
	}
	if mutate != nil {
		mutate(&spec, &e)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		f.t.Fatal(err)
	}
	if e.Spec == nil {
		e.Spec = raw
	}
	f.store.AddExecution(e)
	return e.ID
}

// post 发一个协议请求，返回状态码与应答体。
func (f *fixture) post(path, token string, body any) (int, []byte) {
	f.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			f.t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.srv.URL+path, rd)
	if err != nil {
		f.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return resp.StatusCode, out
}

// claim 领取并返回任务（nil = 没有任务）。
func (f *fixture) claim(token string) *jobcontract.Task {
	f.t.Helper()
	code, body := f.post(jobcontract.RunnerPathClaim, token, nil)
	if code != http.StatusOK {
		f.t.Fatalf("claim: status %d body %s", code, body)
	}
	var out jobcontract.ClaimResponse
	if err := json.Unmarshal(body, &out); err != nil {
		f.t.Fatalf("decode claim response: %v", err)
	}
	return out.Task
}

func (f *fixture) report(token string, req *jobcontract.ReportRequest) (int, jobcontract.ReportResponse) {
	f.t.Helper()
	code, body := f.post(jobcontract.RunnerPathReport, token, req)
	var out jobcontract.ReportResponse
	if code == http.StatusOK {
		if err := json.Unmarshal(body, &out); err != nil {
			f.t.Fatalf("decode report response: %v", err)
		}
	}
	return code, out
}

func chunks(seqs ...int64) []jobcontract.LogChunk {
	out := make([]jobcontract.LogChunk, 0, len(seqs))
	for _, s := range seqs {
		out = append(out, jobcontract.LogChunk{Seq: s, Content: "line " + strconv.FormatInt(s, 10) + "\n"})
	}
	return out
}

func TestAuth(t *testing.T) {
	f := newFixture(t)
	id, token := f.runner("", true)
	_, otherToken := f.runner("", true)
	unknownToken, _, _ := jobcontract.NewRunnerToken(newID(t))
	wrongSecret := id + "." + strings.Repeat("x", 43)
	crossed := id + "." + strings.SplitN(otherToken, ".", 2)[1]

	var unknownBody, wrongBody []byte
	for name, tok := range map[string]string{
		"missing": "", "malformed": "not-a-token", "unknown runner": unknownToken, "wrong secret": wrongSecret,
		"other runner's secret": crossed, "non-uuid id": strings.Repeat("z", 36) + "." + strings.Repeat("x", 43),
	} {
		for _, path := range []string{
			jobcontract.RunnerPathHeartbeat, jobcontract.RunnerPathClaim, jobcontract.RunnerPathReport, jobcontract.RunnerPathFinish,
		} {
			code, body := f.post(path, tok, struct{}{})
			if code != http.StatusUnauthorized {
				t.Fatalf("%s %s: status %d, want 401", name, path, code)
			}
			switch name {
			case "unknown runner":
				unknownBody = body
			case "wrong secret":
				wrongBody = body
			}
		}
	}
	if !bytes.Equal(unknownBody, wrongBody) {
		t.Fatalf("unknown id and wrong secret must be indistinguishable: %q vs %q", unknownBody, wrongBody)
	}
	// 合法令牌可用；Bearer 前缀不区分大小写。
	if code, _ := f.post(jobcontract.RunnerPathHeartbeat, token, jobcontract.HeartbeatRequest{}); code != http.StatusOK {
		t.Fatalf("valid token: status %d", code)
	}
	// 节点被删除后令牌立即失效。
	f.store.RemoveRunner(id)
	if code, _ := f.post(jobcontract.RunnerPathHeartbeat, token, jobcontract.HeartbeatRequest{}); code != http.StatusUnauthorized {
		t.Fatalf("deleted runner: status %d, want 401", code)
	}
	// 令牌不得出现在日志里。
	for _, secret := range []string{token, wrongSecret, strings.SplitN(token, ".", 2)[1]} {
		if strings.Contains(f.logs.String(), secret) {
			t.Fatalf("token leaked into logs")
		}
	}
	// 只接受 POST。
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, f.srv.URL+jobcontract.RunnerPathClaim, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: status %d, want 405", resp.StatusCode)
	}
}

func TestHeartbeat(t *testing.T) {
	f := newFixture(t)
	id, token := f.runner("", true)
	code, body := f.post(jobcontract.RunnerPathHeartbeat, token, jobcontract.HeartbeatRequest{
		AgentVersion: strings.Repeat("v", 50), Hostname: "runner-1\x00", Capacity: 4, Running: -1,
	})
	if code != http.StatusOK || !strings.Contains(string(body), `"enabled":true`) {
		t.Fatalf("heartbeat: %d %s", code, body)
	}
	hb := f.store.Heartbeats[id]
	if len(hb.AgentVersion) != 32 || hb.Hostname != "runner-1" || hb.Capacity != 4 || hb.Running != 0 {
		t.Fatalf("heartbeat not sanitized: %+v", hb)
	}
	if f.store.LastSeen[id].IsZero() {
		t.Fatal("last_seen not written")
	}
	// 停用的节点心跳照常，应答 enabled=false。
	f.store.SetRunnerEnabled(id, false)
	code, body = f.post(jobcontract.RunnerPathHeartbeat, token, jobcontract.HeartbeatRequest{})
	if code != http.StatusOK || !strings.Contains(string(body), `"enabled":false`) {
		t.Fatalf("disabled heartbeat: %d %s", code, body)
	}
	// 请求体不是 JSON → 400。
	if code, _ := f.post(jobcontract.RunnerPathHeartbeat, token, []byte("{not json")); code != http.StatusBadRequest {
		t.Fatalf("malformed body: status %d, want 400", code)
	}
}

func TestClaimOrderingAndZone(t *testing.T) {
	f := newFixture(t)
	_, tokA := f.runner("zone-a", true)
	_, tokNone := f.runner("", true)
	first := f.execution("zone-b", nil) // 只有 zone-b 的节点能领
	second := f.execution("", nil)
	third := f.execution("zone-a", nil)
	canceled := f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) { e.CancelRequested = true })

	// 无区域的节点：跳过 zone-b / zone-a 的，领到 second。
	if task := f.claim(tokNone); task == nil || task.ExecutionID != second {
		t.Fatalf("zoneless runner claimed %+v, want %s", task, second)
	}
	// zone-a 的节点：按提交顺序领到 third（second 已被领走）。
	if task := f.claim(tokA); task == nil || task.ExecutionID != third {
		t.Fatalf("zone-a runner claimed %+v, want %s", task, third)
	}
	// 再没有可领的：已请求取消的与别的区域的都不派发。
	if task := f.claim(tokA); task != nil {
		t.Fatalf("unexpected task %s", task.ExecutionID)
	}
	if task := f.claim(tokNone); task != nil {
		t.Fatalf("unexpected task %s", task.ExecutionID)
	}
	for _, id := range []string{first, canceled} {
		if e := f.store.Get(id); e.Status != jobcontract.StatusQueued {
			t.Fatalf("execution %s status %d, want queued", id, e.Status)
		}
	}
	if f.obs.Call("claim/ok") != 2 || f.obs.Call("claim/empty") != 2 || f.obs.Event(biz.EventClaimed) != 2 {
		t.Fatalf("metrics: %+v %+v", f.obs.Calls, f.obs.Events)
	}
}

func TestClaimBuildsTask(t *testing.T) {
	f := newFixture(t)
	runnerID, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	if task == nil || task.ExecutionID != id {
		t.Fatalf("task = %+v", task)
	}
	e := f.store.Get(id)
	if e.Status != jobcontract.StatusRunning || e.RunnerID != runnerID || !e.Started || e.LeaseToken != task.LeaseToken || len(task.LeaseToken) != 64 {
		t.Fatalf("execution after claim: %+v", e)
	}
	if task.LeaseSeconds != 60 || task.RepoURL != "https://git.example.com/ops/infra.git" || task.Ref != "main" || task.Playbook != "site.yml" {
		t.Fatalf("task basics: %+v", task)
	}
	if task.Git != (jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Username: "deploy", Secret: "git-token-123"}) {
		t.Fatalf("git auth: %+v", task.Git)
	}
	wantCred := jobcontract.TaskCredential{
		Type: jobcontract.CredentialSSHKey, Username: "ops", Secret: "PRIVATE-KEY-MATERIAL",
		BecomeMethod: "sudo", BecomeUser: "root", BecomeSecret: "become-pass",
	}
	if task.Credential != wantCred {
		t.Fatalf("credential: %+v", task.Credential)
	}
	if len(task.Hosts) != 2 || task.Hosts[0].Address != "10.0.0.1" || task.Hosts[1].Port != 2222 {
		t.Fatalf("hosts: %+v", task.Hosts)
	}
	if v := task.Hosts[0].Vars; v["cmdb_name"] != "web-1" || v["cmdb_env"] != "prod" || !idgen.IsUUID(v["cmdb_id"]) {
		t.Fatalf("host vars: %+v", v)
	}
	if task.ExtraVars["version"] != "1.2.3" || task.ExtraVars["replicas"] != float64(3) {
		t.Fatalf("extra vars: %+v", task.ExtraVars)
	}
	if task.Options != (jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, Become: true}) {
		t.Fatalf("options: %+v", task.Options)
	}
	// 凭据明文不得进日志。
	for _, secret := range []string{"PRIVATE-KEY-MATERIAL", "become-pass", "git-token-123", task.LeaseToken} {
		if strings.Contains(f.logs.String(), secret) {
			t.Fatalf("secret %q leaked into logs", secret)
		}
	}
}

func TestClaimWithoutBecomeOmitsBecomeSecret(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	f.execution("", func(spec *jobcontract.Spec, _ *biztest.Execution) { spec.Options.Become = false })
	task := f.claim(token)
	if task == nil || task.Credential.BecomeSecret != "" || task.Credential.BecomeMethod != "" {
		t.Fatalf("become info must not be dispatched when become is off: %+v", task)
	}
}

func TestClaimDisabledRunner(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", false)
	id := f.execution("", nil)
	if code, _ := f.post(jobcontract.RunnerPathClaim, token, nil); code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusQueued {
		t.Fatalf("disabled runner must not claim: status %d", e.Status)
	}
}

func TestClaimFailsUndispatchableAndTakesNext(t *testing.T) {
	cases := map[string]struct {
		setup  func(f *fixture)
		mutate func(spec *jobcontract.Spec, e *biztest.Execution)
		want   string
	}{
		"credential deleted": {setup: func(f *fixture) { f.store.RemoveCredential(credID) }, want: "凭据已被删除"},
		"project deleted":    {setup: func(f *fixture) { f.store.RemoveProject(projID) }, want: "项目已被删除"},
		"master key missing": {setup: func(f *fixture) { f.opener.Fail = true }, want: "解密失败"},
		"bad spec json":      {mutate: func(_ *jobcontract.Spec, e *biztest.Execution) { e.Spec = []byte(`{"options": "oops"}`) }, want: "无法解析"},
		"bad playbook":       {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) { spec.Playbook = "../../etc/passwd.yml" }, want: "Playbook"},
		"bad ref":            {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) { spec.Ref = "--upload-pack=x" }, want: "分支"},
		"bad repo":           {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) { spec.RepoURL = "file:///etc" }, want: "仓库地址"},
		"bad var name": {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) {
			spec.ExtraVars = map[string]any{"ansible_user": "root"}
		}, want: "参数名"},
		"bad options":      {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) { spec.Options.Forks = 1000 }, want: "执行选项"},
		"bad host address": {mutate: func(_ *jobcontract.Spec, e *biztest.Execution) { e.Hosts[0].Address = "-oProxyCommand=x" }, want: "主机地址"},
		"no hosts":         {mutate: func(_ *jobcontract.Spec, e *biztest.Execution) { e.Hosts = nil }, want: "没有目标主机"},
		"auth mismatch": {mutate: func(spec *jobcontract.Spec, _ *biztest.Execution) {
			spec.RepoURL = "git@git.example.com:ops/infra.git"
		}, want: "不匹配"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			_, token := f.runner("", true)
			bad := f.execution("", tc.mutate)
			if tc.setup != nil {
				tc.setup(f)
			}
			// 排在后面的一条正常执行：凭据 / 项目 / 主密钥类故障下它同样失败，其余情况下应被领到。
			var good string
			if tc.setup == nil {
				good = f.execution("", nil)
			}
			task := f.claim(token)
			e := f.store.Get(bad)
			if e.Status != jobcontract.StatusFailed || !e.Finished || !strings.Contains(e.Error, tc.want) {
				t.Fatalf("bad execution: status %d finished %v error %q, want failed with %q", e.Status, e.Finished, e.Error, tc.want)
			}
			for _, secret := range []string{"PRIVATE-KEY-MATERIAL", "become-pass", "git-token-123"} {
				if strings.Contains(e.Error, secret) {
					t.Fatalf("error leaks secret: %q", e.Error)
				}
			}
			if good == "" {
				if task != nil {
					t.Fatalf("unexpected task %+v", task)
				}
				return
			}
			if task == nil || task.ExecutionID != good {
				t.Fatalf("claimed %+v, want the next execution %s", task, good)
			}
			if f.obs.Event(biz.EventDispatchFailed) != 1 {
				t.Fatalf("dispatch_failed metric = %d", f.obs.Event(biz.EventDispatchFailed))
			}
		})
	}
}

func TestClaimReleasesOnTransientFailure(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	f.store.MaterialErr = errors.New("connection reset")
	if code, body := f.post(jobcontract.RunnerPathClaim, token, nil); code != http.StatusInternalServerError || strings.Contains(string(body), "connection reset") {
		t.Fatalf("status %d body %s, want opaque 500", code, body)
	}
	e := f.store.Get(id)
	if e.Status != jobcontract.StatusQueued || e.RunnerID != "" || e.LeaseToken != "" || e.Started {
		t.Fatalf("execution must go back to the queue untouched: %+v", e)
	}
	f.store.MaterialErr = nil
	if task := f.claim(token); task == nil || task.ExecutionID != id {
		t.Fatalf("retry claim = %+v", task)
	}
}

func TestReportAppendsLogsAndExtendsLease(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	req := &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, CommitID: strings.Repeat("a", 40), Chunks: chunks(1, 2)}

	f.store.Advance(30 * time.Second)
	code, resp := f.report(token, req)
	if code != http.StatusOK || resp.AckSeq != 2 || resp.Cancel {
		t.Fatalf("report: %d %+v", code, resp)
	}
	e := f.store.Get(id)
	if e.LogSeq != 2 || len(e.Logs) != 2 || e.LogBytes != int64(len(e.Logs[1])+len(e.Logs[2])) || e.CommitID != strings.Repeat("a", 40) {
		t.Fatalf("after report: %+v", e)
	}
	// 续租：从领取起 80 秒（已超过最初的 60 秒租约）清扫不应判它失联。
	f.store.Advance(50 * time.Second)
	if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 0 {
		t.Fatalf("sweep = %d, %v; lease must have been extended", n, err)
	}
	// 重传已确认的块是空操作；提交号一旦写入不再改。
	req.CommitID = strings.Repeat("b", 40)
	code, resp = f.report(token, req)
	if code != http.StatusOK || resp.AckSeq != 2 {
		t.Fatalf("retransmit: %d %+v", code, resp)
	}
	if e := f.store.Get(id); len(e.Logs) != 2 || e.CommitID != strings.Repeat("a", 40) {
		t.Fatalf("retransmit changed state: %+v", e)
	}
	// 没有日志的上报只续租；取消请求经应答转达。
	f.store.Update(id, func(e *biztest.Execution) { e.CancelRequested = true })
	code, resp = f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken})
	if code != http.StatusOK || !resp.Cancel || resp.AckSeq != 2 {
		t.Fatalf("keepalive: %d %+v", code, resp)
	}
}

func TestReportGapHandling(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	send := func(seqs ...int64) int64 {
		t.Helper()
		code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(seqs...)})
		if code != http.StatusOK {
			t.Fatalf("report %v: status %d", seqs, code)
		}
		return resp.AckSeq
	}
	// 1、2 连续；4 之前有空洞：4、5 都不收。
	if ack := send(1, 2, 4, 5); ack != 2 {
		t.Fatalf("ack = %d, want 2", ack)
	}
	// 开头就有空洞：全不收。
	if ack := send(5, 6); ack != 2 {
		t.Fatalf("ack = %d, want 2", ack)
	}
	// 重传从 ack+1 起，夹带已确认的块。
	if ack := send(1, 2, 3, 4, 5); ack != 5 {
		t.Fatalf("ack = %d, want 5", ack)
	}
	e := f.store.Get(id)
	if len(e.Logs) != 5 || e.LogSeq != 5 {
		t.Fatalf("logs = %d seq = %d", len(e.Logs), e.LogSeq)
	}
	for s := int64(1); s <= 5; s++ {
		if e.Logs[s] != chunks(s)[0].Content {
			t.Fatalf("chunk %d = %q", s, e.Logs[s])
		}
	}
}

func TestReportLimits(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	base := jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken}

	tooMany := base
	for i := int64(1); i <= jobcontract.MaxLogChunks+1; i++ {
		tooMany.Chunks = append(tooMany.Chunks, jobcontract.LogChunk{Seq: i, Content: "x"})
	}
	if code, _ := f.report(token, &tooMany); code != http.StatusBadRequest {
		t.Fatalf("too many chunks: status %d, want 400", code)
	}
	tooBig := base
	tooBig.Chunks = []jobcontract.LogChunk{{Seq: 1, Content: strings.Repeat("x", jobcontract.MaxLogChunkBytes+1)}}
	if code, _ := f.report(token, &tooBig); code != http.StatusBadRequest {
		t.Fatalf("oversized chunk: status %d, want 400", code)
	}
	if e := f.store.Get(id); e.LogSeq != 0 || len(e.Logs) != 0 {
		t.Fatalf("rejected report must not write anything: %+v", e)
	}
	for name, bad := range map[string]jobcontract.ReportRequest{
		"bad execution id": {ExecutionID: "nope", LeaseToken: task.LeaseToken},
		"empty lease":      {ExecutionID: id},
	} {
		if code, _ := f.report(token, &bad); code != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400", name, code)
		}
	}
	// 恰好在上限上的块可以接收；非法 UTF-8 与 NUL 被清洗。
	edge := base
	edge.Chunks = []jobcontract.LogChunk{
		{Seq: 1, Content: strings.Repeat("x", jobcontract.MaxLogChunkBytes)},
		{Seq: 2, Content: "ok\x00\xff!"},
	}
	if code, resp := f.report(token, &edge); code != http.StatusOK || resp.AckSeq != 2 {
		t.Fatalf("edge chunk: %d %+v", code, resp)
	}
	if got := f.store.Get(id).Logs[2]; got != "ok�!" {
		t.Fatalf("sanitized chunk = %q", got)
	}
	// 请求体超过 8MiB → 413（只超出一点：服务端读到上限即止，剩余部分留在套接字缓冲里，客户端不会写失败）。
	huge := append([]byte(`{"execution_id":"`+id+`","lease_token":"`+task.LeaseToken+`","commit_id":"`), bytes.Repeat([]byte("a"), 8<<20)...)
	huge = append(huge, `"}`...)
	if code, _ := f.post(jobcontract.RunnerPathReport, token, huge); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("huge body: status %d, want 413", code)
	}
}

func TestReportTruncation(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	big := strings.Repeat("x", jobcontract.MaxLogChunkBytes)
	total := jobcontract.MaxLogBytes / jobcontract.MaxLogChunkBytes // 恰好填满上限的块数
	seq := int64(0)
	send := func(n int, content string) int64 {
		t.Helper()
		req := &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken}
		for range n {
			seq++
			req.Chunks = append(req.Chunks, jobcontract.LogChunk{Seq: seq, Content: content})
		}
		code, resp := f.report(token, req)
		if code != http.StatusOK {
			t.Fatalf("report: status %d", code)
		}
		return resp.AckSeq
	}
	for sent := 0; sent < total; sent += jobcontract.MaxLogChunks {
		send(min(jobcontract.MaxLogChunks, total-sent), big)
	}
	e := f.store.Get(id)
	if e.LogBytes != jobcontract.MaxLogBytes || e.LogTruncated || len(e.Logs) != total {
		t.Fatalf("at the limit: bytes %d truncated %v chunks %d", e.LogBytes, e.LogTruncated, len(e.Logs))
	}
	// 超出上限：内容丢弃、标记截断，序号照常确认。
	if ack := send(2, "overflow\n"); ack != int64(total)+2 {
		t.Fatalf("ack = %d, want %d", ack, total+2)
	}
	e = f.store.Get(id)
	if !e.LogTruncated || e.LogBytes != jobcontract.MaxLogBytes || len(e.Logs) != total || e.LogSeq != int64(total)+2 {
		t.Fatalf("after overflow: bytes %d truncated %v chunks %d seq %d", e.LogBytes, e.LogTruncated, len(e.Logs), e.LogSeq)
	}
	if f.obs.Event(biz.EventLogTruncated) != 1 {
		t.Fatalf("log_truncated metric = %d, want exactly 1", f.obs.Event(biz.EventLogTruncated))
	}
	// 截断之后执行照常结束。
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{
		ExecutionID: id, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited,
	}); code != http.StatusOK {
		t.Fatalf("finish after truncation: status %d", code)
	}
}

func TestLeaseFencing(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	_, otherToken := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	wrong := strings.Repeat("0", 64)

	report := func(tok, lease string) int {
		code, _ := f.report(tok, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: lease, Chunks: chunks(1)})
		return code
	}
	finish := func(tok, lease string) int {
		code, _ := f.post(jobcontract.RunnerPathFinish, tok, jobcontract.FinishRequest{
			ExecutionID: id, LeaseToken: lease, Outcome: jobcontract.OutcomeExited,
		})
		return code
	}
	// 领取令牌不对 → 409。
	if code := report(token, wrong); code != http.StatusConflict {
		t.Fatalf("report with wrong lease: %d, want 409", code)
	}
	if code := finish(token, wrong); code != http.StatusConflict {
		t.Fatalf("finish with wrong lease: %d, want 409", code)
	}
	// 别的节点拿着正确的领取令牌也不行（令牌泄露不足以冒充）。
	if code := report(otherToken, task.LeaseToken); code != http.StatusConflict {
		t.Fatalf("report from another runner: %d, want 409", code)
	}
	if code := finish(otherToken, task.LeaseToken); code != http.StatusConflict {
		t.Fatalf("finish from another runner: %d, want 409", code)
	}
	// 不存在的执行 → 409（节点丢弃）。
	if code, _ := f.report(token, &jobcontract.ReportRequest{ExecutionID: newID(t), LeaseToken: task.LeaseToken}); code != http.StatusConflict {
		t.Fatalf("unknown execution: %d, want 409", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusRunning || e.LogSeq != 0 {
		t.Fatalf("fenced requests must not change anything: %+v", e)
	}
	// 排队中的执行（尚未领取）不接受上报。
	queued := f.execution("zone-x", nil)
	if code, _ := f.report(token, &jobcontract.ReportRequest{ExecutionID: queued, LeaseToken: wrong}); code != http.StatusConflict {
		t.Fatalf("report on queued execution: %d, want 409", code)
	}
	// 正常结束之后，再上报 → 409。
	if code := finish(token, task.LeaseToken); code != http.StatusOK {
		t.Fatalf("finish: %d", code)
	}
	if code := report(token, task.LeaseToken); code != http.StatusConflict {
		t.Fatalf("report after finish: %d, want 409", code)
	}
	if f.obs.Call("report/lease_lost") == 0 || f.obs.Call("finish/lease_lost") == 0 {
		t.Fatalf("lease_lost metrics missing: %+v", f.obs.Calls)
	}
}

func TestSweepAndReviveFromUnknown(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	if _, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(1)}); resp.AckSeq != 1 {
		t.Fatalf("ack = %d", resp.AckSeq)
	}
	// 租约未到期：清扫不动它。
	f.store.Advance(59 * time.Second)
	if n, _ := f.house.SweepLeases(context.Background()); n != 0 {
		t.Fatalf("swept %d before expiry", n)
	}
	// 到期：置为结果未知，绝不重新排队。
	f.store.Advance(2 * time.Second)
	if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 1 {
		t.Fatalf("sweep = %d, %v", n, err)
	}
	e := f.store.Get(id)
	if e.Status != jobcontract.StatusUnknown || !e.Finished || e.Error != biz.LeaseExpiredReason || e.RunnerID == "" {
		t.Fatalf("after sweep: %+v", e)
	}
	if n, _ := f.house.SweepLeases(context.Background()); n != 0 {
		t.Fatal("sweep must be idempotent")
	}
	if task := f.claim(token); task != nil {
		t.Fatalf("unknown execution must never be re-dispatched, got %s", task.ExecutionID)
	}
	// 节点回来了：同一领取令牌的上报把它恢复为执行中，日志从确认序号之后接续。
	code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(1, 2)})
	if code != http.StatusOK || resp.AckSeq != 2 {
		t.Fatalf("report after expiry: %d %+v", code, resp)
	}
	e = f.store.Get(id)
	if e.Status != jobcontract.StatusRunning || e.Finished || e.Error != "" || len(e.Logs) != 2 {
		t.Fatalf("after revive: %+v", e)
	}
	if f.obs.Event(biz.EventRevived) != 1 || f.obs.Event(biz.EventLeaseExpired) != 1 {
		t.Fatalf("metrics: %+v", f.obs.Events)
	}
	// 再次失联后直接报结束：结果未知的执行落为真实终态。
	f.store.Advance(2 * time.Minute)
	if n, _ := f.house.SweepLeases(context.Background()); n != 1 {
		t.Fatal("second sweep")
	}
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{
		ExecutionID: id, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited,
		Hosts: []jobcontract.HostResult{{Address: "10.0.0.1", OK: 1}, {Address: "10.0.0.2", OK: 1}},
	}); code != http.StatusOK {
		t.Fatalf("finish from unknown: %d", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusSuccess || e.Error != "" {
		t.Fatalf("after finish from unknown: status %d error %q", e.Status, e.Error)
	}
}

func TestReviveBlockedByExclusiveSuccessor(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	tpl := newID(t)
	id := f.execution("", func(_ *jobcontract.Spec, e *biztest.Execution) { e.TemplateID, e.Exclusive = tpl, true })
	task := f.claim(token)
	f.store.Advance(2 * time.Minute)
	if n, _ := f.house.SweepLeases(context.Background()); n != 1 {
		t.Fatal("sweep")
	}
	// 结果未知期间，同一模板又提交了一条互斥执行（占着互斥位）。
	f.execution("zone-x", func(_ *jobcontract.Spec, e *biztest.Execution) { e.TemplateID, e.Exclusive = tpl, true })
	code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(1)})
	if code != http.StatusOK || resp.AckSeq != 1 {
		t.Fatalf("report: %d %+v", code, resp)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusUnknown || len(e.Logs) != 1 {
		t.Fatalf("must stay unknown but keep accepting logs: %+v", e)
	}
	if f.obs.Event(biz.EventRevived) != 0 {
		t.Fatal("revived metric must not count a blocked revive")
	}
	// 未能恢复的执行保持结果未知、原因不清；节点继续上报，应答始终成功且不让节点取消。
	for seq := int64(2); seq <= 4; seq++ {
		f.store.Advance(2 * time.Minute) // 每次都已超过租约
		code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken, Chunks: chunks(seq)})
		if code != http.StatusOK || resp.AckSeq != seq || resp.Cancel {
			t.Fatalf("report %d while blocked: %d %+v", seq, code, resp)
		}
		// 清扫只看执行中的行：结果未知的执行不会被再次处理（原因、结束标记都不变）。
		if n, err := f.house.SweepLeases(context.Background()); err != nil || n != 0 {
			t.Fatalf("sweep touched an unknown execution: %d, %v", n, err)
		}
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusUnknown || e.Error != biz.LeaseExpiredReason || !e.Finished || len(e.Logs) != 4 {
		t.Fatalf("blocked execution changed: %+v", e)
	}
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{
		ExecutionID: id, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited, ExitCode: 2,
	}); code != http.StatusOK {
		t.Fatalf("finish: %d", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusFailed {
		t.Fatalf("status %d, want failed", e.Status)
	}
}

func TestFinish(t *testing.T) {
	cases := map[string]struct {
		req        jobcontract.FinishRequest
		status     int32
		ok, fl, un int32
		hostStatus [2]int32
	}{
		"success": {
			req: jobcontract.FinishRequest{Outcome: jobcontract.OutcomeExited, Hosts: []jobcontract.HostResult{
				{Address: "10.0.0.1", OK: 3, Changed: 1}, {Address: "10.0.0.2", OK: 2},
			}},
			status: jobcontract.StatusSuccess, ok: 2, hostStatus: [2]int32{jobcontract.HostChanged, jobcontract.HostOK},
		},
		"partial": {
			req: jobcontract.FinishRequest{Outcome: jobcontract.OutcomeExited, ExitCode: 2, Hosts: []jobcontract.HostResult{
				{Address: "10.0.0.1", OK: 3}, {Address: "10.0.0.2", OK: 1, Failed: 1},
			}},
			status: jobcontract.StatusPartial, ok: 1, fl: 1, hostStatus: [2]int32{jobcontract.HostOK, jobcontract.HostFailed},
		},
		"failed with unreachable, unreported host stays pending": {
			req: jobcontract.FinishRequest{Outcome: jobcontract.OutcomeExited, ExitCode: 4, Hosts: []jobcontract.HostResult{
				{Address: "10.0.0.2", Unreachable: 1}, {Address: "10.9.9.9", OK: 1},
			}},
			status: jobcontract.StatusFailed, un: 1, hostStatus: [2]int32{jobcontract.HostPending, jobcontract.HostUnreachable},
		},
		"canceled": {
			req:    jobcontract.FinishRequest{Outcome: jobcontract.OutcomeCanceled, ExitCode: -1},
			status: jobcontract.StatusCanceled,
		},
		"timeout": {
			req:    jobcontract.FinishRequest{Outcome: jobcontract.OutcomeTimeout, ExitCode: -1, Error: "执行超时"},
			status: jobcontract.StatusTimeout,
		},
		"error": {
			req:    jobcontract.FinishRequest{Outcome: jobcontract.OutcomeError, Error: strings.Repeat("错", jobcontract.MaxErrorLen+50)},
			status: jobcontract.StatusFailed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			_, token := f.runner("", true)
			id := f.execution("", nil)
			task := f.claim(token)
			tc.req.ExecutionID, tc.req.LeaseToken, tc.req.CommitID = id, task.LeaseToken, strings.Repeat("c", 40)
			if code, body := f.post(jobcontract.RunnerPathFinish, token, tc.req); code != http.StatusOK {
				t.Fatalf("finish: %d %s", code, body)
			}
			e := f.store.Get(id)
			if e.Status != tc.status || !e.Finished || e.CommitID != strings.Repeat("c", 40) {
				t.Fatalf("status %d finished %v commit %q, want status %d", e.Status, e.Finished, e.CommitID, tc.status)
			}
			if e.HostOK != tc.ok || e.HostFailed != tc.fl || e.HostUnreachable != tc.un {
				t.Fatalf("host counters ok=%d failed=%d unreachable=%d", e.HostOK, e.HostFailed, e.HostUnreachable)
			}
			if got := [2]int32{e.Hosts[0].Status, e.Hosts[1].Status}; got != tc.hostStatus {
				t.Fatalf("host statuses %v, want %v", got, tc.hostStatus)
			}
			if n := len([]rune(e.Error)); n > jobcontract.MaxErrorLen {
				t.Fatalf("error not truncated: %d chars", n)
			}
			if tc.req.Error != "" && e.Error == "" {
				t.Fatal("error not stored")
			}
		})
	}
}

func TestFinishIdempotent(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	first := jobcontract.FinishRequest{
		ExecutionID: id, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited,
		Hosts: []jobcontract.HostResult{{Address: "10.0.0.1", OK: 1}, {Address: "10.0.0.2", OK: 1}},
	}
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, first); code != http.StatusOK {
		t.Fatalf("finish: %d", code)
	}
	// 重复的结束请求（哪怕内容不同）是空操作，应答 200。
	second := first
	second.Outcome, second.Error, second.Hosts = jobcontract.OutcomeError, "boom", nil
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, second); code != http.StatusOK {
		t.Fatalf("duplicate finish: %d, want 200", code)
	}
	e := f.store.Get(id)
	if e.Status != jobcontract.StatusSuccess || e.Error != "" || e.HostOK != 2 {
		t.Fatalf("duplicate finish changed the result: %+v", e)
	}
	if f.obs.Event("finished_success") != 1 || f.obs.Event("finished_failed") != 0 {
		t.Fatalf("finish events: %+v", f.obs.Events)
	}
	// 未知的结束方式 → 400。
	bad := first
	bad.Outcome = "exploded"
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, bad); code != http.StatusBadRequest {
		t.Fatalf("unknown outcome: %d, want 400", code)
	}
}

func TestDisabledRunnerMayStillReportAndFinish(t *testing.T) {
	f := newFixture(t)
	id, token := f.runner("", true)
	exec := f.execution("", nil)
	task := f.claim(token)
	f.store.SetRunnerEnabled(id, false)
	if code, resp := f.report(token, &jobcontract.ReportRequest{ExecutionID: exec, LeaseToken: task.LeaseToken, Chunks: chunks(1)}); code != http.StatusOK || resp.AckSeq != 1 {
		t.Fatalf("report from disabled runner: %d %+v", code, resp)
	}
	if code, _ := f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{
		ExecutionID: exec, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited,
	}); code != http.StatusOK {
		t.Fatalf("finish from disabled runner: %d", code)
	}
}

func TestInternalErrorsAreOpaque(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	task := f.claim(token)
	f.store.Err = errors.New(`pq: relation "job_execution" does not exist`)
	// 数据库故障时鉴权本身就失败：500，而不是 401（否则节点会误以为令牌被吊销）。
	code, body := f.post(jobcontract.RunnerPathReport, token, jobcontract.ReportRequest{ExecutionID: id, LeaseToken: task.LeaseToken})
	if code != http.StatusInternalServerError || strings.Contains(string(body), "job_execution") {
		t.Fatalf("status %d body %s, want opaque 500", code, body)
	}
	if !strings.Contains(f.logs.String(), "does not exist") {
		t.Fatal("internal error must be logged")
	}
}

func TestLogRetention(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	done := f.execution("", nil)
	task := f.claim(token)
	f.report(token, &jobcontract.ReportRequest{ExecutionID: done, LeaseToken: task.LeaseToken, Chunks: chunks(1, 2)})
	f.post(jobcontract.RunnerPathFinish, token, jobcontract.FinishRequest{ExecutionID: done, LeaseToken: task.LeaseToken, Outcome: jobcontract.OutcomeExited})
	running := f.execution("", nil)
	task2 := f.claim(token)
	f.report(token, &jobcontract.ReportRequest{ExecutionID: running, LeaseToken: task2.LeaseToken, Chunks: chunks(1)})

	ctx := context.Background()
	f.store.Advance(91 * 24 * time.Hour)
	// 保留期 0 = 永久保留。
	f.house.WithOptions(biz.HousekeepOptions{LogRetention: 0})
	if n, err := f.house.PurgeLogs(ctx); err != nil || n != 0 {
		t.Fatalf("retention 0 purged %d, %v", n, err)
	}
	f.house.WithOptions(biz.HousekeepOptions{LogRetention: biz.DefaultLogRetention})
	if n, err := f.house.PurgeLogs(ctx); err != nil || n != 2 {
		t.Fatalf("purged %d, %v; want 2", n, err)
	}
	// 未结束的执行（这里其实已租约过期但尚未清扫，仍是执行中）的日志不删。
	if e := f.store.Get(running); len(e.Logs) != 1 {
		t.Fatalf("logs of an unfinished execution were purged")
	}
	if e := f.store.Get(done); len(e.Logs) != 0 {
		t.Fatalf("logs left: %d", len(e.Logs))
	}
}
