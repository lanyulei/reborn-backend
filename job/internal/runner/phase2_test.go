package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/job/internal/biztest"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// adhocTask 一个快速执行的任务（两台主机、私钥以外的密码凭据、不提权）。
func adhocTask(kind int32, adhoc jobcontract.Adhoc) *jobcontract.Task {
	return &jobcontract.Task{
		ExecutionID: "018f0000-0000-7000-8000-000000000001", LeaseToken: "lease-token-1", LeaseSeconds: 60,
		Kind: kind, Adhoc: &adhoc,
		Hosts:      []jobcontract.TaskHost{{Address: "10.0.0.1", Port: 22}, {Address: "10.0.0.2", Port: 22}},
		Credential: jobcontract.TaskCredential{Type: jobcontract.CredentialSSHPassword, Username: "ops", Secret: "login-PASSWORD-1"},
		ExtraVars:  map[string]any{},
		Options:    jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22},
	}
}

func fileAdhoc(content []byte) jobcontract.Adhoc {
	sum := sha256.Sum256(content)
	return jobcontract.Adhoc{
		FileName: "app.conf", FileSize: int64(len(content)), FileSHA256: hex.EncodeToString(sum[:]), Dest: "/etc/app/conf.d", Mode: "0640",
	}
}

// decodePlay 读回生成的 Playbook：恰一个 play，返回它与它的任务列表。
func decodePlay(t *testing.T, raw []byte) (map[string]any, []any) {
	t.Helper()
	doc := decodeVars(t, []byte(`{"plays": `+strings.TrimSuffix(string(raw), "\n")+"}\n"))
	plays, ok := doc["plays"].([]any)
	if !ok || len(plays) != 1 {
		t.Fatalf("want exactly one play:\n%s", raw)
	}
	play := plays[0].(map[string]any)
	if play["hosts"] != "all" || play["gather_facts"] != false {
		t.Fatalf("play header: %#v", play)
	}
	return play, play["tasks"].([]any)
}

// 注入尝试：模板语法、YAML 结构字符、引号、换行——都必须原样成为模块参数里的一个字符串。
const nasty = `echo "{{ lookup('pipe','id') }}" {% raw %} # ' ] } , : - | > & * ! ` + "`id`" + ` $(id)` + "\n" + `second: line "x"` + "\t\u0085\u2028end"

func TestBuildAdhocPlaybookCommand(t *testing.T) {
	task := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: nasty})
	raw, err := buildAdhocPlaybook(task, "/work/job")
	if err != nil {
		t.Fatal(err)
	}
	_, tasks := decodePlay(t, raw)
	want := map[string]any{
		"name": "执行命令", "register": "job_result",
		"ansible.builtin.shell": map[string]any{"cmd": unsafeOf(nasty)},
	}
	if len(tasks) != 3 || !reflect.DeepEqual(tasks[0], want) {
		t.Fatalf("tasks:\n got %#v\nwant %#v", tasks[0], want)
	}
	// 其后两个任务只打印登记的输出，表达式由平台写死。
	for i, v := range []string{"job_result.stdout_lines", "job_result.stderr_lines"} {
		dbg := tasks[i+1].(map[string]any)
		if !reflect.DeepEqual(dbg["ansible.builtin.debug"], map[string]any{"var": v}) || !strings.HasPrefix(dbg["when"].(string), v+" is defined") {
			t.Fatalf("output task %d: %#v", i, dbg)
		}
	}
	// 整个文件只有一行：命令里的换行与结构字符没有逃出字符串。
	if n, u := strings.Count(string(raw), "\n"), strings.Count(string(raw), "!unsafe"); n != 1 || u != 1 {
		t.Fatalf("playbook structure altered by the command (newlines %d, tags %d):\n%s", n, u, raw)
	}
}

func TestBuildAdhocPlaybookScript(t *testing.T) {
	args := `--name "{{ lookup('pipe','id') }}" 'a b' ; rm -rf / # ] }`
	task := adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "echo hi\n", Interpreter: jobcontract.InterpreterPython, ScriptArgs: "  " + args + " "})
	raw, err := buildAdhocPlaybook(task, "/work/job")
	if err != nil {
		t.Fatal(err)
	}
	_, tasks := decodePlay(t, raw)
	want := map[string]any{
		"name": "执行脚本", "register": "job_result",
		"ansible.builtin.script": map[string]any{"cmd": unsafeOf("/work/job/script " + args), "executable": "python3"},
	}
	if len(tasks) != 3 || !reflect.DeepEqual(tasks[0], want) {
		t.Fatalf("tasks:\n got %#v\nwant %#v", tasks[0], want)
	}
	// 脚本内容不进 Playbook。
	if strings.Contains(string(raw), "echo hi") {
		t.Fatalf("script content leaked into the playbook:\n%s", raw)
	}
	// 没有参数：cmd 就是脚本路径。
	task.Adhoc.ScriptArgs = ""
	raw, _ = buildAdhocPlaybook(task, "/work/job")
	_, tasks = decodePlay(t, raw)
	if got := tasks[0].(map[string]any)["ansible.builtin.script"].(map[string]any)["cmd"]; got != unsafeOf("/work/job/script") {
		t.Fatalf("cmd without args = %#v", got)
	}
}

func TestBuildAdhocPlaybookFile(t *testing.T) {
	task := adhocTask(jobcontract.KindFile, fileAdhoc([]byte("k = v\n")))
	raw, err := buildAdhocPlaybook(task, "/work/job")
	if err != nil {
		t.Fatal(err)
	}
	_, tasks := decodePlay(t, raw)
	want := []any{
		map[string]any{"name": "确保目标目录存在", "ansible.builtin.file": map[string]any{"path": unsafeOf("/etc/app/conf.d"), "state": "directory"}},
		map[string]any{"name": "分发文件", "ansible.builtin.copy": map[string]any{
			"src": "/work/job/payload/app.conf", "dest": unsafeOf("/etc/app/conf.d/app.conf"), "mode": "0640", // 权限是字符串，不是八进制数
		}},
	}
	if !reflect.DeepEqual(tasks, want) {
		t.Fatalf("tasks:\n got %#v\nwant %#v", tasks, want)
	}
	if !strings.Contains(string(raw), `"mode": "0640"`) || strings.Contains(string(raw), "k = v") {
		t.Fatalf("playbook:\n%s", raw)
	}
	if _, err := buildAdhocPlaybook(sampleTask(), "/work/job"); err == nil {
		t.Fatal("a template job must not get a generated playbook")
	}
}

func TestValidateTaskByKind(t *testing.T) {
	content := []byte("payload")
	valid := map[string]*jobcontract.Task{
		"command": adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: nasty}),
		"script":  adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "#!/bin/sh\r\nid\r\n", Interpreter: "sh", ScriptArgs: "-x 1"}),
		"file":    adhocTask(jobcontract.KindFile, fileAdhoc(content)),
	}
	valid["file"].FileContent = content
	for name, task := range valid {
		if err := validateTask(task); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	bad := map[string]func() *jobcontract.Task{
		"unknown kind": func() *jobcontract.Task { x := adhocTask(9, jobcontract.Adhoc{Command: "id"}); return x },
		"no adhoc": func() *jobcontract.Task {
			x := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{})
			x.Adhoc = nil
			return x
		},
		"empty command": func() *jobcontract.Task { return adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "  "}) },
		"long command": func() *jobcontract.Task {
			return adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: strings.Repeat("x", jobcontract.MaxCommandLen+1)})
		},
		"bad interpreter": func() *jobcontract.Task {
			return adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "id", Interpreter: "perl -e"})
		},
		"multiline args": func() *jobcontract.Task {
			return adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "id", Interpreter: "sh", ScriptArgs: "a\nb"})
		},
		"file name path": func() *jobcontract.Task {
			a := fileAdhoc(content)
			a.FileName = "../../etc/passwd"
			return adhocTask(jobcontract.KindFile, a)
		},
		"dest relative": func() *jobcontract.Task {
			a := fileAdhoc(content)
			a.Dest = "etc/app"
			return adhocTask(jobcontract.KindFile, a)
		},
		"dest traversal": func() *jobcontract.Task {
			a := fileAdhoc(content)
			a.Dest = "/etc/../root"
			return adhocTask(jobcontract.KindFile, a)
		},
		"dest template": func() *jobcontract.Task {
			a := fileAdhoc(content)
			a.Dest = "/tmp/{{ x }}"
			return adhocTask(jobcontract.KindFile, a)
		},
		"setuid mode": func() *jobcontract.Task {
			a := fileAdhoc(content)
			a.Mode = "4755"
			return adhocTask(jobcontract.KindFile, a)
		},
		"repo on adhoc": func() *jobcontract.Task {
			x := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
			x.RepoURL = "https://git.example.com/x.git"
			return x
		},
		"git on adhoc": func() *jobcontract.Task {
			x := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
			x.Git.Secret = "tok"
			return x
		},
		"vars on adhoc": func() *jobcontract.Task {
			x := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "id"})
			x.ExtraVars["v"] = 1
			return x
		},
		"content on script": func() *jobcontract.Task {
			x := adhocTask(jobcontract.KindScript, jobcontract.Adhoc{Script: "id", Interpreter: "sh"})
			x.FileContent = content
			return x
		},
		"adhoc on template": func() *jobcontract.Task { x := sampleTask(); x.Adhoc = &jobcontract.Adhoc{Command: "id"}; return x },
		"batch size":        func() *jobcontract.Task { x := sampleTask(); x.Options.BatchSize = -1; return x },
		"fail percent": func() *jobcontract.Task {
			x := sampleTask()
			x.Options.BatchSize, x.Options.MaxFailPercent = 1, 101
			return x
		},
	}
	for name, mk := range bad {
		if err := validateTask(mk()); err == nil {
			t.Errorf("%s: task accepted", name)
		}
	}
}

func TestNormalizeScriptAndVerifyPayload(t *testing.T) {
	if got := string(normalizeScript("#!/bin/bash\r\necho a\r\n\recho b\n")); got != "#!/bin/bash\necho a\n\recho b\n" {
		t.Fatalf("normalized = %q", got)
	}
	content := []byte("hello")
	task := adhocTask(jobcontract.KindFile, fileAdhoc(content))
	task.FileContent = content
	if err := verifyPayload(task); err != nil {
		t.Fatal(err)
	}
	task.Adhoc.FileSHA256 = strings.ToUpper(task.Adhoc.FileSHA256) // 十六进制大小写不敏感
	if err := verifyPayload(task); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(){
		"content changed": func() { task.FileContent = []byte("hellO") },
		"truncated":       func() { task.FileContent = []byte("hell") },
		"missing":         func() { task.FileContent = nil },
	} {
		mutate()
		if err := verifyPayload(task); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// 空文件是合法的。
	empty := adhocTask(jobcontract.KindFile, fileAdhoc(nil))
	if err := verifyPayload(empty); err != nil {
		t.Fatalf("empty file: %v", err)
	}
}

func TestSplitBatchesAndLimit(t *testing.T) {
	hosts := hostsOf("10.0.0.1", "2001:db8::1", "web-1.example.com", "10.0.0.4", "10.0.0.5")
	sizes := func(b [][]jobcontract.TaskHost) []int {
		out := make([]int, 0, len(b))
		for _, x := range b {
			out = append(out, len(x))
		}
		return out
	}
	for size, want := range map[int][]int{0: {5}, -1: {5}, 1: {1, 1, 1, 1, 1}, 2: {2, 2, 1}, 4: {4, 1}, 5: {5}, 9: {5}} {
		if got := sizes(splitBatches(hosts, size)); !reflect.DeepEqual(got, want) {
			t.Errorf("size %d: %v, want %v", size, got, want)
		}
	}
	// 顺序保持；IPv6 与主机名原样一行一个。
	b := splitBatches(hosts, 3)
	if got := string(buildLimit(b[0])); got != "10.0.0.1\n2001:db8::1\nweb-1.example.com\n" {
		t.Fatalf("limit file = %q", got)
	}
	if got := string(buildLimit(b[1])); got != "10.0.0.4\n10.0.0.5\n" {
		t.Fatalf("limit file = %q", got)
	}
	args := ansibleArgs(sampleTask(), "/work/job", "/work/job/playbook.yml", false, "/work/job/limit-2.txt")
	want := []string{
		"-i", "/work/job/inventory.json", "/work/job/playbook.yml", "-e", "@/work/job/vars.json", "--forks", "7",
		"--limit", "@/work/job/limit-2.txt", "--check", "--diff", "-vv",
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args:\n got %q\nwant %q", args, want)
	}
}

// ---------------------------------------------------------------------------
// 快速执行的作业（假 ansible-playbook）
// ---------------------------------------------------------------------------

func TestJobAdhocCommand(t *testing.T) {
	cfg := testConfig(t)
	eng := newFakeEngine()
	task := adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "uptime; echo {{ 7*7 }}"})
	task.Options.Check, task.Options.Diff = true, true
	// 没有 git：快速执行根本不碰它。
	cfg.Git = "/nonexistent/git"
	newTestJob(cfg, eng, nil, task).run(context.Background())

	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || fin.CommitID != "" || len(fin.Hosts) != 2 {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	jobDir := filepath.Join(cfg.WorkDir, task.ExecutionID)
	for _, want := range []string{
		"[runner] 执行命令（主机 2 台，并发 5）",
		"ARGS: -i " + jobDir + "/inventory.json " + jobDir + "/playbook.yml -e @" + jobDir + "/vars.json -e @" + jobDir + "/secrets.json --forks 5 --check --diff",
		"CWD: ", "/" + task.ExecutionID + "\n",
		`"ansible.builtin.shell": {"cmd": !unsafe "uptime; echo {{ 7*7 }}"}`,
		"FILE vars.json: {}",
		"JOBFILES: -rw-------:./playbook.yml",
		"[runner] 执行结束，退出码 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "拉取代码") || strings.Contains(out, "提交 ") || eng.commit != "" {
		t.Fatalf("quick execution must not check out code:\n%s", out)
	}
	if entries, err := os.ReadDir(cfg.WorkDir); err != nil || len(entries) != 0 {
		t.Fatalf("work dir not empty after the job: %v %v", entries, err)
	}
}

func TestJobAdhocScript(t *testing.T) {
	cfg := testConfig(t)
	eng := newFakeEngine()
	task := adhocTask(jobcontract.KindScript, jobcontract.Adhoc{
		Script: "#!/bin/bash\r\necho SCRIPT-BODY-MARKER\r\n", Interpreter: jobcontract.InterpreterBash, ScriptArgs: "--flag 'a b'",
	})
	newTestJob(cfg, eng, nil, task).run(context.Background())
	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	jobDir := filepath.Join(cfg.WorkDir, task.ExecutionID)
	for _, want := range []string{
		"[runner] 执行脚本（解释器 bash，主机 2 台，并发 5）",
		`"ansible.builtin.script": {"cmd": !unsafe "` + jobDir + `/script --flag 'a b'", "executable": "bash"}`,
		"-rwx------:./script",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	// CRLF 已统一成 LF：脚本文件里没有 \r。
	hex := out[strings.Index(out, "SCRIPT-HEX:"):]
	hex = hex[:strings.Index(hex, "\n")]
	if strings.Contains(hex, `\r`) || !strings.Contains(hex, `\n`) {
		t.Fatalf("script on disk still has CRLF: %s", hex)
	}
	// 平台消息与 Playbook 都不回显脚本内容（这里看到的只有假脚本的十六进制转储）。
	if strings.Contains(strings.ReplaceAll(out, hex, ""), "SCRIPT-BODY-MARKER") {
		t.Fatalf("script content echoed into the log:\n%s", out)
	}
}

func TestJobAdhocFile(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("FILE-BODY-MARKER\nkey = value\n")
	adhoc := fileAdhoc(content)

	eng := newFakeEngine()
	task := adhocTask(jobcontract.KindFile, adhoc)
	task.FileContent = content
	newTestJob(cfg, eng, nil, task).run(context.Background())
	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	jobDir := filepath.Join(cfg.WorkDir, task.ExecutionID)
	for _, want := range []string{
		"[runner] 分发文件 app.conf → /etc/app/conf.d（29 字节，主机 2 台，并发 5）",
		`"src": "` + jobDir + `/payload/app.conf"`, `"dest": !unsafe "/etc/app/conf.d/app.conf"`, `"mode": "0640"`,
		"-rw-------:./payload/app.conf",
		"PAYLOAD-SHA: " + adhoc.FileSHA256,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "FILE-BODY-MARKER") {
		t.Fatalf("file content echoed into the log:\n%s", out)
	}

	// 内容与摘要不符：不执行。
	for name, mutate := range map[string]func(x *jobcontract.Task){
		"tampered":  func(x *jobcontract.Task) { x.FileContent = []byte("FILE-BODY-MARKER\nkey = EVIL!\n") },
		"truncated": func(x *jobcontract.Task) { x.FileContent = content[:10] },
		"missing":   func(x *jobcontract.Task) { x.FileContent = nil },
	} {
		eng := newFakeEngine()
		bad := adhocTask(jobcontract.KindFile, adhoc)
		bad.FileContent = content
		mutate(bad)
		newTestJob(cfg, eng, nil, bad).run(context.Background())
		fin := eng.finish(t)
		if fin.Outcome != jobcontract.OutcomeError || !strings.Contains(fin.Error, "文件内容校验失败") || strings.Contains(eng.log(), "ARGS:") {
			t.Fatalf("%s: %+v\n%s", name, fin, eng.log())
		}
		if entries, err := os.ReadDir(cfg.WorkDir); err != nil || len(entries) != 0 {
			t.Fatalf("%s: work dir not empty: %v %v", name, entries, err)
		}
	}
}

// ---------------------------------------------------------------------------
// 滚动执行
// ---------------------------------------------------------------------------

// rollTask 指向测试仓库的滚动执行任务：主机名决定假 ansible-playbook 给出的结果。
func rollTask(g *gitFixture, playbook string, batch, maxFail int32, hosts ...string) *jobcontract.Task {
	task := gitTask(g, playbook)
	task.Hosts = hostsOf(hosts...)
	task.Options.BatchSize, task.Options.MaxFailPercent = batch, maxFail
	return task
}

func statuses(fin *jobcontract.FinishRequest) map[string]int32 {
	out := make(map[string]int32, len(fin.Hosts))
	for _, h := range fin.Hosts {
		out[h.Address] = h.Status()
	}
	return out
}

func TestJobRollingAllBatches(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	task := rollTask(g, "roll.yml", 2, 0, "ok-1", "2001:db8::1", "ok-3", "ok-4", "ok-5")
	cfg := testConfig(t)
	newTestJob(cfg, eng, nil, task).run(context.Background())
	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 0 || fin.Error != "" || len(fin.Hosts) != 5 {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	jobDir := filepath.Join(cfg.WorkDir, task.ExecutionID)
	for _, want := range []string{
		"[runner] 开始执行 roll.yml（主机 5 台，并发 5）",
		"[runner] 第 1/3 批（2 台）", "BATCH-START: ok-1 2001:db8::1 \n", "--limit @" + jobDir + "/limit-1.txt",
		"[runner] 第 2/3 批（2 台）", "BATCH-START: ok-3 ok-4 \n",
		"[runner] 第 3/3 批（1 台）", "BATCH-START: ok-5 \n",
		"[runner] 执行结束，退出码 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	// 批次按顺序执行；拉取代码只做一次。
	if i1, i2, i3 := strings.Index(out, "第 1/3 批"), strings.Index(out, "第 2/3 批"), strings.Index(out, "第 3/3 批"); i1 >= i2 || i2 >= i3 {
		t.Fatal("batches out of order")
	}
	if strings.Count(out, "[runner] 拉取代码") != 1 {
		t.Fatalf("checkout must happen once:\n%s", out)
	}
	if jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusSuccess {
		t.Fatal("want success")
	}
	// 批量不小于主机数：不分批，也不加 --limit。
	eng2 := newFakeEngine()
	newTestJob(testConfig(t), eng2, nil, rollTask(g, "site.yml", 5, 0, "10.0.0.1", "10.0.0.2")).run(context.Background())
	if out := eng2.log(); eng2.finish(t).ExitCode != 0 || strings.Contains(out, "--limit") || strings.Contains(out, "批（") {
		t.Fatalf("single batch must run unlimited:\n%s", out)
	}
}

func TestJobRollingAbortsOnThreshold(t *testing.T) {
	g := newGitFixture(t, "")
	// 阈值 0：第 2 批出现失败即中止，第 3、4 批不执行。
	eng := newFakeEngine()
	task := rollTask(g, "roll.yml", 2, 0, "ok-1", "ok-2", "bad-3", "gone-4", "ok-5", "ok-6", "ok-7")
	newTestJob(testConfig(t), eng, nil, task).run(context.Background())
	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeExited || fin.ExitCode != 2 || fin.Error != "滚动执行中止：1 台主机失败，1 台不可达，剩余 3 台未执行" {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	want := map[string]int32{
		"ok-1": jobcontract.HostChanged, "ok-2": jobcontract.HostChanged, "bad-3": jobcontract.HostFailed, "gone-4": jobcontract.HostUnreachable,
	}
	if got := statuses(fin); !reflect.DeepEqual(got, want) {
		t.Fatalf("hosts: %v, want %v (unexecuted hosts must not be reported)", got, want)
	}
	if !strings.Contains(out, "[runner] 失败比例超过阈值（50% > 0%），剩余 3 台主机未执行") || strings.Contains(out, "第 3/4 批") || strings.Contains(out, "ok-5 ") {
		t.Fatalf("log:\n%s", out)
	}
	if jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusPartial {
		t.Fatal("want partial")
	}

	// 阈值 50%：2/4 = 50% 不超过，继续；第 3 批后 3/6 = 50% 仍不超过；跑完全部。退出码取最后一个非零的。
	eng = newFakeEngine()
	task = rollTask(g, "roll.yml", 2, 50, "ok-1", "ok-2", "bad-3", "gone-4", "ok-5", "gone-6", "ok-7")
	newTestJob(testConfig(t), eng, nil, task).run(context.Background())
	fin = eng.finish(t)
	if fin.ExitCode != 4 || len(fin.Hosts) != 7 || fin.Error != "1 台主机失败，2 台不可达（ansible-playbook 退出码 4）" {
		t.Fatalf("under threshold: %+v\n%s", fin, eng.log())
	}

	// 阈值 25%：第 1 批 1/2 = 50% > 25% 即中止。第一批全败 → 终态失败。
	eng = newFakeEngine()
	task = rollTask(g, "roll.yml", 1, 25, "bad-1", "ok-2", "ok-3")
	newTestJob(testConfig(t), eng, nil, task).run(context.Background())
	fin = eng.finish(t)
	if fin.Error != "滚动执行中止：1 台主机失败，0 台不可达，剩余 2 台未执行" || len(fin.Hosts) != 1 ||
		jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusFailed {
		t.Fatalf("first batch failed: %+v\n%s", fin, eng.log())
	}
	if !strings.Contains(eng.log(), "失败比例超过阈值（100% > 25%），剩余 2 台主机未执行") {
		t.Fatalf("log:\n%s", eng.log())
	}

	// 有主机失败而 ansible-playbook 却以 0 退出：中止时仍上报非零退出码，终态不会是成功。
	eng = newFakeEngine()
	newTestJob(testConfig(t), eng, nil, rollTask(g, "rollzero.yml", 1, 0, "ok-1", "bad-2", "ok-3")).run(context.Background())
	fin = eng.finish(t)
	if fin.ExitCode == 0 || jobcontract.FinalStatus(fin.Outcome, fin.ExitCode, fin.Hosts) != jobcontract.StatusPartial {
		t.Fatalf("zero exit with failures: %+v", fin)
	}

	// 最后一批的失败不触发「中止」（没有剩余主机），按普通失败上报。
	eng = newFakeEngine()
	newTestJob(testConfig(t), eng, nil, rollTask(g, "roll.yml", 2, 0, "ok-1", "ok-2", "bad-3")).run(context.Background())
	fin = eng.finish(t)
	if fin.Error != "1 台主机失败，0 台不可达（ansible-playbook 退出码 2）" || len(fin.Hosts) != 3 {
		t.Fatalf("failure in the last batch: %+v", fin)
	}
}

// 某一批异常结束（Playbook 自身出错，没有任何主机结果）：中止，不把同一个错误在剩余批次上重复一遍。
func TestJobRollingAbortsOnBrokenBatch(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	newTestJob(testConfig(t), eng, nil, rollTask(g, "rollbroken.yml", 1, 100, "ok-1", "ok-2", "ok-3")).run(context.Background())
	fin := eng.finish(t)
	if fin.ExitCode != 4 || len(fin.Hosts) != 0 || fin.Error != "滚动执行中止：第 1 批异常结束（ansible-playbook 退出码 4），剩余 2 台未执行" {
		t.Fatalf("finish: %+v\n%s", fin, eng.log())
	}
	if strings.Contains(eng.log(), "第 2/3 批") {
		t.Fatalf("log:\n%s", eng.log())
	}
}

// 取消对整个作业生效：之后的批次不再开始，已执行批次的主机结果照常上报。
func TestJobRollingCancelBetweenBatches(t *testing.T) {
	g := newGitFixture(t, "")
	eng := newFakeEngine()
	eng.cancelOn = "第 2/4 批"
	newTestJob(testConfig(t), eng, nil, rollTask(g, "rollslow.yml", 1, 100, "ok-1", "ok-2", "ok-3", "ok-4")).run(context.Background())
	fin := eng.finish(t)
	out := eng.log()
	if fin.Outcome != jobcontract.OutcomeCanceled {
		t.Fatalf("finish: %+v\n%s", fin, out)
	}
	if strings.Contains(out, "第 4/4 批") || !strings.Contains(out, "[runner] 已按取消请求终止") {
		t.Fatalf("log:\n%s", out)
	}
	if got := statuses(fin); got["ok-1"] != jobcontract.HostChanged || len(got) > 2 {
		t.Fatalf("hosts reported: %v", got)
	}
	_, reported4 := statuses(fin)["ok-4"]
	if reported4 {
		t.Fatal("a host from an unstarted batch was reported")
	}

	// 超时同样对整个作业生效。
	eng = newFakeEngine()
	j := newTestJob(testConfig(t), eng, nil, rollTask(g, "rollslow.yml", 1, 100, "ok-1", "ok-2", "ok-3", "ok-4", "ok-5", "ok-6", "ok-7", "ok-8"))
	j.timeout = 2 * time.Second
	j.run(context.Background())
	fin = eng.finish(t)
	if fin.Outcome != jobcontract.OutcomeTimeout || len(fin.Hosts) == 0 || len(fin.Hosts) >= 8 {
		t.Fatalf("timeout: %+v", fin)
	}
}

// ---------------------------------------------------------------------------
// 端到端：真实的引擎处理器 + 执行节点，三种快速执行与滚动执行
// ---------------------------------------------------------------------------

// submitAdhoc 建一条快速执行（相当于 core 建单）：没有项目、没有模板。
func (e *e2e) submitAdhoc(kind int32, adhoc jobcontract.Adhoc, payload []byte, opts jobcontract.Options, hosts ...string) string {
	e.t.Helper()
	id, err := idgen.NewID()
	if err != nil {
		e.t.Fatal(err)
	}
	spec, err := json.Marshal(jobcontract.Spec{Kind: kind, Adhoc: &adhoc, CredentialID: e2eCredID, Options: opts})
	if err != nil {
		e.t.Fatal(err)
	}
	x := biztest.Execution{ID: id, RunnerZone: "dmz", Spec: spec}
	for _, h := range hosts {
		x.Hosts = append(x.Hosts, biztest.HostRow{Host: biz.Host{Address: h, Port: 22, CIID: e2eCredID, Name: h, Env: "prod"}})
	}
	e.store.AddExecution(x)
	if payload != nil {
		e.store.SetPayload(id, payload)
	}
	return id
}

func TestEndToEndQuickExecution(t *testing.T) {
	e := newE2E(t)
	// 没有任何项目：快速执行不读 job_project。
	e.store.RemoveProject(e2eProjID)
	opts := jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22}
	// 4MiB 的文件：领取应答约 5.6MiB，验证两端的大小限制。
	big := make([]byte, jobcontract.MaxFileBytes)
	for i := range big {
		big[i] = byte(i % 251)
	}
	cmdID := e.submitAdhoc(jobcontract.KindCommand, jobcontract.Adhoc{Command: "echo {{ 1+1 }}"}, nil, opts, "10.0.0.1", "10.0.0.2")
	scriptID := e.submitAdhoc(jobcontract.KindScript, jobcontract.Adhoc{Script: "id\r\n", Interpreter: "sh"}, nil, opts, "10.0.0.1", "10.0.0.2")
	fileID := e.submitAdhoc(jobcontract.KindFile, fileAdhoc(big), big, opts, "10.0.0.1", "10.0.0.2")
	tampered := append([]byte{}, big...)
	tampered[100] ^= 0xff
	badID := e.submitAdhoc(jobcontract.KindFile, fileAdhoc(big), tampered, opts, "10.0.0.1")
	missingID := e.submitAdhoc(jobcontract.KindFile, fileAdhoc(big), nil, opts, "10.0.0.1")
	rollID := e.submitAdhoc(jobcontract.KindCommand, jobcontract.Adhoc{Command: "true"}, nil,
		jobcontract.Options{Forks: 5, TimeoutS: 600, Port: 22, BatchSize: 1, MaxFailPercent: 0}, "ok-1", "bad-2", "ok-3")
	e.start()

	for name, id := range map[string]string{"command": cmdID, "script": scriptID, "file": fileID} {
		x := e.wait(id)
		if x.Status != jobcontract.StatusSuccess || x.CommitID != "" || x.HostOK != 2 {
			t.Fatalf("%s: status %d error %q commit %q\n%s", name, x.Status, x.Error, x.CommitID, e.log(x))
		}
	}
	if out := e.log(e.store.Get(cmdID)); !strings.Contains(out, `"cmd": !unsafe "echo {{ 1+1 }}"`) || strings.Contains(out, "拉取代码") {
		t.Fatalf("command log:\n%s", out)
	}
	sum := sha256.Sum256(big)
	if out := e.log(e.store.Get(fileID)); !strings.Contains(out, "PAYLOAD-SHA: "+hex.EncodeToString(sum[:])) {
		t.Fatalf("the 4MiB payload did not arrive intact:\n%s", out)
	}
	// 引擎侧核对文件内容：对不上或缺失的直接置为失败，不下发。
	if x := e.wait(badID); x.Status != jobcontract.StatusFailed || !strings.Contains(x.Error, "不一致") || x.LogSeq != 0 {
		t.Fatalf("tampered payload: status %d error %q", x.Status, x.Error)
	}
	if x := e.wait(missingID); x.Status != jobcontract.StatusFailed || !strings.Contains(x.Error, "不存在") {
		t.Fatalf("missing payload: status %d error %q", x.Status, x.Error)
	}
	// 滚动执行中止：未执行的主机保持「未执行」。
	x := e.wait(rollID)
	if x.Status != jobcontract.StatusPartial || x.Error != "滚动执行中止：1 台主机失败，0 台不可达，剩余 1 台未执行" ||
		x.HostOK != 1 || x.HostFailed != 1 || x.Hosts[2].Status != jobcontract.HostPending {
		t.Fatalf("rolling: status %d error %q ok=%d failed=%d hosts %+v\n%s", x.Status, x.Error, x.HostOK, x.HostFailed, x.Hosts, e.log(x))
	}
}
