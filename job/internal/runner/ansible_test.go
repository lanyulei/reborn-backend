package runner

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 用真实的 ansible-playbook 验证平台生成的清单、变量文件与 Playbook。JOB_TEST_ANSIBLE 指向 ansible-playbook
// （如 python3 -m venv /tmp/a && /tmp/a/bin/pip install ansible-core 之后的 /tmp/a/bin/ansible-playbook）；
// 未设置则跳过。目标主机不可达，所以实际执行时在命令行追加 `-c local`：清单里没有 ansible_connection，
// 连接方式以外的一切（清单、变量、!unsafe 标记、模块参数、--limit 文件、ansible.cfg）都走真实路径。

// realAnsible 在一个作业目录里按节点的做法写好全部文件，返回运行 ansible-playbook 的函数。
type realAnsible struct {
	t        *testing.T
	bin      string
	dir      string
	task     *jobcontract.Task
	playbook string
	workDir  string
	secrets  bool
}

func newRealAnsible(t *testing.T, task *jobcontract.Task, playbookBody string) *realAnsible {
	t.Helper()
	bin := os.Getenv("JOB_TEST_ANSIBLE")
	if bin == "" {
		t.Skip("JOB_TEST_ANSIBLE not set, skipping tests against the real ansible-playbook")
	}
	dir, err := os.MkdirTemp("/tmp", "ra")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := validateTask(task); err != nil {
		t.Fatalf("task rejected: %v", err)
	}
	r := &realAnsible{t: t, bin: bin, dir: dir, task: task, workDir: dir}
	if taskKind(task) == jobcontract.KindPlaybook {
		r.playbook = "site.yml"
		if err := os.WriteFile(filepath.Join(dir, r.playbook), []byte(playbookBody), 0o600); err != nil {
			t.Fatal(err)
		}
	} else if r.playbook, err = writeAdhocFiles(task, dir); err != nil {
		t.Fatal(err)
	}
	inventory, err := buildInventory(task, filepath.Join(dir, "known_hosts"))
	if err != nil {
		t.Fatal(err)
	}
	vars, err := buildVars(task)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := buildSecrets(task)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{inventoryName: inventory, varsName: vars, ansibleCfg: buildAnsibleCfg(filepath.Join(dir, "cp"))}
	if secrets != nil {
		files[secretsName], r.secrets = secrets, true
	}
	for name, data := range files {
		if err := writePrivate(filepath.Join(dir, name), data); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// run 以节点的参数与环境运行 ansible-playbook（外加 extra），返回合并的输出与是否以 0 退出。
func (r *realAnsible) run(limitFile string, extra ...string) (string, bool) {
	r.t.Helper()
	args := append(ansibleArgs(r.task, r.dir, r.playbook, r.secrets, limitFile), extra...)
	cmd := exec.Command(r.bin, args...)
	cmd.Dir = r.workDir
	cmd.Env = ansibleEnv(r.dir, "")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err == nil
}

func (r *realAnsible) syntaxCheck() {
	r.t.Helper()
	if out, ok := r.run("", "--syntax-check"); !ok {
		r.t.Fatalf("ansible-playbook --syntax-check failed:\n%s", out)
	}
}

// localTask 把任务的主机改成两台假想主机（配合 -c local 在本机执行）。
func localHosts(task *jobcontract.Task) *jobcontract.Task {
	task.Hosts = []jobcontract.TaskHost{
		{Address: "10.0.0.1", Port: 22, Vars: map[string]string{"cmdb_id": "ci-1", "cmdb_name": "web {{ 6*7 }}", "cmdb_env": "prod"}},
		{Address: "2001:db8::1", Port: 2222, Vars: map[string]string{"cmdb_id": "ci-2", "cmdb_name": "db", "cmdb_env": "prod"}},
	}
	task.Options.Become, task.Options.Check, task.Options.Diff, task.Options.Verbosity = false, false, false, 0
	return task
}

// 作业模板：执行参数与 cmdb_* 变量经变量文件 / 清单送达，含模板语法的取值保持字面值；整数仍是整数。
func TestRealAnsiblePlaybookVars(t *testing.T) {
	task := localHosts(sampleTask())
	task.RepoURL, task.Git = "https://git.example.com/ops/infra.git", jobcontract.GitAuth{Type: jobcontract.GitAuthNone}
	r := newRealAnsible(t, task, `- hosts: all
  gather_facts: false
  tasks:
    - ansible.builtin.debug:
        msg: "V=[{{ version }}] N=[{{ replicas + 1 }}] D=[{{ debug }}] L=[{{ tags_list | join('+') }}] K=[{{ nested.k }}] C=[{{ cmdb_name }}] U=[{{ ansible_user }}] P=[{{ ansible_port }}]"
`)
	r.syntaxCheck()
	out, ok := r.run("", "-c", "local")
	if !ok {
		t.Fatalf("ansible-playbook failed:\n%s", out)
	}
	for _, want := range []string{
		`V=[{{ lookup('pipe', 'id') }}] N=[4] D=[True] L=[a+1] K=[v] C=[web {{ 6*7 }}] U=[ops] P=[22]`,
		`C=[db] U=[ops] P=[2222]`,
		"10.0.0.1", "2001:db8::1", "PLAY RECAP",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "uid=") || strings.Contains(out, "web 42") {
		t.Fatalf("a task-supplied value was evaluated as a template:\n%s", out)
	}
	// 平台的 PLAY RECAP 解析器读得懂真实输出（含 IPv6 主机）。
	rp := newRecapParser(task.Hosts)
	rp.Write([]byte(out))
	rp.Close()
	if got := rp.Results(); len(got) != 2 || got[0].OK != 1 || got[1].Address != "2001:db8::1" || got[1].OK != 1 {
		t.Fatalf("recap: %+v", got)
	}
}

// 快速执行 · 命令：命令原样交给 shell，其中的 {{ }} 不被 Ansible 求值；输出进日志。
func TestRealAnsibleCommand(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pwned")
	cmd := `echo "A={{ 7*7 }} B={{ lookup('pipe','touch ` + marker + `') }}"; echo "E=$((6*7))" >&2 # ] } : "` + "\n" + `echo second-line`
	r := newRealAnsible(t, localHosts(adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: cmd})), "")
	r.syntaxCheck()
	out, ok := r.run("", "-c", "local")
	if !ok {
		t.Fatalf("ansible-playbook failed:\n%s", out)
	}
	for _, want := range []string{`A={{ 7*7 }} B={{ lookup('pipe','touch ` + marker + `') }}`, "E=42", "second-line", "标准输出", "标准错误"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(marker); err == nil || strings.Contains(out, "A=49") {
		t.Fatalf("the command was evaluated as a template:\n%s", out)
	}
	// 预检模式：命令被跳过，打印输出的任务随之跳过，不报错。
	r.task.Options.Check = true
	if out, ok := r.run("", "-c", "local"); !ok || strings.Contains(out, "second-line") {
		t.Fatalf("check mode: ok=%v\n%s", ok, out)
	}
	// 命令失败：非零退出，统计里记为失败。
	r2 := newRealAnsible(t, localHosts(adhocTask(jobcontract.KindCommand, jobcontract.Adhoc{Command: "echo oops >&2; exit 3"})), "")
	out, ok = r2.run("", "-c", "local")
	rp := newRecapParser(r2.task.Hosts)
	rp.Write([]byte(out))
	rp.Close()
	if ok || len(rp.Results()) != 2 || rp.Results()[0].Failed != 1 || !strings.Contains(out, "oops") {
		t.Fatalf("failing command: ok=%v recap %+v\n%s", ok, rp.Results(), out)
	}
}

// 快速执行 · 脚本：CRLF 的脚本能跑，参数按 shell 规则分词且不被求值。
func TestRealAnsibleScript(t *testing.T) {
	script := "#!/bin/sh\r\necho \"ARGS=$#\"\r\nfor a in \"$@\"; do echo \"ARG=[$a]\"; done\r\n"
	task := localHosts(adhocTask(jobcontract.KindScript, jobcontract.Adhoc{
		Script: script, Interpreter: jobcontract.InterpreterSh, ScriptArgs: `--name '{{ 7*7 }}' "a b" plain`,
	}))
	r := newRealAnsible(t, task, "")
	r.syntaxCheck()
	out, ok := r.run("", "-c", "local")
	if !ok {
		t.Fatalf("ansible-playbook failed:\n%s", out)
	}
	for _, want := range []string{"ARGS=4", "ARG=[--name]", "ARG=[{{ 7*7 }}]", "ARG=[a b]", "ARG=[plain]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}
	// python3 解释器。
	py := localHosts(adhocTask(jobcontract.KindScript, jobcontract.Adhoc{
		Script: "import sys\nprint('PY', sys.argv[1:])\n", Interpreter: jobcontract.InterpreterPython, ScriptArgs: "x y",
	}))
	if out, ok := newRealAnsible(t, py, "").run("", "-c", "local"); !ok || !strings.Contains(out, "PY ['x', 'y']") {
		t.Fatalf("python script: ok=%v\n%s", ok, out)
	}
}

// 快速执行 · 文件：目录不存在则创建，文件内容与权限正确；滚动执行的 --limit 文件只选中那一批（含 IPv6）。
func TestRealAnsibleFileAndLimit(t *testing.T) {
	dest := filepath.Join("/tmp", "ra-dest-"+filepath.Base(t.TempDir()), "conf.d")
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(dest)) })
	content := []byte("key = {{ not a template }}\n\x00\xffbinary\n")
	adhoc := fileAdhoc(content)
	adhoc.Dest, adhoc.Mode = dest, "0640"
	task := localHosts(adhocTask(jobcontract.KindFile, adhoc))
	task.FileContent = content
	r := newRealAnsible(t, task, "")
	r.syntaxCheck()

	// 只跑第二批（IPv6 那台）。
	batches := splitBatches(task.Hosts, 1)
	limit := filepath.Join(r.dir, limitFileName(2))
	if err := writePrivate(limit, buildLimit(batches[1])); err != nil {
		t.Fatal(err)
	}
	out, ok := r.run(limit, "-c", "local")
	if !ok {
		t.Fatalf("ansible-playbook failed:\n%s", out)
	}
	rp := newRecapParser(task.Hosts)
	rp.Write([]byte(out))
	rp.Close()
	if got := rp.Results(); len(got) != 1 || got[0].Address != "2001:db8::1" || got[0].Changed == 0 {
		t.Fatalf("--limit must select exactly the batch: %+v\n%s", got, out)
	}
	got, err := os.ReadFile(filepath.Join(dest, "app.conf"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("distributed file: %q, %v", got, err)
	}
	if info, err := os.Stat(filepath.Join(dest, "app.conf")); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("file mode: %v, %v", info.Mode(), err)
	}
	// 再跑一遍：幂等，没有变更。
	out, ok = r.run(limit, "-c", "local")
	rp = newRecapParser(task.Hosts)
	rp.Write([]byte(out))
	rp.Close()
	if !ok || rp.Results()[0].Changed != 0 {
		t.Fatalf("second run must be idempotent: %+v\n%s", rp.Results(), out)
	}
}
