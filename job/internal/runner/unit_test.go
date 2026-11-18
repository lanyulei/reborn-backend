package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"

	"go.yaml.in/yaml/v3"
)

// ---------------------------------------------------------------------------
// PLAY RECAP 解析
// ---------------------------------------------------------------------------

func hostsOf(addrs ...string) []jobcontract.TaskHost {
	out := make([]jobcontract.TaskHost, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, jobcontract.TaskHost{Address: a, Port: 22})
	}
	return out
}

func TestRecapParser(t *testing.T) {
	output := strings.Join([]string{
		"PLAY [all] *********************************************************************",
		"",
		"TASK [debug] *******************************************************************",
		`ok: [10.0.0.1] => {"msg": "10.0.0.9 : ok=99 changed=99 unreachable=0 failed=0"}`,
		"10.0.0.1 : ok=77 changed=0 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0",
		"",
		"PLAY RECAP *********************************************************************",
		"10.0.0.1                   : ok=3    changed=1    unreachable=0    failed=0    skipped=2    rescued=0    ignored=0   ",
		"10.0.0.2                   : ok=1    changed=0    unreachable=0    failed=1    skipped=0    rescued=0    ignored=0   ",
		"2001:db8::1                : ok=0    changed=0    unreachable=1    failed=0    skipped=0    rescued=0    ignored=0   ",
		"web-1.example.com          : ok=2    changed=0    unreachable=0    failed=0    skipped=5    rescued=1    ignored=1   ",
		"evil.example.com           : ok=9    changed=9    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0   ",
		"",
		"10.0.0.2 : ok=50 changed=0 unreachable=0 failed=0",
		"",
	}, "\n")
	want := []jobcontract.HostResult{
		{Address: "10.0.0.1", OK: 3, Changed: 1, Skipped: 2},
		{Address: "10.0.0.2", OK: 1, Failed: 1},
		{Address: "2001:db8::1", Unreachable: 1},
		{Address: "web-1.example.com", OK: 2, Skipped: 5},
	}
	hosts := hostsOf("10.0.0.1", "10.0.0.2", "2001:db8::1", "web-1.example.com", "10.0.0.3")

	// 无论输出被怎样切段送入，结果都一样。
	for _, step := range []int{1, 7, 64, len(output)} {
		p := newRecapParser(hosts)
		for i := 0; i < len(output); i += step {
			p.Write([]byte(output[i:min(i+step, len(output))]))
		}
		p.Close()
		if got := p.Results(); !reflect.DeepEqual(got, want) {
			t.Fatalf("step %d: got %+v\nwant %+v", step, got, want)
		}
	}
}

func TestRecapParserLastRecapWins(t *testing.T) {
	p := newRecapParser(hostsOf("10.0.0.1", "10.0.0.2"))
	p.Write([]byte("PLAY RECAP ****\r\n10.0.0.1 : ok=1 changed=0 unreachable=0 failed=1\r\n10.0.0.2 : ok=4 changed=0 unreachable=0 failed=0\r\n\r\n"))
	p.Write([]byte("PLAY [second] ***\nPLAY RECAP ****\n10.0.0.1 : ok=5 changed=2 unreachable=0 failed=0\n")) // 结尾没有空行
	p.Write([]byte("10.0.0.2 : ok=6 changed=0 unreachable=0 failed=0"))                                       // 结尾没有换行
	p.Close()
	want := []jobcontract.HostResult{{Address: "10.0.0.1", OK: 5, Changed: 2}, {Address: "10.0.0.2", OK: 6}}
	if got := p.Results(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestRecapParserIgnoresGarbage(t *testing.T) {
	p := newRecapParser(hostsOf("10.0.0.1"))
	p.Write([]byte("PLAY RECAP ***\n"))
	p.Write([]byte(strings.Repeat("x", 3*maxRecapLine))) // 超长行：跳过并结束 RECAP 段
	p.Write([]byte("\n10.0.0.1 : ok=1 changed=0 unreachable=0 failed=0\n"))
	p.Write([]byte("PLAY RECAP ***\n10.0.0.1 : ok=99999999999999999999 changed=1 unreachable=0 failed=0\n")) // 溢出的计数忽略
	p.Close()
	want := []jobcontract.HostResult{{Address: "10.0.0.1", Changed: 1}}
	if got := p.Results(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if empty := newRecapParser(hostsOf("10.0.0.1")); len(empty.Results()) != 0 {
		t.Fatal("no output must yield no results")
	}
}

// ---------------------------------------------------------------------------
// 掩码
// ---------------------------------------------------------------------------

const testKey = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQ\nAAAAMwAAAAtzc2gtZWQyNTUxOQAAACDx\n-----END OPENSSH PRIVATE KEY-----\n"

func TestMaskerMasksEverySecret(t *testing.T) {
	task := &jobcontract.Task{
		Credential: jobcontract.TaskCredential{Secret: testKey, BecomeSecret: `p@ss "w0rd"/x`},
		Git:        jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Username: "deploy", Secret: "glpat-SECRET+token"},
	}
	m := newMasker(taskSecrets(task)...)
	in := strings.Join([]string{
		"key line: b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQ and AAAAMwAAAAtzc2gtZWQyNTUxOQAAACDx",
		`become: p@ss "w0rd"/x`,
		`json: {"ansible_become_password": "p@ss \"w0rd\"/x"}`,
		"url: https://deploy:glpat-SECRET%2Btoken@git.example.com/x.git",
		"token: glpat-SECRET+token",
		"header: Authorization: Basic " + basicAuth("deploy", "glpat-SECRET+token"),
		"-----BEGIN OPENSSH PRIVATE KEY-----",
	}, "\n")
	out := m.Mask(in)
	for _, secret := range []string{
		"b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQ", "AAAAMwAAAAtzc2gtZWQyNTUxOQAAACDx", `p@ss "w0rd"/x`, `p@ss \"w0rd\"/x`,
		"glpat-SECRET+token", "glpat-SECRET%2Btoken", basicAuth("deploy", "glpat-SECRET+token"),
	} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q survived masking:\n%s", secret, out)
		}
	}
	if strings.Count(out, maskText) != 7 {
		t.Fatalf("want 7 masks, got %d:\n%s", strings.Count(out, maskText), out)
	}
	// PEM 标记行不是秘密，不掩码。
	if !strings.Contains(out, "-----BEGIN OPENSSH PRIVATE KEY-----") {
		t.Fatalf("PEM marker must not be masked:\n%s", out)
	}
}

func TestMaskerSkipsShortSecrets(t *testing.T) {
	m := newMasker("abc", "", "1", "abcd")
	if got := m.Mask("abc 1 abcd"); got != "abc 1 "+maskText {
		t.Fatalf("got %q", got)
	}
	none := newMasker("ab")
	if got := none.Write([]byte("ab cd")) + none.Flush(); got != "ab cd" {
		t.Fatalf("got %q", got)
	}
}

func TestMaskerLongestFirst(t *testing.T) {
	m := newMasker("secret", "secret-longer")
	if got := m.Mask("x secret-longer y secret z"); got != "x "+maskText+" y "+maskText+" z" {
		t.Fatalf("got %q", got)
	}
}

// 流式掩码：凭据无论被读管道切在哪里都不会漏出，输出始终是合法 UTF-8。
func TestMaskerStreaming(t *testing.T) {
	secret := "s3cr3t-密码-value"
	in := "前缀 " + secret + " 中间 s3cr3t-密 不是完整的凭据 " + secret + secret + " 结尾 s3c"
	want := "前缀 " + maskText + " 中间 s3cr3t-密 不是完整的凭据 " + maskText + maskText + " 结尾 s3c"
	for step := 1; step <= len(in); step++ {
		m := newMasker(secret)
		var out strings.Builder
		for i := 0; i < len(in); i += step {
			piece := m.Write([]byte(in[i:min(i+step, len(in))]))
			if !utf8.ValidString(piece) {
				t.Fatalf("step %d: invalid UTF-8 piece %q", step, piece)
			}
			if strings.ContainsRune(piece, utf8.RuneError) {
				t.Fatalf("step %d: a character was split: %q", step, piece)
			}
			out.WriteString(piece)
		}
		out.WriteString(m.Flush())
		if out.String() != want {
			t.Fatalf("step %d:\n got %q\nwant %q", step, out.String(), want)
		}
	}
}

func TestMaskerInvalidUTF8(t *testing.T) {
	m := newMasker("topsecret")
	out := m.Write([]byte("a\xffb topsecret \xe4\xb8")) + m.Flush()
	if !utf8.ValidString(out) || strings.Contains(out, "topsecret") || !strings.HasPrefix(out, "a�b "+maskText) {
		t.Fatalf("got %q", out)
	}
}

// ---------------------------------------------------------------------------
// 日志缓冲：分块、重传、背压
// ---------------------------------------------------------------------------

func TestCutPoint(t *testing.T) {
	cases := []struct {
		buf    string
		target int
		want   int
	}{
		{"short", 10, 5},
		{"line1\nline2\nline3", 14, 12}, // 最后一个换行之后
		{"abcdefghij", 4, 4},            // 没有换行：按目标大小切
		{"ab中文字", 4, 2},                 // 不把字符切成两半（"中" 占第 2-4 字节）
		{"ab中文字", 5, 5},
		{"\n" + strings.Repeat("x", 20), 8, 1},
	}
	for _, c := range cases {
		if got := cutPoint([]byte(c.buf), c.target); got != c.want {
			t.Errorf("cutPoint(%q, %d) = %d, want %d", c.buf, c.target, got, c.want)
		}
	}
}

func TestLogBufferChunking(t *testing.T) {
	ctx := context.Background()
	b := newLogBuffer(64, 1<<20)
	var want strings.Builder
	for i := range 40 {
		line := strings.Repeat(string(rune('a'+i%26)), 10+i%7) + " 行尾中文\n"
		want.WriteString(line)
		if err := b.Append(ctx, line); err != nil {
			t.Fatal(err)
		}
	}
	b.Flush()
	b.Flush() // 没有新内容时是空操作
	chunks := b.Pending(1000, 1<<30)
	var got strings.Builder
	for i, c := range chunks {
		if c.Seq != int64(i+1) {
			t.Fatalf("chunk %d has seq %d", i, c.Seq)
		}
		if len(c.Content) == 0 || len(c.Content) > 64 {
			t.Fatalf("chunk %d size %d", c.Seq, len(c.Content))
		}
		if !utf8.ValidString(c.Content) {
			t.Fatalf("chunk %d is not valid UTF-8", c.Seq)
		}
		if i < len(chunks)-1 && !strings.HasSuffix(c.Content, "\n") {
			t.Fatalf("chunk %d not cut on a line boundary: %q", c.Seq, c.Content)
		}
		got.WriteString(c.Content)
	}
	if got.String() != want.String() {
		t.Fatal("chunks do not reassemble into the original output")
	}
}

func TestLogBufferNeverExceedsProtocolChunkLimit(t *testing.T) {
	b := newLogBuffer(chunkTarget, bufferLimit)
	// 一行 1MiB、没有换行的输出。
	if err := b.Append(context.Background(), strings.Repeat("长", (1<<20)/3)); err != nil {
		t.Fatal(err)
	}
	b.Flush()
	for _, c := range b.Pending(1000, 1<<30) {
		if len(c.Content) > jobcontract.MaxLogChunkBytes || !utf8.ValidString(c.Content) {
			t.Fatalf("chunk %d: %d bytes, valid=%v", c.Seq, len(c.Content), utf8.ValidString(c.Content))
		}
	}
	if huge := newLogBuffer(10<<20, bufferLimit); huge.target != jobcontract.MaxLogChunkBytes {
		t.Fatalf("target must be capped at the protocol limit, got %d", huge.target)
	}
}

func TestLogBufferPendingAndAck(t *testing.T) {
	ctx := context.Background()
	b := newLogBuffer(8, 1<<20)
	for i := range 6 {
		_ = b.Append(ctx, strings.Repeat(string(rune('0'+i)), 7)+"\n")
	}
	if got := b.Pending(4, 1<<30); len(got) != 4 || got[0].Seq != 1 || got[3].Seq != 4 {
		t.Fatalf("pending by count: %+v", got)
	}
	if got := b.Pending(100, 20); len(got) != 2 {
		t.Fatalf("pending by bytes: %d chunks, want 2", len(got))
	}
	if got := b.Pending(100, 1); len(got) != 1 {
		t.Fatalf("pending must return at least one chunk, got %d", len(got))
	}
	// 引擎只确认到 2（3 之后丢了）：下一次从 3 起重传。
	b.Ack(2)
	if got := b.Pending(100, 1<<30); len(got) != 4 || got[0].Seq != 3 {
		t.Fatalf("after ack 2: %+v", got)
	}
	// 回退的确认与重复的确认是空操作；超前的确认按已产生的序号算。
	b.Ack(1)
	b.Ack(2)
	if got := b.Pending(100, 1<<30); got[0].Seq != 3 || b.Acked() != 2 {
		t.Fatalf("stale ack changed state: first %d acked %d", got[0].Seq, b.Acked())
	}
	b.Ack(100)
	if !b.Drained() || b.Acked() != 6 {
		t.Fatalf("drained=%v acked=%d", b.Drained(), b.Acked())
	}
	// 尚未封块的内容不算交完。
	_ = b.Append(ctx, "tail")
	if b.Drained() {
		t.Fatal("unsealed content must count as not drained")
	}
	b.Flush()
	if got := b.Pending(100, 1<<30); len(got) != 1 || got[0].Seq != 7 || got[0].Content != "tail" {
		t.Fatalf("tail chunk: %+v", got)
	}
}

// 背压：缓冲满时 Append 阻塞而不是丢弃，确认后继续。
func TestLogBufferBackpressure(t *testing.T) {
	ctx := context.Background()
	b := newLogBuffer(16, 64)
	for i := 0; b.sizeForTest() < 64; i++ {
		if err := b.Append(ctx, strings.Repeat("x", 15)+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	appended := make(chan error, 1)
	go func() { appended <- b.Append(ctx, "blocked-line-01\n") }()
	select {
	case err := <-appended:
		t.Fatalf("Append must block while the buffer is full (returned %v)", err)
	case <-time.After(100 * time.Millisecond):
	}
	b.Ack(1)
	select {
	case err := <-appended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Append still blocked after space was freed")
	}
	// 什么都没丢：全部确认后重新拼出的内容完整。
	b.Flush()
	var total int
	for {
		chunks := b.Pending(100, 1<<30)
		if len(chunks) == 0 {
			break
		}
		for _, c := range chunks {
			total += len(c.Content)
		}
		b.Ack(chunks[len(chunks)-1].Seq)
	}
	if total != 4*16+16-16 { // 首块已在 Ack(1) 时确认
		t.Fatalf("remaining bytes = %d", total)
	}

	// 阻塞中的 Append 响应 ctx 取消与丢弃。
	full := newLogBuffer(16, 16)
	_ = full.Append(ctx, strings.Repeat("y", 16))
	cctx, cancel := context.WithCancel(ctx)
	go func() { appended <- full.Append(cctx, "more") }()
	cancel()
	if err := <-appended; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	go func() { appended <- full.Append(ctx, "more") }()
	time.Sleep(20 * time.Millisecond)
	full.Discard()
	if err := <-appended; !errors.Is(err, errDiscarded) {
		t.Fatalf("got %v, want errDiscarded", err)
	}
	if err := full.Append(ctx, "after"); !errors.Is(err, errDiscarded) {
		t.Fatalf("append after discard: %v", err)
	}
}

// 多个写入方同时被背压阻塞时，腾出空间后全部都能继续（信号不丢）。
func TestLogBufferWakesAllWaiters(t *testing.T) {
	ctx := context.Background()
	b := newLogBuffer(8, 8)
	_ = b.Append(ctx, "12345678")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Append(ctx, "ab")
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	deadline := time.After(10 * time.Second)
	for {
		b.Flush()
		if chunks := b.Pending(100, 1<<30); len(chunks) > 0 {
			b.Ack(chunks[len(chunks)-1].Seq)
		}
		select {
		case <-done:
			return
		case <-deadline:
			t.Fatal("waiters still blocked")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (b *logBuffer) sizeForTest() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// ---------------------------------------------------------------------------
// 任务校验、清单、参数、命令行与环境
// ---------------------------------------------------------------------------

func sampleTask() *jobcontract.Task {
	return &jobcontract.Task{
		ExecutionID: "018f0000-0000-7000-8000-000000000001", LeaseToken: "lease", LeaseSeconds: 60,
		RepoURL: "https://git.example.com/ops/infra.git", Ref: "release/1.2", Playbook: "deploy/site.yml",
		Git: jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Username: "deploy", Secret: "git-token-123"},
		Hosts: []jobcontract.TaskHost{
			{Address: "10.0.0.1", Port: 22, Vars: map[string]string{"cmdb_id": "id-1", "cmdb_name": "web {{ 1 }}", "cmdb_env": "prod"}},
			{Address: "2001:db8::1", Port: 2222},
		},
		Credential: jobcontract.TaskCredential{
			Type: jobcontract.CredentialSSHPassword, Username: "ops", Secret: "login-pass",
			BecomeMethod: jobcontract.BecomeSu, BecomeUser: "admin", BecomeSecret: "become-{{pass}}",
		},
		ExtraVars: map[string]any{
			"version": "{{ lookup('pipe', 'id') }}", "replicas": json.Number("3"), "debug": true,
			"tags_list": []any{"a", json.Number("1")}, "nested": map[string]any{"k": "v"},
		},
		Options: jobcontract.Options{Forks: 7, TimeoutS: 600, Port: 22, Become: true, Check: true, Diff: true, Verbosity: 2},
	}
}

func TestValidateTask(t *testing.T) {
	if err := validateTask(sampleTask()); err != nil {
		t.Fatalf("sample task must be valid: %v", err)
	}
	cases := map[string]func(t *jobcontract.Task){
		"execution id":       func(t *jobcontract.Task) { t.ExecutionID = "../../etc" },
		"lease token":        func(t *jobcontract.Task) { t.LeaseToken = "" },
		"repo file":          func(t *jobcontract.Task) { t.RepoURL = "file:///srv/repo.git" },
		"repo ext helper":    func(t *jobcontract.Task) { t.RepoURL = "ext::sh -c id" },
		"repo with creds":    func(t *jobcontract.Task) { t.RepoURL = "https://u:p@git.example.com/x.git" },
		"ref option":         func(t *jobcontract.Task) { t.Ref = "--upload-pack=touch /tmp/x" },
		"playbook traversal": func(t *jobcontract.Task) { t.Playbook = "../outside.yml" },
		"playbook option":    func(t *jobcontract.Task) { t.Playbook = "-e@/etc/passwd.yml" },
		"playbook absolute":  func(t *jobcontract.Task) { t.Playbook = "/etc/site.yml" },
		"no hosts":           func(t *jobcontract.Task) { t.Hosts = nil },
		"too many hosts":     func(t *jobcontract.Task) { t.Hosts = make([]jobcontract.TaskHost, jobcontract.MaxHosts+1) },
		"address option":     func(t *jobcontract.Task) { t.Hosts[0].Address = "-oProxyCommand=id" },
		"address spaces":     func(t *jobcontract.Task) { t.Hosts[0].Address = "10.0.0.1 ansible_connection=local" },
		"address not normal": func(t *jobcontract.Task) { t.Hosts[0].Address = "WEB-1.Example.com" },
		"duplicate address":  func(t *jobcontract.Task) { t.Hosts[1].Address = "10.0.0.1" },
		"port":               func(t *jobcontract.Task) { t.Hosts[0].Port = 70000 },
		"host var name":      func(t *jobcontract.Task) { t.Hosts[0].Vars["ansible_connection"] = "local" },
		"var name ansible":   func(t *jobcontract.Task) { t.ExtraVars["ansible_ssh_common_args"] = "-oProxyCommand=id" },
		"var name cmdb":      func(t *jobcontract.Task) { t.ExtraVars["cmdb_env"] = "x" },
		"var name reserved":  func(t *jobcontract.Task) { t.ExtraVars["hostvars"] = "x" },
		"var name upper":     func(t *jobcontract.Task) { t.ExtraVars["Bad"] = "x" },
		"too many vars": func(t *jobcontract.Task) {
			for i := range jobcontract.MaxParams + 1 {
				t.ExtraVars["v"+strings.Repeat("x", i+1)] = 1
			}
		},
		"forks":             func(t *jobcontract.Task) { t.Options.Forks = 1000 },
		"verbosity":         func(t *jobcontract.Task) { t.Options.Verbosity = 9 },
		"credential type":   func(t *jobcontract.Task) { t.Credential.Type = 9 },
		"username option":   func(t *jobcontract.Task) { t.Credential.Username = "-oProxyCommand=id" },
		"username space":    func(t *jobcontract.Task) { t.Credential.Username = "ops x" },
		"empty secret":      func(t *jobcontract.Task) { t.Credential.Secret = "" },
		"become method":     func(t *jobcontract.Task) { t.Credential.BecomeMethod = "doas; id" },
		"become user":       func(t *jobcontract.Task) { t.Credential.BecomeUser = "root\nx" },
		"git type":          func(t *jobcontract.Task) { t.Git.Type = 7 },
		"token over ssh":    func(t *jobcontract.Task) { t.RepoURL = "git@git.example.com:ops/infra.git" },
		"token missing":     func(t *jobcontract.Task) { t.Git.Secret = "" },
		"git user newline":  func(t *jobcontract.Task) { t.Git.Username = "x\r\nX-Evil: 1" },
		"ssh key over http": func(t *jobcontract.Task) { t.Git.Type = jobcontract.GitAuthSSHKey },
	}
	for name, mutate := range cases {
		task := sampleTask()
		mutate(task)
		err := validateTask(task)
		if err == nil {
			t.Errorf("%s: task accepted", name)
			continue
		}
		for _, secret := range []string{"login-pass", "become-{{pass}}", "git-token-123"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: error leaks a secret: %v", name, err)
			}
		}
	}
	// 未设置的选项按缺省值算，不算不合法；校验不改任务本身。
	task := sampleTask()
	task.Options = jobcontract.Options{}
	if err := validateTask(task); err != nil || task.Options.Forks != 0 {
		t.Fatalf("zero options: err %v, options %+v", err, task.Options)
	}
}

// untrusted 测试里表示「带 !unsafe 标记的字符串」。
type untrusted string

func unsafeOf(s string) untrusted { return untrusted(s) }

// decodeVars 用 YAML 解析器读回变量文件：带 !unsafe 标记的字符串还原为 untrusted，其余标量按 YAML 的
// 类型推断（整数为 float64，与 encoding/json 的习惯一致，便于写期望值）。
func decodeVars(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("invalid YAML: %v\n%s", err, raw)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 {
		t.Fatalf("unexpected document: %s", raw)
	}
	out, ok := nodeValue(t, doc.Content[0]).(map[string]any)
	if !ok {
		t.Fatalf("top level is not a mapping: %s", raw)
	}
	return out
}

func nodeValue(t *testing.T, n *yaml.Node) any {
	t.Helper()
	switch n.Kind {
	case yaml.MappingNode:
		out := make(map[string]any, len(n.Content)/2)
		for i := 0; i < len(n.Content); i += 2 {
			if n.Content[i].Tag != "!!str" {
				t.Fatalf("mapping key %q has tag %s", n.Content[i].Value, n.Content[i].Tag)
			}
			out[n.Content[i].Value] = nodeValue(t, n.Content[i+1])
		}
		return out
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			out = append(out, nodeValue(t, c))
		}
		return out
	case yaml.ScalarNode:
		switch n.Tag {
		case "!unsafe":
			return untrusted(n.Value)
		case "!!str":
			return n.Value
		case "!!null":
			return nil
		}
		var v any
		if err := n.Decode(&v); err != nil {
			t.Fatalf("decode scalar %q (%s): %v", n.Value, n.Tag, err)
		}
		switch x := v.(type) {
		case int:
			return float64(x)
		case int64:
			return float64(x)
		}
		return v
	}
	t.Fatalf("unexpected node kind %d", n.Kind)
	return nil
}

func TestEncodeVars(t *testing.T) {
	nasty := []string{
		"", "plain", "{{ lookup('pipe', 'id') }}", "{% raw %}", `quote " backslash \ slash /`, "tab\tnewline\ncr\r", "nul\x00bell\x07esc\x1b",
		"del\x7fnel\u0085c1\u009f", "ls\u2028ps\u2029", "bom\ufeffnonchar\ufffe\uffff", "emoji 😀 中文", "bad\xffutf8",
		"!unsafe tag", "- not a list", "key: value", "# comment", "'single'", "yes", "null", "~", "1e3", "0x10", "@at `tick` %pct &amp *star",
		" leading and trailing ", "multi\n  line\n",
	}
	list := make([]any, 0, len(nasty))
	for _, s := range nasty {
		list = append(list, s)
	}
	in := map[string]any{
		"strings": list, "trusted": plain("-o A=b {{ not a template here }}"),
		"int": json.Number("3"), "neg": json.Number("-12"), "big": json.Number("12345678901234567890"), "frac": json.Number("1.50"),
		"exp": json.Number("1e+21"), "small": json.Number("2.5E-3"), "f64": 2.5, "i": 7, "i32": int32(8), "i64": int64(9),
		"yes": true, "no": false, "nothing": nil, "empty_list": []any{}, "empty_map": map[string]any{},
		"nested":                        map[string]any{"list": []any{map[string]any{"k": "v"}, json.Number("1")}},
		"key with \"quote\" and: colon": "x",
	}
	raw, err := encodeVars(in)
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.Valid(raw) || bytes.ContainsAny(raw[:len(raw)-1], "\n\r\x00\x7f\u0085") {
		t.Fatalf("output must be single-line printable UTF-8: %q", raw)
	}
	got := decodeVars(t, raw)
	wantStrings := make([]any, 0, len(nasty))
	for _, s := range nasty {
		wantStrings = append(wantStrings, unsafeOf(strings.ToValidUTF8(s, "\ufffd")))
	}
	want := map[string]any{
		"strings": wantStrings, "trusted": "-o A=b {{ not a template here }}",
		"int": float64(3), "neg": float64(-12), "big": uint64(12345678901234567890), "frac": 1.5,
		"exp": 1e21, "small": 0.0025, "f64": 2.5, "i": float64(7), "i32": float64(8), "i64": float64(9),
		"yes": true, "no": false, "nothing": nil, "empty_list": []any{}, "empty_map": map[string]any{},
		"nested":                        map[string]any{"list": []any{map[string]any{"k": unsafeOf("v")}, float64(1)}},
		"key with \"quote\" and: colon": unsafeOf("x"),
	}
	if !reflect.DeepEqual(got, want) {
		for k := range want {
			if !reflect.DeepEqual(got[k], want[k]) {
				t.Errorf("%s:\n got %#v\nwant %#v", k, got[k], want[k])
			}
		}
		t.Fatalf("round trip mismatch:\n%s", raw)
	}
	// 结果确定：键按字典序。
	again, _ := encodeVars(in)
	if !bytes.Equal(raw, again) {
		t.Fatal("output is not deterministic")
	}
	for _, bad := range []any{struct{}{}, json.Number("1; rm -rf /"), json.Number("0x10"), map[string]any{"k": []string{"typed slice"}}, math.Inf(1)} {
		if _, err := encodeVars(map[string]any{"v": bad}); err == nil {
			t.Errorf("%#v accepted", bad)
		}
	}
}

// 用 PyYAML（Ansible 的 YAML 解析器）读回变量文件，确认它与 Go 侧的理解一致：每个来自任务的字符串
// 都带 !unsafe 标记且内容原样，数字是数字。没有 python3 / PyYAML 时跳过。
func TestEncodeVarsWithPyYAML(t *testing.T) {
	const script = `
import json, sys, yaml
class Unsafe(str): pass
def construct(loader, node):
    return Unsafe(loader.construct_scalar(node))
yaml.SafeLoader.add_constructor("!unsafe", construct)
def tag(v):
    if isinstance(v, Unsafe): return {"__unsafe__": str(v)}
    if isinstance(v, dict): return {k: tag(x) for k, x in v.items()}
    if isinstance(v, list): return [tag(x) for x in v]
    return v
json.dump(tag(yaml.load(sys.stdin.read(), Loader=yaml.SafeLoader)), sys.stdout)
`
	python := os.Getenv("JOB_TEST_PYTHON") // 指定带 PyYAML 的解释器；缺省用 PATH 里的 python3
	if python == "" {
		python = "python3"
	}
	if err := exec.Command(python, "-c", "import yaml").Run(); err != nil {
		t.Skipf("%s with PyYAML not available (set JOB_TEST_PYTHON): %v", python, err)
	}
	task := sampleTask()
	task.ExtraVars["nasty"] = "del\x7f nel\u0085 ls\u2028 quote\" back\\ {{ x }} 😀 \ufffe"
	task.ExtraVars["exp"] = json.Number("1e+21")
	task.ExtraVars["yes"] = "yes"
	inventory, err := buildInventory(task, "/kh")
	if err != nil {
		t.Fatal(err)
	}
	vars, err := buildVars(task)
	if err != nil {
		t.Fatal(err)
	}
	load := func(doc []byte) map[string]any {
		t.Helper()
		cmd := exec.Command(python, "-c", script)
		cmd.Stdin = bytes.NewReader(doc)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("PyYAML rejected the document: %v\n%s", err, doc)
		}
		var v map[string]any
		if err := json.Unmarshal(out, &v); err != nil {
			t.Fatalf("decode PyYAML output: %v\n%s", err, out)
		}
		return v
	}
	u := func(s string) any { return map[string]any{"__unsafe__": s} }
	gotVars := load(vars)
	wantVars := map[string]any{
		"version": u("{{ lookup('pipe', 'id') }}"), "replicas": float64(3), "debug": true,
		"tags_list": []any{u("a"), float64(1)}, "nested": map[string]any{"k": u("v")},
		"nasty": u("del\x7f nel\u0085 ls\u2028 quote\" back\\ {{ x }} 😀 \ufffe"), "exp": 1e21, "yes": u("yes"),
	}
	if !reflect.DeepEqual(gotVars, wantVars) {
		t.Fatalf("PyYAML reads the vars file differently:\n got %#v\nwant %#v\n%s", gotVars, wantVars, vars)
	}
	all := load(inventory)["all"].(map[string]any)
	host := all["hosts"].(map[string]any)["10.0.0.1"].(map[string]any)
	if host["ansible_host"] != "10.0.0.1" || host["ansible_port"] != float64(22) || !reflect.DeepEqual(host["cmdb_name"], u("web {{ 1 }}")) {
		t.Fatalf("PyYAML host vars: %#v", host)
	}
	if _, ok := all["hosts"].(map[string]any)["2001:db8::1"]; !ok {
		t.Fatalf("IPv6 host key lost: %#v", all["hosts"])
	}
	gv := all["vars"].(map[string]any)
	if gv["ansible_become"] != true || gv["ansible_become_method"] != "su" || !reflect.DeepEqual(gv["ansible_user"], u("ops")) ||
		gv["ansible_ssh_common_args"] != "-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/kh" {
		t.Fatalf("PyYAML group vars: %#v", gv)
	}
}

func TestBuildInventory(t *testing.T) {
	raw, err := buildInventory(sampleTask(), "/var/lib/runner/known_hosts")
	if err != nil {
		t.Fatal(err)
	}
	all := decodeVars(t, raw)["all"].(map[string]any)
	wantHosts := map[string]any{
		"10.0.0.1": map[string]any{
			"ansible_host": "10.0.0.1", "ansible_port": float64(22),
			"cmdb_id": unsafeOf("id-1"), "cmdb_name": unsafeOf("web {{ 1 }}"), "cmdb_env": unsafeOf("prod"),
		},
		"2001:db8::1": map[string]any{"ansible_host": "2001:db8::1", "ansible_port": float64(2222)},
	}
	if !reflect.DeepEqual(all["hosts"], wantHosts) {
		t.Fatalf("hosts:\n got %#v\nwant %#v", all["hosts"], wantHosts)
	}
	wantVars := map[string]any{
		"ansible_user":            unsafeOf("ops"),
		"ansible_ssh_common_args": "-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/var/lib/runner/known_hosts",
		"ansible_become":          true,
		"ansible_become_method":   "su",
		"ansible_become_user":     unsafeOf("admin"),
	}
	if !reflect.DeepEqual(all["vars"], wantVars) {
		t.Fatalf("vars:\n got %#v\nwant %#v", all["vars"], wantVars)
	}
	// 凭据不在清单里。
	for _, secret := range []string{"login-pass", "become-", "git-token-123"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("inventory contains a secret: %s", raw)
		}
	}

	// 不提权：没有任何 become 变量；提权但凭据未指定方式 / 用户：sudo 到 root。
	task := sampleTask()
	task.Options.Become = false
	raw, _ = buildInventory(task, "/kh")
	if strings.Contains(string(raw), "become") {
		t.Fatalf("become vars present when become is off: %s", raw)
	}
	task.Options.Become, task.Credential.BecomeMethod, task.Credential.BecomeUser = true, "", ""
	raw, _ = buildInventory(task, "/kh")
	vars := decodeVars(t, raw)["all"].(map[string]any)["vars"].(map[string]any)
	if vars["ansible_become_method"] != "sudo" || !reflect.DeepEqual(vars["ansible_become_user"], unsafeOf("root")) {
		t.Fatalf("become defaults: %#v", vars)
	}
}

func TestBuildVarsAndSecrets(t *testing.T) {
	task := sampleTask()
	raw, err := buildVars(task)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		// 字符串一律标为不可信：{{ … }} 不会被 Ansible 当模板求值。
		"version":   unsafeOf("{{ lookup('pipe', 'id') }}"),
		"replicas":  float64(3),
		"debug":     true,
		"tags_list": []any{unsafeOf("a"), float64(1)},
		"nested":    map[string]any{"k": unsafeOf("v")},
	}
	if got := decodeVars(t, raw); !reflect.DeepEqual(got, want) {
		t.Fatalf("vars:\n got %#v\nwant %#v", got, want)
	}
	if !strings.Contains(string(raw), `"replicas": 3,`) {
		t.Fatalf("integers must stay integers: %s", raw)
	}
	empty := sampleTask()
	empty.ExtraVars = nil
	if raw, _ := buildVars(empty); string(raw) != "{}\n" {
		t.Fatalf("no vars must yield an empty object, got %s", raw)
	}

	raw, err = buildSecrets(task)
	if err != nil {
		t.Fatal(err)
	}
	wantSecrets := map[string]any{"ansible_password": unsafeOf("login-pass"), "ansible_become_password": unsafeOf("become-{{pass}}")}
	if got := decodeVars(t, raw); !reflect.DeepEqual(got, wantSecrets) {
		t.Fatalf("secrets: %#v", got)
	}
	// 私钥凭据 + 不提权：没有口令文件；私钥绝不写进任何文件。
	keyTask := sampleTask()
	keyTask.Credential.Type, keyTask.Credential.Secret, keyTask.Options.Become = jobcontract.CredentialSSHKey, testKey, false
	if raw, _ := buildSecrets(keyTask); raw != nil {
		t.Fatalf("unexpected secrets file: %s", raw)
	}
	// 私钥凭据 + 提权密码：只有提权密码。
	keyTask.Options.Become = true
	raw, _ = buildSecrets(keyTask)
	if got := decodeVars(t, raw); len(got) != 1 || got["ansible_become_password"] == nil || strings.Contains(string(raw), "b3Blbn") {
		t.Fatalf("secrets for key credential: %s", raw)
	}
	// 提权但免密：没有口令文件。
	keyTask.Credential.BecomeSecret = ""
	if raw, _ := buildSecrets(keyTask); raw != nil {
		t.Fatalf("unexpected secrets file: %s", raw)
	}
}

func TestAnsibleArgsAndEnv(t *testing.T) {
	task := sampleTask()
	got := ansibleArgs(task, "/work/job", task.Playbook, true, "")
	want := []string{
		"-i", "/work/job/inventory.json", "deploy/site.yml", "-e", "@/work/job/vars.json", "-e", "@/work/job/secrets.json",
		"--forks", "7", "--check", "--diff", "-vv",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args:\n got %q\nwant %q", got, want)
	}
	task.Options = jobcontract.Options{Forks: 10}
	got = ansibleArgs(task, "/work/job", task.Playbook, false, "")
	want = []string{"-i", "/work/job/inventory.json", "deploy/site.yml", "-e", "@/work/job/vars.json", "--forks", "10"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("minimal args:\n got %q\nwant %q", got, want)
	}
	// 参数取值与凭据绝不出现在命令行里。
	full := strings.Join(ansibleArgs(sampleTask(), "/work/job", "deploy/site.yml", true, ""), " ")
	for _, s := range []string{"lookup", "login-pass", "become-", "git-token"} {
		if strings.Contains(full, s) {
			t.Fatalf("argv contains %q: %s", s, full)
		}
	}

	// 环境从零构造：节点自己的环境变量（接入令牌等）不会传给子进程。
	t.Setenv("KRATOS_RUNNER_TOKEN", "runner-token-value")
	t.Setenv("KRATOS_DATA_DATABASE_SOURCE", "dsn-value")
	t.Setenv("PATH", "/opt/ansible/bin:/usr/bin")
	t.Setenv("LANG", "en_US.UTF-8")
	env := ansibleEnv("/work/job", "/work/job/agent.sock")
	wantEnv := []string{
		"PATH=/opt/ansible/bin:/usr/bin", "HOME=/work/job", "LANG=en_US.UTF-8", "LC_ALL=en_US.UTF-8",
		"ANSIBLE_CONFIG=/work/job/ansible.cfg", "ANSIBLE_FORCE_COLOR=0", "ANSIBLE_NOCOLOR=1", "PYTHONUNBUFFERED=1",
		"SSH_AUTH_SOCK=/work/job/agent.sock",
	}
	if !reflect.DeepEqual(env, wantEnv) {
		t.Fatalf("env:\n got %q\nwant %q", env, wantEnv)
	}
	t.Setenv("LANG", "POSIX")
	if env := ansibleEnv("/work/job", ""); env[2] != "LANG=C.UTF-8" || strings.Contains(strings.Join(env, " "), "SSH_AUTH_SOCK") {
		t.Fatalf("env without agent / non-UTF-8 locale: %q", env)
	}

	cfg := string(buildAnsibleCfg("/work/c0123456789ab"))
	for _, line := range []string{
		"host_key_checking = True", "retry_files_enabled = False", "nocows = True", "stdout_callback = default", "interpreter_python = auto_silent",
		"pipelining = True", "ControlMaster=auto", "control_path_dir = /work/c0123456789ab",
	} {
		if !strings.Contains(cfg, line) {
			t.Fatalf("ansible.cfg lacks %q:\n%s", line, cfg)
		}
	}
}

func TestGitArgsAndEnv(t *testing.T) {
	t.Setenv("KRATOS_RUNNER_TOKEN", "runner-token-value")
	t.Setenv("PATH", "/usr/bin")
	cfg := &Config{GitAllowProtocols: defaultGitProtocols, KnownHosts: "/var/lib/runner/known_hosts"}
	task := sampleTask()

	args := cloneArgs(task)
	want := []string{"clone", "--depth", "1", "--single-branch", "--branch", "release/1.2", "--", "https://git.example.com/ops/infra.git", "repo"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("clone args: %q", args)
	}
	// 令牌只经环境变量里的 git 配置传递：不在命令行、不在仓库地址里。
	if strings.Contains(strings.Join(args, " "), "git-token-123") {
		t.Fatal("token in argv")
	}
	env := gitEnv(cfg, "/work/job", task.Git, "")
	joined := strings.Join(env, "\n")
	for _, line := range []string{
		"HOME=/work/job", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=http:https:ssh", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_COUNT=2", "GIT_CONFIG_KEY_0=http.extraHeader",
		"GIT_CONFIG_VALUE_0=Authorization: Basic " + basicAuth("deploy", "git-token-123"),
		"GIT_CONFIG_KEY_1=http.followRedirects", "GIT_CONFIG_VALUE_1=false",
		"GIT_SSH_COMMAND=ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/var/lib/runner/known_hosts -o BatchMode=yes",
	} {
		if !strings.Contains(joined, line) {
			t.Fatalf("git env lacks %q:\n%s", line, joined)
		}
	}
	if strings.Contains(joined, "runner-token-value") || strings.Contains(joined, "SSH_AUTH_SOCK") {
		t.Fatalf("git env leaks runner environment or has an agent it should not have:\n%s", joined)
	}
	// 用户名为空时用占位用户名（多数平台只看令牌）。
	anon := gitEnv(cfg, "/work/job", jobcontract.GitAuth{Type: jobcontract.GitAuthToken, Secret: "tok-1234"}, "")
	if !strings.Contains(strings.Join(anon, "\n"), "Basic "+basicAuth("git", "tok-1234")) {
		t.Fatalf("anonymous token env: %q", anon)
	}
	// 私钥认证：没有 extraHeader，带 agent。
	ssh := strings.Join(gitEnv(cfg, "/work/job", jobcontract.GitAuth{Type: jobcontract.GitAuthSSHKey, Secret: testKey}, "/work/job/git.sock"), "\n")
	if strings.Contains(ssh, "GIT_CONFIG_COUNT") || !strings.Contains(ssh, "SSH_AUTH_SOCK=/work/job/git.sock") || strings.Contains(ssh, "b3Blbn") {
		t.Fatalf("ssh git env:\n%s", ssh)
	}
}

func TestResolvePlaybook(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(repo, "deploy"), 0o700))
	must(os.WriteFile(filepath.Join(repo, "deploy", "site.yml"), []byte("- hosts: all\n"), 0o600))
	must(os.WriteFile(filepath.Join(root, "outside.yml"), []byte("- hosts: all\n"), 0o600))
	must(os.Symlink("deploy/site.yml", filepath.Join(repo, "link.yml")))
	must(os.Symlink("../outside.yml", filepath.Join(repo, "escape.yml")))
	must(os.Symlink(root, filepath.Join(repo, "updir")))
	must(os.Mkdir(filepath.Join(repo, "dir.yml"), 0o700))

	for _, ok := range []string{"deploy/site.yml", "link.yml"} {
		if err := resolvePlaybook(repo, ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"missing.yml", "escape.yml", "updir/outside.yml", "dir.yml"} {
		if err := resolvePlaybook(repo, bad); err == nil {
			t.Errorf("%s: accepted", bad)
		}
	}
}

func TestNewConfig(t *testing.T) {
	token, _, _ := jobcontract.NewRunnerToken("018f0000-0000-7000-8000-000000000009")
	base := func() *confRunner {
		return &confRunner{EngineUrl: "https://engine.example.com:8051/", Token: token, WorkDir: "/dev/shm/job"}
	}
	cfg, err := NewConfig(base(), "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EngineURL != "https://engine.example.com:8051" || cfg.Capacity != 2 || cfg.AnsiblePlaybook != "ansible-playbook" ||
		cfg.Git != "git" || cfg.SSHAgent != "ssh-agent" || cfg.SSHAdd != "ssh-add" || cfg.KnownHosts != "/dev/shm/job/known_hosts" ||
		cfg.PollInterval != 2*time.Second || cfg.HeartbeatInterval != 10*time.Second || cfg.CancelGrace != 30*time.Second ||
		cfg.DrainTimeout != 10*time.Minute || cfg.GitAllowProtocols != "http:https:ssh" || cfg.Version != "1.0.0" {
		t.Fatalf("defaults: %+v", cfg)
	}
	// 没有经 ldflags 注入版本号时，心跳上报的版本也不为空。
	if noVersion, err := NewConfig(base(), ""); err != nil || noVersion.Version == "" {
		t.Fatalf("version fallback: %+v %v", noVersion, err)
	}
	for in, want := range map[string]string{"": "dev", "(devel)": "dev", "v1.2.3": "v1.2.3"} {
		if got := fallbackVersion(in); got != want {
			t.Errorf("fallbackVersion(%q) = %q, want %q", in, got, want)
		}
	}
	// 工作目录至多 52 字节：其下最长的套接字路径不超过 100 字节。
	long := base()
	long.WorkDir = "/" + strings.Repeat("d", 51)
	if _, err := NewConfig(long, ""); err != nil {
		t.Fatalf("52-byte work dir must be accepted: %v", err)
	}
	if got := controlDirFor("/w", "018f0000-0000-7000-8000-0123456789ab"); got != "/w/c0123456789ab" || !controlDirPattern.MatchString("c0123456789ab") {
		t.Fatalf("control dir = %q", got)
	}
	bad := map[string]func(c *confRunner){
		"no url":          func(c *confRunner) { c.EngineUrl = "" },
		"url scheme":      func(c *confRunner) { c.EngineUrl = "ftp://engine" },
		"url credentials": func(c *confRunner) { c.EngineUrl = "https://u:p@engine" },
		"no token":        func(c *confRunner) { c.Token = "" },
		"bad token":       func(c *confRunner) { c.Token = "abc" },
		"no work dir":     func(c *confRunner) { c.WorkDir = "" },
		"work dir space":  func(c *confRunner) { c.WorkDir = "/tmp/my jobs" },
		"work dir colon":  func(c *confRunner) { c.WorkDir = "/tmp/a:b" },
		"work dir comma":  func(c *confRunner) { c.WorkDir = "/tmp/a,b" },
		"work dir long":   func(c *confRunner) { c.WorkDir = "/" + strings.Repeat("d", 52) },
		"known hosts":     func(c *confRunner) { c.KnownHosts = "/tmp/known hosts" },
		"protocols":       func(c *confRunner) { c.GitAllowProtocols = "http::ssh" },
	}
	for name, mutate := range bad {
		c := base()
		mutate(c)
		_, err := NewConfig(c, "")
		if err == nil {
			t.Errorf("%s: config accepted", name)
			continue
		}
		if strings.Contains(err.Error(), strings.SplitN(token, ".", 2)[1]) {
			t.Errorf("%s: error leaks the token", name)
		}
	}
}
