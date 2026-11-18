package jobcontract

import "testing"

func TestNormalizeAddress(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"10.0.0.1", "10.0.0.1", true},
		{" 10.0.0.1 ", "10.0.0.1", true},
		{"2001:DB8::1", "2001:db8::1", true},
		{"Web-01.Example.com", "web-01.example.com", true},
		{"a", "a", true},
		{"", "", false},
		{"-oProxyCommand=x", "", false},
		{"host name", "", false},
		{"host;rm", "", false},
		{"fe80::1%eth0", "", false},
		{"host.", "", false},
		{"a\nb", "", false},
		{"all", "", false},
		{"Ungrouped", "", false},
		{"localhost", "", false},
	}
	for _, c := range cases {
		got, ok := NormalizeAddress(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("NormalizeAddress(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestValidPlaybook(t *testing.T) {
	t.Parallel()
	for p, want := range map[string]bool{
		"site.yml": true, "deploy/app.yaml": true, "a/b/c.yml": true, "_x.yml": true,
		"": false, "/etc/x.yml": false, "../x.yml": false, "a/../x.yml": false, "a//x.yml": false,
		"-e.yml": false, "x.sh": false, "a/./x.yml": false, "x y.yml": false, ".hidden.yml": false,
	} {
		if got := ValidPlaybook(p); got != want {
			t.Errorf("ValidPlaybook(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestValidRef(t *testing.T) {
	t.Parallel()
	for ref, want := range map[string]bool{
		"main": true, "release/1.2": true, "v1.0.0": true,
		"": false, "-x": false, "a..b": false, "a b": false, "x/": false, "x.lock": false, "--upload-pack=x": false,
	} {
		if got := ValidRef(ref); got != want {
			t.Errorf("ValidRef(%q) = %v, want %v", ref, got, want)
		}
	}
}

func TestValidVarName(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]bool{
		"version": true, "app_port": true, "v2": true,
		"": false, "Version": false, "1x": false, "ansible_user": false, "ansible_ssh_common_args": false,
		"cmdb_id": false, "hostvars": false, "environment": false, "a-b": false,
		// Play / 任务关键字与模板内置函数：Ansible 会告警「reserved name」。
		"strategy": false, "serial": false, "port": false, "timeout": false, "tags": false, "name": false,
		"when": false, "lookup": false, "item": false, "role_names": false,
		"deploy_strategy": true, "release_tag": true,
	} {
		if got := ValidVarName(name); got != want {
			t.Errorf("ValidVarName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestParseRepoURL(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]RepoScheme{
		"https://git.example.com/ops/playbooks.git": RepoHTTP,
		"http://10.0.0.5/ops/playbooks":             RepoHTTP,
		"ssh://git@git.example.com:2222/ops/p.git":  RepoSSH,
		"git@git.example.com:ops/playbooks.git":     RepoSSH,
		"":                                          RepoInvalid,
		"/srv/repo":                                 RepoInvalid,
		"file:///srv/repo":                          RepoInvalid,
		"git://git.example.com/x.git":               RepoInvalid,
		"ext::sh -c id":                             RepoInvalid,
		"ext::id":                                   RepoInvalid,
		"https://user:pw@git.example.com/x.git":     RepoInvalid,
		"https://git.example.com/x.git?a=1":         RepoInvalid,
		"https://git.example.com":                   RepoInvalid,
		"--upload-pack=id":                          RepoInvalid,
		"ssh://git:pw@git.example.com/x.git":        RepoInvalid,
		"-oProxyCommand=id@host:x":                  RepoInvalid,
	} {
		if got := ParseRepoURL(raw); got != want {
			t.Errorf("ParseRepoURL(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestOptionsNormalize(t *testing.T) {
	t.Parallel()
	var o Options
	if err := o.Normalize(); err != nil {
		t.Fatal(err)
	}
	if o.Forks != DefaultForks || o.TimeoutS != DefaultTimeoutS || o.Port != DefaultPort {
		t.Fatalf("defaults not applied: %+v", o)
	}
	for _, bad := range []Options{{Forks: 101}, {TimeoutS: 10}, {Port: 70000}, {Verbosity: 4}, {Forks: -1}} {
		if err := bad.Normalize(); err == nil {
			t.Errorf("Normalize(%+v) accepted an out-of-range value", bad)
		}
	}
}

func TestFinalStatus(t *testing.T) {
	t.Parallel()
	ok := HostResult{Address: "a", OK: 3}
	changed := HostResult{Address: "b", OK: 3, Changed: 1}
	failed := HostResult{Address: "c", OK: 1, Failed: 1}
	down := HostResult{Address: "d", Unreachable: 1}
	cases := []struct {
		name    string
		outcome string
		exit    int32
		hosts   []HostResult
		want    int32
	}{
		{"all ok", OutcomeExited, 0, []HostResult{ok, changed}, StatusSuccess},
		{"no recap", OutcomeExited, 0, nil, StatusSuccess},
		{"all failed", OutcomeExited, 2, []HostResult{failed}, StatusFailed},
		{"partial", OutcomeExited, 2, []HostResult{ok, failed}, StatusPartial},
		{"partial unreachable", OutcomeExited, 4, []HostResult{changed, down}, StatusPartial},
		{"nonzero without recap", OutcomeExited, 1, nil, StatusFailed},
		{"nonzero but hosts fine", OutcomeExited, 1, []HostResult{ok}, StatusFailed},
		{"zero exit with failed host", OutcomeExited, 0, []HostResult{ok, failed}, StatusPartial},
		{"canceled", OutcomeCanceled, -1, []HostResult{ok}, StatusCanceled},
		{"timeout", OutcomeTimeout, -1, nil, StatusTimeout},
		{"error", OutcomeError, 0, nil, StatusFailed},
		{"garbage", "???", 0, nil, StatusFailed},
	}
	for _, c := range cases {
		if got := FinalStatus(c.outcome, c.exit, c.hosts); got != c.want {
			t.Errorf("%s: FinalStatus = %d, want %d", c.name, got, c.want)
		}
	}
	if s := (HostResult{Skipped: 2}).Status(); s != HostSkipped {
		t.Errorf("skipped-only host status = %d", s)
	}
}

func TestRunnerToken(t *testing.T) {
	t.Parallel()
	const id = "018f3e1a-0000-7000-8000-000000000001"
	token, hash, err := NewRunnerToken(id)
	if err != nil {
		t.Fatal(err)
	}
	gotID, secret, ok := ParseRunnerToken(token)
	if !ok || gotID != id {
		t.Fatalf("ParseRunnerToken(%q) = %q, _, %v", token, gotID, ok)
	}
	if !RunnerSecretMatches(secret, hash) {
		t.Fatal("secret does not match its own hash")
	}
	if RunnerSecretMatches(secret+"x", hash) {
		t.Fatal("altered secret matched")
	}
	other, _, _ := NewRunnerToken(id)
	if other == token {
		t.Fatal("two tokens are identical")
	}
	for _, bad := range []string{"", "abc", id, id + ".", id + ".short", "x." + secret} {
		if _, _, ok := ParseRunnerToken(bad); ok {
			t.Errorf("ParseRunnerToken(%q) accepted", bad)
		}
	}
}

func TestAdhocValidate(t *testing.T) {
	t.Parallel()
	good := []struct {
		kind int32
		a    Adhoc
	}{
		{KindCommand, Adhoc{Command: "uptime"}},
		{KindScript, Adhoc{Script: "#!/bin/bash\necho hi\n", Interpreter: InterpreterBash, ScriptArgs: "--force a b"}},
		{KindFile, Adhoc{FileName: "app-1.2.tar.gz", Dest: "/opt/app/releases", Mode: "0644", FileSize: 10}},
		{KindFile, Adhoc{FileName: "x", Dest: "/", Mode: "0755"}},
	}
	for _, c := range good {
		if err := c.a.Validate(c.kind); err != nil {
			t.Errorf("Validate(%d, %+v) = %v", c.kind, c.a, err)
		}
	}
	bad := []struct {
		name string
		kind int32
		a    Adhoc
	}{
		{"blank command", KindCommand, Adhoc{Command: "  "}},
		{"nul in command", KindCommand, Adhoc{Command: "a\x00b"}},
		{"long command", KindCommand, Adhoc{Command: string(make([]byte, MaxCommandLen+1))}},
		{"blank script", KindScript, Adhoc{Interpreter: InterpreterSh}},
		{"unknown interpreter", KindScript, Adhoc{Script: "x", Interpreter: "perl"}},
		{"multiline args", KindScript, Adhoc{Script: "x", Interpreter: InterpreterSh, ScriptArgs: "a\nb"}},
		{"file name with slash", KindFile, Adhoc{FileName: "a/b", Dest: "/tmp", Mode: "0644"}},
		{"hidden file name", KindFile, Adhoc{FileName: ".bashrc", Dest: "/tmp", Mode: "0644"}},
		{"option-like file name", KindFile, Adhoc{FileName: "-x", Dest: "/tmp", Mode: "0644"}},
		{"relative dest", KindFile, Adhoc{FileName: "a", Dest: "tmp", Mode: "0644"}},
		{"dest traversal", KindFile, Adhoc{FileName: "a", Dest: "/opt/../etc", Mode: "0644"}},
		{"dest with space", KindFile, Adhoc{FileName: "a", Dest: "/opt/my dir", Mode: "0644"}},
		{"setuid mode", KindFile, Adhoc{FileName: "a", Dest: "/tmp", Mode: "4755"}},
		{"bad mode", KindFile, Adhoc{FileName: "a", Dest: "/tmp", Mode: "644"}},
		{"too large", KindFile, Adhoc{FileName: "a", Dest: "/tmp", Mode: "0644", FileSize: MaxFileBytes + 1}},
		{"playbook kind", KindPlaybook, Adhoc{Command: "x"}},
	}
	for _, c := range bad {
		if err := c.a.Validate(c.kind); err == nil {
			t.Errorf("%s: accepted", c.name)
		}
	}
}

func TestKindsStatusesAndRollingOptions(t *testing.T) {
	t.Parallel()
	if (Spec{}).JobKind() != KindPlaybook || (Spec{Kind: KindScript}).JobKind() != KindScript {
		t.Fatal("JobKind does not normalize the zero value")
	}
	if !ValidKind(KindFile) || ValidKind(0) || ValidKind(5) {
		t.Fatal("ValidKind range is wrong")
	}
	if !ValidStatus(StatusRejected) || ValidStatus(11) || !Active(StatusPending) || Active(StatusRejected) || Active(StatusUnknown) {
		t.Fatal("status helpers are wrong")
	}
	o := Options{BatchSize: 0, MaxFailPercent: 30}
	if err := o.Normalize(); err != nil || o.MaxFailPercent != 0 {
		t.Fatalf("fail threshold must be cleared without batching: %+v, %v", o, err)
	}
	o = Options{BatchSize: 5, MaxFailPercent: 30}
	if err := o.Normalize(); err != nil || o.BatchSize != 5 || o.MaxFailPercent != 30 {
		t.Fatalf("rolling options changed: %+v, %v", o, err)
	}
	for _, bad := range []Options{{BatchSize: -1}, {BatchSize: MaxHosts + 1}, {BatchSize: 1, MaxFailPercent: 101}, {BatchSize: 1, MaxFailPercent: -1}} {
		if err := bad.Normalize(); err == nil {
			t.Errorf("Normalize(%+v) accepted", bad)
		}
	}
}
