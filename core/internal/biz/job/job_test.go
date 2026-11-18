package biz

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"strings"
	"testing"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"

	"golang.org/x/crypto/ssh"
)

func testKey(t *testing.T, passphrase string) string {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var block *pem.Block
	if passphrase == "" {
		block, err = ssh.MarshalPrivateKey(priv, "")
	} else {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte(passphrase))
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(block))
}

func TestNormalizePrivateKey(t *testing.T) {
	t.Parallel()
	key := testKey(t, "")
	// Windows 换行、首尾空白、缺结尾换行都规整为同一份。
	messy := "  \r\n" + strings.ReplaceAll(strings.TrimSpace(key), "\n", "\r\n")
	got, fp, err := normalizePrivateKey(messy)
	if err != nil || got != key || !strings.HasPrefix(fp, "SHA256:") {
		t.Fatalf("normalizePrivateKey() = %q, %q, %v", got, fp, err)
	}
	for name, in := range map[string]string{"blank": "  ", "garbage": "not a key", "passphrase": testKey(t, "secret-pass")} {
		_, _, err := normalizePrivateKey(in)
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		// 错误说明不得带出私钥内容。
		if strings.Contains(err.Error(), "BEGIN") || strings.Contains(err.Error(), "secret-pass") {
			t.Fatalf("%s: error leaks key material: %v", name, err)
		}
	}
	if _, _, err := normalizePrivateKey(testKey(t, "x")); err == nil || !strings.Contains(err.Error(), "passphrase") {
		t.Fatalf("passphrase-protected key: %v", err)
	}
}

func TestResolveParams(t *testing.T) {
	t.Parallel()
	defs := []jc.Param{
		{Code: "version", Name: "v", Type: jc.ParamString, Required: true},
		{Code: "replicas", Name: "n", Type: jc.ParamInt, Default: "2"},
		{Code: "ratio", Name: "r", Type: jc.ParamFloat, Default: "0.5"},
		{Code: "mode", Name: "m", Type: jc.ParamEnum, Options: []string{"rolling", "all"}, Default: "rolling"},
		{Code: "force", Name: "f", Type: jc.ParamBool, Default: "true"},
		{Code: "note", Name: "t", Type: jc.ParamText},
		{Code: "dry", Name: "d", Type: jc.ParamBool},
	}
	if err := validateParams(defs); err != nil {
		t.Fatalf("validateParams() error = %v", err)
	}
	got, err := resolveParams(defs, map[string]any{"version": "1.0", "replicas": float64(5), "note": nil})
	if err != nil {
		t.Fatalf("resolveParams() error = %v", err)
	}
	if got["version"] != "1.0" || got["replicas"] != int64(5) || got["ratio"] != 0.5 || got["mode"] != "rolling" || got["force"] != true {
		t.Fatalf("resolveParams() = %#v", got)
	}
	if got["dry"] != false {
		t.Fatalf("unset switch must resolve to false: %#v", got)
	}
	if _, ok := got["note"]; ok {
		t.Fatalf("unset optional param present: %#v", got)
	}
	bad := map[string]map[string]any{
		"missing required": {},
		"blank required":   {"version": ""},
		"wrong type":       {"version": "1", "replicas": "x"},
		"fraction for int": {"version": "1", "replicas": 1.5},
		"not an option":    {"version": "1", "mode": "yolo"},
		"undefined key":    {"version": "1", "ansible_become": true},
		"nested value":     {"version": map[string]any{"a": 1}},
		"too long":         {"version": strings.Repeat("x", 5000)},
	}
	for name, values := range bad {
		if _, err := resolveParams(defs, values); !errorsIs(err, ErrParamsInvalid) {
			t.Errorf("%s: err = %v, want ErrParamsInvalid", name, err)
		}
	}
	for name, defs := range map[string][]jc.Param{
		"reserved":     {{Code: "ansible_user", Name: "x", Type: jc.ParamString}},
		"duplicate":    {{Code: "a", Name: "x", Type: jc.ParamString}, {Code: "a", Name: "y", Type: jc.ParamInt}},
		"enum empty":   {{Code: "a", Name: "x", Type: jc.ParamEnum}},
		"bad default":  {{Code: "a", Name: "x", Type: jc.ParamBool, Default: "yes"}},
		"unknown type": {{Code: "a", Name: "x", Type: "json"}},
		"no name":      {{Code: "a", Type: jc.ParamString}},
	} {
		if err := validateParams(defs); err == nil {
			t.Errorf("validateParams(%s) accepted", name)
		}
	}
}

func TestBuildHosts(t *testing.T) {
	t.Parallel()
	hosts := buildHosts([]Resource{
		{ID: "1", Name: "a", Address: " 10.0.0.1 "},
		{ID: "2", Name: "b", Address: "10.0.0.1"},
		{ID: "3", Name: "c", Address: ""},
		{ID: "4", Name: "d", Address: "-oProxyCommand=id"},
		{ID: "5", Name: "e", Address: "Web-5.Example.com"},
		{ID: "6", Name: "f", Address: "x\ny" + strings.Repeat("z", 200)},
	})
	want := []struct{ addr, problem string }{
		{"10.0.0.1", ""},
		{"10.0.0.1", ProblemAddressDuplicate},
		{"", ProblemAddressEmpty},
		{"-oProxyCommand=id", ProblemAddressInvalid},
		{"web-5.example.com", ""},
	}
	for i, w := range want {
		if hosts[i].Address != w.addr || hosts[i].Problem != w.problem {
			t.Errorf("host %d = %+v, want %+v", i, hosts[i], w)
		}
	}
	if h := hosts[5]; h.Problem != ProblemAddressInvalid || strings.ContainsAny(h.Address, "\n\r") || len([]rune(h.Address)) > 65 {
		t.Errorf("unprintable address not sanitized: %q", h.Address)
	}
	// 收窄后重新判定重复：只剩一台时不再算重复。
	if got := recheckDuplicates([]Host{hosts[1]}); got[0].Problem != "" {
		t.Errorf("recheckDuplicates kept a stale duplicate flag: %+v", got[0])
	}
}

type fakeExecutions struct {
	exec   *Execution
	chunks []string
}

func (f *fakeExecutions) List(context.Context, ExecutionFilter, int64, int64) ([]*Execution, int64, error) {
	return nil, 0, nil
}

func (f *fakeExecutions) Get(context.Context, string) (*Execution, error) {
	e := *f.exec
	return &e, nil
}

func (f *fakeExecutions) ListHosts(context.Context, string, ExecutionHostFilter, int64, int64) ([]*ExecutionHost, int64, error) {
	return nil, 0, nil
}

func (f *fakeExecutions) ReadLog(_ context.Context, _ string, afterSeq int64, maxBytes int) (string, int64, error) {
	var b strings.Builder
	var last int64
	for i, c := range f.chunks {
		if seq := int64(i + 1); seq > afterSeq && c != "" && b.Len() < maxBytes {
			b.WriteString(c)
			last = seq
		}
	}
	return b.String(), last, nil
}

type fakeTemplates struct{ tpl *Template }

func (f fakeTemplates) List(context.Context, TemplateFilter, int64, int64) ([]*Template, int64, error) {
	return nil, 0, nil
}

func (f fakeTemplates) Get(context.Context, string) (*Template, error) { return f.tpl, nil }

// 存量模板的参数名在契约收紧后变成了保留名：提交时就拒绝并说明原因，不会进入执行队列。
func TestSubmitRejectsStaleParamDefinitions(t *testing.T) {
	t.Parallel()
	tpl := &Template{ID: "t", Name: "deploy", Enabled: true, Params: []jc.Param{{Code: "strategy", Name: "策略", Type: jc.ParamString}}}
	uc := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{})
	_, err := uc.Submit(context.Background(), "u", SubmitInput{TemplateID: "t", Params: map[string]any{"strategy": "rolling"}})
	if !errorsIs(err, ErrTemplateInvalid) || !strings.Contains(err.Error(), "strategy") {
		t.Fatalf("Submit() error = %v, want ErrTemplateInvalid naming the param", err)
	}
	tpl.Enabled = false
	if _, err := uc.Submit(context.Background(), "u", SubmitInput{TemplateID: "t"}); !errorsIs(err, ErrTemplateDisabled) {
		t.Fatalf("disabled template: %v", err)
	}
}

type noNames struct{}

func (noNames) UserNames(context.Context, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

// 日志游标：大日志分页时 More 为真且游标停在读到的最后一块；读完或遇到截断的空洞时游标推到最大序号。
func TestExecutionLogCursor(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", logPageBytes/2+1)
	f := &fakeExecutions{exec: &Execution{ID: "e", Status: jc.StatusRunning, LogSeq: 5, LogTruncated: true}, chunks: []string{big, big, big, "", ""}}
	uc := NewExecutionUsecase(f, f, nil, nil, nil, nil, nil, nil, noNames{}, ExecutionDeps{})
	ctx := context.Background()

	page, err := uc.Log(ctx, "e", 0)
	if err != nil || !page.More || page.LastSeq != 2 || len(page.Content) != 2*len(big) {
		t.Fatalf("first page = more %v, last %d, %d bytes, %v", page.More, page.LastSeq, len(page.Content), err)
	}
	page, err = uc.Log(ctx, "e", page.LastSeq)
	if err != nil || page.More || page.LastSeq != 5 || len(page.Content) != len(big) || !page.Truncated {
		t.Fatalf("second page = more %v, last %d, %d bytes, %v", page.More, page.LastSeq, len(page.Content), err)
	}
	page, err = uc.Log(ctx, "e", 5)
	if err != nil || page.More || page.LastSeq != 5 || page.Content != "" || page.Status != jc.StatusRunning {
		t.Fatalf("no news = %+v, %v", page, err)
	}
}
