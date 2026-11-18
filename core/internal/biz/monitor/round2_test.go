package biz_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 评审第一轮（docs/monitor-review-round1.md）各项的回归测试。

// ---------------------------------------------------------------------------
// C2 / C11 数据源：沿用已保存的凭据与请求头
// ---------------------------------------------------------------------------

// savedSource 建一个带 Bearer 凭据与一个请求头的数据源。
func savedSource(t *testing.T, e *env) *biz.Datasource {
	t.Helper()
	token := "tok-secret"
	ds, err := e.sources.Create(context.Background(), uAdmin, biz.DatasourceInput{
		Name: "vm", Enabled: true, Secret: &token,
		Conn: biz.Connection{URL: "http://vm:8428/select/0", AuthType: mc.AuthBearer, Headers: map[string]string{"X-API-Key": "key-1", "X-Scope": "team-a"}},
	})
	if err != nil {
		t.Fatalf("create datasource: %v", err)
	}
	return ds
}

// originCases 与已保存地址 http://vm:8428/select/0 相比：同源的可以沿用，其余必须重新输入。
var originCases = []struct {
	url  string
	same bool
}{
	{"http://vm:8428/select/0", true},
	{"http://vm:8428", true},                     // 路径不同
	{"http://VM:8428/other/prefix/", true},       // 主机不区分大小写
	{"http://evil.example:8428/select/0", false}, // 主机不同
	{"http://vm:9090/select/0", false},           // 端口不同
	{"http://vm/select/0", false},                // 缺省端口 80 ≠ 8428
	{"https://vm:8428/select/0", false},          // 协议不同
	{"http://vm.evil.example:8428", false},       // 以原主机名为前缀的另一台主机
	{"http://vm:8428@evil.example/", false},      // userinfo 里藏着原地址
}

func TestDatasource_TestConfigReusesSecretOnlyOnSameOrigin(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ds := savedSource(t, e)
	for _, c := range originCases {
		before := len(e.query.Conns())
		_, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: c.url, AuthType: mc.AuthBearer}, nil, ds.ID)
		conns := e.query.Conns()
		if c.same {
			if err != nil || len(conns) != before+1 || conns[before].Secret != "tok-secret" {
				t.Errorf("%s: same origin must reuse the saved secret: err=%v", c.url, err)
			}
			continue
		}
		if !kerrors.Is(err, biz.ErrDatasourceSecretRequired) && !kerrors.Is(err, biz.ErrDatasourceInvalid) {
			t.Errorf("%s: err = %v, want MONITOR_DATASOURCE_SECRET_REQUIRED", c.url, err)
		}
		if len(conns) != before {
			t.Errorf("%s: the saved secret was sent to a different origin", c.url)
		}
	}
	// 给了新凭据就与已保存的无关：任何地址都可以测。
	fresh := "fresh-token"
	if _, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://evil.example:8428", AuthType: mc.AuthBearer}, &fresh, ds.ID); err != nil {
		t.Fatalf("a freshly entered secret may go anywhere: %v", err)
	}
	if conns := e.query.Conns(); conns[len(conns)-1].Secret != fresh {
		t.Fatalf("the entered secret must be used, not the saved one")
	}
	// 不需要凭据的配置同样不受限。
	if _, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://elsewhere:9090", AuthType: mc.AuthNone}, nil, ds.ID); err != nil {
		t.Fatalf("auth=none: %v", err)
	}
	if conns := e.query.Conns(); conns[len(conns)-1].Secret != "" {
		t.Fatalf("auth=none must not carry the saved secret")
	}
}

func TestDatasource_TestConfigReuseRequiresEditPermission(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ds := savedSource(t, e)
	e.org.DatasourceEditors = []string{uAlice}
	same := biz.Connection{URL: "http://vm:8428", AuthType: mc.AuthBearer}
	// bob 只有新建权限（接口同时归属「新建」元素）：不能借已保存的凭据发请求。
	before := len(e.query.Conns())
	_, err := e.sources.TestConfig(ctx, uBob, same, nil, ds.ID)
	wantErr(t, "reuse without edit permission", err, biz.ErrDatasourceSecretRequired)
	masked := biz.Connection{URL: "http://vm:8428", AuthType: mc.AuthNone, Headers: map[string]string{"X-API-Key": biz.HeaderMask}}
	_, err = e.sources.TestConfig(ctx, uBob, masked, nil, ds.ID)
	wantErr(t, "reuse masked header without edit permission", err, biz.ErrDatasourceSecretRequired)
	if len(e.query.Conns()) != before {
		t.Fatalf("no request may be sent")
	}
	// 自己输入凭据不需要编辑权限。
	own := "own"
	if _, err := e.sources.TestConfig(ctx, uBob, same, &own, ds.ID); err != nil {
		t.Fatalf("own secret: %v", err)
	}
	if _, err := e.sources.TestConfig(ctx, uAlice, same, nil, ds.ID); err != nil {
		t.Fatalf("editor reuses: %v", err)
	}
}

func TestDatasource_UpdateKeepsSecretOnlyOnSameOrigin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, c := range originCases {
		e := newEnv(t)
		ds := savedSource(t, e)
		in := biz.DatasourceInput{Name: "vm", Enabled: true, Conn: biz.Connection{URL: c.url, AuthType: mc.AuthBearer}}
		got, err := e.sources.Update(ctx, uAdmin, ds.ID, in, ds.Version)
		if c.same {
			if err != nil || !got.HasSecret {
				t.Errorf("%s: same origin keeps the secret: %+v err=%v", c.url, got, err)
			}
			continue
		}
		if !kerrors.Is(err, biz.ErrDatasourceSecretRequired) && !kerrors.Is(err, biz.ErrDatasourceInvalid) {
			t.Errorf("%s: err = %v, want MONITOR_DATASOURCE_SECRET_REQUIRED", c.url, err)
		}
		// 没有落库：地址还是原来的，后续测试连接不会把凭据带到新地址。
		if cur, _ := e.sources.Get(ctx, ds.ID); cur.URL != "http://vm:8428/select/0" || cur.Version != ds.Version {
			t.Errorf("%s: a refused update must not be saved: %+v", c.url, cur)
		}
	}
	// 换地址时重新输入凭据：允许。
	e := newEnv(t)
	ds := savedSource(t, e)
	fresh := "fresh"
	moved := biz.DatasourceInput{Name: "vm", Enabled: true, Secret: &fresh, Conn: biz.Connection{URL: "http://new-host:8428", AuthType: mc.AuthBearer}}
	if _, err := e.sources.Update(ctx, uAdmin, ds.ID, moved, ds.Version); err != nil {
		t.Fatalf("moving with a re-entered secret: %v", err)
	}
	if _, err := e.sources.Test(ctx, ds.ID); err != nil {
		t.Fatal(err)
	}
	if conns := e.query.Conns(); conns[len(conns)-1].Secret != fresh || conns[len(conns)-1].URL != "http://new-host:8428" {
		t.Fatalf("the new host must get the new secret only")
	}
	// 换地址并改为无认证：凭据被清除，没有什么可沿用的。
	e = newEnv(t)
	ds = savedSource(t, e)
	open := biz.DatasourceInput{Name: "vm", Enabled: true, Conn: biz.Connection{URL: "http://new-host:8428", AuthType: mc.AuthNone}}
	if got, err := e.sources.Update(ctx, uAdmin, ds.ID, open, ds.Version); err != nil || got.HasSecret {
		t.Fatalf("auth=none on a new host: %+v err=%v", got, err)
	}
}

func TestDatasource_HeadersAreMaskedAndRoundTrip(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	ds := savedSource(t, e)
	if biz.HeaderMask != "******" {
		t.Fatalf("HeaderMask = %q: the literal is part of the API contract", biz.HeaderMask)
	}
	masked := map[string]string{"X-API-Key": "******", "X-Scope": "******"}
	assertMasked := func(what string, d *biz.Datasource) {
		t.Helper()
		if len(d.Headers) != len(masked) {
			t.Fatalf("%s: headers = %v", what, d.Headers)
		}
		for k, v := range d.Headers {
			if v != biz.HeaderMask {
				t.Fatalf("%s: header %s leaked its value %q", what, k, v)
			}
		}
	}
	assertMasked("create response", ds)
	got, _ := e.sources.Get(ctx, ds.ID)
	assertMasked("get", got)
	list, _, _ := e.sources.List(ctx, biz.DatasourceFilter{Keyword: "vm"}, 1, 10)
	for _, d := range list {
		if d.ID == ds.ID {
			assertMasked("list", d)
		}
	}
	sent := func() map[string]string {
		t.Helper()
		if _, err := e.sources.Test(ctx, ds.ID); err != nil {
			t.Fatal(err)
		}
		conns := e.query.Conns()
		return conns[len(conns)-1].Headers
	}
	in := func(headers map[string]string) biz.DatasourceInput {
		return biz.DatasourceInput{Name: "vm", Enabled: true, Conn: biz.Connection{URL: "http://vm:8428/select/0", AuthType: mc.AuthBearer, Headers: headers}}
	}
	// 原样把掩码传回：已保存的值不变（前端的普通编辑保存）。
	upd, err := e.sources.Update(ctx, uAdmin, ds.ID, in(masked), 1)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	assertMasked("update response", upd)
	if h := sent(); h["X-API-Key"] != "key-1" || h["X-Scope"] != "team-a" {
		t.Fatalf("round-tripped masks must keep the stored values, sent %v", h)
	}
	// 改一个、留一个、加一个。
	if _, err := e.sources.Update(ctx, uAdmin, ds.ID, in(map[string]string{"X-API-Key": "key-2", "X-Scope": "******", "X-New": "n"}), 2); err != nil {
		t.Fatal(err)
	}
	if h := sent(); h["X-API-Key"] != "key-2" || h["X-Scope"] != "team-a" || h["X-New"] != "n" {
		t.Fatalf("sent %v", h)
	}
	// 删掉一个键。
	if _, err := e.sources.Update(ctx, uAdmin, ds.ID, in(map[string]string{"X-Scope": "******"}), 3); err != nil {
		t.Fatal(err)
	}
	if h := sent(); len(h) != 1 || h["X-Scope"] != "team-a" {
		t.Fatalf("sent %v", h)
	}
	// 掩码对应的键没有保存过：拒绝，而不是把 6 个星号当成值存下来。
	_, err = e.sources.Update(ctx, uAdmin, ds.ID, in(map[string]string{"X-Unknown": "******"}), 4)
	wantErr(t, "mask for an unknown key", err, biz.ErrDatasourceSecretRequired)
	_, err = e.sources.Create(ctx, uAdmin, biz.DatasourceInput{
		Name: "new", Enabled: true,
		Conn: biz.Connection{URL: "http://x:1", AuthType: mc.AuthNone, Headers: map[string]string{"X-API-Key": "******"}},
	})
	wantErr(t, "mask on create", err, biz.ErrDatasourceSecretRequired)
	_, err = e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://x:1", AuthType: mc.AuthNone, Headers: map[string]string{"X-API-Key": "******"}}, nil, "")
	wantErr(t, "mask when testing an unsaved config", err, biz.ErrDatasourceSecretRequired)
	// 沿用掩码请求头同样要求同源：否则改个地址就能把 X-API-Key 发到别处。
	moved := in(map[string]string{"X-Scope": "******"})
	moved.Conn.URL, moved.Conn.AuthType = "http://evil.example:8428", mc.AuthNone
	_, err = e.sources.Update(ctx, uAdmin, ds.ID, moved, 4)
	wantErr(t, "masked header to another origin (update)", err, biz.ErrDatasourceSecretRequired)
	before := len(e.query.Conns())
	_, err = e.sources.TestConfig(ctx, uAdmin, moved.Conn, nil, ds.ID)
	wantErr(t, "masked header to another origin (test)", err, biz.ErrDatasourceSecretRequired)
	if len(e.query.Conns()) != before {
		t.Fatalf("no request may be sent")
	}
	// 测试连接里的掩码：同源时换回已保存的值。
	if _, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://vm:8428", AuthType: mc.AuthNone, Headers: map[string]string{"X-Scope": "******", "X-Extra": "e"}}, nil, ds.ID); err != nil {
		t.Fatal(err)
	}
	if h := e.query.Conns()[before].Headers; h["X-Scope"] != "team-a" || h["X-Extra"] != "e" {
		t.Fatalf("sent %v", h)
	}
}

// 地址被策略拒绝是调用方要处理的错误（400），不是「连接失败」的测试结果。
func TestDatasource_BlockedAddressIsAnError(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.query.Err = biz.DatasourceAddressBlocked("link-local or metadata address 169.254.169.254 is not allowed")
	_, err := e.sources.TestConfig(ctx, uAdmin, biz.Connection{URL: "http://metadata.example", AuthType: mc.AuthNone}, nil, "")
	wantErr(t, "test config", err, biz.ErrDatasourceAddressBlocked)
	_, err = e.sources.Test(ctx, dsVM)
	wantErr(t, "test saved", err, biz.ErrDatasourceAddressBlocked)
	_, err = e.sources.Query(ctx, dsVM, "up", time.Time{})
	wantErr(t, "query", err, biz.ErrDatasourceAddressBlocked)
	now := time.Now()
	_, err = e.sources.QueryRange(ctx, dsVM, "up", now.Add(-time.Hour), now, time.Minute)
	wantErr(t, "query range", err, biz.ErrDatasourceAddressBlocked)
}

// 写进接口文档的两个字面量（前端依赖它们）必须与代码里的常量一致：proto 无法引用 Go 常量，靠本测试守护。
func TestContractLiteralsMatchProto(t *testing.T) {
	t.Parallel()
	protos, err := os.OpenRoot(filepath.Join("..", "..", "..", "api", "monitor", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = protos.Close() }()
	for file, literal := range map[string]string{
		"datasource.proto": "掩码 `" + biz.HeaderMask + "`",
		"rule.proto":       `"format": "` + biz.RuleExportFormat + `"`,
	} {
		raw, err := protos.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), literal) {
			t.Errorf("%s no longer documents %s", file, literal)
		}
	}
}

// ---------------------------------------------------------------------------
// C6 / C7 引用校验
// ---------------------------------------------------------------------------

func TestRule_NotifyRejectsDisabledUsers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// uGone 是已停用的用户：名字还查得到（历史记录要显示），但不能新加为通知对象。
	s := spec(gHost, "r")
	s.Notify.UserIDs = []string{uAlice, uGone}
	_, err := e.rules.Create(ctx, uAlice, s)
	wantErr(t, "disabled user on create", err, biz.ErrRuleInvalid)
	s.Notify.UserIDs = []string{"u-nobody"}
	_, err = e.rules.Create(ctx, uAlice, s)
	wantErr(t, "unknown user", err, biz.ErrRuleInvalid)
	s.Notify.UserIDs = []string{uAlice, uBob}
	rule := e.mustRule(t, uAlice, s)
	_, err = e.rules.Batch(ctx, uAlice, biz.BatchInput{IDs: []string{rule.ID}, Action: biz.BatchSetNotify, Notify: &mc.NotifyTargets{UserIDs: []string{uGone}}})
	wantErr(t, "disabled user via batch", err, biz.ErrRuleInvalid)
	_, err = e.rules.PreviewImport(ctx, uAlice, biz.ImportOptions{
		Content: importYAML, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 1, Notify: mc.NotifyTargets{UserIDs: []string{uGone}},
	})
	wantErr(t, "disabled user via import options", err, biz.ErrRuleInvalid)

	// bob 之后离职：规则仍能保存（他原本就在里面），但不能再把别的停用用户加进来。
	delete(e.org.Users, uBob)
	e.org.Disabled[uBob] = "Bob"
	next := rule.Spec
	next.Description = "改个说明"
	upd, err := e.rules.Update(ctx, uAlice, rule.ID, next, rule.Version, "")
	if err != nil {
		t.Fatalf("a rule must stay editable after one of its notify users is disabled: %v", err)
	}
	if !upd.NotifyUsers[1].Missing && upd.NotifyUsers[1].Name == "" {
		t.Fatalf("notify users = %+v", upd.NotifyUsers)
	}
	next.Notify.UserIDs = []string{uAlice, uBob, uGone}
	_, err = e.rules.Update(ctx, uAlice, rule.ID, next, upd.Version, "")
	wantErr(t, "adding another disabled user", err, biz.ErrRuleInvalid)
}

func TestSubscription_ReceiversRejectDisabledUsers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	s := subSpec(gHost)
	s.Receivers = mc.Receivers{UserIDs: []string{uGone}}
	_, err := e.subs.Create(ctx, uAlice, s)
	wantErr(t, "disabled receiver", err, biz.ErrSubscriptionInvalid)
	s.Receivers = mc.Receivers{UserIDs: []string{uBob}}
	sub, err := e.subs.Create(ctx, uAlice, s)
	if err != nil {
		t.Fatal(err)
	}
	delete(e.org.Users, uBob)
	e.org.Disabled[uBob] = "Bob"
	s.Name = "改名"
	if _, err := e.subs.Update(ctx, uAlice, sub.ID, s, sub.Version); err != nil {
		t.Fatalf("a subscription must stay editable after a receiver is disabled: %v", err)
	}
}

func TestMuteAndSubscription_RulesMustBeInSelectedGroups(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	hostRule := e.mustRule(t, uAlice, spec(gHost, "host-rule"))
	dbRule := e.mustRule(t, uAlice, spec(gDB, "db-rule"))
	appRule := e.mustRule(t, uCarol, spec(gApp, "app-secret-rule"))
	const missing = "018f3e1a-ffff-7000-8000-00000000dead"

	// 屏蔽规则：别的分组的规则与不存在的规则是同一个错误，且不回显规则名。
	sameMessage := func(what string, errOther, errMissing error, want *kerrors.Error) {
		t.Helper()
		wantErr(t, what+" (other group)", errOther, want)
		wantErr(t, what+" (missing)", errMissing, want)
		if a, b := kerrors.FromError(errOther).Message, kerrors.FromError(errMissing).Message; a != b {
			t.Fatalf("%s: messages differ, which tells the caller the rule exists: %q vs %q", what, a, b)
		}
		if strings.Contains(errOther.Error(), "app-secret-rule") || strings.Contains(errOther.Error(), "db-rule") {
			t.Fatalf("%s: the error echoes a rule name from another group: %v", what, errOther)
		}
	}
	m := onceMute(gHost)
	m.RuleIDs = []string{appRule.ID}
	_, errOther := e.mutes.Create(ctx, uAlice, m)
	m.RuleIDs = []string{missing}
	_, errMissing := e.mutes.Create(ctx, uAlice, m)
	sameMessage("mute", errOther, errMissing, biz.ErrMuteInvalid)
	// 同一棵树里的兄弟分组也不行（alice 对它可管理，但屏蔽规则作用不到它）。
	m.RuleIDs = []string{dbRule.ID}
	_, err := e.mutes.Create(ctx, uAlice, m)
	wantErr(t, "mute on host with a db rule", err, biz.ErrMuteInvalid)
	// 作用分组的下级里的规则可以。
	m = onceMute(gInfra)
	m.RuleIDs = []string{hostRule.ID, dbRule.ID}
	created, err := e.mutes.Create(ctx, uAlice, m)
	if err != nil || len(created.Rules) != 2 || created.Rules[0].Name != "host-rule" {
		t.Fatalf("mute on infra with rules of its subgroups: %+v err=%v", created, err)
	}
	// 编辑同样校验。
	m.RuleIDs = []string{appRule.ID}
	_, err = e.mutes.Update(ctx, uAlice, created.ID, m, created.Version)
	wantErr(t, "mute update", err, biz.ErrMuteInvalid)
	// 全局屏蔽规则（超级管理员）可以限定任何分组的规则。
	g := onceMute("")
	g.RuleIDs = []string{appRule.ID, hostRule.ID}
	if _, err := e.mutes.Create(ctx, uAdmin, g); err != nil {
		t.Fatalf("global mute: %v", err)
	}

	// 订阅：同样的口径。
	s := subSpec(gHost)
	s.RuleIDs = []string{appRule.ID}
	_, errOther = e.subs.Create(ctx, uAlice, s)
	s.RuleIDs = []string{missing}
	_, errMissing = e.subs.Create(ctx, uAlice, s)
	sameMessage("subscription", errOther, errMissing, biz.ErrSubscriptionInvalid)
	s.RuleIDs = []string{dbRule.ID}
	_, err = e.subs.Create(ctx, uAlice, s)
	wantErr(t, "subscription on host with a db rule", err, biz.ErrSubscriptionInvalid)
	s = subSpec(gHost, gDB)
	s.RuleIDs = []string{hostRule.ID, dbRule.ID}
	sub, err := e.subs.Create(ctx, uAlice, s)
	if err != nil || len(sub.Rules) != 2 {
		t.Fatalf("subscription with rules of its own groups: %+v err=%v", sub, err)
	}
	s.RuleIDs = []string{appRule.ID}
	_, err = e.subs.Update(ctx, uAlice, sub.ID, s, sub.Version)
	wantErr(t, "subscription update", err, biz.ErrSubscriptionInvalid)
	// 订阅上级分组时，下级分组里的规则可以。
	s = subSpec(gInfra)
	s.RuleIDs = []string{dbRule.ID}
	if _, err := e.subs.Create(ctx, uBob, s); err != nil {
		t.Fatalf("subscription on infra with a rule of a subgroup: %v", err)
	}
}

// ---------------------------------------------------------------------------
// C8 导入 overwrite
// ---------------------------------------------------------------------------

// movingReader 包住规则读仓储：第 n 次 Get 之前先执行 hook（模拟预览与导入之间别人改了规则）。
type hookedRules struct {
	biz.RuleReader
	hook func(id string)
}

func (h hookedRules) Get(ctx context.Context, id string) (*biz.Rule, error) {
	if h.hook != nil {
		h.hook(id)
	}
	return h.RuleReader.Get(ctx, id)
}

func TestRule_ImportOverwriteRechecksTargetGroup(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	existing := e.mustRule(t, uAlice, spec(gHost, "existing"))
	appSpec := existing.Spec
	appSpec.GroupID = gApp
	// 预览（import 的第一步）判定重名之后、覆盖之前，管理员把那条规则移到了 app 分组——alice 对 app 没有
	// 范围。覆盖不能再落到它头上。
	moved := false
	hook := func(id string) {
		if id != existing.ID || moved {
			return
		}
		moved = true
		if _, err := e.rules.Update(ctx, uAdmin, existing.ID, appSpec, existing.Version, "移到 app"); err != nil {
			t.Errorf("move rule: %v", err)
		}
	}
	rules := biz.NewRuleUsecase(e.scopes, hookedRules{RuleReader: e.store.Rules(), hook: hook}, e.store.Rules(), e.store.Rules(),
		e.store.Versions(), e.store.Evals(), e.store.Datasources(), e.org, e.org)
	opts := biz.ImportOptions{
		Content: "groups:\n- name: g\n  rules:\n  - alert: existing\n    expr: up == 1\n    labels: {severity: critical}\n",
		GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityInfo,
	}
	results, err := rules.Import(ctx, uAlice, opts, biz.ConflictOverwrite, true)
	if err != nil || len(results) != 1 || !moved {
		t.Fatalf("import = %+v err=%v moved=%v", results, err, moved)
	}
	// 被移走的规则原封不动：仍在 app、表达式没变、没有被拉回 host。
	after, err := e.rules.Get(ctx, uAdmin, existing.ID)
	if err != nil || after.Spec.GroupID != gApp || after.Spec.Queries[0].Expr != "up == 0" || after.Version != existing.Version+1 {
		t.Fatalf("the moved rule must be left alone: %+v err=%v", after.Spec, err)
	}
	// 导入的内容作为 host 分组里的新规则落库。
	if results[0].Action != biz.ImportCreated || results[0].RuleID == existing.ID {
		t.Fatalf("result = %+v, want a new rule in the target group", results[0])
	}
	created, err := e.rules.Get(ctx, uAlice, results[0].RuleID)
	if err != nil || created.Spec.GroupID != gHost || created.Spec.Name != "existing" || created.Spec.Queries[0].Expr != "up == 1" {
		t.Fatalf("created = %+v err=%v", created, err)
	}
}

// ---------------------------------------------------------------------------
// C5 模板校验与引擎同一口径
// ---------------------------------------------------------------------------

func TestRule_TemplatesUseTheSharedValidator(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	// 引擎拒绝执行的模板（monitorcontract.ValidateTemplate）在保存时就拦下。
	for name, text := range map[string]string{
		"reassign labels":   `{{$labels = 9000000000000000000}}{{range $labels}}{{end}}`,
		"declare value":     `{{$value := 1}}{{$value}}`,
		"range over int":    `{{range 9000000000000000000}}x{{end}}`,
		"range .Labels":     `{{range .Labels}}x{{end}}`,
		"printf arg index":  `{{printf "%[1]d" $value}}`,
		"printf star width": `{{printf "%*d" 100000 1}}`,
		"define + template": `{{define "x"}}{{len $labels}}{{end}}`,
		"index builtin":     `{{index $labels "a"}}`,
	} {
		if err := mc.ValidateTemplate(text); err == nil {
			t.Fatalf("%s: the shared validator accepts %q; the test premise is wrong", name, text)
		}
		s := spec(gHost, "tpl")
		s.Annotations = map[string]string{"summary": text}
		_, err := e.rules.Create(ctx, uAlice, s)
		wantErr(t, "annotation: "+name, err, biz.ErrRuleInvalid)
		s.Annotations = nil
		s.RecoveryExpr = "up{x=\"" + text + "\"}"
		_, err = e.rules.Create(ctx, uAlice, s)
		wantErr(t, "recovery expr: "+name, err, biz.ErrRuleInvalid)
	}
	// 引擎接受的写法照常能保存。
	ok := spec(gHost, "tpl-ok")
	ok.Annotations = map[string]string{
		"summary": `{{ $labels.instance }} CPU {{ printf "%.1f" $value }}{{ if gt $value 90.0 }} 严重{{ end }}`,
		"all":     `{{range $k, $v := $labels}}{{$k}}={{$v}} {{end}}`,
	}
	ok.RecoveryExpr = `cpu_usage{instance="{{ $labels.instance }}"}`
	if _, err := e.rules.Create(ctx, uAlice, ok); err != nil {
		t.Fatalf("valid templates: %v", err)
	}
	// 导入：附加信息不合规的条目在预览里就标为无法转换，而不是存进去以后渲染成错误占位。
	content := `
groups:
- name: g
  rules:
  - alert: good
    expr: up == 0
    labels: {severity: critical}
    annotations: {summary: "{{ $labels.instance }} down"}
  - alert: bad
    expr: up == 0
    labels: {severity: critical}
    annotations: {summary: "{{ range .Labels }}x{{ end }}"}
`
	items, err := e.rules.PreviewImport(ctx, uAlice, biz.ImportOptions{Content: content, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 1})
	if err != nil || len(items) != 2 || items[0].Status != biz.ImportNew || items[1].Status != biz.ImportInvalid || !strings.Contains(items[1].Error, "summary") {
		t.Fatalf("preview = %+v err=%v", items, err)
	}
	results, err := e.rules.Import(ctx, uAlice, biz.ImportOptions{Content: content, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 1}, biz.ConflictSkip, false)
	if err != nil || results[0].Action != biz.ImportCreated || results[1].Action != biz.ImportFailed {
		t.Fatalf("import = %+v err=%v", results, err)
	}
}

// ---------------------------------------------------------------------------
// C15 删除分组
// ---------------------------------------------------------------------------

func TestGroup_DeleteRefusedWhileReferenced(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	g, err := e.groups.Create(ctx, uAlice, gInfra, "临时", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	mute, err := e.mutes.Create(ctx, uAlice, onceMute(g.ID))
	if err != nil {
		t.Fatal(err)
	}
	s1, err := e.subs.Create(ctx, uAlice, subSpec(g.ID))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := e.subs.Create(ctx, uBob, subSpec(g.ID, gHost))
	if err != nil {
		t.Fatal(err)
	}
	err = e.groups.Delete(ctx, uAlice, g.ID)
	wantErr(t, "delete referenced group", err, biz.ErrGroupInUse)
	ke := kerrors.FromError(err)
	if ke.Code != 409 || ke.Metadata["mute_count"] != "1" || ke.Metadata["subscription_count"] != "2" || !strings.Contains(ke.Message, "1 mute") {
		t.Fatalf("error = %+v, want 409 with the counts", ke)
	}
	if err := e.mutes.Delete(ctx, uAlice, mute.ID); err != nil {
		t.Fatal(err)
	}
	err = e.groups.Delete(ctx, uAlice, g.ID)
	if ke := kerrors.FromError(err); ke.Metadata["mute_count"] != "0" || ke.Metadata["subscription_count"] != "2" {
		t.Fatalf("after removing the mute: %+v", ke)
	}
	if err := e.subs.Delete(ctx, uAlice, s1.ID); err != nil {
		t.Fatal(err)
	}
	// 订阅改为不再订阅这个分组也算解除引用。
	keep := s2.Spec
	keep.GroupIDs = []string{gHost}
	if _, err := e.subs.Update(ctx, uBob, s2.ID, keep, s2.Version); err != nil {
		t.Fatal(err)
	}
	if err := e.groups.Delete(ctx, uAlice, g.ID); err != nil {
		t.Fatalf("delete after the references are gone: %v", err)
	}
}

// 分组列表的计数：分组树本身不带计数，列表页另取。
func TestGroup_ListCounts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.mustRule(t, uAlice, spec(gHost, "a"))
	e.mustRule(t, uAlice, spec(gHost, "b"))
	e.mustRule(t, uCarol, spec(gApp, "c"))
	views, _, err := e.groups.List(ctx, uAlice)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][2]int32{}
	for _, v := range views {
		got[v.ID] = [2]int32{v.RuleCount, v.ChildCount}
	}
	if got[gInfra] != [2]int32{0, 2} || got[gHost] != [2]int32{2, 0} || got[gDB] != [2]int32{0, 0} {
		t.Fatalf("counts = %v", got)
	}
	// dave 只对 host 只读：上级 infra 只为保持树连通，不透出计数。
	views, _, _ = e.groups.List(ctx, uDave)
	for _, v := range views {
		if v.ID == gInfra && (v.Level != mc.ScopeNone || v.RuleCount != 0 || v.ChildCount != 0) {
			t.Fatalf("out-of-scope ancestor leaks counts: %+v", v)
		}
	}
}

// ---------------------------------------------------------------------------
// X1 导入级别对照
// ---------------------------------------------------------------------------

func TestRule_ImportSeverityMap(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	content := `
groups:
- name: g
  rules:
  - {alert: a, expr: up == 0, labels: {severity: SEV-1}}
  - {alert: b, expr: up == 0, labels: {severity: " sev-2 "}}
  - {alert: c, expr: up == 0, labels: {severity: critical}}
  - {alert: d, expr: up == 0, labels: {severity: warning}}
  - {alert: e, expr: up == 0, labels: {severity: whatever}}
  - {alert: f, expr: up == 0}
`
	opts := biz.ImportOptions{
		Content: content, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityInfo,
		// 键不区分大小写；可以覆盖内置对照（critical → 致命）。
		SeverityMap: map[string]int32{"Sev-1": mc.SeverityFatal, "sev-2": mc.SeverityCritical, "CRITICAL": mc.SeverityFatal},
	}
	items, err := e.rules.PreviewImport(ctx, uAlice, opts)
	if err != nil || len(items) != 6 {
		t.Fatalf("preview = %d err=%v", len(items), err)
	}
	want := []int32{mc.SeverityFatal, mc.SeverityCritical, mc.SeverityFatal, mc.SeverityWarning, mc.SeverityInfo, mc.SeverityInfo}
	for i, w := range want {
		if items[i].Spec == nil || items[i].Spec.Queries[0].Severity != w {
			t.Errorf("rule %s: severity = %+v, want %d", items[i].Name, items[i].Spec, w)
		}
	}
	// 命中对照表的不提示；落到默认级别的提示。
	if len(items[0].Warnings) != 0 || len(items[4].Warnings) == 0 || len(items[5].Warnings) == 0 {
		t.Fatalf("warnings = %v / %v / %v", items[0].Warnings, items[4].Warnings, items[5].Warnings)
	}
	// 校验：值域、条数、大小写折叠后的重复键。
	bad := opts
	bad.SeverityMap = map[string]int32{"x": 5}
	_, err = e.rules.PreviewImport(ctx, uAlice, bad)
	wantErr(t, "value out of range", err, biz.ErrRuleInvalid)
	bad.SeverityMap = map[string]int32{"P1": 4, "p1": 3}
	_, err = e.rules.PreviewImport(ctx, uAlice, bad)
	wantErr(t, "case-insensitive duplicate", err, biz.ErrRuleInvalid)
	bad.SeverityMap = map[string]int32{}
	for i := range 51 {
		bad.SeverityMap[fmt.Sprintf("k%d", i)] = 1
	}
	_, err = e.rules.Import(ctx, uAlice, bad, biz.ConflictSkip, false)
	wantErr(t, "too many entries", err, biz.ErrRuleInvalid)
}

// ---------------------------------------------------------------------------
// X2 JSON 导出 / 导入
// ---------------------------------------------------------------------------

// 合法的 UUID（JSON 导入会丢弃不是 UUID 的引用）。
const (
	xUser, xUserOff = "018f3e1a-0f00-7000-8000-000000000001", "018f3e1a-0f00-7000-8000-000000000002"
	xSched, xDept   = "018f3e1a-0f01-7000-8000-000000000001", "018f3e1a-0f02-7000-8000-000000000001"
	xGone           = "018f3e1a-0f03-7000-8000-00000000dead"
)

// exportEnv 在 env 上加一个经用例创建的数据源（ID 是 UUID）以及 UUID 形式的用户 / 值班表 / 部门，
// 并建一条用到它们全部的规则。
func exportEnv(t *testing.T) (e *env, ds *biz.Datasource, rule *biz.RuleView) {
	t.Helper()
	e = newEnv(t)
	e.org.Users[xUser], e.org.Users[xUserOff] = "Xavier", "Olga"
	e.org.Schedules[xSched], e.org.Depts[xDept] = "数据库值班", "数据库部"
	ds, err := e.sources.Create(context.Background(), uAdmin, biz.DatasourceInput{Name: "vm-prod", Enabled: true, Conn: biz.Connection{URL: "http://vm-prod:8428", AuthType: mc.AuthNone}})
	if err != nil {
		t.Fatal(err)
	}
	s := spec(gHost, "CPU 使用率过高")
	s.Description, s.Labels = "主机 CPU", map[string]string{"team": "ops"}
	s.DatasourceScope, s.DatasourceIDs = mc.DatasourceScopeSpecified, []string{ds.ID}
	s.Queries = []mc.Query{{Expr: "cpu_usage > 90", Severity: mc.SeverityCritical}, {Expr: "cpu_usage > 80", Severity: mc.SeverityWarning}}
	s.RecoverS, s.NodataKeepS, s.RepeatIntervalM, s.MaxNotify = 120, 300, 30, 5
	s.Effective = mc.Effective{Timezone: "Asia/Shanghai", Windows: []mc.TimeWindow{{Days: []int32{1, 2, 3, 4, 5}, Start: "09:00", End: "18:00"}}}
	s.Annotations = map[string]string{"summary": "{{ $labels.instance }} CPU {{ $value }}"}
	s.RecoveryExpr = `cpu_usage{instance="{{ $labels.instance }}"}`
	s.Notify = mc.NotifyTargets{UserIDs: []string{xUser, xUserOff}, ScheduleIDs: []string{xSched}, DeptIDs: []string{xDept}}
	s.GroupKeyLabels = []string{"instance"}
	return e, ds, e.mustRule(t, uAlice, s)
}

func TestRule_ExportJSONFormat(t *testing.T) {
	t.Parallel()
	e, ds, rule := exportEnv(t)
	ctx := context.Background()
	content, n, err := e.rules.Export(ctx, uAlice, []string{rule.ID}, "", biz.ExportJSON)
	if err != nil || n != 1 {
		t.Fatalf("export = %d err=%v", n, err)
	}
	// 按契约里写明的键名解读（不借用内部结构体）。
	var file struct {
		Format     string `json:"format"`
		ExportTime string `json:"export_time"`
		Rules      []map[string]json.RawMessage
	}
	if err := json.Unmarshal([]byte(content), &file); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, content)
	}
	if file.Format != "reborn.monitor.rules/v1" {
		t.Fatalf("format = %q", file.Format)
	}
	if biz.RuleExportFormat != "reborn.monitor.rules/v1" {
		t.Fatalf("RuleExportFormat = %q: the literal is part of the API contract", biz.RuleExportFormat)
	}
	if _, err := time.Parse(time.RFC3339, file.ExportTime); err != nil || len(file.Rules) != 1 {
		t.Fatalf("export_time = %q rules = %d", file.ExportTime, len(file.Rules))
	}
	r := file.Rules[0]
	for _, key := range []string{
		"name", "description", "enabled", "group", "labels", "datasource_scope", "datasources", "queries", "level_inhibit",
		"eval_interval_s", "for_s", "recover_s", "nodata_keep_s", "effective", "annotations", "recovery_expr", "notify",
		"notify_recovery", "repeat_interval_m", "max_notify", "group_key_labels",
		// 第二、三期增加的键（格式标识不变，只增不改）。
		"ci_match", "resource_scope", "relabel", "drop_expr", "topology_inhibit", "escalation", "actions",
	} {
		if _, ok := r[key]; !ok {
			t.Errorf("rule is missing %q", key)
		}
	}
	if len(r) != 28 {
		t.Errorf("rule has %d keys, want exactly the 28 documented ones", len(r))
	}
	type ref struct{ ID, Name string }
	var group ref
	var datasources []ref
	var notify struct{ Users, Schedules, Depts []ref }
	for raw, dst := range map[string]any{"group": &group, "datasources": &datasources, "notify": &notify} {
		if err := json.Unmarshal(r[raw], dst); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	if group != (ref{gHost, "基础设施 / 主机"}) || !slices.Equal(datasources, []ref{{ds.ID, "vm-prod"}}) {
		t.Fatalf("group = %+v datasources = %+v", group, datasources)
	}
	if !slices.Equal(notify.Users, []ref{{xUser, "Xavier"}, {xUserOff, "Olga"}}) || !slices.Equal(notify.Schedules, []ref{{xSched, "数据库值班"}}) ||
		!slices.Equal(notify.Depts, []ref{{xDept, "数据库部"}}) {
		t.Fatalf("notify = %+v", notify)
	}
	// YAML 仍是缺省；未知格式拒绝；JSON 同样只导出范围内的规则。
	yamlOut, _, err := e.rules.Export(ctx, uAlice, []string{rule.ID}, "", "")
	if err != nil || !strings.HasPrefix(yamlOut, "groups:") {
		t.Fatalf("default format must stay YAML: %v\n%s", err, yamlOut)
	}
	_, _, err = e.rules.Export(ctx, uAlice, []string{rule.ID}, "", "xml")
	wantErr(t, "unknown format", err, biz.ErrRuleInvalid)
	if _, n, err := e.rules.Export(ctx, uCarol, []string{rule.ID}, "", biz.ExportJSON); err != nil || n != 0 {
		t.Fatalf("out of scope export = %d err=%v", n, err)
	}
}

func TestRule_ImportJSONRoundTrip(t *testing.T) {
	t.Parallel()
	e, ds, rule := exportEnv(t)
	ctx := context.Background()
	content, _, err := e.rules.Export(ctx, uAlice, []string{rule.ID}, "", biz.ExportJSON)
	if err != nil {
		t.Fatal(err)
	}
	opts := biz.ImportOptions{Content: content, GroupID: gDB, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: mc.SeverityInfo}
	items, err := e.rules.PreviewImport(ctx, uAlice, opts)
	if err != nil || len(items) != 1 || items[0].Status != biz.ImportNew || len(items[0].Warnings) != 0 {
		t.Fatalf("preview = %+v err=%v", items, err)
	}
	if items[0].SourceGroup != "基础设施 / 主机" || items[0].Name != "CPU 使用率过高" {
		t.Fatalf("item = %+v", items[0])
	}
	results, err := e.rules.Import(ctx, uAlice, opts, biz.ConflictSkip, false)
	if err != nil || results[0].Action != biz.ImportCreated {
		t.Fatalf("import = %+v err=%v", results, err)
	}
	got, err := e.rules.Get(ctx, uAlice, results[0].RuleID)
	if err != nil {
		t.Fatal(err)
	}
	// 除了目标分组与启用状态（由导入选项决定），内容与原规则逐字段相同。
	want := rule.Spec
	want.GroupID, want.Enabled = gDB, false
	a, _ := json.Marshal(want)
	b, _ := json.Marshal(got.Spec)
	if string(a) != string(b) {
		t.Fatalf("round trip changed the rule:\nwant %s\n got %s", a, b)
	}
	// 同一个分组里再导一次：与现有规则重名。
	items, _ = e.rules.PreviewImport(ctx, uAlice, opts)
	if items[0].Status != biz.ImportConflict || items[0].ExistingRuleID != got.ID {
		t.Fatalf("second preview = %+v", items[0])
	}

	// 数据源停用：新建规则时不能选停用的数据源，导入同一口径——丢弃它并改用导入选项里的数据源范围。
	if _, err := e.sources.Update(ctx, uAdmin, ds.ID, biz.DatasourceInput{Name: "vm-prod", Conn: biz.Connection{URL: "http://vm-prod:8428", AuthType: mc.AuthNone}}, ds.Version); err != nil {
		t.Fatal(err)
	}
	fallback := opts
	fallback.GroupID, fallback.DatasourceScope, fallback.DatasourceIDs = gHost, mc.DatasourceScopeSpecified, []string{dsVM}
	items, err = e.rules.PreviewImport(ctx, uAlice, fallback)
	if err != nil || items[0].Spec == nil || !slices.Equal(items[0].Spec.DatasourceIDs, []string{dsVM}) || !strings.Contains(strings.Join(items[0].Warnings, ""), "vm-prod") {
		t.Fatalf("a disabled datasource must be dropped like a missing one: %+v err=%v", items[0], err)
	}

	// 引用按 ID 判断：数据源删除、用户停用、值班表停用、部门删除后再导入——丢弃并逐项提示。
	if err := e.rules.Delete(ctx, uAlice, rule.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.rules.Delete(ctx, uAlice, got.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.sources.Delete(ctx, uAdmin, ds.ID); err != nil {
		t.Fatal(err)
	}
	delete(e.org.Users, xUserOff)
	e.org.Disabled[xUserOff] = "Olga"
	delete(e.org.Schedules, xSched)
	delete(e.org.Depts, xDept)
	opts.DatasourceScope, opts.DatasourceIDs = mc.DatasourceScopeSpecified, []string{dsVM}
	items, err = e.rules.PreviewImport(ctx, uAlice, opts)
	if err != nil || items[0].Status != biz.ImportNew {
		t.Fatalf("preview with stale refs = %+v err=%v", items, err)
	}
	s := items[0].Spec
	if !slices.Equal(s.Notify.UserIDs, []string{xUser}) || len(s.Notify.ScheduleIDs) != 0 || len(s.Notify.DeptIDs) != 0 {
		t.Fatalf("notify after dropping stale refs = %+v", s.Notify)
	}
	// 指定的数据源全部不可用：改用导入选项里的数据源范围。
	if s.DatasourceScope != mc.DatasourceScopeSpecified || !slices.Equal(s.DatasourceIDs, []string{dsVM}) {
		t.Fatalf("datasources = %d %v", s.DatasourceScope, s.DatasourceIDs)
	}
	warnings := strings.Join(items[0].Warnings, "\n")
	for _, want := range []string{"数据源", "vm-prod", "通知用户", "Olga", "值班表", "数据库值班", "部门", "数据库部", "导入选项"} {
		if !strings.Contains(warnings, want) {
			t.Errorf("warnings do not mention %q:\n%s", want, warnings)
		}
	}
	// 导入选项给了通知对象：覆盖文件里的。
	opts.Notify = mc.NotifyTargets{UserIDs: []string{uAlice}}
	items, _ = e.rules.PreviewImport(ctx, uAlice, opts)
	if !slices.Equal(items[0].Spec.Notify.UserIDs, []string{uAlice}) || strings.Contains(strings.Join(items[0].Warnings, ""), "通知用户") {
		t.Fatalf("notify override = %+v warnings=%v", items[0].Spec.Notify, items[0].Warnings)
	}
}

func TestRule_ImportJSONIsValidatedLikeCreate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	file := func(rules ...string) string {
		return `{"format":"reborn.monitor.rules/v1","export_time":"2026-11-17T00:00:00Z","rules":[` + strings.Join(rules, ",") + `]}`
	}
	rule := func(name, expr, extra string) string {
		return `{"name":"` + name + `","datasource_scope":2,"queries":[{"expr":"` + expr + `","severity":3}],"eval_interval_s":60` + extra + `}`
	}
	opts := func(content string) biz.ImportOptions {
		return biz.ImportOptions{Content: content, GroupID: gHost, DatasourceScope: mc.DatasourceScopeAll, DefaultSeverity: 1}
	}
	// 文件级：版本、结构、条数、大小。
	for name, content := range map[string]string{
		"other version":  strings.Replace(file(rule("a", "up == 0", "")), "/v1", "/v2", 1),
		"empty format":   `{"format":"","rules":[]}`,
		"no rules":       file(),
		"broken json":    `{"format":"reborn.monitor.rules/v1","rules":[`,
		"trailing data":  file(rule("a", "up == 0", "")) + `{"x":1}`,
		"too many rules": file(slices.Repeat([]string{rule("a", "up == 0", "")}, biz.MaxImportRules+1)...),
		"too large":      file(rule("a", "up == 0", `,"description":"`+strings.Repeat("x", 1<<20)+`"`)),
	} {
		_, err := e.rules.PreviewImport(ctx, uAlice, opts(content))
		wantErr(t, name, err, biz.ErrRuleImportInvalid)
	}
	// 条目级：每条规则走与新建相同的校验，坏的那条不影响其余。
	content := file(
		rule("good", "up == 0", ""),
		rule("with-bomb", "WITH (a = up) a", ""),
		rule("bad-interval", "up == 0", `,"eval_interval_s":1`),
		rule("bad-template", "up == 0", `,"annotations":{"summary":"{{ range .Labels }}x{{ end }}"}`),
		rule("", "up == 0", ""),
		rule("bad-severity", "up == 0", `,"queries":[{"expr":"up","severity":9}]`),
		rule("not-uuid-refs", "up == 0", `,"datasource_scope":1,"datasources":[{"id":"'; DROP TABLE x; --","name":"x"}],"notify":{"users":[{"id":"nope","name":"n"}]}`),
		rule("unknown-fields", "up == 0", `,"future_field":{"a":[1,2,3]}`),
		rule("too-many-refs", "up == 0", `,"notify":{"users":[`+strings.TrimSuffix(strings.Repeat(`{"id":"`+xUser+`"},`, 51), ",")+`]}`),
	)
	items, err := e.rules.PreviewImport(ctx, uAlice, opts(content))
	if err != nil || len(items) != 9 {
		t.Fatalf("preview = %d err=%v", len(items), err)
	}
	want := []string{
		biz.ImportNew, biz.ImportInvalid, biz.ImportInvalid, biz.ImportInvalid, biz.ImportInvalid, biz.ImportInvalid, biz.ImportNew, biz.ImportNew,
		biz.ImportInvalid,
	}
	for i, w := range want {
		if items[i].Status != w {
			t.Errorf("item %d (%s) = %s (%s), want %s", i, items[i].Name, items[i].Status, items[i].Error, w)
		}
	}
	if !strings.Contains(items[1].Error, "WITH") {
		t.Errorf("the WITH rule must be refused by the expression guard: %s", items[1].Error)
	}
	// 不是 UUID 的引用被当作不存在丢弃（不会原样交给数据库），数据源退回导入选项。
	if s := items[6].Spec; s.DatasourceScope != mc.DatasourceScopeAll || len(s.DatasourceIDs) != 0 || len(s.Notify.UserIDs) != 0 || len(items[6].Warnings) < 2 {
		t.Errorf("non-uuid refs = %+v warnings=%v", s, items[6].Warnings)
	}
	// 用 JSON 写的 Prometheus 规则文件（没有 format 键）仍按规则文件处理。
	promJSON := `{"groups":[{"name":"g","rules":[{"alert":"from-json-yaml","expr":"up == 0","labels":{"severity":"critical"}}]}]}`
	items, err = e.rules.PreviewImport(ctx, uAlice, opts(promJSON))
	if err != nil || len(items) != 1 || items[0].Name != "from-json-yaml" || items[0].Status != biz.ImportNew {
		t.Fatalf("prometheus rule file written as JSON = %+v err=%v", items, err)
	}
}

// ---------------------------------------------------------------------------
// X6 通知对象候选项
// ---------------------------------------------------------------------------

func TestTargetOptions(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	all, err := e.targets.Options(ctx, "", nil)
	if err != nil || len(all.Schedules) != 1 || len(all.Depts) != 2 || len(all.Users) != 2 {
		t.Fatalf("all kinds = %+v err=%v", all, err)
	}
	if all.Schedules[0] != (biz.TargetSchedule{ID: schedOps, Name: "运维值班", GroupName: "运维组"}) || all.Depts[1].ParentID != "dept-tech" {
		t.Fatalf("options = %+v", all)
	}
	users, err := e.targets.Options(ctx, "  ali ", []string{biz.TargetKindUser})
	if err != nil || users.Schedules != nil || users.Depts != nil || len(users.Users) != 1 || users.Users[0].Username != "alice" {
		t.Fatalf("users only = %+v err=%v", users, err)
	}
	rest, _ := e.targets.Options(ctx, "zzz", []string{biz.TargetKindSchedule, biz.TargetKindDept})
	if len(rest.Schedules) != 1 || len(rest.Depts) != 2 || rest.Users != nil {
		t.Fatalf("keyword must only affect users: %+v", rest)
	}
}

// ---------------------------------------------------------------------------
// X5 告警详情的访问控制
// ---------------------------------------------------------------------------

func TestAlert_GetAccessRules(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	ctx := context.Background()
	// 订阅把通知发给 dave 与一个群；规则通知到 alice、bob（值班）、carol（部门负责人）。
	d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name, s.Receivers = "给 dave 和群", mc.Receivers{UserIDs: []string{uDave}, Groups: []string{"$group:ops"}}
	})
	d.store.SetAlert(d.alert, func(a *biz.Alert) { a.MuteID = "mute-1" })
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	const uEve = "u-eve" // 可用用户，没有任何监控权限，也不是接收方
	d.org.Users[uEve] = "Eve"

	// 没有接口权限的人一律按接收方判定。alice 有范围、也被通知过；carol 没有范围但被通知过。
	d.org.AlertReaders = []string{uBob}
	type want struct {
		ok   bool
		full bool
	}
	for user, w := range map[string]want{
		uAdmin: {true, true},   // 超级管理员
		uBob:   {true, true},   // 接口权限 + 范围（infra 只读）
		uAlice: {true, false},  // 有范围但没有接口权限：只因为是接收方才看得到 → 收件人视图
		uCarol: {true, false},  // 既无权限也无范围，但是接收方
		uDave:  {true, false},  // 订阅的接收方
		uEve:   {false, false}, // 什么都不是
	} {
		detail, err := d.alerts.Get(ctx, user, d.alert)
		if !w.ok {
			wantErr(t, user, err, biz.ErrAlertNotFound)
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", user, err)
			continue
		}
		if detail.CanViewRule != w.full {
			t.Errorf("%s: canViewRule = %v, want %v", user, detail.CanViewRule, w.full)
		}
		if w.full {
			if len(detail.Logs) != 2 || detail.Alert.MuteID != "mute-1" || len(detail.Logs[0].NotifyEventIDs) == 0 {
				t.Errorf("%s: full view = %+v logs=%d", user, detail.Alert.Alert, len(detail.Logs))
			}
			continue
		}
		// 收件人视图：只有这一条事件与发给本人的通知；看不到其他接收方、通知事件 ID、屏蔽规则。
		if detail.Alert.MuteID != "" || detail.Alert.MuteName != "" || len(detail.Logs) != 1 {
			t.Errorf("%s: recipient view leaks: mute=%q logs=%d", user, detail.Alert.MuteID, len(detail.Logs))
			continue
		}
		l := detail.Logs[0]
		if !slices.Equal(l.Recipients, []string{user}) || len(l.RecipientRefs) != 1 || l.RecipientRefs[0].ID != user || len(l.NotifyEventIDs) != 0 || l.SubscriptionID != "" {
			t.Errorf("%s: recipient timeline = %+v", user, l)
		}
		if detail.Alert.RuleName != "cpu-high" || detail.Alert.Labels["instance"] != "10.0.0.1" {
			t.Errorf("%s: the alert itself must be readable: %+v", user, detail.Alert.Alert)
		}
	}
	// 原始的存储内容没有被收件人视图改掉。
	if a := d.store.Alert(d.alert); a.MuteID != "mute-1" {
		t.Fatalf("the recipient view mutated the stored alert")
	}

	// 有接口权限但不在范围内、也不是接收方：404（与事件不存在同一个错误）。
	d.org.AlertReaders = []string{uEve}
	_, err := d.alerts.Get(ctx, uEve, d.alert)
	wantErr(t, "grant without scope", err, biz.ErrAlertNotFound)
	_, errMissing := d.alerts.Get(ctx, uEve, "no-such-alert")
	if kerrors.FromError(err).Message != kerrors.FromError(errMissing).Message {
		t.Fatalf("an out-of-scope alert must be indistinguishable from a missing one")
	}
	// 有范围但没有接口权限、不是接收方：404。
	const uFrank = "u-frank"
	d.org.Users[uFrank], d.org.UserRoles[uFrank] = "Frank", []string{rInfraRead}
	_, err = d.alerts.Get(ctx, uFrank, d.alert)
	wantErr(t, "scope without grant", err, biz.ErrAlertNotFound)
	d.org.AlertReaders = []string{uFrank}
	if detail, err := d.alerts.Get(ctx, uFrank, d.alert); err != nil || !detail.CanViewRule {
		t.Fatalf("grant + scope: %+v err=%v", detail, err)
	}
	// 接收方账号停用后不能再看（没有别的途径时）。
	delete(d.org.Users, uCarol)
	d.org.Disabled[uCarol] = "Carol"
	_, err = d.alerts.Get(ctx, uCarol, d.alert)
	wantErr(t, "disabled recipient", err, biz.ErrAlertNotFound)
}

// ---------------------------------------------------------------------------
// 通知派发：C4 / C10 / C13 / X4 与联调未覆盖的场景
// ---------------------------------------------------------------------------

func TestDispatch_TitleFitsNotifyByteLimit(t *testing.T) {
	t.Parallel()
	for name, ruleName := range map[string]string{
		"4-byte runes": strings.Repeat("🔥", 120),
		"cjk":          strings.Repeat("告警规则名称很长", 16),
		"mixed":        "磁盘 " + strings.Repeat("𠜎x", 70),
		"ascii":        strings.Repeat("a", 128),
	} {
		d := newDispatchEnv(t)
		d.store.SetAlert(d.alert, func(a *biz.Alert) {
			a.RuleName = ruleName
			a.Labels = map[string]string{"实例": strings.Repeat("节点", 40), "env": "prod"}
		})
		d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
		// 修复前：标题按字符截到 80 个，4 字节字符就是 320 字节，超过 notify 的 255 字节上限，这一行
		// 每次都失败、十次后被丢弃——这条告警的通知就此永久消失。
		if n := d.run(t); n != 1 {
			t.Fatalf("%s: processed = %d, want 1", name, n)
		}
		events := d.notifyEvents(t)
		if len(events) != 1 {
			t.Fatalf("%s: events = %d", name, len(events))
		}
		title := events[0].Title
		if len(title) > 255 || !utf8.ValidString(title) || title == "" || !strings.HasPrefix(ruleName, title[:min(len(title), 12)]) {
			t.Fatalf("%s: title is %d bytes, valid utf8 = %v: %q", name, len(title), utf8.ValidString(title), title)
		}
		if row := d.store.OutboxRow("ob-1"); row.Status != mc.OutboxDone || row.Attempts != 0 {
			t.Fatalf("%s: outbox row = %+v", name, row)
		}
	}
}

func TestDispatch_SiteURL(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]string{
		"":                               "/monitor/alert/",
		"https://ops.example.com":        "https://ops.example.com/monitor/alert/",
		"https://ops.example.com/":       "https://ops.example.com/monitor/alert/",
		" http://10.0.0.5:8000/reborn/ ": "http://10.0.0.5:8000/reborn/monitor/alert/",
	} {
		d := newDispatchEnv(t)
		d.dispatch = d.newDispatch(t, &conf.Monitor{SiteUrl: raw})
		d.subscribe(t, uDave, nil)
		d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
		d.run(t)
		events := d.notifyEvents(t)
		if len(events) != 2 {
			t.Fatalf("site_url %q: events = %d", raw, len(events))
		}
		for _, ev := range events {
			if got := payloadOf(t, ev).AlertURL; got != want+d.alert {
				t.Errorf("site_url %q (%s): alert_url = %q, want %q", raw, ev.EventType, got, want+d.alert)
			}
		}
	}
	// 写错的配置在启动时就暴露。
	e := newEnv(t)
	for _, bad := range []string{"ops.example.com", "ftp://ops.example.com", "https://user:pw@ops.example.com", "https://ops.example.com/?x=1", "https://ops.example.com/#/", "https://"} {
		if _, err := biz.NewDispatchUsecase(nil, e.store.Outbox(), e.store.Outbox(), e.store.Alerts(), e.store.Rules(), e.store.Subscriptions(),
			e.scopes, e.org, e.org, e.org, nil, nil, &conf.Monitor{SiteUrl: bad}); err == nil {
			t.Errorf("site_url %q must be rejected", bad)
		}
	}
}

// 同一个人命中多条订阅只收一份；群标识同理。
func TestDispatch_DedupsRecipientsAcrossSubscriptions(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	const uEve = "u-eve"
	d.org.Users[uEve] = "Eve"
	for i := range 4 {
		d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
			s.Name = fmt.Sprintf("订阅-%d", i)
			s.Receivers = mc.Receivers{UserIDs: []string{uDave, uEve}, Groups: []string{"$group:ops"}}
		})
	}
	// 第五条多一个新的群：只发给这个群。
	extra := d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name, s.Receivers = "多一个群", mc.Receivers{UserIDs: []string{uDave}, Groups: []string{"$group:ops", "$group:dba"}}
	})
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	seen := map[string]int{}
	for _, ev := range d.notifyEvents(t) {
		for _, r := range ev.Recipients {
			seen[r]++
		}
	}
	for r, n := range seen {
		if n != 1 {
			t.Errorf("%s was notified %d times, want once", r, n)
		}
	}
	for _, r := range []string{uAlice, uBob, uCarol, uDave, uEve, "$group:ops", "$group:dba"} {
		if seen[r] != 1 {
			t.Errorf("%s was not notified", r)
		}
	}
	var subLogs []*biz.NotifyLog
	for _, l := range d.store.NotifyLogs() {
		if l.Source == mc.NotifySourceSubscription {
			subLogs = append(subLogs, l)
		}
	}
	// 只有真正发出通知的订阅才留通知记录（命中统计、「通知过我的」都以它为准）。
	if len(subLogs) != 2 {
		t.Fatalf("subscription logs = %d, want 2 (the first subscription and the one with a new group)", len(subLogs))
	}
	for _, l := range subLogs {
		if l.SubscriptionID == extra.ID && !slices.Equal(l.Recipients, []string{"$group:dba"}) {
			t.Fatalf("the later subscription must only reach the new group: %v", l.Recipients)
		}
	}
}

// 一轮派发里分组树只读一次，订阅所有者的范围只解析一次。
func TestDispatch_ReadsGroupTreeOncePerRun(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.subscribe(t, uDave, nil)
	const rows = 12
	for i := range rows {
		id := fmt.Sprintf("018f3e1a-aaaa-7000-8000-00000000e%03d", i)
		d.seedAlert(id, mc.SeverityCritical, map[string]string{"i": fmt.Sprint(i)})
		d.store.SeedOutbox(fmt.Sprintf("ob-%02d", i), id, mc.NotifyKindFiring, 1)
	}
	treeBefore, countsBefore := d.store.GroupReads()
	if n := d.run(t); n != rows {
		t.Fatalf("processed = %d, want %d", n, rows)
	}
	tree, counts := d.store.GroupReads()
	if tree-treeBefore != 1 {
		t.Errorf("group tree read %d times for %d rows, want once per run", tree-treeBefore, rows)
	}
	if counts != countsBefore {
		t.Errorf("dispatch must not compute per-group rule counts (read %d times)", counts-countsBefore)
	}
	// 范围解析（Resolve）同样不带计数。
	if _, err := d.scopes.Resolve(context.Background(), uAlice); err != nil {
		t.Fatal(err)
	}
	if _, after := d.store.GroupReads(); after != countsBefore {
		t.Errorf("scope resolution must not compute rule counts")
	}
	// 下一轮重新读：授权变更最迟下一轮生效。
	d.org.UserRoles[uDave] = nil
	other := "018f3e1a-aaaa-7000-8000-00000000efff"
	d.seedAlert(other, mc.SeverityCritical, nil)
	d.store.SeedOutbox("ob-next", other, mc.NotifyKindFiring, 1)
	d.run(t)
	for _, l := range d.store.NotifyLogs() {
		if l.AlertID == other && l.Source == mc.NotifySourceSubscription {
			t.Fatalf("an owner who lost scope must not be notified in the next run")
		}
	}
}

// 命中的订阅超过上限：前 500 条照常通知，并记一条带总数的告警日志（不再静默截断）。
func TestDispatch_WarnsWhenSubscriptionCandidatesAreTruncated(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	ctx := context.Background()
	const total = 503
	for i := range total {
		s := &biz.Subscription{ID: fmt.Sprintf("sub-%04d", i), OwnerID: uAlice, Spec: biz.SubscriptionSpec{
			Name: "s", GroupIDs: []string{gHost}, Severities: []int32{mc.SeverityCritical}, OnFiring: true, Enabled: true,
			Receivers: mc.Receivers{Groups: []string{fmt.Sprintf("$g:%04d", i)}},
		}}
		if err := d.store.Subscriptions().Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	subLogs := 0
	for _, l := range d.store.NotifyLogs() {
		if l.Source == mc.NotifySourceSubscription {
			subLogs++
		}
	}
	if subLogs != 500 {
		t.Fatalf("notified subscriptions = %d, want the first 500", subLogs)
	}
	logged := d.logs.String()
	if !strings.Contains(logged, "candidates truncated") || !strings.Contains(logged, "matched=503") || !strings.Contains(logged, "skipped=3") || !strings.Contains(logged, "level=WARN") {
		t.Fatalf("truncation must be logged with the counts: %q", logged)
	}
	// 没有超过上限时不告警。
	d2 := newDispatchEnv(t)
	d2.subscribe(t, uDave, nil)
	d2.store.SeedOutbox("ob-1", d2.alert, mc.NotifyKindFiring, 1)
	d2.run(t)
	if strings.Contains(d2.logs.String(), "truncated") {
		t.Fatalf("unexpected truncation warning: %q", d2.logs.String())
	}
}

// 订阅的接收方：值班表当前值班人 + 群标识，超过 20 个时分片；群标识原样作为收件人。
func TestDispatch_SubscriptionScheduleAndGroupReceiversAreChunked(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	const schedBig = "sched-big"
	d.org.Schedules[schedBig] = "大值班表"
	var crowd []string
	for i := range 37 {
		id := fmt.Sprintf("u-duty-%02d", i)
		d.org.Users[id] = id
		crowd = append(crowd, id)
	}
	// 值班表里有已停用的人、有规则已经通知过的人（bob）：都不在订阅通知里。
	d.org.OnDutyUsers[schedBig] = append(slices.Clone(crowd), uGone, uBob)
	groups := []string{"$group:ops", "$group:dba", "$dingtalk:room-1"}
	sub := d.subscribe(t, uAlice, func(s *biz.SubscriptionSpec) {
		s.Name = "值班表 + 群"
		s.Receivers = mc.Receivers{ScheduleIDs: []string{schedBig}, Groups: groups}
	})
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	var subEvents []string
	got := map[string]int{}
	keys := map[string]bool{}
	for _, ev := range d.notifyEvents(t) {
		if ev.EventType != mc.EventSubscriptionFiring {
			continue
		}
		subEvents = append(subEvents, ev.ID)
		if len(ev.Recipients) > 20 || len(ev.Recipients) == 0 {
			t.Fatalf("chunk size = %d", len(ev.Recipients))
		}
		for _, r := range ev.Recipients {
			got[r]++
		}
		keys[ev.DedupKey] = true
		if p := payloadOf(t, ev); p.SubscriptionName != "值班表 + 群" {
			t.Fatalf("payload = %+v", p)
		}
	}
	// 37 个值班人 + 3 个群 = 40 个收件人 → 2 条事件，去重键各不相同。
	if len(subEvents) != 2 || len(keys) != 2 || len(got) != 40 {
		t.Fatalf("subscription events = %d, distinct keys = %d, recipients = %d; want 2 / 2 / 40", len(subEvents), len(keys), len(got))
	}
	for _, r := range append(slices.Clone(crowd), groups...) {
		if got[r] != 1 {
			t.Errorf("%s notified %d times", r, got[r])
		}
	}
	if got[uBob] != 0 || got[uGone] != 0 {
		t.Fatalf("already-notified or disabled users must be left out")
	}
	for _, l := range d.store.NotifyLogs() {
		if l.SubscriptionID == sub.ID && (len(l.Recipients) != 40 || len(l.NotifyEventIDs) != 2) {
			t.Fatalf("one log row must record all chunks: recipients=%d events=%d", len(l.Recipients), len(l.NotifyEventIDs))
		}
	}
}

// 规则的通知对象只有值班表与部门负责人：主备值班都通知、部门负责人各部门都取、彼此去重。
func TestDispatch_OnCallAndDeptLeaderRecipients(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const schedDB, deptDB = "sched-db", "dept-db"
	e.org.Schedules[schedDB], e.org.Depts[deptDB] = "数据库值班", "数据库部"
	e.org.OnDutyUsers = map[string][]string{schedOps: {uBob, uDave}, schedDB: {uDave, uGone}}
	e.org.Leaders = map[string][]string{deptOps: {uCarol, uBob}, deptDB: {uAlice}}
	s := spec(gHost, "duty-only")
	s.Notify = mc.NotifyTargets{ScheduleIDs: []string{schedOps, schedDB}, DeptIDs: []string{deptOps, deptDB}}
	d := &dispatchEnv{env: e, rule: e.mustRule(t, uAlice, s), alert: "018f3e1a-aaaa-7000-8000-00000000b001"}
	d.seedAlert(d.alert, mc.SeverityFatal, map[string]string{"instance": "db-1"})
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.run(t)
	events := d.notifyEvents(t)
	// 值班人在前（主、备，按值班表顺序），部门负责人在后；重复的人只出现一次，停用的不出现。
	if len(events) != 1 || !slices.Equal(events[0].Recipients, []string{uBob, uDave, uCarol, uAlice}) {
		t.Fatalf("recipients = %+v", events)
	}
	// 值班表换班后，下一次通知发给新的值班人（每次派发都重新解析）。
	e.org.OnDutyUsers[schedOps] = []string{uCarol}
	e.org.OnDutyUsers[schedDB] = nil
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindRepeat, 2)
	d.run(t)
	var repeat []string
	for _, l := range d.store.NotifyLogs() {
		if l.Kind == mc.NotifyKindRepeat {
			repeat = l.Recipients
		}
	}
	if !slices.Equal(repeat, []string{uCarol, uBob, uAlice}) {
		t.Fatalf("recipients after the shift change = %v", repeat)
	}
	// 值班表空档且部门没有负责人：不带收件人写一条事件，通知记录注明。
	e.org.OnDutyUsers, e.org.Leaders = map[string][]string{}, map[string][]string{}
	d.store.SeedOutbox("ob-3", d.alert, mc.NotifyKindRepeat, 3)
	d.run(t)
	logs := d.store.NotifyLogs()
	if last := logs[len(logs)-1]; last.Note != "未解析到通知对象" || len(last.Recipients) != 0 || len(last.NotifyEventIDs) != 1 {
		t.Fatalf("empty resolution = %+v", last)
	}
}

// 发件箱重试耗尽：前九次失败都留在待派发，第十次置为已丢弃；之后不再领取，同一事件后面入箱的行
// （恢复通知）得以继续，且因从未发出过触发通知而被丢弃——不会单独发一条「已恢复」。
func TestDispatch_RetryExhaustionDropsRowAndUnblocksLaterOnes(t *testing.T) {
	t.Parallel()
	d := newDispatchEnv(t)
	d.store.SeedOutbox("ob-1", d.alert, mc.NotifyKindFiring, 1)
	d.store.SeedOutbox("ob-2", d.alert, mc.NotifyKindResolved, 2)
	d.store.SetAlert(d.alert, func(a *biz.Alert) {
		now := time.Now()
		a.Status, a.ResolveTime = mc.AlertResolved, &now
	})
	for i := 1; i <= mc.OutboxMaxAttempts; i++ {
		d.store.FailNextLogInsert(errBoom)
		n, err := d.dispatch.Run(context.Background())
		if i < mc.OutboxMaxAttempts {
			if n != 0 || err != nil || d.store.OutboxRow("ob-2").Status != mc.OutboxPending {
				t.Fatalf("attempt %d: Run() = %d, %v; the later row must keep waiting", i, n, err)
			}
			continue
		}
		// 第十次：ob-1 被丢弃后，同一轮里 ob-2 就能被领取。
		if err != nil {
			t.Fatalf("final attempt: %v", err)
		}
	}
	d.run(t)
	first, second := d.store.OutboxRow("ob-1"), d.store.OutboxRow("ob-2")
	if first.Status != mc.OutboxDropped || first.Attempts != mc.OutboxMaxAttempts || !strings.Contains(first.LastError, "boom") {
		t.Fatalf("exhausted row = %+v", first)
	}
	if second.Status != mc.OutboxDropped || second.LastError != "未发出过触发通知，不发恢复通知" {
		t.Fatalf("the resolved row after a dropped firing row = %+v", second)
	}
	if a := d.store.Alert(d.alert); a.NotifyCount != 0 || a.LastNotifyTime != nil {
		t.Fatalf("nothing was delivered, notify progress must stay untouched: %+v", a)
	}
	for _, l := range d.store.NotifyLogs() {
		if l.Kind == mc.NotifyKindResolved {
			t.Fatalf("no resolved notification may be recorded")
		}
	}
}
