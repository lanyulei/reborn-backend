//go:build integration

package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// uniqueFixture 一个类型 + 若干字段，返回各仓储。
type uniqueFixture struct {
	t      *testing.T
	d      *basedata.Data
	fields *fieldRepo
	rules  biz.UniqueRuleRepo
	cis    *ciRepo
	model  string
	byCode map[string]*biz.Field
}

func newUniqueFixture(t *testing.T, model string, fieldCodes ...string) *uniqueFixture {
	t.Helper()
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	now := time.Now()
	if err := NewModelRepo(d).Create(ctx, &biz.Model{Code: model, Name: model, Category: "compute", Status: 1, CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatalf("create model: %v", err)
	}
	fields := NewFieldRepo(d)
	fx := &uniqueFixture{t: t, d: d, fields: fields, rules: NewUniqueRuleRepo(fields), cis: NewCIRepo(d), model: model, byCode: map[string]*biz.Field{}}
	for i, code := range fieldCodes {
		f := &biz.Field{ID: mustID(t), ModelCode: model, Code: code, Name: strings.ToUpper(code), Type: biz.FieldTypeString, Owner: "manual", Sort: int32(i), CreateTime: now, UpdateTime: now}
		if err := fields.Create(ctx, f); err != nil {
			t.Fatalf("create field %s: %v", code, err)
		}
		fx.byCode[code] = f
	}
	return fx
}

func (fx *uniqueFixture) rule(name string, codes ...string) *biz.UniqueRule {
	now := time.Now()
	return &biz.UniqueRule{ID: mustID(fx.t), ModelCode: fx.model, Name: name, FieldCodes: codes, CreateTime: now, UpdateTime: now}
}

func (fx *uniqueFixture) mustRule(name string, codes ...string) *biz.UniqueRule {
	fx.t.Helper()
	r := fx.rule(name, codes...)
	if err := fx.rules.Create(context.Background(), r, biz.MaxUniqueRulesPerModel); err != nil {
		fx.t.Fatalf("create rule %s: %v", name, err)
	}
	return r
}

func (fx *uniqueFixture) ci(name string, attrs map[string]any) *biz.CI {
	now := time.Now()
	return &biz.CI{ID: mustID(fx.t), ModelCode: fx.model, Name: name, Status: "active", Tags: []string{}, Source: "manual", Attrs: attrs, Version: 1, CreateTime: now, UpdateTime: now}
}

func (fx *uniqueFixture) create(name string, attrs map[string]any) (*biz.CI, error) {
	ci := fx.ci(name, attrs)
	return ci, fx.cis.Create(context.Background(), ci, hist(fx.t, ci.ID, "create"))
}

func (fx *uniqueFixture) mustCreate(name string, attrs map[string]any) *biz.CI {
	fx.t.Helper()
	ci, err := fx.create(name, attrs)
	if err != nil {
		fx.t.Fatalf("create ci %s: %v", name, err)
	}
	return ci
}

func (fx *uniqueFixture) update(ci *biz.CI, attrs map[string]any) error {
	next := *ci
	next.Attrs, next.Version, next.UpdateTime = attrs, ci.Version+1, time.Now()
	if err := fx.cis.Update(context.Background(), &next, ci.Version, hist(fx.t, ci.ID, "update")); err != nil {
		return err
	}
	*ci = next
	return nil
}

func (fx *uniqueFixture) keyCount(ruleID string) int {
	fx.t.Helper()
	var n int
	if err := fx.d.Pool.QueryRow(context.Background(), "SELECT count(*) FROM cmdb_ci_unique WHERE rule_id = $1", ruleID).Scan(&n); err != nil {
		fx.t.Fatal(err)
	}
	return n
}

func (fx *uniqueFixture) uniqueFlag(code string) bool {
	fx.t.Helper()
	f, err := fx.fields.Get(context.Background(), fx.model, fx.byCode[code].ID)
	if err != nil {
		fx.t.Fatal(err)
	}
	return f.Unique
}

// assertKeysConsistent 核心不变量：cmdb_ci_unique 恰好等于"未删除资源 × 适用规则 × 键非空"，
// 且键值与当前属性一致——既无缺失（约束漏洞）也无残留（误报冲突）。
func (fx *uniqueFixture) assertKeysConsistent() {
	fx.t.Helper()
	const expected = `
		SELECT c.id AS ci_id, r.id AS rule_id, k.key
		FROM cmdb_ci c JOIN cmdb_model_unique_rule r ON r.model_code = c.model_code
		CROSS JOIN LATERAL (SELECT cmdb_unique_key(c.attrs, r.field_codes) AS key) k
		WHERE c.delete_time IS NULL AND k.key IS NOT NULL`
	var missing, stale int
	if err := fx.d.Pool.QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM (`+expected+` EXCEPT SELECT ci_id, rule_id, key FROM cmdb_ci_unique) m),
		        (SELECT count(*) FROM (SELECT ci_id, rule_id, key FROM cmdb_ci_unique EXCEPT `+expected+`) s)`).Scan(&missing, &stale); err != nil {
		fx.t.Fatal(err)
	}
	if missing != 0 || stale != 0 {
		fx.t.Fatalf("cmdb_ci_unique inconsistent: %d missing, %d stale", missing, stale)
	}
}

// 单字段 / 多条独立规则 / 联合规则 / 空值语义，以及资源写入全生命周期对键的维护。
func TestIntegration_UniqueRuleEnforcement(t *testing.T) {
	fx := newUniqueFixture(t, "uq_host", "ip", "serial", "dc", "rack")
	ctx := context.Background()

	ipRule := fx.mustRule("IP", "ip")
	serialRule := fx.mustRule("序列号", "serial")
	slot := fx.mustRule("机位", "dc", "rack")
	if !fx.uniqueFlag("ip") || !fx.uniqueFlag("serial") || !fx.uniqueFlag("dc") || !fx.uniqueFlag("rack") {
		t.Fatal("fields referenced by a rule must be flagged unique")
	}

	h1 := fx.mustCreate("h1", map[string]any{"ip": "10.0.0.1", "serial": "SN1", "dc": "bj", "rack": "A1"})
	if fx.keyCount(ipRule.ID) != 1 || fx.keyCount(serialRule.ID) != 1 || fx.keyCount(slot.ID) != 1 {
		t.Fatal("h1 must hold one key per rule")
	}

	// 多条规则互相独立：只撞其中一条即拒绝（旧口径下 ip 相同但 serial 不同可以并存）。
	_, err := fx.create("h2", map[string]any{"ip": "10.0.0.1", "serial": "SN2"})
	if !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), `"IP"`) || !strings.Contains(err.Error(), "ip") {
		t.Fatalf("same ip must violate rule IP, got %v", err)
	}
	_, err = fx.create("h2", map[string]any{"ip": "10.0.0.2", "serial": "SN1"})
	if !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), "序列号") {
		t.Fatalf("same serial must violate rule 序列号, got %v", err)
	}
	// 名称重复仍由名称唯一索引拦截，且错误可区分。
	_, err = fx.create("h1", map[string]any{"ip": "10.0.0.9"})
	if !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), "name") {
		t.Fatalf("duplicate name error = %v", err)
	}

	// 联合规则：全部字段相同才冲突。
	fx.mustCreate("h2", map[string]any{"ip": "10.0.0.2", "serial": "SN2", "dc": "bj", "rack": "A2"})
	_, err = fx.create("h3", map[string]any{"ip": "10.0.0.3", "serial": "SN3", "dc": "bj", "rack": "A1"})
	if !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), "机位") || !strings.Contains(err.Error(), "dc + rack") {
		t.Fatalf("same dc+rack must violate rule 机位, got %v", err)
	}

	// 空值语义：规则内任一字段为空（缺失 / 空串）即不受该规则约束，可以有多条。
	h3 := fx.mustCreate("h3", map[string]any{"ip": "10.0.0.3", "dc": "bj"})
	fx.mustCreate("h4", map[string]any{"ip": "10.0.0.4", "dc": "bj", "serial": ""})
	if fx.keyCount(serialRule.ID) != 2 || fx.keyCount(slot.ID) != 2 || fx.keyCount(ipRule.ID) != 4 {
		t.Fatalf("keys: serial=%d slot=%d ip=%d", fx.keyCount(serialRule.ID), fx.keyCount(slot.ID), fx.keyCount(ipRule.ID))
	}
	fx.assertKeysConsistent()

	// 更新：改成别人的值被拒绝且不留痕；改成新值后旧值释放。
	if err := fx.update(h3, map[string]any{"ip": "10.0.0.1", "dc": "bj"}); !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), `"IP"`) {
		t.Fatalf("update into conflict error = %v", err)
	}
	fx.assertKeysConsistent()
	if err := fx.update(h1, map[string]any{"ip": "10.0.0.11", "serial": "SN1", "dc": "bj", "rack": "A1"}); err != nil {
		t.Fatalf("update h1: %v", err)
	}
	if err := fx.update(h3, map[string]any{"ip": "10.0.0.1", "dc": "bj", "rack": "B1"}); err != nil {
		t.Fatalf("freed ip must be reusable: %v", err)
	}
	// 补全了联合规则的字段后开始受约束。
	if err := fx.update(h3, map[string]any{"ip": "10.0.0.1", "dc": "bj", "rack": "A1"}); !errors.Is(err, biz.ErrCIDuplicate) {
		t.Fatalf("completing the composite key into a conflict must fail, got %v", err)
	}
	fx.assertKeysConsistent()

	// 软删释放键；被占用过的值可被新资源使用。
	if err := fx.cis.SoftDelete(ctx, h1.ID, time.Now(), hist(t, h1.ID, "delete")); err != nil {
		t.Fatal(err)
	}
	fx.mustCreate("h5", map[string]any{"ip": "10.0.0.11", "serial": "SN1", "dc": "bj", "rack": "A1"})
	fx.assertKeysConsistent()

	// 不影响键的更新（状态流转等）不改动键。
	before := fx.keyCount(ipRule.ID)
	h2, _ := fx.cis.GetByName(ctx, fx.model, "h2")
	next := *h2
	next.Status, next.Version = "maintenance", h2.Version+1
	if err := fx.cis.UpdateBatch(ctx, []*biz.CI{&next}, []*biz.History{hist(t, h2.ID, "transition")}); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if fx.keyCount(ipRule.ID) != before {
		t.Fatal("status transition must not touch keys")
	}
	fx.assertKeysConsistent()
}

// 规则变更：存量重复拒绝且整体回滚、字段组变更重算、改名不动键、删除解除约束、上限与查重。
func TestIntegration_UniqueRuleLifecycle(t *testing.T) {
	fx := newUniqueFixture(t, "uq_vm", "ip", "zone", "tag")
	ctx := context.Background()
	fx.mustCreate("a", map[string]any{"ip": "1.1.1.1", "zone": "z1", "tag": "x"})
	fx.mustCreate("b", map[string]any{"ip": "1.1.1.1", "zone": "z2", "tag": "x"})
	fx.mustCreate("c", map[string]any{"ip": "1.1.1.2", "zone": "z1"})

	// 预检：ip 有 1 组重复（a、b）；ip+zone 无重复。
	groups, samples, err := fx.rules.Conflicts(ctx, fx.model, []string{"ip"}, 5)
	if err != nil || groups != 1 || len(samples) != 1 || samples[0].Count != 2 || strings.Join(samples[0].Names, ",") != "a,b" {
		t.Fatalf("conflicts(ip) = %d %+v err=%v", groups, samples, err)
	}
	if groups, _, _ := fx.rules.Conflicts(ctx, fx.model, []string{"ip", "zone"}, 5); groups != 0 {
		t.Fatalf("conflicts(ip, zone) = %d", groups)
	}

	// 存量重复：拒绝，且规则、键、派生标记都不留痕。
	bad := fx.rule("IP", "ip")
	if err := fx.rules.Create(ctx, bad, biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleConflict) || !strings.Contains(err.Error(), "1 group") {
		t.Fatalf("create rule over duplicates error = %v", err)
	}
	if list, _ := fx.rules.ListByModel(ctx, fx.model); len(list) != 0 {
		t.Fatalf("failed rule must not persist: %+v", list)
	}
	if fx.uniqueFlag("ip") || fx.keyCount(bad.ID) != 0 {
		t.Fatal("failed rule must leave no flag / keys")
	}

	// 联合规则可建；键只为字段齐全的资源生成（3 条都有 ip+zone）。
	rule := fx.mustRule("IP+可用区", "ip", "zone")
	if fx.keyCount(rule.ID) != 3 || !fx.uniqueFlag("ip") || !fx.uniqueFlag("zone") || fx.uniqueFlag("tag") {
		t.Fatalf("keys=%d flags ip=%v zone=%v tag=%v", fx.keyCount(rule.ID), fx.uniqueFlag("ip"), fx.uniqueFlag("zone"), fx.uniqueFlag("tag"))
	}
	fx.assertKeysConsistent()

	// 同名 / 同字段组的规则拒绝。
	if err := fx.rules.Create(ctx, fx.rule("IP+可用区", "tag"), biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleExists) {
		t.Fatalf("duplicate name error = %v", err)
	}
	if err := fx.rules.Create(ctx, fx.rule("另一个名字", "ip", "zone"), biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleExists) {
		t.Fatalf("duplicate fields error = %v", err)
	}

	// 改名：不重算键，也不写字段组（即使调用方带来的是过期的字段组）。
	renamed := *rule
	renamed.Name, renamed.UpdateTime = "地址", time.Now()
	stale := renamed
	stale.FieldCodes = []string{"tag"}
	if err := fx.rules.Update(ctx, &stale, true, false); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got, _ := fx.rules.Get(ctx, fx.model, rule.ID); got.Name != "地址" || strings.Join(got.FieldCodes, ",") != "ip,zone" || fx.keyCount(rule.ID) != 3 {
		t.Fatalf("after rename: %+v keys=%d", got, fx.keyCount(rule.ID))
	}
	fx.assertKeysConsistent()

	// 字段组改成有重复的（ip）：拒绝，原规则与键原样保留。
	toIP := renamed
	toIP.FieldCodes = []string{"ip"}
	if err := fx.rules.Update(ctx, &toIP, false, true); !errors.Is(err, biz.ErrUniqueRuleConflict) {
		t.Fatalf("rekey into duplicates error = %v", err)
	}
	if got, _ := fx.rules.Get(ctx, fx.model, rule.ID); strings.Join(got.FieldCodes, ",") != "ip,zone" || fx.keyCount(rule.ID) != 3 {
		t.Fatalf("failed rekey must roll back: %+v keys=%d", got, fx.keyCount(rule.ID))
	}
	fx.assertKeysConsistent()

	// 字段组改成 zone+tag：a(z1,x) b(z2,x) 有键，c 缺 tag 无键；派生标记随之变化。
	toZoneTag := renamed
	toZoneTag.FieldCodes = []string{"tag", "zone"}
	// 只改字段组：不写名称（调用方带来的名称可能已被并发改名取代）。
	toZoneTag.Name = "过期的名称"
	if err := fx.rules.Update(ctx, &toZoneTag, false, true); err != nil {
		t.Fatalf("rekey: %v", err)
	}
	if got, _ := fx.rules.Get(ctx, fx.model, rule.ID); got.Name != "地址" {
		t.Fatalf("rekey must not overwrite the name: %+v", got)
	}
	if fx.keyCount(rule.ID) != 2 || fx.uniqueFlag("ip") || !fx.uniqueFlag("zone") || !fx.uniqueFlag("tag") {
		t.Fatalf("after rekey keys=%d flags ip=%v zone=%v tag=%v", fx.keyCount(rule.ID), fx.uniqueFlag("ip"), fx.uniqueFlag("zone"), fx.uniqueFlag("tag"))
	}
	fx.assertKeysConsistent()
	// 新规则立即生效。
	if _, err := fx.create("d", map[string]any{"zone": "z1", "tag": "x"}); !errors.Is(err, biz.ErrCIDuplicate) {
		t.Fatalf("new rule must be enforced, got %v", err)
	}

	// 引用关系：按字段查规则。
	if list, _ := fx.rules.ListByField(ctx, fx.model, "zone"); len(list) != 1 || list[0].ID != rule.ID {
		t.Fatalf("ListByField(zone) = %+v", list)
	}
	if list, _ := fx.rules.ListByField(ctx, fx.model, "ip"); len(list) != 0 {
		t.Fatalf("ListByField(ip) = %+v", list)
	}

	// 上限：按每类型计数（此处上限设为当前条数 → 再建即超限）。
	if err := fx.rules.Create(ctx, fx.rule("超限", "ip", "zone"), 1); !errors.Is(err, biz.ErrUniqueRuleInvalid) {
		t.Fatalf("limit error = %v", err)
	}

	// 删除：键与派生标记清除，约束解除。
	if err := fx.rules.Delete(ctx, fx.model, rule.ID); err != nil {
		t.Fatalf("delete rule: %v", err)
	}
	if fx.keyCount(rule.ID) != 0 || fx.uniqueFlag("zone") || fx.uniqueFlag("tag") {
		t.Fatal("deleted rule must clear keys and flags")
	}
	fx.mustCreate("d", map[string]any{"zone": "z1", "tag": "x"})
	if err := fx.rules.Delete(ctx, fx.model, rule.ID); !errors.Is(err, biz.ErrUniqueRuleNotFound) {
		t.Fatalf("delete missing rule error = %v", err)
	}
	if _, err := fx.rules.Get(ctx, "other_model", rule.ID); !errors.Is(err, biz.ErrUniqueRuleNotFound) {
		t.Fatalf("get across models error = %v", err)
	}
	fx.assertKeysConsistent()
}

// 字段与规则的互斥：被规则引用的字段不可废弃 / 设为敏感；已废弃或敏感的字段不可入规则。
func TestIntegration_UniqueRuleFieldGuards(t *testing.T) {
	fx := newUniqueFixture(t, "uq_guard", "ip", "note", "secret")
	ctx := context.Background()
	fx.mustRule("IP", "ip")

	if err := fx.fields.Deprecate(ctx, fx.model, fx.byCode["ip"].ID); !errors.Is(err, biz.ErrFieldInUse) {
		t.Fatalf("deprecate field in rule error = %v", err)
	}
	yes := true
	if err := fx.fields.Update(ctx, fx.model, fx.byCode["ip"].ID, biz.FieldUpdate{Sensitive: &yes}); !errors.Is(err, biz.ErrFieldInUse) {
		t.Fatalf("mark field in rule sensitive error = %v", err)
	}
	if f, _ := fx.fields.Get(ctx, fx.model, fx.byCode["ip"].ID); f.Deprecated || f.Sensitive {
		t.Fatalf("guarded field must be unchanged: %+v", f)
	}

	// 未被引用的字段照常废弃 / 设为敏感；之后不能再入规则（锁内复核）。
	if err := fx.fields.Deprecate(ctx, fx.model, fx.byCode["note"].ID); err != nil {
		t.Fatalf("deprecate free field: %v", err)
	}
	if err := fx.fields.Update(ctx, fx.model, fx.byCode["secret"].ID, biz.FieldUpdate{Sensitive: &yes}); err != nil {
		t.Fatalf("mark free field sensitive: %v", err)
	}
	for _, code := range []string{"note", "secret", "missing"} {
		if err := fx.rules.Create(ctx, fx.rule("r-"+code, code), biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleInvalid) {
			t.Fatalf("rule on unusable field %s error = %v", code, err)
		}
	}
	// 字段仓储不再接受 unique 写入：新字段的派生标记恒为 false。
	now := time.Now()
	f := &biz.Field{ID: mustID(t), ModelCode: fx.model, Code: "legacy", Name: "L", Type: biz.FieldTypeString, Unique: true, Owner: "manual", CreateTime: now, UpdateTime: now}
	if err := fx.fields.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if got, _ := fx.fields.Get(ctx, fx.model, f.ID); got.Unique {
		t.Fatal("unique flag is derived and must not be writable through the field")
	}
}

// 并发：同一唯一值的并发写入恰好成功一条；规则变更与资源写入并发后键仍与规则严格对应。
func TestIntegration_UniqueRuleConcurrency(t *testing.T) {
	fx := newUniqueFixture(t, "uq_race", "ip", "serial")
	ctx := context.Background()
	fx.mustRule("IP", "ip")

	const writers = 16
	var ok, dup atomic.Int32
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fx.create(fmt.Sprintf("race-%d", i), map[string]any{"ip": "9.9.9.9"})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, biz.ErrCIDuplicate):
				dup.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || dup.Load() != writers-1 {
		t.Fatalf("concurrent same-value writes: ok=%d dup=%d", ok.Load(), dup.Load())
	}
	fx.assertKeysConsistent()

	// 写入不断的同时反复新建 / 删除规则：任何时刻提交的资源都不应漏键或留残键。
	stop := make(chan struct{})
	var writes atomic.Int32
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				name := fmt.Sprintf("w%d-%d", w, i)
				ci, err := fx.create(name, map[string]any{"ip": fmt.Sprintf("10.%d.%d.%d", w, i/250, i%250), "serial": name})
				if err != nil {
					t.Errorf("writer create: %v", err)
					return
				}
				writes.Add(1)
				if i%3 == 0 {
					if err := fx.update(ci, map[string]any{"ip": ci.Attrs["ip"], "serial": name + "-v2"}); err != nil {
						t.Errorf("writer update: %v", err)
						return
					}
				}
			}
		}()
	}
	for i := range 12 {
		r := fx.rule(fmt.Sprintf("序列号-%d", i), "serial")
		if err := fx.rules.Create(ctx, r, biz.MaxUniqueRulesPerModel); err != nil {
			t.Errorf("rule create under load: %v", err)
			break
		}
		if err := fx.rules.Delete(ctx, fx.model, r.ID); err != nil {
			t.Errorf("rule delete under load: %v", err)
			break
		}
	}
	final := fx.mustRule("序列号", "serial")
	close(stop)
	wg.Wait()
	if writes.Load() == 0 {
		t.Fatal("writers made no progress")
	}
	fx.assertKeysConsistent()
	var cis int
	_ = fx.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci WHERE model_code = $1 AND delete_time IS NULL AND attrs ? 'serial'", fx.model).Scan(&cis)
	if fx.keyCount(final.ID) != cis {
		t.Fatalf("every resource with a serial must hold a key: keys=%d resources=%d", fx.keyCount(final.ID), cis)
	}
}

// 等锁超时：该类型有在途的资源写入事务时，规则变更在超时后返回可重试的业务错误，
// 而不是无限期阻塞（并让后续写入在其后排队）。
func TestIntegration_UniqueRuleLockTimeout(t *testing.T) {
	fx := newUniqueFixture(t, "uq_busy", "ip")
	ctx := context.Background()
	old := uniqueRuleLockTimeout
	uniqueRuleLockTimeout = 300 * time.Millisecond
	t.Cleanup(func() { uniqueRuleLockTimeout = old })

	// 在途写入：一个尚未提交的资源写入事务——cmdb_ci 的触发器已为它取了该类型的共享键锁。
	// （同时验证触发器与规则变更用的确实是同一把锁。）
	tx, err := fx.d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO cmdb_ci (id, model_code, name, attrs) VALUES ($1, $2, 'in-flight', '{"ip":"7.7.7.7"}')`, mustID(t), fx.model); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = fx.rules.Create(ctx, fx.rule("IP", "ip"), biz.MaxUniqueRulesPerModel)
	if !errors.Is(err, biz.ErrUniqueRuleBusy) {
		t.Fatalf("rule change while writers hold the lock: %v", err)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("lock wait not bounded: %s", waited)
	}
	if list, _ := fx.rules.ListByModel(ctx, fx.model); len(list) != 0 {
		t.Fatal("timed-out rule change must not persist")
	}
	// 资源写入不受等待中的规则变更影响；锁释放后规则变更成功。
	fx.mustCreate("still-writable", map[string]any{"ip": "1.2.3.4"})
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	fx.mustRule("IP", "ip")
	fx.assertKeysConsistent()
}

// 软删与规则变更互斥：软删事务未提交时，规则变更不得把"正在删除"的资源算作存量并为它建键
// （否则删除提交后键残留，同值的新资源被误判为重复）。
func TestIntegration_UniqueRuleVsSoftDelete(t *testing.T) {
	fx := newUniqueFixture(t, "uq_softdel", "ip")
	ctx := context.Background()
	old := uniqueRuleLockTimeout
	uniqueRuleLockTimeout = 300 * time.Millisecond
	t.Cleanup(func() { uniqueRuleLockTimeout = old })
	x := fx.mustCreate("x", map[string]any{"ip": "5.5.5.5"})

	tx, err := fx.d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE cmdb_ci SET delete_time = now() WHERE id = $1", x.ID); err != nil {
		t.Fatal(err)
	}
	// 软删在途：规则变更须等待（此处超时返回可重试错误），而不是基于"仍未删除"的快照建键。
	if err := fx.rules.Create(ctx, fx.rule("IP", "ip"), biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleBusy) {
		t.Fatalf("rule change during an in-flight soft delete: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rule := fx.mustRule("IP", "ip")
	if fx.keyCount(rule.ID) != 0 {
		t.Fatal("soft-deleted resource must hold no key")
	}
	fx.assertKeysConsistent()
	// 被删资源的值可以立刻被新资源使用。
	fx.mustCreate("y", map[string]any{"ip": "5.5.5.5"})

	// 反向：规则已存在时软删，键被清除；并发重算（改字段组）后也不残留。
	y, _ := fx.cis.GetByName(ctx, fx.model, "y")
	if err := fx.cis.SoftDelete(ctx, y.ID, time.Now(), hist(t, y.ID, "delete")); err != nil {
		t.Fatal(err)
	}
	if fx.keyCount(rule.ID) != 0 {
		t.Fatal("soft delete must release the key")
	}
	fx.assertKeysConsistent()
}

// 改名不被在途的资源写入阻塞（名称唯一用表达式索引，改名不升级为键级行锁）；
// 规则名在类型内不区分大小写唯一。
func TestIntegration_UniqueRuleRename(t *testing.T) {
	fx := newUniqueFixture(t, "uq_rename", "ip", "sn")
	ctx := context.Background()
	rule := fx.mustRule("Addr", "ip")

	// 在途写入：未提交事务已为该规则写入键（持有规则行的 KEY SHARE 锁）。
	tx, err := fx.d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO cmdb_ci (id, model_code, name, attrs) VALUES ($1, $2, 'in-flight', '{"ip":"8.8.8.8"}')`, mustID(t), fx.model); err != nil {
		t.Fatal(err)
	}
	renamed := *rule
	renamed.Name, renamed.UpdateTime = "地址", time.Now()
	done := make(chan error, 1)
	go func() { done <- fx.rules.Update(ctx, &renamed, true, false) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rename must not wait for in-flight resource writes")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	fx.assertKeysConsistent()

	// 不区分大小写的重名。
	fx.mustRule("Serial", "sn")
	if err := fx.rules.Create(ctx, fx.rule("serial", "ip", "sn"), biz.MaxUniqueRulesPerModel); !errors.Is(err, biz.ErrUniqueRuleExists) {
		t.Fatalf("case-insensitive duplicate name error = %v", err)
	}
	dup := renamed
	dup.Name = "SERIAL"
	if err := fx.rules.Update(ctx, &dup, true, false); !errors.Is(err, biz.ErrUniqueRuleExists) {
		t.Fatalf("rename into an existing name error = %v", err)
	}
}

// 存量迁移来的规则可能不满足新建限制（如浮点 / 敏感字段）：约束照常生效；经用例改名
// （即使回传原字段组）不被拒；改字段组时按新限制校验。数值键不受小数位写法影响。
func TestIntegration_UniqueRuleLegacy(t *testing.T) {
	fx := newUniqueFixture(t, "uq_legacy", "ip")
	ctx := context.Background()
	now := time.Now()
	ratio := &biz.Field{ID: mustID(t), ModelCode: fx.model, Code: "ratio", Name: "比率", Type: biz.FieldTypeFloat, Owner: "manual", CreateTime: now, UpdateTime: now}
	if err := fx.fields.Create(ctx, ratio); err != nil {
		t.Fatal(err)
	}
	// 迁移的做法：直接落规则行（不经用例校验）。
	legacyID := mustID(t)
	if _, err := fx.d.Pool.Exec(ctx, `INSERT INTO cmdb_model_unique_rule (id, model_code, name, field_codes) VALUES ($1, $2, '比率', '{ratio}')`, legacyID, fx.model); err != nil {
		t.Fatal(err)
	}
	// 1 / 1.0 / 1.00 是同一个值（jsonb 保留写入方的小数位，键函数做了归一）。
	fx.mustCreate("a", map[string]any{"ip": "1.1.1.1", "ratio": 1.5})
	if _, err := fx.d.Pool.Exec(ctx, `INSERT INTO cmdb_ci (id, model_code, name, attrs) VALUES ($1, $2, 'b', '{"ratio": 1.50}')`, mustID(t), fx.model); err == nil || !strings.Contains(err.Error(), "uq_cmdb_ci_unique_rule_key") {
		t.Fatalf("1.50 must collide with 1.5, got %v", err)
	}
	fx.assertKeysConsistent()

	uc := biz.NewModelUsecase(NewModelRepo(fx.d), fx.fields, nil, nil, nil, nil, nil, nil, fx.rules)
	name, same := "比率（旧）", []string{"ratio"}
	got, err := uc.UpdateUniqueRule(ctx, fx.model, legacyID, biz.UniqueRuleUpdate{Name: &name, FieldCodes: &same})
	if err != nil || got.Name != name {
		t.Fatalf("rename of a legacy rule echoing its fields must pass: %+v err=%v", got, err)
	}
	// 新建 / 改成不受支持的字段组仍被拒。
	if _, err := uc.CreateUniqueRule(ctx, &biz.UniqueRule{ModelCode: fx.model, Name: "新比率", FieldCodes: []string{"ratio", "ip"}}); !errors.Is(err, biz.ErrUniqueRuleInvalid) {
		t.Fatalf("new rule on a float field error = %v", err)
	}
	wider := []string{"ip", "ratio"}
	if _, err := uc.UpdateUniqueRule(ctx, fx.model, legacyID, biz.UniqueRuleUpdate{FieldCodes: &wider}); !errors.Is(err, biz.ErrUniqueRuleInvalid) {
		t.Fatalf("changing a legacy rule to unsupported fields error = %v", err)
	}
	// 可以改成合规的字段组，或删除。
	ipOnly := []string{"ip"}
	if _, err := uc.UpdateUniqueRule(ctx, fx.model, legacyID, biz.UniqueRuleUpdate{FieldCodes: &ipOnly}); err != nil {
		t.Fatalf("legacy rule to supported fields: %v", err)
	}
	fx.assertKeysConsistent()
	if err := uc.DeleteUniqueRule(ctx, fx.model, legacyID); err != nil {
		t.Fatal(err)
	}
}
