//go:build integration

package data

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
)

// 回收站与恢复（真实 PG）：恢复时触发器按当前规则重建唯一键；名称 / 唯一规则 / 同源外部
// 标识被占用时拒绝且不留痕；回收站列表与负责人显示名。
func TestIntegration_RecycleRestore(t *testing.T) {
	fx := newUniqueFixture(t, "rc_host", "ip")
	ctx := context.Background()
	rule := fx.mustRule("IP", "ip")

	a := fx.mustCreate("a", map[string]any{"ip": "1.1.1.1"})
	if _, err := fx.cis.GetDeleted(ctx, a.ID); !errors.Is(err, biz.ErrCINotFound) {
		t.Fatalf("live ci in recycle bin: %v", err)
	}
	if err := fx.cis.SoftDelete(ctx, a.ID, time.Now(), hist(t, a.ID, "delete")); err != nil {
		t.Fatal(err)
	}
	if fx.keyCount(rule.ID) != 0 {
		t.Fatal("deleted ci must hold no key")
	}
	del, err := fx.cis.GetDeleted(ctx, a.ID)
	if err != nil || del.DeleteTime == nil || del.Name != "a" {
		t.Fatalf("GetDeleted = %+v err=%v", del, err)
	}
	// 回收站列表 / 缺省列表互斥。
	bin, total, err := fx.cis.List(ctx, biz.CIFilter{ModelCode: fx.model, Deleted: true}, 1, 10)
	if err != nil || total != 1 || len(bin) != 1 || bin[0].ID != a.ID || bin[0].DeleteTime == nil {
		t.Fatalf("recycle list = %+v total=%d err=%v", bin, total, err)
	}
	if _, total, _ := fx.cis.List(ctx, biz.CIFilter{ModelCode: fx.model}, 1, 10); total != 0 {
		t.Fatalf("live list total = %d", total)
	}

	restore := func(ci *biz.CI) error {
		cur, err := fx.cis.GetDeleted(ctx, ci.ID)
		if err != nil {
			return err
		}
		return fx.cis.Restore(ctx, cur, time.Now(), hist(t, ci.ID, "restore"))
	}

	// 唯一值被新资源占用：恢复被拒并指明规则；仍在回收站。
	b := fx.mustCreate("b", map[string]any{"ip": "1.1.1.1"})
	if err := restore(a); !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), `"IP"`) {
		t.Fatalf("restore into a taken unique value: %v", err)
	}
	if _, err := fx.cis.GetDeleted(ctx, a.ID); err != nil {
		t.Fatalf("failed restore must leave the ci in the bin: %v", err)
	}
	fx.assertKeysConsistent()
	// 名称被占用。
	if err := fx.update(b, map[string]any{"ip": "1.1.1.2"}); err != nil {
		t.Fatal(err)
	}
	c := fx.mustCreate("tmp", map[string]any{"ip": "1.1.1.3"})
	rename := *c
	rename.Name, rename.Version = "a", c.Version+1
	if err := fx.cis.Update(ctx, &rename, c.Version, hist(t, c.ID, "update")); err != nil {
		t.Fatal(err)
	}
	if err := restore(a); !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), "name") {
		t.Fatalf("restore into a taken name: %v", err)
	}
	// 让出名称后恢复成功：版本 +1、键重建、历史写入。
	rename.Name, rename.Version = "c", rename.Version+1
	if err := fx.cis.Update(ctx, &rename, rename.Version-1, hist(t, c.ID, "update")); err != nil {
		t.Fatal(err)
	}
	if err := restore(a); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := fx.cis.Get(ctx, a.ID)
	if err != nil || got.Version != a.Version+1 || got.DeleteTime != nil {
		t.Fatalf("restored = %+v err=%v", got, err)
	}
	if fx.keyCount(rule.ID) != 3 {
		t.Fatalf("keys after restore = %d", fx.keyCount(rule.ID))
	}
	fx.assertKeysConsistent()
	if err := restore(a); !errors.Is(err, biz.ErrCINotFound) {
		t.Fatalf("restore a live ci: %v", err)
	}
	if _, err := fx.create("dup", map[string]any{"ip": "1.1.1.1"}); !errors.Is(err, biz.ErrCIDuplicate) {
		t.Fatalf("restored value must be protected again: %v", err)
	}

	// 同步资源：删除期间同一外部标识被重新发现并新建 → 恢复被拒（对账锚点不能重复）。
	if _, err := fx.d.Pool.Exec(ctx,
		`INSERT INTO cmdb_ci (id, model_code, name, source, external_id, attrs, delete_time) VALUES ($1, $2, 'old-sync', 'sync:s1', 'ext-1', '{}', now()),
		 ($3, $2, 'new-sync', 'sync:s1', 'ext-1', '{}', NULL)`, mustID(t), fx.model, mustID(t)); err != nil {
		t.Fatal(err)
	}
	old, _, _ := fx.cis.List(ctx, biz.CIFilter{ModelCode: fx.model, Deleted: true, Keyword: "old-sync"}, 1, 10)
	if len(old) != 1 {
		t.Fatalf("deleted sync ci not listed: %+v", old)
	}
	if err := fx.cis.Restore(ctx, old[0], time.Now(), hist(t, old[0].ID, "restore")); !errors.Is(err, biz.ErrCIDuplicate) || !strings.Contains(err.Error(), "external id") {
		t.Fatalf("restore with a re-discovered external id: %v", err)
	}

	// 负责人：owner 存用户 ID 时带出显示名；自由文本原样、无显示名；按负责人筛选；候选最小投影。
	// 自建用户（其它测试包会清空用户表，不能依赖预置的 admin）；另建一个停用用户，不应出现在候选里。
	adminID, offID := mustID(t), mustID(t)
	t.Cleanup(func() {
		_, _ = fx.d.Pool.Exec(context.Background(), `DELETE FROM "system_user" WHERE id = ANY($1::uuid[])`, []string{adminID, offID})
	})
	if _, err := fx.d.Pool.Exec(ctx,
		`INSERT INTO "system_user" (id, username, password, nickname, status) VALUES ($1::uuid, $3, 'x', '回收站管理员', 1), ($2::uuid, $4, 'x', '', 2)`,
		adminID, offID, "rc_adm_"+adminID[len(adminID)-8:], "rc_adm_off_"+offID[len(offID)-8:]); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.d.Pool.Exec(ctx, `UPDATE cmdb_ci SET owner = CASE name WHEN 'a' THEN $1 WHEN 'b' THEN '张三（外部）' ELSE owner END WHERE model_code = $2`, adminID, fx.model); err != nil {
		t.Fatal(err)
	}
	if ci, _ := fx.cis.Get(ctx, a.ID); ci.Owner != adminID || ci.OwnerName == "" {
		t.Fatalf("owner name must be resolved: %+v", ci)
	}
	if ci, _ := fx.cis.Get(ctx, b.ID); ci.Owner != "张三（外部）" || ci.OwnerName != "" {
		t.Fatalf("free-text owner: %+v", ci)
	}
	if mine, total, _ := fx.cis.List(ctx, biz.CIFilter{ModelCode: fx.model, Owner: adminID}, 1, 10); total != 1 || mine[0].ID != a.ID {
		t.Fatalf("filter by owner = %+v total=%d", mine, total)
	}
	cands, err := fx.cis.ListOwnerCandidates(ctx, "回收站管理", 10)
	if err != nil || len(cands) != 1 || cands[0].ID != adminID || cands[0].Name != "回收站管理员" {
		t.Fatalf("owner candidates = %+v err=%v", cands, err)
	}
	// 只按展示名匹配：有昵称的用户不能用登录名搜到（不让关键字成为探测登录名的手段）。
	if byLogin, _ := fx.cis.ListOwnerCandidates(ctx, "rc_adm", 10); len(byLogin) != 0 {
		t.Fatalf("candidates must not match on the login name: %+v", byLogin)
	}
	// 已删除的用户不再解析显示名（展示 owner 原文）。
	if _, err := fx.d.Pool.Exec(ctx, `UPDATE "system_user" SET delete_time = now() WHERE id = $1::uuid`, adminID); err != nil {
		t.Fatal(err)
	}
	if ci, _ := fx.cis.Get(ctx, a.ID); ci.Owner != adminID || ci.OwnerName != "" {
		t.Fatalf("deleted user must not resolve: %+v", ci)
	}
	// 回收站按删除时间倒序：很久以前更新、刚刚删除的资源排在最前。
	oldest := fx.mustCreate("old-edit", map[string]any{"ip": "9.9.9.1"})
	if _, err := fx.d.Pool.Exec(ctx, `UPDATE cmdb_ci SET update_time = now() - interval '1 year' WHERE id = $1`, oldest.ID); err != nil {
		t.Fatal(err)
	}
	if err := fx.cis.SoftDelete(ctx, oldest.ID, time.Now(), hist(t, oldest.ID, "delete")); err != nil {
		t.Fatal(err)
	}
	if bin, _, _ := fx.cis.List(ctx, biz.CIFilter{ModelCode: fx.model, Deleted: true}, 1, 10); len(bin) == 0 || bin[0].ID != oldest.ID {
		t.Fatalf("recycle bin must list the most recently deleted first: %+v", bin)
	}
	if cands, _ := fx.cis.ListOwnerCandidates(ctx, "%", 10); len(cands) != 0 {
		t.Fatalf("LIKE wildcard must be escaped: %+v", cands)
	}
}
