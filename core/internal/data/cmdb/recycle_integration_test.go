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

// 认领同步冲突：把冲突行绑定到已有的人工资源（来源与外部标识改写、版本 +1、写历史、冲突行移除）。
func TestIntegration_ClaimConflict(t *testing.T) {
	fx := newUniqueFixture(t, "claim_host", "ip")
	ctx := context.Background()
	runs := NewSyncRunRepo(fx.d)
	const sourceID = "00000000-0000-7000-8000-00000000c1a1"
	source := "sync:" + sourceID
	if _, err := fx.d.Pool.Exec(ctx, "DELETE FROM cmdb_sync_conflict WHERE source = $1", source); err != nil {
		t.Fatal(err)
	}
	conflict := func(ext string) {
		t.Helper()
		if _, err := fx.d.Pool.Exec(ctx,
			`INSERT INTO cmdb_sync_conflict (source, model_code, external_id, name, reason) VALUES ($1, $2, $3, 'discovered', 'name already used by another resource')`,
			source, fx.model, ext); err != nil {
			t.Fatal(err)
		}
	}
	claim := func(ext, ciID string) error {
		return runs.ClaimConflict(ctx, &biz.ConflictClaim{
			SourceID: sourceID, ModelCode: fx.model, ExternalID: ext, CIID: ciID,
			Actor: biz.Actor{UserID: mustID(t)}, At: time.Now(), HistoryID: mustID(t),
		})
	}
	manual := fx.mustCreate("srv-1", map[string]any{"ip": "3.3.3.3"})
	conflict("ext-1")

	// 冲突行不存在 / 资源不存在 / 类型不符。
	if err := claim("ext-none", manual.ID); !errors.Is(err, biz.ErrConflictNotFound) {
		t.Fatalf("claim a missing conflict: %v", err)
	}
	if err := claim("ext-1", mustID(t)); !errors.Is(err, biz.ErrCINotFound) {
		t.Fatalf("claim onto a missing resource: %v", err)
	}

	if err := claim("ext-1", manual.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	got, err := fx.cis.Get(ctx, manual.ID)
	if err != nil || got.Source != source || got.ExternalID != "ext-1" || got.Version != manual.Version+1 {
		t.Fatalf("claimed resource = %+v err=%v", got, err)
	}
	var left, hist int
	_ = fx.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_sync_conflict WHERE source = $1", source).Scan(&left)
	_ = fx.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci_history WHERE ci_id = $1 AND action = 'update' AND diff -> 'after' ->> 'externalId' = 'ext-1'", manual.ID).Scan(&hist)
	if left != 0 || hist != 1 {
		t.Fatalf("after claim: conflicts=%d history=%d", left, hist)
	}
	// 唯一键不受影响（属性未变）。
	fx.assertKeysConsistent()

	// 已由同步维护的资源不能再被认领；外部标识已有在册资源时拒绝。
	conflict("ext-2")
	if err := claim("ext-2", manual.ID); !errors.Is(err, biz.ErrClaimNotManual) {
		t.Fatalf("claim a sync-owned resource: %v", err)
	}
	other := fx.mustCreate("srv-2", map[string]any{"ip": "3.3.3.4"})
	conflict("ext-1")
	if err := claim("ext-1", other.ID); !errors.Is(err, biz.ErrCIDuplicate) {
		t.Fatalf("claim an external id that is already bound: %v", err)
	}
	if o, _ := fx.cis.Get(ctx, other.ID); o.Source != "manual" || o.Version != other.Version {
		t.Fatalf("failed claim must not change the resource: %+v", o)
	}

	// 解除同步托管：改回人工维护（外部标识清空、版本 +1、写历史）；人工资源不适用；之后可再次认领。
	// 该资源上由同步建立的关联一并转为人工关联（之后的同步不再移除它们）；别的资源的同步关联不动。
	if _, err := fx.d.Pool.Exec(ctx,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality) VALUES ('claim_rel', 'r', 'f', 'b', $1, $1, 'many_to_many') ON CONFLICT DO NOTHING`, fx.model); err != nil {
		t.Fatal(err)
	}
	peerA, peerB := fx.mustCreate("peer-a", map[string]any{"ip": "3.3.4.1"}), fx.mustCreate("peer-b", map[string]any{"ip": "3.3.4.2"})
	if _, err := fx.d.Pool.Exec(ctx,
		`INSERT INTO cmdb_relation (id, type_code, src_id, dst_id, source) VALUES ($1, 'claim_rel', $2, $3, $6), ($4, 'claim_rel', $3, $5, $6), ($7, 'claim_rel', $3, $2, 'manual')`,
		mustID(t), manual.ID, peerA.ID, mustID(t), peerB.ID, source, mustID(t)); err != nil {
		t.Fatal(err)
	}
	claimed, _ := fx.cis.Get(ctx, manual.ID)
	if err := fx.cis.Unclaim(ctx, claimed, biz.Actor{UserID: mustID(t)}, time.Now(), mustID(t)); err != nil {
		t.Fatalf("unclaim: %v", err)
	}
	back, _ := fx.cis.Get(ctx, manual.ID)
	if back.Source != "manual" || back.ExternalID != "" || back.Version != claimed.Version+1 {
		t.Fatalf("unclaimed resource = %+v", back)
	}
	var mine, others int
	_ = fx.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_relation WHERE (src_id = $1 OR dst_id = $1) AND source LIKE 'sync:%'", manual.ID).Scan(&mine)
	_ = fx.d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_relation WHERE src_id = $1 AND dst_id = $2 AND source = $3", peerA.ID, peerB.ID, source).Scan(&others)
	if mine != 0 || others != 1 {
		t.Fatalf("unclaim must detach only this resource's sync relations: mine=%d others=%d", mine, others)
	}
	if err := fx.cis.Unclaim(ctx, back, biz.Actor{}, time.Now(), mustID(t)); !errors.Is(err, biz.ErrNotSyncManaged) {
		t.Fatalf("unclaim a manual resource: %v", err)
	}
	if err := claim("ext-2", manual.ID); err != nil {
		t.Fatalf("re-claim after unclaim: %v", err)
	}
	fx.assertKeysConsistent()

	// 冲突清单达到留存上限时如实报告"不完整"。
	if capped, _ := runs.ConflictsCapped(ctx, sourceID); capped {
		t.Fatal("a short conflict list must not be reported as capped")
	}
	// 以数据面记录的真实总数为准：刚好等于上限不算不完整；超过才算，且不随清单里的行被认领 / 清理而变化。
	stat := func(total int) {
		t.Helper()
		if _, err := fx.d.Pool.Exec(ctx,
			`INSERT INTO cmdb_sync_conflict_stat (source, model_code, total) VALUES ($1, $2, $3)
			 ON CONFLICT (source, model_code) DO UPDATE SET total = EXCLUDED.total`, source, fx.model, total); err != nil {
			t.Fatal(err)
		}
	}
	stat(2000)
	if capped, _ := runs.ConflictsCapped(ctx, sourceID); capped {
		t.Fatal("exactly the limit is a complete list")
	}
	stat(2001)
	if _, err := fx.d.Pool.Exec(ctx, "DELETE FROM cmdb_sync_conflict WHERE source = $1", source); err != nil {
		t.Fatal(err)
	}
	if capped, err := runs.ConflictsCapped(ctx, sourceID); err != nil || !capped {
		t.Fatalf("capped must not depend on the rows left in the list: %v err=%v", capped, err)
	}
	if _, err := fx.d.Pool.Exec(ctx, "DELETE FROM cmdb_sync_conflict_stat WHERE source = $1", source); err != nil {
		t.Fatal(err)
	}
	conflict("ext-1")
	conflict("ext-2")

	// 采集到的名称被另一条资源占用：认领别的资源解决不了冲突，拒绝且不改动。
	fx.mustCreate("discovered", map[string]any{"ip": "3.3.3.9"})
	bystander := fx.mustCreate("srv-x", map[string]any{"ip": "3.3.3.8"})
	conflict("ext-9")
	if err := claim("ext-9", bystander.ID); !errors.Is(err, biz.ErrClaimWrongTarget) {
		t.Fatalf("claim a resource that is not the one holding the name: %v", err)
	}
	if b, _ := fx.cis.Get(ctx, bystander.ID); b.Source != "manual" {
		t.Fatalf("rejected claim must not change the resource: %+v", b)
	}
}
