//go:build integration

package data

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/cmdb"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// Link 预检的批量查询（ExistingPeers / CountSides）与逐个查询（Exists / CountSide）口径一致：
// 对同一份数据逐项比对两个方向。
func TestIntegration_CmdbRelationPrecheckMatchesPerRow(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	rels := NewRelationRepo(d)
	for _, q := range []string{
		`INSERT INTO cmdb_model (code, name, category) VALUES ('pc_host', '主机', 'compute'), ('pc_sw', '交换机', 'network')`,
		`INSERT INTO cmdb_relation_type (code, name, forward_name, backward_name, src_model, dst_model, cardinality) VALUES
		   ('pc_uplink', '上联', '上联交换机', '下联主机', 'pc_host', 'pc_sw', 'many_to_many'),
		   ('pc_backup', '备用上联', '备用交换机', '备用下联', 'pc_host', 'pc_sw', 'many_to_many')`,
	} {
		if _, err := d.Pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	hosts, switches := []string{mustID(t), mustID(t), mustID(t)}, []string{mustID(t), mustID(t), mustID(t)}
	for i, id := range hosts {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name) VALUES ($1, 'pc_host', $2)", id, "pc-h"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i, id := range switches {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name) VALUES ($1, 'pc_sw', $2)", id, "pc-s"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	// h0 → s0, s1；h1 → s1；另一类型 h0 → s2（不得串到 pc_uplink 的结果里）；h2 / s2 在 pc_uplink 上没有关系。
	for _, r := range [][3]string{
		{"pc_uplink", hosts[0], switches[0]},
		{"pc_uplink", hosts[0], switches[1]},
		{"pc_uplink", hosts[1], switches[1]},
		{"pc_backup", hosts[0], switches[2]},
	} {
		if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_relation (id, type_code, src_id, dst_id) VALUES ($1, $2, $3, $4)", mustID(t), r[0], r[1], r[2]); err != nil {
			t.Fatal(err)
		}
	}
	absent := mustID(t) // 不存在的资源：两种查法都应当是「没有关系」
	for _, typeCode := range []string{"pc_uplink", "pc_backup"} {
		for _, c := range []struct {
			direction    string
			selves, peer []string
		}{
			{biz.DirectionOutgoing, hosts, append(append([]string{}, switches...), absent)},
			{biz.DirectionIncoming, switches, append(append([]string{}, hosts...), absent)},
		} {
			for _, self := range c.selves {
				got, err := rels.ExistingPeers(ctx, typeCode, c.direction, self, c.peer)
				if err != nil {
					t.Fatal(err)
				}
				for _, peer := range c.peer {
					src, dst := self, peer
					if c.direction == biz.DirectionIncoming {
						src, dst = peer, self
					}
					want, err := rels.Exists(ctx, typeCode, src, dst)
					if err != nil {
						t.Fatal(err)
					}
					if _, ok := got[peer]; ok != want {
						t.Errorf("%s %s self=%s peer=%s: ExistingPeers=%v, Exists=%v", typeCode, c.direction, self, peer, ok, want)
					}
				}
				if len(got) > len(c.peer) {
					t.Errorf("ExistingPeers returned ids outside the request: %v", got)
				}
			}
		}
		all := append(append(append([]string{}, hosts...), switches...), absent)
		for _, direction := range []string{biz.DirectionOutgoing, biz.DirectionIncoming} {
			got, err := rels.CountSides(ctx, typeCode, direction, all)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range all {
				want, err := rels.CountSide(ctx, typeCode, direction, id)
				if err != nil {
					t.Fatal(err)
				}
				if n, ok := got[id]; n != want || (ok && n == 0) {
					t.Errorf("%s %s id=%s: CountSides=%d (present=%v), CountSide=%d", typeCode, direction, id, n, ok, want)
				}
			}
		}
	}
	// 空集合：不报错、结果为空。
	if got, err := rels.ExistingPeers(ctx, "pc_uplink", biz.DirectionOutgoing, hosts[0], nil); err != nil || len(got) != 0 {
		t.Fatalf("ExistingPeers(nil) = %v err=%v", got, err)
	}
	if got, err := rels.CountSides(ctx, "pc_uplink", biz.DirectionIncoming, nil); err != nil || len(got) != 0 {
		t.Fatalf("CountSides(nil) = %v err=%v", got, err)
	}
}

// insertHistories 多条走 pgx.Batch、单条走 Exec：两条路径落库内容与输入逐列一致（nil 项跳过），
// 批内任一条失败整个事务回滚。
func TestIntegration_CmdbInsertHistoriesBatch(t *testing.T) {
	d := newIntegrationCmdbData(t)
	ctx := context.Background()
	if _, err := d.Pool.Exec(ctx, `INSERT INTO cmdb_model (code, name, category) VALUES ('ih_host', '主机', 'compute')`); err != nil {
		t.Fatal(err)
	}
	ciID := mustID(t)
	if _, err := d.Pool.Exec(ctx, "INSERT INTO cmdb_ci (id, model_code, name) VALUES ($1, 'ih_host', 'ih-1')", ciID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	mk := func(i int, diff map[string]any) *biz.History {
		return &biz.History{
			ID: mustID(t), CIID: ciID, Action: biz.ActionUpdate, Diff: diff, Operator: mustID(t), Source: biz.SourceManual,
			RequestID: "req-" + strconv.Itoa(i), CreateTime: now.Add(time.Duration(i) * time.Millisecond),
		}
	}
	single := []*biz.History{mk(0, map[string]any{"before": map[string]any{"name": "a"}, "after": map[string]any{"name": "b"}})}
	multi := []*biz.History{
		mk(1, map[string]any{"relationType": "x", "direction": "outgoing"}),
		nil,
		mk(2, nil),
		mk(3, map[string]any{"after": map[string]any{"attrs": map[string]any{"n": float64(1), "s": "中文 'quote'"}}}),
	}
	for _, items := range [][]*biz.History{nil, {nil}, single, multi} {
		if err := dbx.WithTx(ctx, d.Pool, func(tx pgx.Tx) error { return insertHistories(ctx, tx, items) }); err != nil {
			t.Fatalf("insertHistories(%d items): %v", len(items), err)
		}
	}
	want := append(append([]*biz.History{}, single...), multi[0], multi[2], multi[3])
	rows, err := d.Pool.Query(ctx,
		`SELECT id::text, ci_id::text, action, diff, operator, source, request_id, create_time FROM cmdb_ci_history WHERE ci_id = $1::uuid ORDER BY create_time`, ciID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []*biz.History
	for rows.Next() {
		var h biz.History
		if err := rows.Scan(&h.ID, &h.CIID, &h.Action, &h.Diff, &h.Operator, &h.Source, &h.RequestID, &h.CreateTime); err != nil {
			t.Fatal(err)
		}
		got = append(got, &h)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("stored %d histories, want %d", len(got), len(want))
	}
	for i := range want {
		// 时间按时刻比较（time.Time 的内部表示不参与），其余列逐字段比较。
		g, w := *got[i], *want[i]
		sameTime := g.CreateTime.Equal(w.CreateTime)
		g.CreateTime, w.CreateTime = time.Time{}, time.Time{}
		if !sameTime || !reflect.DeepEqual(g, w) {
			t.Errorf("history %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}

	// 批内第二条主键冲突：整批不落库（含排在它前面的第一条）。
	fresh := mk(4, map[string]any{"k": "v"})
	dup := *multi[0]
	err = dbx.WithTx(ctx, d.Pool, func(tx pgx.Tx) error { return insertHistories(ctx, tx, []*biz.History{fresh, &dup, mk(5, nil)}) })
	if err == nil {
		t.Fatal("duplicate history id within a batch must fail")
	}
	var n int
	if err := d.Pool.QueryRow(ctx, "SELECT count(*) FROM cmdb_ci_history WHERE ci_id = $1::uuid", ciID).Scan(&n); err != nil || n != len(want) {
		t.Fatalf("failed batch must not leave rows: count=%d err=%v, want %d", n, err, len(want))
	}
}
