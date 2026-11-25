//go:build integration

package data

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 优化项的等价性集成测试：改写后的语句与改写前的语句在同一份数据上给出同样的结果。

// legacyStatGroup 改动前的写法：每个维度一条 GROUP BY。
func legacyStatGroup(t *testing.T, e *env, column string, since time.Time) []biz.StatCount {
	t.Helper()
	rows, err := e.pool.Query(context.Background(),
		"SELECT "+column+", count(*) FROM job_execution WHERE create_time >= $1 GROUP BY 1 ORDER BY 1", since)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []biz.StatCount{}
	for rows.Next() {
		var key int32
		var c biz.StatCount
		if err := rows.Scan(&key, &c.Count); err != nil {
			t.Fatal(err)
		}
		c.Key = strconv.Itoa(int(key))
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// 执行统计的三组分布合并成一条 GROUPING SETS 语句后（core-job 优化项 C），状态 / 类型 / 触发来源三组的键、
// 计数与顺序都与原来的三条 GROUP BY 相同；时间范围内没有数据时三组都是空切片（非 nil）。
func TestStatsDistributionsMatchLegacyGroupBy(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tpl := e.base()
	statuses := []int32{jc.StatusSuccess, jc.StatusSuccess, jc.StatusFailed, jc.StatusTimeout, jc.StatusRejected, jc.StatusPending}
	for i := range 8 {
		_, id, _, err := e.execution(tpl, uAlice, "")
		if err != nil {
			t.Fatal(err)
		}
		if i < len(statuses) {
			e.setStatus(id, statuses[i])
		}
	}
	// 让三组的键互相交叠（状态 3、类型 3、触发来源 3 同时出现），核对分组不串。类型 ≠ 1 的执行没有模板
	// （ck_job_execution_template）；类型与触发来源的取值受 CHECK 约束，逐个试、库不接受的只记一笔——
	// 下面的比对以改动前的语句在同一份数据上的结果为准，种子少一种取值不影响结论。
	if _, err := e.pool.Exec(ctx,
		`UPDATE job_execution SET kind = 3, template_id = NULL
		 WHERE id IN (SELECT id FROM job_execution ORDER BY id LIMIT 2)`); err != nil {
		t.Logf("kind 3 is not accepted by this schema: %v", err)
	}
	for _, trigger := range []int{2, 3, 4} {
		if _, err := e.pool.Exec(ctx,
			`UPDATE job_execution SET trigger = $1::smallint
			 WHERE id IN (SELECT id FROM job_execution WHERE trigger = 1 ORDER BY id DESC LIMIT 1)`, trigger); err != nil {
			t.Logf("trigger %d is not accepted by this schema: %v", trigger, err)
		}
	}
	// 一条落在时间范围之外。
	if _, err := e.pool.Exec(ctx,
		`UPDATE job_execution SET create_time = now() - interval '3 hours'
		 WHERE id IN (SELECT id FROM job_execution ORDER BY id DESC LIMIT 1)`); err != nil {
		t.Fatalf("age one execution: %v", err)
	}
	for _, since := range []time.Time{time.Now().Add(-time.Hour), time.Now().Add(-24 * time.Hour), time.Now().Add(time.Hour)} {
		st, err := e.stats.Stats(ctx, since, "UTC")
		if err != nil {
			t.Fatalf("Stats() error = %v", err)
		}
		for column, got := range map[string][]biz.StatCount{"status": st.ByStatus, "kind": st.ByKind, "trigger": st.ByTrigger} {
			want := legacyStatGroup(t, e, column, since)
			if got == nil || !slices.Equal(got, want) {
				t.Fatalf("since %v: by %s = %+v, legacy GROUP BY = %+v", since, column, got, want)
			}
		}
		var sum int64
		for _, c := range st.ByStatus {
			sum += c.Count
		}
		if sum != st.Total {
			t.Fatalf("since %v: status distribution sums to %d, total = %d", since, sum, st.Total)
		}
	}
}
