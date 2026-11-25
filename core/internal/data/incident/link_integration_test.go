//go:build integration

package data

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/kitconf"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newIntegrationIncidentData 连接 TEST_DATABASE_SOURCE（库名须含 test）并清空故障域表。
func newIntegrationIncidentData(t *testing.T) *basedata.Data {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_SOURCE")
	if dsn == "" {
		t.Skip("TEST_DATABASE_SOURCE not set, skipping data integration tests")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_SOURCE: %v", err)
	}
	if db := cfg.ConnConfig.Database; !strings.Contains(db, "test") {
		t.Fatalf("TEST_DATABASE_SOURCE 指向库 %q：库名须含 \"test\"", db)
	}
	d, cleanup, err := basedata.NewData(&conf.Data{Database: &kitconf.Database{Source: dsn}})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	if _, err := d.Pool.Exec(context.Background(), "TRUNCATE TABLE incident_record CASCADE"); err != nil {
		t.Fatalf("truncate incident tables: %v", err)
	}
	return d
}

// 固定测试 ID（uuid 形态，只用 hex 字符）。
const (
	incA     = "018f3e1a-0a10-7000-8000-000000000001"
	incB     = "018f3e1a-0a10-7000-8000-000000000002"
	incC     = "018f3e1a-0a10-7000-8000-000000000003"
	alertOne = "018f3e1a-0a20-7000-8000-0000000000a1"
	alertTwo = "018f3e1a-0a20-7000-8000-0000000000a2"
)

func seedIncident(t *testing.T, d *basedata.Data, id string, status int32) {
	t.Helper()
	number := "INC-" + strings.ToUpper(strings.ReplaceAll(id, "-", "")[20:])
	if _, err := d.Pool.Exec(context.Background(),
		`INSERT INTO incident_record (id, number, title, severity, status, occur_time) VALUES ($1, $2, 'title', 2, $3, now())`,
		id, number, status); err != nil {
		t.Fatalf("seed incident %s: %v", id, err)
	}
}

type linkRow struct {
	incident, alert string
	released        bool
	method          int32
}

func linkRows(t *testing.T, d *basedata.Data, alertID string) []linkRow {
	t.Helper()
	rows, err := d.Pool.Query(context.Background(),
		`SELECT incident_id::text, alert_id::text, released, method FROM incident_alert WHERE alert_id = $1 ORDER BY incident_id`,
		alertID)
	if err != nil {
		t.Fatalf("query links: %v", err)
	}
	defer rows.Close()
	var out []linkRow
	for rows.Next() {
		var r linkRow
		if err := rows.Scan(&r.incident, &r.alert, &r.released, &r.method); err != nil {
			t.Fatalf("scan link: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("query links: %v", err)
	}
	return out
}

// 合并故障：源的关联全部迁到目标（method=4），目标已有的同一告警保留原行。此前语句把 DELETE 写在
// FROM 子查询里，是 PostgreSQL 语法错误，整个事务直接失败。
func TestLinkRepo_MoveAllTx(t *testing.T) {
	d := newIntegrationIncidentData(t)
	repo, tm := NewLinkRepo(d), basedata.NewTxManager(d)
	ctx := context.Background()
	seedIncident(t, d, incA, biz.StatusOpen)
	seedIncident(t, d, incB, biz.StatusOpen)
	for _, a := range []string{alertOne, alertTwo} {
		if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
			_, err := repo.AttachTx(ctx, tx, &biz.AlertLink{IncidentID: incA, AlertID: a, Method: 1, RuleName: "r"})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 目标早已有一条「已释放」的同告警历史关联：主键冲突，源的那条被丢弃。
	if _, err := d.Pool.Exec(ctx,
		`INSERT INTO incident_alert (incident_id, alert_id, method, released) VALUES ($1, $2, 1, true)`, incB, alertTwo); err != nil {
		t.Fatal(err)
	}

	var moved int
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		n, err := repo.MoveAllTx(ctx, tx, incA, incB)
		moved = n
		return err
	}); err != nil {
		t.Fatalf("MoveAllTx() error = %v", err)
	}
	if moved != 1 {
		t.Fatalf("MoveAllTx() = %d, want 1 (alert two conflicts with the existing row)", moved)
	}
	got := linkRows(t, d, alertOne)
	if len(got) != 1 || got[0].incident != incB || got[0].released || got[0].method != 4 {
		t.Fatalf("alert one must move to the target as an unreleased merge link, got %+v", got)
	}
	if got := linkRows(t, d, alertTwo); len(got) != 1 || got[0].incident != incB || !got[0].released || got[0].method != 1 {
		t.Fatalf("alert two must keep the target's existing row, got %+v", got)
	}
	if n := len(linkRows(t, d, alertOne)) + len(linkRows(t, d, alertTwo)); n != 2 {
		t.Fatalf("source rows must be gone, total rows = %d", n)
	}
}

// 告警只属于**活跃**故障：已恢复 / 已关闭的故障即使留着未释放的链接也不再占用告警。
func TestLinkRepo_ActiveIncidentOnlyCountsActive(t *testing.T) {
	d := newIntegrationIncidentData(t)
	repo, tm := NewLinkRepo(d), basedata.NewTxManager(d)
	ctx := context.Background()
	seedIncident(t, d, incA, biz.StatusOpen)
	if err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
		_, err := repo.AttachTx(ctx, tx, &biz.AlertLink{IncidentID: incA, AlertID: alertOne, Method: 1})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, status := range []int32{biz.StatusOpen, biz.StatusResponding, biz.StatusMitigated} {
		if _, err := d.Pool.Exec(ctx, "UPDATE incident_record SET status = $2 WHERE id = $1", incA, status); err != nil {
			t.Fatal(err)
		}
		if id, err := repo.ActiveIncident(ctx, alertOne); err != nil || id != incA {
			t.Fatalf("status %d: ActiveIncident() = %q, %v; want %q", status, id, err, incA)
		}
	}
	for _, status := range []int32{biz.StatusResolved, biz.StatusClosed} {
		if _, err := d.Pool.Exec(ctx, "UPDATE incident_record SET status = $2 WHERE id = $1", incA, status); err != nil {
			t.Fatal(err)
		}
		if id, err := repo.ActiveIncident(ctx, alertOne); err != nil || id != "" {
			t.Fatalf("status %d: ActiveIncident() = %q, %v; want empty", status, id, err)
		}
	}
	// 历史关联仍在该故障名下（详情页继续列出）。
	if got := linkRows(t, d, alertOne); len(got) != 1 || got[0].incident != incA || got[0].released {
		t.Fatalf("resolved incident must keep its (unreleased) history link, got %+v", got)
	}
}

// 关联告警时，告警若仍挂在已非活跃的故障上，先释放旧链接；活跃故障的归属不受影响（撞唯一索引 →
// ErrAlertLinked）；对同一故障的重复关联不改原行。
func TestLinkRepo_AttachTxReleasesStaleLink(t *testing.T) {
	d := newIntegrationIncidentData(t)
	repo, tm := NewLinkRepo(d), basedata.NewTxManager(d)
	ctx := context.Background()
	seedIncident(t, d, incA, biz.StatusResolved) // 已恢复：不释放链接
	seedIncident(t, d, incB, biz.StatusOpen)
	seedIncident(t, d, incC, biz.StatusMitigated) // 活跃
	attach := func(incident, alert string, method int32) (bool, error) {
		var ok bool
		err := tm.WithinTx(ctx, func(tx txrun.Tx) error {
			var err error
			ok, err = repo.AttachTx(ctx, tx, &biz.AlertLink{IncidentID: incident, AlertID: alert, Method: method})
			return err
		})
		return ok, err
	}
	// alertOne 由活跃故障 incC 持有；alertTwo 由已恢复故障 incA 持有（链接未释放）。
	for _, a := range []string{alertOne, alertTwo} {
		if _, err := attach(incC, a, 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.Pool.Exec(ctx, "UPDATE incident_alert SET incident_id = $2 WHERE incident_id = $3 AND alert_id = $1",
		alertTwo, incA, incC); err != nil {
		t.Fatal(err)
	}

	// 活跃故障的归属不能被抢：ErrAlertLinked，原链接原样保留。
	if _, err := attach(incB, alertOne, 2); !errors.Is(err, biz.ErrAlertLinked) {
		t.Fatalf("attach to an active incident's alert: error = %v, want ErrAlertLinked", err)
	}
	if got := linkRows(t, d, alertOne); len(got) != 1 || got[0].incident != incC || got[0].released {
		t.Fatalf("rejected attach must leave the owner untouched, got %+v", got)
	}

	// 对已恢复故障自身的重复关联：主键冲突吸收，原行不被改成已释放。
	if ok, err := attach(incA, alertTwo, 2); err != nil || ok {
		t.Fatalf("re-attach to the same resolved incident = %v, %v; want (false, nil)", ok, err)
	}
	if got := linkRows(t, d, alertTwo); len(got) != 1 || got[0].incident != incA || got[0].released || got[0].method != 1 {
		t.Fatalf("re-attach must keep the original row, got %+v", got)
	}

	// 关联到别的活跃故障：已恢复故障上的旧链接被释放，新链接生效。
	if ok, err := attach(incB, alertTwo, 2); err != nil || !ok {
		t.Fatalf("attach to another incident = %v, %v; want (true, nil)", ok, err)
	}
	got := linkRows(t, d, alertTwo)
	if len(got) != 2 {
		t.Fatalf("links = %+v, want the stale one and the new one", got)
	}
	if !(got[0].incident == incA && got[0].released) || !(got[1].incident == incB && !got[1].released && got[1].method == 2) {
		t.Fatalf("stale link must be released and the new link active, got %+v", got)
	}
	if id, err := repo.ActiveIncident(ctx, alertTwo); err != nil || id != incB {
		t.Fatalf("ActiveIncident() = %q, %v; want %q", id, err, incB)
	}
	// 现在归属是活跃故障：再被别的故障关联必须被拒。
	if _, err := attach(incC, alertTwo, 2); !errors.Is(err, biz.ErrAlertLinked) {
		t.Fatalf("attach to an alert held by an active incident: error = %v, want ErrAlertLinked", err)
	}
}
