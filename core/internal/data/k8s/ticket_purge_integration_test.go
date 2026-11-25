//go:build integration

package data

import (
	"context"
	"testing"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/k8s"
	kc "github.com/lanyulei/reborn/backend/kit/k8scontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 过期票据的清理降到每个实例每分钟至多一次之后：
//   - 没被清掉的过期票据仍然不可核销（判定在 Redeem 的 expire_time > now()，与清理无关）；
//   - 实例的第一次签发、以及间隔过后的签发照旧把过期一分钟以上的行清掉；间隔内的签发不清；
//   - 未过期、以及过期不足一分钟的行任何时候都不被清理动到。
func TestTicketPurgeThrottled(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c := e.create("ticket-purge", kc.AccessAgent, nil)
	tk := biz.ExecTicket{UserID: uAlice, ClusterID: c.ID, Params: kc.ExecParams{Namespace: "default", Pod: "web", Kind: 1}}
	count := func() int {
		t.Helper()
		var n int
		if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM k8s_exec_ticket").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	issue := func(ttl time.Duration) []byte {
		t.Helper()
		_, hash, err := kc.NewTicket()
		if err != nil {
			t.Fatal(err)
		}
		if err := e.tickets.Create(ctx, hash, tk, ttl); err != nil {
			t.Fatal(err)
		}
		return hash
	}

	// 第一次签发（清理一次，表里没东西可清）；随后几张都在节流间隔内。
	live := issue(kc.ExecTicketTTL)
	stale := issue(-2 * time.Minute)   // 过期两分钟：够得上清理条件
	recent := issue(-10 * time.Second) // 过期不足一分钟：清理不动它
	if n := count(); n != 3 {
		t.Fatalf("tickets = %d, want 3 (no purge within the interval)", n)
	}
	// 留在表里的过期票据不可核销。
	for _, h := range [][]byte{stale, recent} {
		if _, err := e.tickets.Redeem(ctx, h); !kerrors.Is(err, biz.ErrTicketInvalid) {
			t.Fatalf("redeem expired ticket: %v, want ErrTicketInvalid", err)
		}
	}
	if n := count(); n != 3 {
		t.Fatalf("tickets = %d, want 3 (a rejected redeem deletes nothing)", n)
	}

	// 间隔过后（把本实例的节流点拨回去）的下一次签发：过期一分钟以上的被清掉，其余都在。
	e.tickets.nextPurge.Store(0)
	issue(kc.ExecTicketTTL)
	if n := count(); n != 3 { // live + recent + 刚签发的
		t.Fatalf("tickets after the purge = %d, want 3", n)
	}
	if _, err := e.tickets.Redeem(ctx, stale); !kerrors.Is(err, biz.ErrTicketInvalid) {
		t.Fatalf("redeem purged ticket: %v, want ErrTicketInvalid", err)
	}
	if got, err := e.tickets.Redeem(ctx, live); err != nil || got.UserID != uAlice {
		t.Fatalf("redeem live ticket = %+v, %v", got, err)
	}

	// 另一个实例（新的仓储对象）的第一次签发同样会清。
	issue(-2 * time.Minute)
	other := NewTicketRepo(e.data)
	_, hash, _ := kc.NewTicket()
	if err := other.Create(ctx, hash, tk, kc.ExecTicketTTL); err != nil {
		t.Fatal(err)
	}
	var staleLeft int
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM k8s_exec_ticket WHERE expire_time < now() - interval '1 minute'").Scan(&staleLeft); err != nil {
		t.Fatal(err)
	}
	if staleLeft != 0 {
		t.Fatalf("stale tickets after another instance issued = %d, want 0", staleLeft)
	}
}
