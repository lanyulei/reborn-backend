package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// hangUpOnClaim 领取成功后通知测试让节点断开连接，并等请求 ctx 感知到（等不到就放行：那正是修复前的行为）。
type hangUpOnClaim struct {
	biz.QueueRepo
	hangUp   func()
	canceled chan<- bool
}

func (q hangUpOnClaim) ClaimNext(ctx context.Context, runnerID, zone, leaseToken string, lease time.Duration) (*biz.Claimed, error) {
	c, err := q.QueueRepo.ClaimNext(ctx, runnerID, zone, leaseToken, lease)
	if c == nil {
		return c, err
	}
	q.hangUp()
	select {
	case <-ctx.Done():
		q.canceled <- true
	case <-time.After(3 * time.Second):
		q.canceled <- false
	}
	return c, err
}

// 节点在领取请求进行中断开：应答没送达，执行放回队列。此前领取处理器不读请求体，net/http 不启动后台读，
// 断开不会取消请求 ctx，应答的 Encode 也不报错——从未运行过的执行停在「执行中」，租约过期后被判成
// 「结果未知：作业可能仍在运行」。
func TestClaimUnclaimsWhenRunnerDisconnectsMidClaim(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)

	claimed, canceled := make(chan struct{}), make(chan bool, 1)
	queue := hangUpOnClaim{QueueRepo: f.store, hangUp: func() { close(claimed) }, canceled: canceled}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	uc := biz.NewRunnerUsecase(f.store, queue, f.store, f.store, f.store, f.opener, f.obs, logger)
	mux := http.NewServeMux()
	for path, h := range NewRunnerService(uc, logger).Routes() {
		mux.HandleFunc(path, h)
	}
	srv := httptest.NewServer(httpx.LimitRequestBody(mux))
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	// 与真实节点一致：领取请求带一个空 JSON 对象。
	if _, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: engine\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
		jobcontract.RunnerPathClaim, token); err != nil {
		t.Fatal(err)
	}
	select {
	case <-claimed:
		_ = conn.Close() // 引擎已把执行置为执行中，节点此刻断开
	case <-time.After(10 * time.Second):
		t.Fatal("claim was never attempted")
	}
	select {
	case ok := <-canceled:
		if !ok {
			t.Fatal("request context was not canceled after the runner hung up: the claim handler must drain the request body")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim never returned")
	}
	// 处理器随后发现应答送不到，放回队列（Unclaim 在处理器返回前同步完成，这里只等处理器跑完）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		e := f.store.Get(id)
		if e.Status == jobcontract.StatusQueued && e.LeaseToken == "" && e.RunnerID == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution must be released back to the queue, got %+v", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 放回之后能被正常领走（常规路径不受影响：应答送达就不 Unclaim）。
	if task := f.claim(token); task == nil || task.ExecutionID != id {
		t.Fatalf("released execution must be claimable again: %+v", task)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusRunning {
		t.Fatalf("a delivered claim must stay running: %+v", e)
	}
}

// 领取请求体超限：读尽时即 413，不领取任何执行（此前领取不读请求体，这类请求照常领取）。
func TestClaimRejectsOversizedBody(t *testing.T) {
	f := newFixture(t)
	_, token := f.runner("", true)
	id := f.execution("", nil)
	code, _ := f.post(jobcontract.RunnerPathClaim, token, make([]byte, httpx.MaxRequestBodyBytes+1))
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", code)
	}
	if e := f.store.Get(id); e.Status != jobcontract.StatusQueued {
		t.Fatalf("nothing may be claimed: %+v", e)
	}
}
