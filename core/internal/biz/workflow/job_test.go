package biz

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// queueFake 作业队列桩：记录续租 / 重试 / 放回。重试与放回如同数据库调用一样，ctx 已取消就直接失败。
type queueFake struct {
	claimed  []*Job
	onRenew  func(ctx context.Context, j *Job) (bool, error)
	retried  map[string]string
	released []string
}

func (q *queueFake) ClaimDue(context.Context, bool, int, time.Duration) ([]*Job, error) {
	jobs := q.claimed
	q.claimed = nil
	return jobs, nil
}

func (q *queueFake) Renew(ctx context.Context, j *Job, _ time.Duration) (bool, error) {
	if q.onRenew != nil {
		return q.onRenew(ctx, j)
	}
	return true, nil
}

func (q *queueFake) Retry(ctx context.Context, j *Job, _ time.Duration, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.retried[j.ID] = lastError
	return nil
}

func (q *queueFake) Release(ctx context.Context, j *Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.released = append(q.released, j.ID)
	return nil
}

func (q *queueFake) Purge(context.Context, time.Time, int) (int, error) { return 0, nil }

// txManagerFake txrun.Manager 桩：ctx 已取消时如同数据库调用一样直接失败，否则返回 onTx 的结果。
// 不执行事务体——这里只关心作业执行器 / 交接待办在一条命令失败之后怎么处置。
type txManagerFake struct {
	onTx  func(ctx context.Context) error
	calls int
}

func (m *txManagerFake) WithinTx(ctx context.Context, _ func(tx txrun.Tx) error) error {
	m.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.onTx != nil {
		return m.onTx(ctx)
	}
	return nil
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// jobUC 只装配作业执行器走到的那几个依赖（运行时用例的事务管理与实例查询）。
func jobUC(q *queueFake, txs *txManagerFake, lookup InstanceLookup, hooks WebhookResolver, caller ServiceCaller) *JobUsecase {
	rt := NewRuntimeUsecase(nil, nil, nil, lookup, nil, nil, nil, nil, txs, nil, nil, nil, nil)
	return NewJobUsecase(rt, q, nil, hooks, caller, quietLogger())
}

var engineJobs = []*Job{{ID: "j1", Kind: JobTimer, InstanceID: "i1"}, {ID: "j2", Kind: JobResume, InstanceID: "i2"}, {ID: "j3", Kind: JobStop, InstanceID: "i3"}}

// 停机（Loop.Stop 取消 ctx）时在途的引擎内部作业被打断：不算它的一次失败，租约立即放回——而不是
// 用已取消的 ctx 去登记重试（必然失败），让租约留到 2 分钟后过期、还多计一次尝试。
func TestRunDueReleasesInFlightJobOnShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := &queueFake{claimed: slices.Clone(engineJobs), retried: map[string]string{}}
	q.onRenew = func(_ context.Context, j *Job) (bool, error) {
		if j.ID == "j1" {
			cancel() // 处理第一条时执行器开始停机
		}
		return true, nil
	}
	txs := &txManagerFake{}

	n, err := jobUC(q, txs, nil, nil, nil).RunDue(ctx)
	if err != nil || n != 3 {
		t.Fatalf("RunDue() = %d, %v", n, err)
	}
	// 在途的 j1 被放回；同批还没轮到的 j2、j3 同样放回；没有任何一条被登记为失败重试。
	if !slices.Equal(q.released, []string{"j1", "j2", "j3"}) {
		t.Errorf("released = %v, want all three jobs back in the queue", q.released)
	}
	if len(q.retried) != 0 {
		t.Errorf("no job may be recorded as failed on shutdown, got %v", q.retried)
	}
	if txs.calls != 1 {
		t.Errorf("only the in-flight job may have tried to open a transaction, got %d", txs.calls)
	}
}

// 续租失败（恰逢停机）：放回作业的落账不随停机取消。
func TestRunDueReleasesAfterFailedRenewOnShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := &queueFake{claimed: slices.Clone(engineJobs), retried: map[string]string{}}
	q.onRenew = func(context.Context, *Job) (bool, error) {
		cancel()
		return false, errors.New("context canceled")
	}

	if _, err := jobUC(q, txsOf(nil), nil, nil, nil).RunDue(ctx); err != nil {
		t.Fatalf("RunDue() error = %v", err)
	}
	if !slices.Equal(q.released, []string{"j1", "j2", "j3"}) {
		t.Errorf("released = %v, want all three jobs back in the queue", q.released)
	}
}

func txsOf(onTx func(ctx context.Context) error) *txManagerFake { return &txManagerFake{onTx: onTx} }

// 没停机时的失败照旧：内部错误脱敏后登记重试，不放回。
func TestRunDueRetriesRealFailures(t *testing.T) {
	t.Parallel()
	q := &queueFake{claimed: engineJobs[:1], retried: map[string]string{}}
	txs := txsOf(func(context.Context) error { return errors.New("pq: connection reset by peer") })

	if _, err := jobUC(q, txs, nil, nil, nil).RunDue(context.Background()); err != nil {
		t.Fatalf("RunDue() error = %v", err)
	}
	if q.retried["j1"] != internalReason || len(q.released) != 0 {
		t.Errorf("retry reason = %q, released = %v; want %q and nothing released", q.retried["j1"], q.released, internalReason)
	}
}

// 自动任务路径上的内部错误（实例已结束、作废作业时数据库出错）不得把原文写进 last_error——它会经
// 实例详情展示给所有参与者；原文只进日志。
func TestServiceJobInternalErrorIsNotPersisted(t *testing.T) {
	t.Parallel()
	const dbErr = "pq: failed to connect to `host=db-internal user=app database=workflow`: connection refused"
	q := &queueFake{
		claimed: []*Job{{ID: "j1", Kind: JobService, InstanceID: "i1", Payload: map[string]any{"webhookId": "hook"}, Attempts: 1}},
		retried: map[string]string{},
	}
	lookup := &treeLookup{in: &Instance{ID: "i1", Status: InstanceTerminated}}
	txs := txsOf(func(context.Context) error { return errors.New(dbErr) })

	if _, err := jobUC(q, txs, lookup, nil, nil).RunServices(context.Background()); err != nil {
		t.Fatalf("RunServices() error = %v", err)
	}
	if got := q.retried["j1"]; got != internalReason {
		t.Errorf("last_error = %q, want %q", got, internalReason)
	}
	if strings.Contains(strings.Join([]string{q.retried["j1"]}, "\n"), "db-internal") {
		t.Error("internal connection details leaked into last_error")
	}
}

// 端点自己返回的失败原因按契约可展示，保留原文（管理员排障要用）。
func TestServiceJobKeepsEndpointErrorText(t *testing.T) {
	t.Parallel()
	q := &queueFake{
		claimed: []*Job{{ID: "j1", Kind: JobService, InstanceID: "i1", Payload: map[string]any{"webhookId": "hook"}, Attempts: 1}},
		retried: map[string]string{},
	}
	lookup := &treeLookup{in: &Instance{ID: "i1", Status: InstanceRunning, Variables: map[string]any{}}}
	hooks := hooksFake{}
	caller := callerFake(func(context.Context, *Webhook, string, []byte) ([]byte, bool, error) {
		return nil, true, errors.New("webhook responded with status 503")
	})

	if _, err := jobUC(q, txsOf(nil), lookup, hooks, caller).RunServices(context.Background()); err != nil {
		t.Fatalf("RunServices() error = %v", err)
	}
	if got := q.retried["j1"]; got != "webhook responded with status 503" {
		t.Errorf("last_error = %q, want the endpoint's own reason", got)
	}
}

// 请求体编不出来（流程变量里有 JSON 表示不了的值）：编码器的报错会带出变量的取值，同样不外泄；
// 重试无益，直接按不可重试处置。
func TestServiceJobEncodeErrorIsNotExposed(t *testing.T) {
	t.Parallel()
	j := &Job{ID: "j1", Kind: JobService, InstanceID: "i1", Payload: map[string]any{"webhookId": "hook"}, Attempts: 1}
	lookup := &treeLookup{in: &Instance{ID: "i1", Status: InstanceRunning, Variables: map[string]any{"ratio": math.NaN()}}}
	called := false
	caller := callerFake(func(context.Context, *Webhook, string, []byte) ([]byte, bool, error) {
		called = true
		return nil, false, nil
	})

	_, retryable, err := jobUC(&queueFake{}, txsOf(nil), lookup, hooksFake{}, caller).call(context.Background(), j)
	if err == nil || err.Error() != internalReason || retryable || called {
		t.Fatalf("call() = retryable %v, err %v, endpoint called %v; want the fixed reason, not retryable, no call", retryable, err, called)
	}
}

type hooksFake struct{ WebhookResolver }

func (hooksFake) Resolve(context.Context, string) (*Webhook, error) { return &Webhook{}, nil }

type callerFake func(ctx context.Context, hook *Webhook, jobID string, body []byte) ([]byte, bool, error)

func (f callerFake) Call(ctx context.Context, hook *Webhook, jobID string, body []byte) ([]byte, bool, error) {
	return f(ctx, hook, jobID, body)
}
