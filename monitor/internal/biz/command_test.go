package biz

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// —— 事件指令：连续失败的上限（间隔与起点的语义在 data 层的集成测试里，这里只验用例的决策） ——

// failingCommandRepo 一条恒失败的手动关闭指令。
func failingCommandRepo(err error) *fakeCommandRepo {
	return &fakeCommandRepo{
		pending: []AlertCommand{{ID: "c1", AlertID: "a1", Kind: monitorcontract.AlertCmdClose}},
		outcome: map[string]error{"c1": err},
	}
}

func TestCommandFailureIsRecordedButNotAbandoned(t *testing.T) {
	boom := errors.New("deadlock")
	repo, obs, logs := failingCommandRepo(boom), &fakeObserver{}, &logBuffer{}
	uc := NewCommandUsecase(repo, repo, obs, slog.New(slog.NewTextHandler(logs, nil)))
	// 首次失败：记一笔，不满上限，不放弃；指令仍是待处理，全部失败才向循环返回错误（退避）。
	for range 2 {
		if n, err := uc.RunOnce(context.Background()); n != 0 || !errors.Is(err, boom) {
			t.Fatalf("RunOnce() = %d, %v; want the failure returned to the loop", n, err)
		}
	}
	// 每次失败都带着两个阈值交给仓储：间隔阈值用于「两次失败隔得太久就重新计时」，由数据库侧判定。
	if !slices.Equal(repo.gaps, []time.Duration{commandFailureGap, commandFailureGap}) ||
		!slices.Equal(repo.giveUps, []time.Duration{commandGiveUpAfter, commandGiveUpAfter}) {
		t.Fatalf("thresholds passed to the repo: gaps = %v, giveUps = %v", repo.gaps, repo.giveUps)
	}
	if repo.attempts != nil || len(repo.reasons) != 0 {
		t.Fatalf("a command below the limit must not be abandoned: %v %v", repo.attempts, repo.reasons)
	}
	if obs.commands[CommandFailed] != 2 || obs.commands[CommandAbandoned] != 0 {
		t.Fatalf("metrics = %v", obs.commands)
	}
	if logs.count("alert command failed, it stays pending", "command_id=c1", "alert_id=a1", "error=deadlock") != 2 ||
		logs.count("abandoned") != 0 {
		t.Fatalf("logs =\n%s", logs.String())
	}
}

func TestCommandIsAbandonedAfterContinuousFailures(t *testing.T) {
	boom := errors.New("deadlock")
	repo, obs, logs := failingCommandRepo(boom), &fakeObserver{}, &logBuffer{}
	// 第 3 次记失败时数据库报告「连续失败已满 5 分钟」。
	repo.giveUpAt, repo.abandonOK = 3, true
	uc := NewCommandUsecase(repo, repo, obs, slog.New(slog.NewTextHandler(logs, nil)))
	for range 2 {
		if _, err := uc.RunOnce(context.Background()); err == nil {
			t.Fatal("below the limit the failure must still reach the loop")
		}
	}
	if repo.attempts != nil {
		t.Fatalf("abandoned too early: %v", repo.attempts)
	}
	// 第三次：满上限 → 放弃。指令不再是待处理：计入处理条数，不再向循环返回错误。
	if n, err := uc.RunOnce(context.Background()); n != 0 || err != nil {
		t.Fatalf("RunOnce() = %d, %v; want the abandoned command counted as handled", n, err)
	}
	if !slices.Equal(repo.attempts, []string{"c1"}) || !slices.Equal(repo.reasons, []string{"手动关闭未执行：引擎连续 5 分钟执行失败：deadlock"}) {
		t.Fatalf("abandon attempts = %v, reasons = %q", repo.attempts, repo.reasons)
	}
	// 放弃时复核用的时限与记失败时是同一个。
	if !slices.Equal(repo.giveUps, []time.Duration{commandGiveUpAfter, commandGiveUpAfter, commandGiveUpAfter, commandGiveUpAfter}) {
		t.Fatalf("giveUps = %v", repo.giveUps)
	}
	// 每次执行失败各记一笔 failed，放弃的指令另记一笔 abandoned。
	if obs.commands[CommandFailed] != 3 || obs.commands[CommandAbandoned] != 1 {
		t.Fatalf("metrics = %v", obs.commands)
	}
	if logs.count("alert command failed, it stays pending") != 2 ||
		logs.count("alert command abandoned after continuous failures", "command_id=c1", "alert_id=a1", "error=deadlock") != 1 {
		t.Fatalf("logs =\n%s", logs.String())
	}
	// 放弃也算处理：处理满一批时循环立即再跑一轮（把余下的待处理指令接着处理掉）。
	uc.WithOptions(CommandOptions{Batch: 1})
	if n, err := uc.RunOnce(context.Background()); n != 1 || err != nil {
		t.Fatalf("RunOnce() = %d, %v; want a full batch reported as handled", n, err)
	}
}

// 放弃动作的围栏未命中（别的实例正在处理或刚处理掉这条指令）：不记 abandoned，指令仍按失败处理。
func TestCommandGiveUpFenceMissKeepsTheCommandPending(t *testing.T) {
	boom := errors.New("deadlock")
	repo, obs, logs := failingCommandRepo(boom), &fakeObserver{}, &logBuffer{}
	repo.giveUpAt = 1
	uc := NewCommandUsecase(repo, repo, obs, slog.New(slog.NewTextHandler(logs, nil)))
	if n, err := uc.RunOnce(context.Background()); n != 0 || !errors.Is(err, boom) {
		t.Fatalf("RunOnce() = %d, %v; want the failure returned to the loop", n, err)
	}
	if !slices.Equal(repo.attempts, []string{"c1"}) || len(repo.reasons) != 0 {
		t.Fatalf("attempts = %v, reasons = %q", repo.attempts, repo.reasons)
	}
	if obs.commands[CommandFailed] != 1 || obs.commands[CommandAbandoned] != 0 {
		t.Fatalf("metrics = %v", obs.commands)
	}
	if logs.count("alert command failed, it stays pending", "give_up_skipped=true") != 1 || logs.count("abandoned after") != 0 {
		t.Fatalf("logs =\n%s", logs.String())
	}
}

// 记失败或放弃本身出错（数据库不可用等）：指令仍是待处理，不影响其余指令，下一轮重来。
func TestCommandGiveUpErrorsKeepTheCommandPending(t *testing.T) {
	boom := errors.New("deadlock")
	dbDown := errors.New("dbdown")
	for name, tc := range map[string]struct {
		set         func(*fakeCommandRepo)
		abandonTry  bool   // 是否走到了放弃这一步
		want, noLog string // 失败日志里必须带 / 不能带的属性
	}{
		// 记失败时指令已被别的实例处理：不再放弃，由那个实例负责。
		"gone":    {set: func(r *fakeCommandRepo) { r.recordErr = ErrCommandGone }, want: "command_id=c1", noLog: "failure_not_recorded"},
		"record":  {set: func(r *fakeCommandRepo) { r.recordErr = dbDown }, want: "failure_not_recorded=dbdown", noLog: "abandon_error"},
		"abandon": {set: func(r *fakeCommandRepo) { r.giveUpAt, r.abandonErr = 1, dbDown }, abandonTry: true, want: "abandon_error=dbdown"},
	} {
		t.Run(name, func(t *testing.T) {
			repo, obs, logs := failingCommandRepo(boom), &fakeObserver{}, &logBuffer{}
			tc.set(repo)
			uc := NewCommandUsecase(repo, repo, obs, slog.New(slog.NewTextHandler(logs, nil)))
			if n, err := uc.RunOnce(context.Background()); n != 0 || !errors.Is(err, boom) {
				t.Fatalf("RunOnce() = %d, %v; want the failure returned to the loop", n, err)
			}
			if (len(repo.attempts) > 0) != tc.abandonTry || len(repo.reasons) != 0 {
				t.Fatalf("attempts = %v, reasons = %q", repo.attempts, repo.reasons)
			}
			if obs.commands[CommandFailed] != 1 || obs.commands[CommandAbandoned] != 0 {
				t.Fatalf("metrics = %v", obs.commands)
			}
			if logs.count("alert command failed, it stays pending", tc.want) != 1 || logs.count("abandoned after") != 0 ||
				(tc.noLog != "" && logs.count(tc.noLog) != 0) {
				t.Fatalf("logs =\n%s", logs.String())
			}
		})
	}
}

// 成功、被拒、被别的实例处理、不认识的指令：都不记失败，更不会放弃。
func TestCommandOtherOutcomesRecordNoFailure(t *testing.T) {
	repo := &fakeCommandRepo{
		pending: []AlertCommand{
			{ID: "1", AlertID: "a-1", Kind: monitorcontract.AlertCmdClose},
			{ID: "2", AlertID: "a-2", Kind: monitorcontract.AlertCmdClose},
			{ID: "3", AlertID: "a-3", Kind: 99},
			{ID: "4", AlertID: "a-4", Kind: monitorcontract.AlertCmdClose},
		},
		open:    map[string]bool{"1": true},
		outcome: map[string]error{"4": ErrCommandGone},
	}
	obs := &fakeObserver{}
	// 4 条里处理了 2 条（关闭、被拒），不满一批：返回 0、无错误。
	n, err := NewCommandUsecase(repo, repo, obs, quietLogger()).RunOnce(context.Background())
	if err != nil || n != 0 || !slices.Equal(repo.closed, []string{"a-1"}) {
		t.Fatalf("RunOnce() = %d, %v; closed = %v", n, err, repo.closed)
	}
	if repo.failures != nil || repo.attempts != nil {
		t.Fatalf("failures = %v, attempts = %v", repo.failures, repo.attempts)
	}
	if obs.commands[CommandDone] != 1 || obs.commands[CommandRejected] != 1 || obs.commands[CommandFailed] != 0 || obs.commands[CommandAbandoned] != 0 {
		t.Fatalf("metrics = %v", obs.commands)
	}
}

// 停机中被取消的那一次执行不算指令的失败：不记失败，更不会因此放弃。
func TestCommandFailureDuringShutdownIsNotRecorded(t *testing.T) {
	repo, obs := failingCommandRepo(nil), &fakeObserver{}
	repo.giveUpAt = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc := NewCommandUsecase(repo, repo, obs, quietLogger())
	if uc.failed(ctx, AlertCommand{ID: "c1", AlertID: "a1"}, context.Canceled) {
		t.Fatal("a cancelled run must not abandon the command")
	}
	if repo.failures != nil || repo.attempts != nil || obs.commands[CommandFailed] != 1 {
		t.Fatalf("failures = %v, attempts = %v, metrics = %v", repo.failures, repo.attempts, obs.commands)
	}
}

// 写进 result（varchar(255)）与时间线的说明不能超长，且不切断字符、不带 NUL；时长随阈值常量。
func TestCommandAbandonReasonIsBounded(t *testing.T) {
	if why := abandonReason(errors.New("boom")); why != "手动关闭未执行：引擎连续 5 分钟执行失败：boom" {
		t.Fatalf("abandonReason() = %q", why)
	}
	long := abandonReason(errors.New(strings.Repeat("错\x00", 400)))
	if len(long) > maxCommandResultBytes || !utf8.ValidString(long) || strings.Contains(long, "\x00") ||
		!strings.HasPrefix(long, "手动关闭未执行：引擎连续 5 分钟执行失败：") || !strings.HasSuffix(long, "错") {
		t.Fatalf("abandonReason() = %q (%d bytes)", long, len(long))
	}
}
