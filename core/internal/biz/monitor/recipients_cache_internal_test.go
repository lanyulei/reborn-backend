package biz

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// countingDir 与 system 域 DirectoryUsecase.ActiveUserIDs 同一口径的目录桩：先转小写、去重、丢掉非法 ID，
// 再按顺序筛出可用用户；有合法 ID 时返回非 nil 切片。calls 记每次调用收到的入参。
type countingDir struct {
	Directory
	active map[string]bool
	calls  [][]string
}

func (d *countingDir) ActiveUserIDs(_ context.Context, ids []string) ([]string, error) {
	d.calls = append(d.calls, slices.Clone(ids))
	var valid []string
	for _, id := range ids {
		id = strings.ToLower(id)
		if idgen.IsUUID(id) && !slices.Contains(valid, id) {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(valid))
	for _, id := range valid {
		if d.active[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

type fixedDuty map[string][]string

func (f fixedDuty) OnDuty(_ context.Context, scheduleID string, _ time.Time) ([]string, error) {
	return f[scheduleID], nil
}

func cacheUser(n int) string { return fmt.Sprintf("018f3e1a-0000-7000-8000-%012x", n) }

func cacheSub(owner string, self bool, schedules []string, users ...string) *Subscription {
	sub := &Subscription{ID: "sub-" + owner, OwnerID: owner}
	sub.Spec.Receivers.Self, sub.Spec.Receivers.UserIDs, sub.Spec.Receivers.ScheduleIDs = self, users, schedules
	return sub
}

// 收件人解析的按条查询（core-monitor-dispatch 优化项 1）：会话内缓存 + 预取之后，每条订阅解析出的接收方与
// 「每条订阅各问一次目录」逐条相同（含顺序），而目录往返从每条订阅一次降到预取一次（外加值班人里的生面孔）。
func TestResolveSessionCachesDirectoryLookupsWithoutChangingResults(t *testing.T) {
	active := map[string]bool{}
	for i := 1; i <= 40; i++ {
		active[cacheUser(i)] = i%4 != 0 // 每 4 个里有 1 个已停用
	}
	duty := fixedDuty{"sch-1": {cacheUser(3), cacheUser(100)}, "sch-2": nil}
	active[cacheUser(100)] = true
	var subs []*Subscription
	for i := 1; i <= 30; i++ {
		var schedules []string
		if i%5 == 0 {
			schedules = []string{"sch-1", "sch-2"}
		}
		// 所有者本人、与别的订阅重叠的指定用户、重复的 ID、停用的用户。
		subs = append(subs, cacheSub(cacheUser(i), i%3 != 0, schedules, cacheUser(i+1), cacheUser(i+1), cacheUser(40-i)))
	}
	// 非规范写法：大写 ID、非法 ID、所有者不是 UUID——这些订阅必须原样转给目录。
	subs = append(subs,
		cacheSub(cacheUser(7), true, nil, strings.ToUpper(cacheUser(8)), cacheUser(9)),
		cacheSub("not-a-uuid", true, nil, cacheUser(1), "bogus"),
		cacheSub("", false, nil),
	)

	// 基线：改动前的做法——每条订阅把候选用户去重后整个交给目录。
	base := &countingDir{active: active}
	want := make([][]string, len(subs))
	for i, sub := range subs {
		users := slices.Clone(sub.Spec.Receivers.UserIDs)
		if sub.Spec.Receivers.Self {
			users = append(users, sub.OwnerID)
		}
		for _, sch := range sub.Spec.Receivers.ScheduleIDs {
			users = append(users, duty[sch]...)
		}
		if u := uniq(users); len(u) > 0 {
			var err error
			if want[i], err = base.ActiveUserIDs(context.Background(), u); err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, prefetch := range []bool{true, false} {
		dir := &countingDir{active: active}
		sess := NewRecipientResolver(dir, nil, duty, nil, nil).Session(time.Now())
		if prefetch {
			if err := sess.Prefetch(context.Background(), subs); err != nil {
				t.Fatal(err)
			}
			if len(dir.calls) != 1 {
				t.Fatalf("Prefetch() made %d directory calls, want 1", len(dir.calls))
			}
		}
		for i, sub := range subs {
			got, err := sess.Subscription(context.Background(), sub)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want[i]) {
				t.Fatalf("prefetch=%v sub %d: recipients = %v, want %v", prefetch, i, got, want[i])
			}
		}
		// 同一个用户至多问一次（非规范写法的订阅原样转发，不计）。
		asked := map[string]int{}
		for _, call := range dir.calls {
			if slices.ContainsFunc(call, func(id string) bool { return !idgen.IsUUID(id) }) {
				continue
			}
			for _, id := range call {
				asked[id]++
			}
		}
		for id, n := range asked {
			if n > 1 {
				t.Fatalf("prefetch=%v: user %s was looked up %d times", prefetch, id, n)
			}
		}
		if prefetch && len(dir.calls) > 4 {
			// 预取 1 次 + 值班人里的生面孔 1 次 + 两条非规范写法的订阅各 1 次。
			t.Fatalf("directory calls = %d (%v), want at most 4 for %d subscriptions", len(dir.calls), dir.calls, len(subs))
		}
		if len(base.calls) < 30 {
			t.Fatalf("baseline made only %d calls: the test no longer exercises the N+1", len(base.calls))
		}
	}
}

// 规则通知对象的解析同样走缓存：结果、说明（有停用用户被略过、资源负责人不是可用用户）都不变；第二次解析
// 同一批用户不再问目录。
func TestResolveSessionRuleNotesSurviveTheCache(t *testing.T) {
	dir := &countingDir{active: map[string]bool{cacheUser(1): true, cacheUser(2): false, cacheUser(3): false}}
	sess := NewRecipientResolver(dir, nil, fixedDuty{}, nil, nil).Session(time.Now())
	targets := mc.NotifyTargets{UserIDs: []string{cacheUser(1), cacheUser(2)}, CIOwner: true}
	ci := &CIEntry{ID: "ci-1", Owner: strings.ToUpper(cacheUser(3))}
	for round := range 2 {
		users, notes, err := sess.Rule(context.Background(), targets, ci)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(users, []string{cacheUser(1)}) {
			t.Fatalf("round %d: users = %v", round, users)
		}
		wantNotes := []ResolveNote{{Kind: NoteUsersInactive}, {Kind: NoteCIOwnerNotUser, ID: "ci-1"}}
		if !slices.Equal(notes, wantNotes) {
			t.Fatalf("round %d: notes = %v, want %v", round, notes, wantNotes)
		}
	}
	if len(dir.calls) != 1 {
		t.Fatalf("directory calls = %d, want 1 (the second resolution is served from the session)", len(dir.calls))
	}
	// 全部不可用时仍返回空而非报错；候选为空时不问目录。
	if users, err := sess.active(context.Background(), []string{cacheUser(2)}, nil); err != nil || len(users) != 0 {
		t.Fatalf("active() = %v, %v", users, err)
	}
	if users, err := sess.active(context.Background(), nil, nil); err != nil || users != nil || len(dir.calls) != 1 {
		t.Fatalf("active(nil) = %v, %v, calls = %d", users, err, len(dir.calls))
	}
}
