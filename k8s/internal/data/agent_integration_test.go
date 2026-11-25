//go:build integration

package data

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/jackc/pgx/v5"
)

// tokenRow 一枚 Agent 令牌的断言视图。
type tokenRow struct {
	Kind    int32
	Cluster string
	Hash    []byte
	Expires *time.Time
	Created time.Time
}

// tokenRow 读出一枚令牌；不存在返回 ok = false。
func (e *env) tokenRow(id string) (tokenRow, bool) {
	e.t.Helper()
	var r tokenRow
	err := e.admin.QueryRow(ctxb(), `SELECT kind, cluster_id::text, token_hash, expire_time, create_time FROM k8s_agent_token WHERE id = $1`,
		id).Scan(&r.Kind, &r.Cluster, &r.Hash, &r.Expires, &r.Created)
	if errors.Is(err, pgx.ErrNoRows) {
		return tokenRow{}, false
	}
	if err != nil {
		e.t.Fatalf("read token: %v", err)
	}
	return r, true
}

// longTerm 某集群当前存在的长期令牌 ID（按签发时间）。
func (e *env) longTerm(clusterID string) []string {
	e.t.Helper()
	rows, err := e.admin.Query(ctxb(), `SELECT id::text FROM k8s_agent_token WHERE cluster_id = $1 AND kind = $2 ORDER BY create_time, id`,
		clusterID, k8scontract.TokenLongTerm)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// waitBlocked 等到有别的会话在等锁（上限 10 秒）：用来确认一个后台调用确实卡在了行锁上。
func (e *env) waitBlocked() {
	e.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if e.count(`SELECT count(*) FROM pg_locks WHERE NOT granted`) > 0 {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatal("no session ended up waiting on a lock")
		}
	}
}

// 注册令牌在「读出」之后、「消费」之前被别的事务删掉（控制面吊销 / 重置注册令牌、删除集群，或过期清理——
// 它们都不走换发的咨询锁）：换发必须作废，不能给一枚已经不存在的注册令牌换发出永不过期的长期令牌。
func TestAgentTokenExchangeRevokedMidway(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	reg := e.token(agent, k8scontract.TokenRegister, "secret-1", ptr(time.Hour))
	before := len(e.longTerm(agent))

	// 另一个事务先删掉注册令牌但不提交：换发读得到这一行（读到的是已提交版本），消费时才会等它。
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `DELETE FROM k8s_agent_token WHERE id = $1`, reg); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, xerr := repo.Exchange(ctx, reg, "secret-1")
		done <- xerr
	}()
	e.waitBlocked()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, biz.ErrAgentToken) {
		t.Fatalf("Exchange() error = %v, want ErrAgentToken once the register token was deleted midway", err)
	}
	_, present := e.tokenRow(reg)
	if after := len(e.longTerm(agent)); present || after != before {
		t.Fatalf("register token present = %v, long-term tokens = %d (want %d): a revoked register token must not issue one", present, after, before)
	}
}

// 注册令牌换长期令牌：一次性（并发提交只成功一次）、密钥与类型都要对、每个集群的长期令牌有上限。
func TestAgentTokenExchange(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	direct := e.cluster(clusterSpec{})
	reg := e.token(agent, k8scontract.TokenRegister, "secret-1", ptr(time.Hour))

	// 换不到的情形（对外一律 ErrAgentToken）：密钥不对、令牌不存在、已过期、类型不符、不是 Agent 集群的令牌。
	for name, tc := range map[string]struct{ id, secret string }{
		"wrong secret":   {reg, "wrong"},
		"unknown token":  {e.id(), "secret-1"},
		"expired token":  {e.token(agent, k8scontract.TokenRegister, "s", ptr(-time.Second)), "s"},
		"wrong kind":     {e.token(agent, k8scontract.TokenLongTerm, "lt", nil), "lt"},
		"direct cluster": {e.token(direct, k8scontract.TokenRegister, "d", ptr(time.Hour)), "d"},
	} {
		if _, err := repo.Exchange(ctx, tc.id, tc.secret); !errors.Is(err, biz.ErrAgentToken) {
			t.Errorf("%s: Exchange() error = %v, want ErrAgentToken", name, err)
		}
	}
	if _, ok := e.tokenRow(reg); !ok {
		t.Fatal("a rejected exchange must leave the register token in place")
	}

	// 正向：换发出长期令牌；注册令牌被消费；长期令牌不过期、库里只存密钥的摘要。
	grant, err := repo.Exchange(ctx, reg, "secret-1")
	if err != nil || grant.ClusterID != agent || !idgen.IsUUID(grant.TokenID) {
		t.Fatalf("Exchange() = %+v, %v", grant, err)
	}
	tokenID, secret, ok := k8scontract.ParseToken(grant.Token)
	if !ok || tokenID != grant.TokenID {
		t.Fatalf("issued token does not carry its own ID: %q", grant.Token)
	}
	row, ok := e.tokenRow(grant.TokenID)
	if _, still := e.tokenRow(reg); !ok || still || row.Kind != k8scontract.TokenLongTerm || row.Cluster != agent || row.Expires != nil ||
		!slices.Equal(row.Hash, k8scontract.HashSecret(secret)) || row.Created.Before(e.dbNow().Add(-time.Minute)) {
		t.Fatalf("issued token row = %+v (exists %v), register token still present = %v", row, ok, still)
	}
	if g, err := repo.Verify(ctx, grant.TokenID, secret); err != nil || g.TokenID != grant.TokenID || g.ClusterID != agent {
		t.Fatalf("Verify() of the issued token = %+v, %v", g, err)
	}

	// 并发提交同一个注册令牌：只成功一次，其余一律 ErrAgentToken；只多出一枚长期令牌。
	reg2 := e.token(agent, k8scontract.TokenRegister, "secret-2", ptr(time.Hour))
	var (
		successes atomic.Int32
		wg        sync.WaitGroup
		mu        sync.Mutex
		winner    string
	)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := repo.Exchange(ctx, reg2, "secret-2")
			switch {
			case err == nil:
				successes.Add(1)
				mu.Lock()
				winner = g.TokenID
				mu.Unlock()
			case !errors.Is(err, biz.ErrAgentToken):
				t.Errorf("Exchange() error = %v, want ErrAgentToken", err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || winner == "" || len(e.longTerm(agent)) != 2 {
		t.Fatalf("%d concurrent exchanges succeeded (long-term tokens: %v), want exactly 1 and 2 tokens", successes.Load(), e.longTerm(agent))
	}

	// 上限：同一集群的长期令牌最多 k8scontract.MaxLongTermTokens 枚，超出时删最旧的（重新安装的 Agent 顶替旧的）。
	var issued []string
	for i := range k8scontract.MaxLongTermTokens + 1 {
		r := e.token(agent, k8scontract.TokenRegister, fmt.Sprintf("more-%d", i), ptr(time.Hour))
		g, err := repo.Exchange(ctx, r, fmt.Sprintf("more-%d", i))
		if err != nil {
			t.Fatalf("Exchange() #%d error = %v", i, err)
		}
		issued = append(issued, g.TokenID)
	}
	if got := e.longTerm(agent); len(got) != k8scontract.MaxLongTermTokens || !sameSet(got, issued[1:]) {
		t.Fatalf("long-term tokens = %v, want the %d newest of %v", got, k8scontract.MaxLongTermTokens, issued)
	}
	alive, err := repo.Alive(ctx, append([]string{grant.TokenID, winner}, issued...))
	if err != nil || !sameSet(slices.Collect(maps.Keys(alive)), issued[1:]) {
		t.Fatalf("Alive() = %v, %v; want the two newest tokens only", maps.Keys(alive), err)
	}
}

// 长期令牌校验与存活判定：无效的对外一律相同；停用的集群与「令牌无效」区分开。
func TestAgentTokenVerifyAndAlive(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	off := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent, Disabled: true})
	okToken := e.token(agent, k8scontract.TokenLongTerm, "s-ok", nil)
	expired := e.token(agent, k8scontract.TokenLongTerm, "s-exp", ptr(-time.Second))
	wrongKind := e.token(agent, k8scontract.TokenRegister, "s-reg", ptr(time.Hour))
	offToken := e.token(off, k8scontract.TokenLongTerm, "s-off", nil)

	g, err := repo.Verify(ctx, okToken, "s-ok")
	if err != nil || g.TokenID != okToken || g.ClusterID != agent || g.ClusterName == "" {
		t.Fatalf("Verify() = %+v, %v", g, err)
	}
	for name, tc := range map[string]struct{ id, secret string }{
		"wrong secret":  {okToken, "nope"},
		"unknown token": {e.id(), "s-ok"},
		"expired":       {expired, "s-exp"},
		"wrong kind":    {wrongKind, "s-reg"},
	} {
		if _, err := repo.Verify(ctx, tc.id, tc.secret); !errors.Is(err, biz.ErrAgentToken) {
			t.Errorf("%s: Verify() error = %v, want ErrAgentToken", name, err)
		}
	}
	// 集群已停用：密钥比对通过之后才区分——Agent 遇到它应保留令牌、稍后重连。
	if _, err := repo.Verify(ctx, offToken, "wrong"); !errors.Is(err, biz.ErrAgentToken) {
		t.Fatalf("Verify() on a disabled cluster with a wrong secret error = %v, want ErrAgentToken", err)
	}
	if _, err := repo.Verify(ctx, offToken, "s-off"); !errors.Is(err, biz.ErrAgentClusterDisabled) {
		t.Fatalf("Verify() on a disabled cluster error = %v, want ErrAgentClusterDisabled", err)
	}
	// 集群被删除：令牌随之失效。
	deleted := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	deletedToken := e.token(deleted, k8scontract.TokenLongTerm, "s-del", nil)
	e.exec(`UPDATE k8s_cluster SET delete_time = now() WHERE id = $1`, deleted)
	if _, err := repo.Verify(ctx, deletedToken, "s-del"); !errors.Is(err, biz.ErrAgentToken) {
		t.Fatalf("Verify() on a deleted cluster error = %v, want ErrAgentToken", err)
	}

	// Alive：给定长期令牌里仍然有效的——停用 / 删除的集群、其它类型、不认识的 ID 都不在结果里。
	alive, err := repo.Alive(ctx, []string{okToken, wrongKind, offToken, deletedToken, e.id()})
	if err != nil || !sameSet(slices.Collect(maps.Keys(alive)), []string{okToken}) {
		t.Fatalf("Alive() = %v, %v; want only the valid token", maps.Keys(alive), err)
	}
	// 边界（现状）：Alive 不看 expire_time——带过期时间且已过期的长期令牌仍算「在」（Verify 判它无效）。
	// 长期令牌由数据面签发、expire_time 恒为 NULL，只有有人手工给长期令牌设了过期时间才有差别。
	if alive, err = repo.Alive(ctx, []string{expired}); err != nil || !sameSet(slices.Collect(maps.Keys(alive)), []string{expired}) {
		t.Fatalf("Alive() of an expired long-term token = %v, %v (current behaviour: reported alive)", maps.Keys(alive), err)
	}
	if alive, err = repo.Alive(ctx, nil); err != nil || len(alive) != 0 {
		t.Fatalf("Alive(nil) = %v, %v; want empty", alive, err)
	}
}

// Agent 集群的归属：以「持有隧道」为由领取 / 续租；另一个存活实例的有效租约挡住备用隧道；
// 原主实例下线或租约过期即可接管；断开后以归属围栏放弃。
func TestAgentAttachDetach(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	const lease = time.Minute
	agent := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})
	direct := e.cluster(clusterSpec{})
	disabled := e.cluster(clusterSpec{Mode: k8scontract.AccessAgent, Disabled: true})
	attach := func(instance, clusterID, version string, want bool) {
		t.Helper()
		if ok, err := repo.Attach(ctx, instance, clusterID, version, lease); err != nil || ok != want {
			t.Fatalf("Attach(%s, %s) = %v, %v; want %v", instance, clusterID[:8], ok, err, want)
		}
	}

	// 直连集群与停用的集群接入不了：归属不变。
	attach("i1", direct, "1.0.0", false)
	attach("i1", disabled, "1.0.0", false)
	if r := e.runtime(direct); r.Owner != "" || e.count("SELECT count(*) FROM k8s_cluster_runtime WHERE cluster_id = $1", agent) != 0 {
		t.Fatalf("a rejected attach changed ownership: %+v", r)
	}
	// 正向：领取归属，回写 Agent 版本与在线时间。
	before := e.dbNow()
	attach("i1", agent, "1.2.3", true)
	r := e.runtime(agent)
	if r.Owner != "i1" || r.LeaseEnds == nil || r.AgentVersion != "1.2.3" || r.AgentConnected == nil || r.AgentSeen == nil ||
		r.AgentConnected.Before(before) || r.AgentSeen.Before(before) {
		t.Fatalf("runtime after Attach: %+v", r)
	}
	// 同一实例再次接入（Agent 重连）：续租，连接建立时间不变，版本更新。
	e.exec(`UPDATE k8s_cluster_runtime SET agent_connect_time = agent_connect_time - interval '1 hour' WHERE cluster_id = $1`, agent)
	e.exec(`UPDATE k8s_cluster_runtime SET lease_until = lease_until - interval '30 seconds' WHERE cluster_id = $1`, agent)
	connected := e.dbNow().Add(-time.Hour)
	attach("i1", agent, "1.2.4", true)
	if r = e.runtime(agent); r.Owner != "i1" || r.AgentVersion != "1.2.4" || r.AgentConnected == nil || r.AgentConnected.After(connected.Add(time.Minute)) ||
		r.LeaseEnds == nil || r.LeaseEnds.Before(e.dbNow().Add(50*time.Second)) {
		t.Fatalf("runtime after a same-instance attach: %+v", r)
	}
	// 另一个实例：原主实例存活且租约有效 → 接不上（它的隧道作为备用保持连接）。
	e.instance("i1", "http://i1:8030", 1, 0)
	attach("i2", agent, "1.2.4", false)
	if r = e.runtime(agent); r.Owner != "i1" {
		t.Fatalf("a standby attach stole the cluster: %+v", r)
	}
	// 原主实例已无心跳：不必等租约到期，另一个实例立即接管；连接时间重新计。
	e.exec(`UPDATE k8s_instance SET heartbeat_time = now() - interval '1 hour' WHERE id = 'i1'`)
	before = e.dbNow()
	attach("i2", agent, "1.2.4", true)
	if r = e.runtime(agent); r.Owner != "i2" || r.AgentConnected == nil || r.AgentConnected.Before(before) {
		t.Fatalf("takeover from a dead instance: %+v", r)
	}
	// 租约仍有效且原主存活时挡住别人；租约过期后可被接管。
	e.instance("i2", "http://i2:8030", 1, 0)
	attach("i1", agent, "1.2.4", false)
	e.exec(`UPDATE k8s_cluster_runtime SET lease_until = now() - interval '1 second' WHERE cluster_id = $1`, agent)
	attach("i1", agent, "1.2.4", true)
	// Agent 版本超过列宽（varchar(32)）：截断后写入。
	attach("i1", agent, strings.Repeat("v", 40), true)
	if r = e.runtime(agent); len(r.AgentVersion) != 32 {
		t.Fatalf("agent version = %q, want 32 characters", r.AgentVersion)
	}
	// Detach：以归属围栏——别人放弃不了；持有者放弃后归属清空、连接时间清掉、在线时间记到现在。
	e.exec(`UPDATE k8s_cluster_runtime SET agent_last_seen = agent_last_seen - interval '1 hour' WHERE cluster_id = $1`, agent)
	if err := repo.Detach(ctx, "i2", agent); err != nil {
		t.Fatalf("Detach() by a non-owner error = %v", err)
	}
	if r = e.runtime(agent); r.Owner != "i1" {
		t.Fatalf("a non-owner detached the cluster: %+v", r)
	}
	before = e.dbNow()
	if err := repo.Detach(ctx, "i1", agent); err != nil {
		t.Fatalf("Detach() error = %v", err)
	}
	if r = e.runtime(agent); r.Owner != "" || r.LeaseEnds != nil || r.AgentConnected != nil || r.AgentSeen == nil || r.AgentSeen.Before(before) {
		t.Fatalf("runtime after Detach: %+v", r)
	}
	attach("i3", agent, "1.0.0", true)
}

// Agent 离线判定：没有任何实例持有隧道、且离线超过宽限期的集群标为不可达，返回本次翻转的集群
// （并标出是否由在线翻转）；在线的、租约仍有效的、宽限期内的、已是不可达的都不动。
func TestSweepOffline(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	const lease, grace = time.Minute, 20 * time.Second
	agent := func() string { return e.cluster(clusterSpec{Mode: k8scontract.AccessAgent}) }
	never, online, gone, held, down, fresh := agent(), agent(), agent(), agent(), agent(), agent()
	e.exec(`UPDATE k8s_cluster SET create_time = now() - interval '1 hour' WHERE id = $1`, never) // 从未接入过
	for _, id := range []string{online, gone, held, down, fresh} {
		if ok, err := repo.Attach(ctx, "i1", id, "1.0.0", lease); err != nil || !ok {
			t.Fatalf("Attach(%s) = %v, %v", id[:8], ok, err)
		}
	}
	e.instance("i1", "http://i1:8030", 5, 0)
	if _, _, err := NewRuntimeRepo(e.d).SaveProbe(ctx, "i1", biz.ProbeRecord{
		ClusterID: online, Status: k8scontract.ClusterOnline,
		Info: biz.ProbeInfo{Version: "v1.32.1", NodeTotal: 2, NodeReady: 2},
	}); err != nil {
		t.Fatalf("SaveProbe() error = %v", err)
	}
	// gone / fresh / down：隧道已断（归属放弃、在线时间记到现在）；gone 的在线时间与状态说明它之前是在线的。
	for _, id := range []string{gone, fresh, down} {
		if err := repo.Detach(ctx, "i1", id); err != nil {
			t.Fatalf("Detach() error = %v", err)
		}
	}
	e.exec(`UPDATE k8s_cluster_runtime SET status = 1, agent_last_seen = now() - interval '1 hour' WHERE cluster_id = $1`, gone)
	e.exec(`UPDATE k8s_cluster_runtime SET status = $2, agent_last_seen = now() - interval '1 hour', status_since = now() - interval '2 hours' WHERE cluster_id = $1`,
		down, k8scontract.ClusterUnreachable)
	// held：隧道断了之后在线时间停在一小时前，但归属租约仍有效（实例还在收尾）。
	e.exec(`UPDATE k8s_cluster_runtime SET agent_last_seen = now() - interval '1 hour' WHERE cluster_id = $1`, held)
	e.cluster(clusterSpec{}) // 直连集群不参与

	offline, err := repo.SweepOffline(ctx, grace)
	if err != nil {
		t.Fatalf("SweepOffline() error = %v", err)
	}
	var ids []string
	for _, o := range offline {
		ids = append(ids, o.Cluster.ID)
		if o.Cluster.Name == "" || o.Cluster.Env != "test" || o.Since.Before(e.dbNow().Add(-time.Minute)) {
			t.Fatalf("offline agent = %+v", o)
		}
		if wasOnline := o.Cluster.ID == gone; o.WasOnline != wasOnline {
			t.Fatalf("cluster %s: WasOnline = %v, want %v", o.Cluster.ID[:8], o.WasOnline, wasOnline)
		}
	}
	if !sameSet(ids, []string{gone, never}) {
		t.Fatalf("swept %v, want the cluster offline beyond the grace and the never-connected one", ids)
	}
	for _, id := range []string{gone, never} {
		if r := e.runtime(id); r.Status != k8scontract.ClusterUnreachable || r.LastError != biz.AgentOfflineMessage ||
			r.Owner != "" || r.LeaseEnds != nil || r.Probed == nil || r.Since.Before(e.dbNow().Add(-time.Minute)) {
			t.Fatalf("runtime of swept cluster %s: %+v", id[:8], r)
		}
	}
	if r := e.runtime(online); r.Status != k8scontract.ClusterOnline || r.Owner != "i1" {
		t.Fatalf("an online cluster was swept: %+v", r)
	}
	if r := e.runtime(fresh); r.Status != k8scontract.ClusterUnknown || r.Owner != "" || r.LastError != "" {
		t.Fatalf("a cluster within the grace was swept: %+v", r)
	}
	if r := e.runtime(held); r.Status != k8scontract.ClusterUnknown || r.Owner != "i1" || r.LeaseEnds == nil {
		t.Fatalf("a cluster whose lease is still held was swept: %+v", r)
	}
	if r := e.runtime(down); r.Status != k8scontract.ClusterUnreachable || r.Since.After(e.dbNow().Add(-time.Hour)) || r.LastError != "" {
		t.Fatalf("a cluster that is already unreachable was rewritten: %+v", r)
	}
	// 幂等：没有新的翻转。
	if offline, err = repo.SweepOffline(ctx, grace); err != nil || len(offline) != 0 {
		t.Fatalf("second SweepOffline() = %+v, %v; want none", offline, err)
	}
	// 宽限期过了之后 fresh 才翻：之前是「未知」，不算由在线翻转。
	e.exec(`UPDATE k8s_cluster_runtime SET agent_last_seen = now() - interval '1 hour' WHERE cluster_id = $1`, fresh)
	if offline, err = repo.SweepOffline(ctx, grace); err != nil || len(offline) != 1 || offline[0].Cluster.ID != fresh || offline[0].WasOnline {
		t.Fatalf("SweepOffline() after the grace = %+v, %v; want fresh, not WasOnline", offline, err)
	}
}

// 多实例并发清扫：同一个集群只被一个实例翻转（FOR UPDATE SKIP LOCKED）。
func TestSweepOfflineIsExclusive(t *testing.T) {
	e := newEnv(t)
	repo := NewAgentRepo(e.d)
	ctx := ctxb()
	stale := []string{e.cluster(clusterSpec{Mode: k8scontract.AccessAgent}), e.cluster(clusterSpec{Mode: k8scontract.AccessAgent})}
	for _, id := range stale {
		if ok, err := repo.Attach(ctx, "i1", id, "1.0.0", time.Minute); err != nil || !ok {
			t.Fatalf("Attach() = %v, %v", ok, err)
		}
		if err := repo.Detach(ctx, "i1", id); err != nil {
			t.Fatal(err)
		}
		e.exec(`UPDATE k8s_cluster_runtime SET agent_last_seen = now() - interval '1 hour' WHERE cluster_id = $1`, id)
	}
	var (
		mu       sync.Mutex
		swept    []string
		wg       sync.WaitGroup
		failures atomic.Int32
	)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offline, err := repo.SweepOffline(ctx, 20*time.Second)
			if err != nil {
				failures.Add(1)
				return
			}
			mu.Lock()
			for _, o := range offline {
				swept = append(swept, o.Cluster.ID)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if failures.Load() != 0 || !sameSet(swept, stale) || len(swept) != len(stale) {
		t.Fatalf("clusters swept %d times across instances: %v (failures %d); want each exactly once", len(swept), swept, failures.Load())
	}
}
