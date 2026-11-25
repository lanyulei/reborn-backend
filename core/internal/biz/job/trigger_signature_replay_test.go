package biz

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// oneTrigger 只按 ID 返回一个触发器。
type oneTrigger struct{ t *Trigger }

func (o oneTrigger) List(context.Context, TriggerFilter, int64, int64) ([]*Trigger, int64, error) {
	return nil, 0, nil
}

func (o oneTrigger) Get(context.Context, string) (*Trigger, error) { return o.t, nil }

func (o oneTrigger) Write(context.Context, WriteOp, *Trigger, int32, string, *TriggerToken) error {
	return nil
}

// 签名认证只有时间窗：不带幂等键时，窗口内重放同一份「时间戳 + 签名 + 请求体」此前每次都新建触发记录并提交
// 一次执行。现在由签名派生隐式幂等键，重放落到同一条记录上；不同签名各自触发；显式幂等键与 Bearer 认证不变。
func TestFireSignatureReplayIsDeduplicated(t *testing.T) {
	t.Parallel()
	token, hash, err := jc.NewTriggerToken()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	sign := func(ts int64, body string) FireAuth {
		mac := hmac.New(sha256.New, []byte(token))
		mac.Write([]byte(strconv.FormatInt(ts, 10) + "." + body))
		return FireAuth{Timestamp: strconv.FormatInt(ts, 10), Signature: "sha256=" + hex.EncodeToString(mac.Sum(nil)), Body: []byte(body)}
	}
	newUsecase := func() (*TriggerUsecase, *alertFires) {
		tpl := &Template{ID: "t", Name: "x", Enabled: false} // 停用：提交被业务规则拒绝 → 记为失败，但登记成立
		execs := NewExecutionUsecase(nil, nil, nil, fakeTemplates{tpl}, nil, nil, nil, nil, noNames{}, ExecutionDeps{Runs: testAllow{}})
		fires := &alertFires{}
		trg := &Trigger{ID: triggerID, Type: jc.TriggerTypeAPI, TemplateID: "t", Owner: "u", Enabled: true}
		uc := NewTriggerUsecase(TriggerDeps{
			Repo: oneTrigger{trg}, Secrets: fakeSecrets{TriggerToken{Hash: hash, Sealed: []byte("sealed:" + token)}},
			Fires: fires, Opener: plainOpener{},
		}, fakeTemplates{tpl}, nil, execs, noNames{})
		uc.now = func() time.Time { return now }
		return uc, fires
	}
	ctx := context.Background()
	const body = `{"params":{}}`

	t.Run("same signature replayed", func(t *testing.T) {
		t.Parallel()
		uc, fires := newUsecase()
		auth := sign(now.Unix(), body)
		for i := range 3 {
			if _, err := uc.Fire(ctx, triggerID, auth, FireRequest{}); err != nil {
				t.Fatalf("fire %d: %v", i, err)
			}
		}
		if len(fires.begun) != 1 {
			t.Fatalf("replayed signature registered %d firings (%v), want 1", len(fires.begun), fires.begun)
		}
		ref := strings.TrimPrefix(fires.begun[0], triggerID+"/")
		if !strings.HasPrefix(ref, signatureRefPrefix) || len(ref) != len(signatureRefPrefix)+64 || len(ref) > 128 {
			t.Fatalf("implicit ref = %q, want %q + 64 hex chars within the ref column width", ref, signatureRefPrefix)
		}
		if strings.Contains(ref, strings.TrimPrefix(auth.Signature, "sha256=")) {
			t.Fatalf("implicit ref %q must not embed the raw signature", ref)
		}
	})

	t.Run("different signatures fire separately", func(t *testing.T) {
		t.Parallel()
		uc, fires := newUsecase()
		for _, auth := range []FireAuth{sign(now.Unix(), body), sign(now.Unix()-1, body), sign(now.Unix(), `{"params":{"a":1}}`)} {
			if _, err := uc.Fire(ctx, triggerID, auth, FireRequest{}); err != nil {
				t.Fatal(err)
			}
		}
		if len(fires.begun) != 3 {
			t.Fatalf("distinct signatures registered %d firings (%v), want 3", len(fires.begun), fires.begun)
		}
	})

	t.Run("explicit ref unchanged", func(t *testing.T) {
		t.Parallel()
		uc, fires := newUsecase()
		// 显式幂等键优先：不同签名带同一个键只触发一次，同一份签名带不同的键各自触发——与此前完全一致。
		for _, c := range []struct {
			auth FireAuth
			ref  string
		}{{sign(now.Unix(), body), "r1"}, {sign(now.Unix()-1, body), "r1"}, {sign(now.Unix(), body), "r2"}} {
			if _, err := uc.Fire(ctx, triggerID, c.auth, FireRequest{Ref: c.ref}); err != nil {
				t.Fatal(err)
			}
		}
		if want := []string{triggerID + "/r1", triggerID + "/r2"}; len(fires.begun) != 2 || fires.begun[0] != want[0] || fires.begun[1] != want[1] {
			t.Fatalf("explicit refs registered %v, want %v", fires.begun, want)
		}
	})

	t.Run("bearer unchanged", func(t *testing.T) {
		t.Parallel()
		uc, fires := newUsecase()
		// Bearer 认证没有签名可取：不带幂等键时照旧每次调用都是新的一次触发（即使请求里夹带了签名头）。
		replay := sign(now.Unix(), body)
		replay.Bearer = token
		for _, auth := range []FireAuth{{Bearer: token}, {Bearer: token}, replay, replay} {
			if _, err := uc.Fire(ctx, triggerID, auth, FireRequest{}); err != nil {
				t.Fatal(err)
			}
		}
		if len(fires.begun) != 4 {
			t.Fatalf("bearer calls registered %d firings, want 4", len(fires.begun))
		}
		for _, k := range fires.begun {
			if strings.Contains(k, signatureRefPrefix) {
				t.Fatalf("bearer call got a signature-derived ref: %q", k)
			}
		}
	})
}
