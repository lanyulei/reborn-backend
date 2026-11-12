package webhooksig

import (
	"strconv"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	t.Parallel()
	body := []byte(`{"a":1}`)
	now := time.Unix(1_800_000_000, 0)
	ts := strconv.FormatInt(now.Unix(), 10)
	sig := Sign("s", ts, body)
	if !Verify("s", ts, body, sig) || Verify("x", ts, body, sig) || Verify("s", ts, []byte("{}"), sig) {
		t.Fatal("verify semantics")
	}
	if !VerifyWithin("s", ts, body, sig, now.Add(2*time.Minute), 5*time.Minute) {
		t.Fatal("within skew must pass")
	}
	if VerifyWithin("s", ts, body, sig, now.Add(10*time.Minute), 5*time.Minute) || VerifyWithin("s", "nope", body, sig, now, time.Minute) {
		t.Fatal("outside skew / bad ts must fail")
	}
}
