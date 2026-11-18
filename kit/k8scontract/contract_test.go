package k8scontract

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestToken(t *testing.T) {
	t.Parallel()
	id := "018f3e1a-0cd1-7000-8000-000000000001"
	token, hash, err := NewToken(id)
	if err != nil {
		t.Fatal(err)
	}
	gotID, secret, ok := ParseToken(token)
	if !ok || gotID != id {
		t.Fatalf("ParseToken(%q) = %q, %v", token, gotID, ok)
	}
	if !SecretMatches(secret, hash) {
		t.Fatal("secret must match its hash")
	}
	if SecretMatches(secret+"x", hash) {
		t.Fatal("tampered secret must not match")
	}
	sec := strings.Repeat("a", 40)
	for _, bad := range []string{
		"", "abc", id, id + ".short", "x." + sec,
		strings.ToUpper(id) + "." + sec,                     // 大小写变体：数据库里等价，缓存键上不等价
		strings.Repeat("z", 36) + "." + sec,                 // 36 个字符但不是 UUID
		"018f3e1a0cd1-7000-8000-0000000000001-" + "." + sec, // 连字符位置不对
	} {
		if _, _, ok := ParseToken(bad); ok {
			t.Errorf("ParseToken(%q) must fail", bad)
		}
	}
}

func TestIdentity(t *testing.T) {
	t.Parallel()
	key := []byte(strings.Repeat("k", 32))
	now := time.Unix(1_800_000_000, 0)
	id := Identity{UserID: "u1", Username: "alice", RoleIDs: []string{"r1"}, IssuedAt: now.Unix()}
	h := http.Header{}
	if err := SignIdentity(h, key, id); err != nil {
		t.Fatal(err)
	}
	got, err := VerifyIdentity(h, key, now.Add(10*time.Second))
	if err != nil || got.UserID != "u1" || got.Username != "alice" {
		t.Fatalf("VerifyIdentity = %+v, %v", got, err)
	}
	if _, err := VerifyIdentity(h, key, now.Add(IdentityTTL+time.Second)); err != ErrIdentityExpired {
		t.Fatalf("expired identity: err = %v", err)
	}
	if _, err := VerifyIdentity(h, key, now.Add(-time.Minute)); err != ErrIdentityExpired {
		t.Fatalf("identity from the future: err = %v", err)
	}
	if _, err := VerifyIdentity(h, []byte(strings.Repeat("x", 32)), now); err != ErrIdentityInvalid {
		t.Fatalf("wrong key: err = %v", err)
	}
	// 篡改身份内容（提权）必须被拒绝。
	forged := http.Header{}
	_ = SignIdentity(forged, key, Identity{UserID: "u1", IsAdmin: true, IssuedAt: now.Unix()})
	h.Set(IdentityHeader, forged.Get(IdentityHeader))
	if _, err := VerifyIdentity(h, key, now); err != ErrIdentityInvalid {
		t.Fatalf("tampered identity: err = %v", err)
	}
	if _, err := VerifyIdentity(http.Header{}, key, now); err != ErrIdentityMissing {
		t.Fatalf("missing identity: err = %v", err)
	}
	if err := SignIdentity(http.Header{}, []byte("short"), id); err != ErrInternalKey {
		t.Fatalf("short key: err = %v", err)
	}
}

func TestLevelsAndGroups(t *testing.T) {
	t.Parallel()
	want := map[int32]string{LevelView: "view", LevelEdit: "edit", LevelAdmin: "admin", LevelClusterAdmin: "cluster-admin", 0: "", 5: ""}
	for level, role := range want {
		if got := LevelClusterRole(level); got != role {
			t.Errorf("LevelClusterRole(%d) = %q, want %q", level, got, role)
		}
	}
	groups := ImpersonateGroups([]string{"a", "b"}, true)
	if strings.Join(groups, ",") != "reborn:role:a,reborn:role:b,reborn:admin,system:authenticated" {
		t.Fatalf("groups = %v", groups)
	}
	if g := ImpersonateGroups(nil, false); len(g) != 1 || g[0] != "system:authenticated" {
		t.Fatalf("groups without roles = %v", g)
	}
}

func TestCachedResources(t *testing.T) {
	t.Parallel()
	if !Cached(GVRPod) || Cached(GVREvent) || Cached(GVR{"example.com", "v1", "widgets"}) {
		t.Fatal("unexpected cached set")
	}
	if !MetadataOnly(GVRSecret) || MetadataOnly(GVRPod) {
		t.Fatal("unexpected metadata-only set")
	}
	if len(CachedResources()) != 16 {
		t.Fatalf("cached resources = %d", len(CachedResources()))
	}
}

func TestEventNotifyType(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"FailedScheduling": "k8s.event.failedscheduling",
		"OOMKilling":       "k8s.event.oomkilling",
		"Weird Reason/1":   "k8s.event.weird-reason-1",
		"":                 "k8s.event.unknown",
	}
	for in, want := range cases {
		if got := EventNotifyType(in); got != want {
			t.Errorf("EventNotifyType(%q) = %q, want %q", in, got, want)
		}
	}
	if got := EventNotifyType(strings.Repeat("x", 300)); len(got) > 128 {
		t.Fatalf("too long: %d", len(got))
	}
}
