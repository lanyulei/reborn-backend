package idgen

import (
	"regexp"
	"testing"
)

// uuidV7Pattern 校验 UUIDv7：版本位为 7、variant 位为 10xx。
var uuidV7Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewID(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		id, err := NewID()
		if err != nil {
			t.Fatalf("NewID() error = %v", err)
		}
		if !uuidV7Pattern.MatchString(id) {
			t.Fatalf("NewID() = %q, want UUIDv7", id)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("NewID() duplicated id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestIsUUID(t *testing.T) {
	t.Parallel()
	id, err := NewID()
	if err != nil || !IsUUID(id) || !IsUUIDFold(id) {
		t.Fatalf("generated id %q must be a UUID (err %v)", id, err)
	}
	const lower, upper = "018f3e1a-0a00-7000-8000-00000000000a", "018F3E1A-0A00-7000-8000-00000000000A"
	if !IsUUID(lower) || IsUUID(upper) || !IsUUIDFold(upper) {
		t.Fatal("case handling is wrong")
	}
	for _, s := range []string{
		"", "not-a-uuid", "018f3e1a0a0070008000000000000000000a", "018f3e1a-0a00-7000-8000-00000000000", "018f3e1a-0a00-7000-8000-00000000000ab",
		"018f3e1a-0a00-7000-8000-00000000000g", "018f3e1a_0a00-7000-8000-00000000000a", "../../etc/passwd", "018f3e1a-0a00-7000-8000-00000000000a\n",
		"{018f3e1a-0a00-7000-8000-00000000000a}", " 018f3e1a-0a00-7000-8000-00000000000a",
	} {
		if IsUUID(s) || IsUUIDFold(s) {
			t.Errorf("%q must not be accepted", s)
		}
	}
}
