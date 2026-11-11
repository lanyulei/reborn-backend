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
