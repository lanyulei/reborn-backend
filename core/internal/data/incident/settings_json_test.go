package data

import (
	"bytes"
	"testing"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
)

// 设置的持久化必须区分「个人提醒保存为空」与「没保存过」：空列表编码成 []，读回是非 nil 的空切片，biz 的 merge
// 才会认它是已保存的空值；字段缺失时读回 nil，merge 落回缺省值。
func TestSettingsJSONKeepsEmptyPersonalEvents(t *testing.T) {
	t.Parallel()
	raw, err := encodeJSON(&biz.Settings{PersonalEvents: []string{}}, "incident settings")
	if err != nil {
		t.Fatalf("encodeJSON() error = %v", err)
	}
	if !bytes.Contains(raw, []byte(`"personal_events":[]`)) {
		t.Fatalf("encoded settings %s must keep personal_events as an empty list", raw)
	}
	var saved biz.Settings
	if err := decodeJSON(raw, &saved, "incident settings"); err != nil {
		t.Fatalf("decodeJSON() error = %v", err)
	}
	if saved.PersonalEvents == nil || len(saved.PersonalEvents) != 0 {
		t.Fatalf("decoded personal events = %#v, want an empty non-nil list", saved.PersonalEvents)
	}

	var absent biz.Settings
	if err := decodeJSON([]byte(`{}`), &absent, "incident settings"); err != nil {
		t.Fatalf("decodeJSON() error = %v", err)
	}
	if absent.PersonalEvents != nil {
		t.Fatalf("a missing field must decode to nil so that defaults apply, got %#v", absent.PersonalEvents)
	}
}
