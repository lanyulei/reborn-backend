package service

import (
	"context"
	"slices"
	"testing"

	v1 "github.com/lanyulei/reborn/backend/core/api/incident/v1"
	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"

	"google.golang.org/protobuf/encoding/protojson"
)

// capturingSettingRepo 记下保存的设置，并把它作为库里的内容返回。
type capturingSettingRepo struct {
	biz.SettingRepo
	saved *biz.Settings
}

func (r *capturingSettingRepo) Get(context.Context) (*biz.Settings, error) { return r.saved, nil }
func (r *capturingSettingRepo) Save(_ context.Context, s *biz.Settings, _ int32, _ string) error {
	cp := *s
	r.saved = &cp
	return nil
}

func settingsFromJSON(t *testing.T, body string) *biz.Settings {
	t.Helper()
	req := &v1.UpdateSettingsRequest{}
	if err := protojson.Unmarshal([]byte(body), req); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return toSettings(req.GetSettings())
}

// 个人提醒全部取消（personal_events 为空列表）必须原样保存：proto 的空 repeated 字段解码后是 nil，直接透传会被
// biz 的 merge 当成「没保存过」而落回缺省值——管理员看到已关闭，实际仍按缺省发送。
func TestToSettingsKeepsEmptyPersonalEvents(t *testing.T) {
	t.Parallel()
	got := settingsFromJSON(t, `{"settings":{"personalEvents":[]}}`)
	if got.PersonalEvents == nil || len(got.PersonalEvents) != 0 {
		t.Fatalf("personal events = %#v, want an empty non-nil list", got.PersonalEvents)
	}
	repo := &capturingSettingRepo{}
	st, err := biz.NewSettingsUsecase(repo).Update(context.Background(), "u1", got, 0)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if len(st.PersonalEvents) != 0 || st.Personal(biz.NotifyOpened) {
		t.Fatalf("after saving an empty list: personal events = %v, want no personal notification", st.PersonalEvents)
	}
	// 换一个实例重新读取（不吃缓存），同样是空。
	fresh, err := biz.NewSettingsUsecase(repo).Get(context.Background())
	if err != nil || len(fresh.PersonalEvents) != 0 || fresh.Personal(biz.NotifyReviewDue) {
		t.Fatalf("reloaded personal events = %v (%v), want empty", fresh.PersonalEvents, err)
	}
}

// 非空列表与未涉及的字段照旧。
func TestToSettingsKeepsChosenPersonalEvents(t *testing.T) {
	t.Parallel()
	got := settingsFromJSON(t, `{"settings":{"personalEvents":["opened","merged"],"stormThreshold":9}}`)
	if !slices.Equal(got.PersonalEvents, []string{"opened", "merged"}) || got.StormThreshold != 9 {
		t.Fatalf("settings = personal %v, storm threshold %d; want the given list and threshold", got.PersonalEvents, got.StormThreshold)
	}
	repo := &capturingSettingRepo{}
	st, err := biz.NewSettingsUsecase(repo).Update(context.Background(), "u1", got, 0)
	if err != nil || !slices.Equal(st.PersonalEvents, []string{"opened", "merged"}) || !st.Personal(biz.NotifyMerged) || st.Personal(biz.NotifyStale) {
		t.Fatalf("after saving a chosen list: %v (%v), want exactly opened and merged", st.PersonalEvents, err)
	}
}
