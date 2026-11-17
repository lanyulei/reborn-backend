package biz

import (
	"context"
	"slices"
	"testing"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	bizworkflow "github.com/lanyulei/reborn/backend/core/internal/biz/workflow"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/core/internal/testfake"

	"github.com/go-kratos/kratos/v3/errors"
)

const (
	fileA = "018f3e1a-0000-7000-8000-0000000000a1"
	fileB = "018f3e1a-0000-7000-8000-0000000000b2"
	fileC = "018f3e1a-0000-7000-8000-0000000000c3"
)

// formAccessStub 用户 → 他在流程上的表单权限视图；没登记的用户没有席位。
type formAccessStub map[string]*bizworkflow.FormAccess

func (s formAccessStub) FormAccess(_ context.Context, _, userID string) (*bizworkflow.FormAccess, error) {
	if fa, ok := s[userID]; ok {
		return fa, nil
	}
	return &bizworkflow.FormAccess{}, nil
}

func (formAccessStub) Amend(context.Context, string, string, func(context.Context, txrun.Tx, map[string]string) (map[string]any, error)) error {
	return nil
}
func (formAccessStub) PendingUsers(context.Context, string) ([]string, error) { return nil, nil }
func (formAccessStub) Admins(context.Context, string) ([]string, error)       { return nil, nil }

var fileForm = []FormField{
	{Name: "合同", Code: "contract", Type: formschema.TypeFile},
	{Name: "截图", Code: "shots", Type: formschema.TypeFile, Multiple: true},
	{Name: "说明", Code: "note", Type: formschema.TypeString},
}

func TestFileOwners(t *testing.T) {
	owners, dup := fileOwners(fileForm, map[string]any{
		"contract": []any{"018F3E1A-0000-7000-8000-0000000000A1"},
		"shots":    []any{fileB, fileC, 7, ""},
		// 不是附件字段的取值即使长得像文件 ID 也不算。
		"note": fileC,
	})
	if dup != "" || len(owners) != 3 || owners[fileA] != "contract" || owners[fileB] != "shots" || owners[fileC] != "shots" {
		t.Fatalf("fileOwners() = %v, dup %q", owners, dup)
	}
	if _, dup := fileOwners(fileForm, map[string]any{"contract": []any{fileA}, "shots": []any{fileA}}); dup != fileA {
		t.Errorf("same file in two fields: dup = %q, want %q", dup, fileA)
	}
	// 同一个字段里重复不算跨字段。
	if _, dup := fileOwners(fileForm, map[string]any{"shots": []any{fileB, fileB}}); dup != "" {
		t.Errorf("repeat inside one field: dup = %q, want none", dup)
	}
	if owners, _ := fileOwners(fileForm, nil); len(owners) != 0 {
		t.Errorf("no data: owners = %v", owners)
	}
}

// 挂在字段上的文件：字段对查看者隐藏时不可读；流程图无法解析时全部不可读；提单人与管理视图
// 始终可读。
func TestFileAccessByField(t *testing.T) {
	ctx := context.Background()
	ticket := &Ticket{
		ID: "t1", Creator: "creator", InstanceID: "i1", Form: fileForm,
		Data: map[string]any{"contract": []any{fileA}, "shots": []any{fileB}},
	}
	access := NewAccess(readerStub{"t1": ticket}, participantSet{"i1": {"handler", "blind", "cc", "boss"}}, supervisorStub("boss"))
	files := NewFileAccess(access, formAccessStub{
		"handler": {Seated: true, Hidden: []string{"contract"}},
		"blind":   {Seated: true, HideAll: true},
		"cc":      {Seated: true},
		"boss":    {Seated: true, Hidden: []string{"contract"}},
		"creator": {Seated: true, Hidden: []string{"contract"}},
	})
	cases := []struct {
		user, file string
		want       bool
	}{
		{"creator", fileA, true}, // 提单人看到全部，即使他在流程上也有席位
		{"boss", fileA, true},    // 管理视图看到全部
		{"handler", fileA, false},
		{"handler", fileB, true},
		{"handler", "018F3E1A-0000-7000-8000-0000000000A1", false}, // 大小写不影响判定
		{"handler", fileC, true},                                   // 不挂在字段上：工单级附件
		{"blind", fileA, false},
		{"blind", fileB, false},
		{"blind", fileC, true},
		{"cc", fileA, true},
		{"stranger", fileB, false}, // 看不了工单
		{"stranger", fileC, false},
	}
	for _, c := range cases {
		if got, err := files.CanReadFile(ctx, c.user, "t1", c.file); err != nil || got != c.want {
			t.Errorf("CanReadFile(%s, %s) = %v, %v; want %v", c.user, c.file, got, err, c.want)
		}
	}
	if got, err := files.CanReadFile(ctx, "creator", "missing", fileA); err != nil || got {
		t.Errorf("CanReadFile(missing ticket) = %v, %v; want false", got, err)
	}
}

func TestVisibleFiles(t *testing.T) {
	owners := map[string]string{fileA: "contract", fileB: "shots"}
	all := []*bizfile.File{{ID: fileA}, {ID: fileB}, {ID: fileC}}
	got := visibleFiles(all, owners, fileForm[1:]) // contract 已对查看者隐藏
	if len(got) != 2 || got[0].ID != fileB || got[1].ID != fileC {
		t.Fatalf("visibleFiles() = %v", got)
	}
	if got := visibleFiles(all, owners, nil); len(got) != 1 || got[0].ID != fileC {
		t.Fatalf("visibleFiles(whole form hidden) = %v", got)
	}
	if len(all) != 3 {
		t.Fatal("visibleFiles() must not modify its input")
	}
}

// 处理人改表单后文件跟着走：新带上的须是他本人刚上传的临时文件，拿掉的作废；别人的文件、
// 已绑定在别处的文件、跨字段重复都被拒绝。
func TestSyncFieldFiles(t *testing.T) {
	ctx := context.Background()
	const creator, handler = "018f3e1a-0000-7000-8000-00000000000a", "018f3e1a-0000-7000-8000-00000000000b"
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 32)...)
	files, _, _, err := testfake.NewFileUsecase(bizfile.AccessCheckers{BusinessType: testfake.FileChecker{creator + "|T-1": true, creator + "|T-2": true}})
	if err != nil {
		t.Fatalf("NewFileUsecase() error = %v", err)
	}
	uc := &Usecase{files: files}
	tx := testfake.Tx{}
	upload := func(user string) string {
		t.Helper()
		f, err := files.Upload(ctx, user, "a.png", png)
		if err != nil {
			t.Fatalf("Upload() error = %v", err)
		}
		return f.ID
	}
	readable := func(id string) bool {
		_, err := files.Get(ctx, creator, id)
		return err == nil
	}
	old, kept := upload(creator), upload(creator)
	before := map[string]any{"contract": []any{old}, "shots": []any{kept}}
	if err := uc.syncFieldFiles(ctx, tx, creator, "T-1", fileForm, nil, before); err != nil {
		t.Fatalf("initial bind error = %v", err)
	}

	// 处理人换掉合同、给截图补一张。
	added, replaced := upload(handler), upload(handler)
	after := map[string]any{"contract": []any{replaced}, "shots": []any{kept, added}}
	if err := uc.syncFieldFiles(ctx, tx, handler, "T-1", fileForm, before, after); err != nil {
		t.Fatalf("syncFieldFiles() error = %v", err)
	}
	if readable(old) || !readable(kept) || !readable(added) || !readable(replaced) {
		t.Fatalf("after sync: old=%v kept=%v added=%v replaced=%v", readable(old), readable(kept), readable(added), readable(replaced))
	}
	if bound, _ := files.ListByBiz(ctx, BusinessType, "T-1"); len(bound) != 3 {
		t.Fatalf("bound files = %d, want 3", len(bound))
	}

	rejected := map[string]map[string]any{
		// 别人上传、尚未绑定的临时文件。
		"someone else's upload": {"contract": []any{replaced}, "shots": []any{kept, added, upload(creator)}},
		// 已经绑定在另一张工单上的文件。
		"file of another ticket":  {"contract": []any{replaced}, "shots": []any{kept, added, boundElsewhere(ctx, t, uc, tx, upload(creator), creator)}},
		"unknown file":            {"contract": []any{replaced}, "shots": []any{kept, added, fileC}},
		"same file in two fields": {"contract": []any{kept}, "shots": []any{kept, added}},
	}
	for name, data := range rejected {
		if err := uc.syncFieldFiles(ctx, tx, handler, "T-1", fileForm, after, data); !errors.Is(err, ErrFormInvalid) {
			t.Errorf("%s: error = %v, want ErrFormInvalid", name, err)
		}
	}
	// 清空字段：它的文件全部作废，其余不动。
	if err := uc.syncFieldFiles(ctx, tx, handler, "T-1", fileForm, after, map[string]any{"contract": []any{replaced}}); err != nil {
		t.Fatalf("clear field error = %v", err)
	}
	if readable(kept) || readable(added) || !readable(replaced) {
		t.Fatalf("after clearing shots: kept=%v added=%v replaced=%v", readable(kept), readable(added), readable(replaced))
	}
}

// boundElsewhere 把文件绑定到另一张工单并返回它的 ID。
func boundElsewhere(ctx context.Context, t *testing.T, uc *Usecase, tx txrun.Tx, id, owner string) string {
	t.Helper()
	if err := uc.bindFieldFiles(ctx, tx, owner, "T-2", []string{id}); err != nil {
		t.Fatalf("bind to another ticket error = %v", err)
	}
	return id
}

// participantSet instanceID → 参与者们。
type participantSet map[string][]string

func (p participantSet) IsParticipant(_ context.Context, instanceID, userID string) (bool, error) {
	return slices.Contains(p[instanceID], userID), nil
}
