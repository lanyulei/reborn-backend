package biz

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// batchCountingCIRepo 记录 GetByIDs 的调用次数（memCIRepo 只记 ID，分不出是几次调用）。
type batchCountingCIRepo struct {
	*memCIRepo
	batches int
}

func (r *batchCountingCIRepo) GetByIDs(ctx context.Context, ids []string) ([]*CI, error) {
	r.batches++
	return r.memCIRepo.GetByIDs(ctx, ids)
}

// perFieldCheckRefs 改批量之前的 checkRefs（逐个 ref 字段各查一次），作为等价性比对的基准。
func perFieldCheckRefs(ctx context.Context, cis CIRepo, scope Scope, fields []*Field, attrs map[string]any) error {
	for _, f := range fields {
		if f.Type != FieldTypeRef || f.Deprecated {
			continue
		}
		v, ok := attrs[f.Code]
		if !ok || v == nil {
			continue
		}
		id, _ := v.(string)
		if !idgen.IsUUIDFold(id) {
			return detail(ErrCIInvalid, "field %q must be a resource id", f.Code)
		}
		if scope.Unrestricted {
			exists, err := cis.ExistsInModel(ctx, f.RefModel, id)
			if err != nil {
				return err
			}
			if !exists {
				return missingRef(f)
			}
			continue
		}
		targets, err := cis.GetByIDs(ctx, []string{id})
		if err != nil {
			return err
		}
		if len(targets) == 0 || targets[0].ModelCode != f.RefModel || !scope.Allows(targets[0].ModelCode, targets[0].Env) {
			return missingRef(f)
		}
	}
	return nil
}

// 多个 ref 字段：受限路径一次取齐被引用资源，结论与逐个查询完全一致（含报错落在哪个字段上）。
func TestCheckRefs_BatchMatchesPerField(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		idApp    = "018f3e1a-0400-7000-8000-0000000000a1"
		idDB     = "018f3e1a-0400-7000-8000-0000000000a2"
		idDevApp = "018f3e1a-0400-7000-8000-0000000000a3"
		idGone   = "018f3e1a-0400-7000-8000-0000000000a4"
	)
	fields := []*Field{
		{Code: "ip", Type: FieldTypeIP},
		{Code: "app", Type: FieldTypeRef, RefModel: "app"},
		{Code: "db", Type: FieldTypeRef, RefModel: "db"},
		{Code: "standby", Type: FieldTypeRef, RefModel: "db"},
		{Code: "legacy", Type: FieldTypeRef, RefModel: "app", Deprecated: true},
	}
	restricted := Scope{Models: map[string]struct{}{"host": {}, "app": {}, "db": {}}, Envs: map[string]struct{}{"prod": {}}}
	newRepo := func() *batchCountingCIRepo {
		cis := newMemCIRepo()
		cis.seed(&CI{ID: idApp, ModelCode: "app", Env: "prod"})
		cis.seed(&CI{ID: idDB, ModelCode: "db", Env: "prod"})
		cis.seed(&CI{ID: idDevApp, ModelCode: "app", Env: "dev"})
		cis.exists["app|"+idApp], cis.exists["db|"+idDB], cis.exists["app|"+idDevApp] = true, true, true
		return &batchCountingCIRepo{memCIRepo: cis}
	}
	cases := []struct {
		name  string
		attrs map[string]any
		// wantBatches 受限路径下 GetByIDs 的调用次数。
		wantBatches int
	}{
		{"no ref values", map[string]any{"ip": "10.0.0.1"}, 0},
		{"all valid", map[string]any{"app": idApp, "db": idDB, "standby": idDB}, 1},
		{"nil ref is skipped", map[string]any{"app": nil, "db": idDB}, 1},
		{"deprecated ref is ignored", map[string]any{"legacy": idGone, "db": idDB}, 1},
		{"first missing", map[string]any{"app": idGone, "db": idDB}, 1},
		{"second missing", map[string]any{"app": idApp, "db": idGone}, 1},
		{"wrong model on second", map[string]any{"app": idApp, "db": idApp}, 1},
		{"out of scope env", map[string]any{"app": idDevApp, "db": idDB}, 1},
		{"bad format first: no lookup at all", map[string]any{"app": "not-an-id", "db": idDB}, 0},
		{"missing first wins over bad format later", map[string]any{"app": idGone, "db": "not-an-id"}, 1},
		{"valid first, bad format later", map[string]any{"app": idApp, "db": "not-an-id"}, 1},
		{"non-string value", map[string]any{"app": 42}, 0},
	}
	for _, scope := range []Scope{restricted, {Unrestricted: true}} {
		for _, tc := range cases {
			name := fmt.Sprintf("%s/unrestricted=%v", tc.name, scope.Unrestricted)
			repo := newRepo()
			got := (&CIUsecase{cis: repo}).checkRefs(ctx, scope, fields, tc.attrs)
			want := perFieldCheckRefs(ctx, newRepo(), scope, fields, tc.attrs)
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s: checkRefs = %v, per-field = %v", name, got, want)
			}
			wantBatches := tc.wantBatches
			if scope.Unrestricted {
				wantBatches = 0 // 不限路径仍走 ExistsInModel
			}
			if repo.batches != wantBatches {
				t.Errorf("%s: GetByIDs called %d times, want %d", name, repo.batches, wantBatches)
			}
		}
	}
	// 至少有一组用例确实报错（防止两边「一致地放行」）。
	if err := (&CIUsecase{cis: newRepo()}).checkRefs(ctx, restricted, fields, map[string]any{"app": idApp, "db": idGone}); err == nil ||
		!strings.Contains(err.Error(), `field "db" references a missing db resource`) {
		t.Fatalf("missing second ref: %v", err)
	}
	// 大写 ID：库里按 uuid 比较能命中、返回小写 ID，结果按小写对上。
	upper := newRepo()
	upper.items[strings.ToUpper(idApp)] = upper.items[idApp] // 桩按字符串精确匹配，这里模拟 uuid 比较不分大小写
	if err := (&CIUsecase{cis: upper}).checkRefs(ctx, restricted, fields, map[string]any{"app": strings.ToUpper(idApp)}); err != nil {
		t.Fatalf("upper-case id must resolve: %v", err)
	}
}
