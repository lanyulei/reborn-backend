package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 审计 patch 超过审计单个字段的上限时，values 截断并标注 truncated，仓库 / Chart / 版本 / 上一个版本保留。
func TestFitHelmAuditPatch(t *testing.T) {
	t.Parallel()

	// 未超限：逐字节不变（不加 truncated，values 仍是对象）。
	small := map[string]any{"repo": "stable", "chart": "web", "version": "1.0.0", "values": map[string]any{"replicas": int64(3)}}
	before, _ := json.Marshal(small)
	fitHelmAuditPatch(small)
	if after, _ := json.Marshal(small); !bytes.Equal(before, after) {
		t.Fatalf("a patch within the limit must not be touched:\n%s\n%s", before, after)
	}

	// 超限：升级时两份 values 合计超过上限。
	patch := map[string]any{
		"repo": "stable", "chart": "kube-prometheus-stack", "version": "1.2.3",
		"values": map[string]any{"notes": strings.Repeat("x", 200<<10)},
		"previous": map[string]any{
			"chart": "kube-prometheus-stack", "version": "1.2.2", "revision": 4,
			"values": map[string]any{"old": strings.Repeat("y", 100<<10)},
		},
	}
	raw, _ := json.Marshal(patch)
	if len(raw) <= k8scontract.MaxAuditObjectBytes {
		t.Fatalf("the sample must exceed the audit limit: %d bytes", len(raw))
	}
	fitHelmAuditPatch(patch)
	raw, _ = json.Marshal(patch)
	if len(raw)+helmAuditPatchSlack > k8scontract.MaxAuditObjectBytes {
		t.Fatalf("truncated patch is still too large: %d bytes", len(raw))
	}
	if patch["truncated"] != true || patch["repo"] != "stable" || patch["chart"] != "kube-prometheus-stack" || patch["version"] != "1.2.3" {
		t.Fatalf("patch = %s", raw)
	}
	prev, _ := patch["previous"].(map[string]any)
	for _, part := range []map[string]any{patch, prev} {
		text, ok := part["values"].(string)
		if !ok || !strings.HasSuffix(text, "…") || len(text) > helmAuditValuesBytes+len("…") {
			t.Fatalf("values must be a truncated text of at most %d bytes, got %v", helmAuditValuesBytes, part["values"])
		}
	}
	if prev["chart"] != "kube-prometheus-stack" || prev["version"] != "1.2.2" || prev["revision"] != 4 {
		t.Fatalf("facts about the previous release must be kept: %v", prev)
	}

	// JSON 转义后仍超限（每个字符转成 6 字节）：values 不再保留，标记与其余事实留下。
	angry := map[string]any{"repo": "r", "chart": "c", "version": "1", "values": map[string]any{"v": strings.Repeat("<", 300<<10)}}
	fitHelmAuditPatch(angry)
	if raw, _ := json.Marshal(angry); len(raw) > k8scontract.MaxAuditObjectBytes {
		t.Fatalf("still too large: %d bytes", len(raw))
	}
	if angry["values"] != nil || angry["truncated"] != true || angry["chart"] != "c" || angry["version"] != "1" || angry["repo"] != "r" {
		t.Fatalf("escaping-heavy values must be dropped, not the whole patch: %v", angry)
	}
}

type patchAuditRepo struct{ patches []string }

func (p *patchAuditRepo) Insert(_ context.Context, r *biz.AuditRecord) error {
	p.patches = append(p.patches, string(r.Patch))
	return nil
}

type noopObserver struct{}

func (noopObserver) AuditFailed()            {}
func (noopObserver) StreamDelta(string, int) {}

// 经 Auditor 落库：超限的 Helm 审计 patch 不再整份丢失（原先超过上限即不存，且没有截断标记）。
func TestOversizedHelmAuditPatchIsRecorded(t *testing.T) {
	t.Parallel()
	patch := map[string]any{
		"repo": "stable", "chart": "big", "version": "1.0.0", "values": map[string]any{"notes": strings.Repeat("x", 300<<10)},
	}
	ev := helmAudit(k8scontract.ActionHelmInstall, "ns", "web", patch)
	repo := &patchAuditRepo{}
	aud := biz.NewAuditor(repo, noopObserver{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// 对照：不处理的话 Auditor 确实会丢掉整份 patch。
	aud.Record(context.Background(), &biz.Cluster{ID: "c1", Name: "c1"}, biz.Identity{UserID: uid1}, []*biz.AuditEvent{ev})
	if len(repo.patches) != 1 || repo.patches[0] != "" {
		t.Fatalf("without truncation the auditor drops the whole patch; got %q", strings.Join(repo.patches, ","))
	}

	fitHelmAuditPatch(patch)
	aud.Record(context.Background(), &biz.Cluster{ID: "c1", Name: "c1"}, biz.Identity{UserID: uid1}, []*biz.AuditEvent{ev})
	if len(repo.patches) != 2 || repo.patches[1] == "" {
		t.Fatalf("the audit row must keep the patch: %d rows", len(repo.patches))
	}
	for _, want := range []string{`"truncated":true`, `"chart":"big"`, `"repo":"stable"`, `"version":"1.0.0"`} {
		if !strings.Contains(repo.patches[1], want) {
			t.Fatalf("patch %s does not contain %s", repo.patches[1], want)
		}
	}
}
