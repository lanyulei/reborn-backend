package biz

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// AuditEvent 一次需要审计的操作（data/kube 在执行写操作时产生，成功与失败都有）。
type AuditEvent struct {
	Ref    k8scontract.Ref
	Kind   string
	Action string
	// Before / After 变更前后的对象原文；由 Auditor 清洗后落库，调用方不必处理。
	Before map[string]any
	After  map[string]any
	// Patch 本次改动或动作参数（可 JSON 序列化；不得含凭据）。
	Patch any
	// Err 非空 = 该操作失败。
	Err error
}

// AuditRecord 审计的落库形态（k8s_audit）。
type AuditRecord struct {
	Identity    Identity
	ClusterID   string
	ClusterName string
	Ref         k8scontract.Ref
	Kind        string
	Action      string
	Result      int32
	Error       string
	Before      []byte
	After       []byte
	Patch       []byte
	Truncated   bool
}

// AuditRepo 审计写入。
type AuditRepo interface {
	Insert(ctx context.Context, r *AuditRecord) error
}

// Auditor 清洗并写入审计。审计写入失败不回滚已完成的集群操作：记日志并计数。
type Auditor struct {
	repo AuditRepo
	obs  Observer
	log  *slog.Logger
}

// auditBatchTimeout 一批审计事件写入的总时限。
const auditBatchTimeout = 10 * time.Second

// NewAuditor new an Auditor.
func NewAuditor(repo AuditRepo, obs Observer, log *slog.Logger) *Auditor {
	return &Auditor{repo: repo, obs: obs, log: log}
}

// Record 写入一批审计事件。用脱离请求取消的 ctx：客户端断开不应丢审计。
func (a *Auditor) Record(ctx context.Context, c *Cluster, id Identity, events []*AuditEvent) {
	if len(events) == 0 {
		return
	}
	// 整批一个总时限：数据库不可用时，一份 50 个对象的清单不应让请求的收尾等上几分钟（每条各等一次超时）。
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditBatchTimeout)
	defer cancel()
	for _, ev := range events {
		if ev == nil {
			continue
		}
		rec := &AuditRecord{
			Identity: id, ClusterID: c.ID, ClusterName: c.Name, Ref: ev.Ref, Kind: ev.Kind, Action: ev.Action,
			Result: k8scontract.AuditOK,
		}
		if ev.Err != nil {
			rec.Result, rec.Error = k8scontract.AuditFailed, truncate(AuditErrorText(ev.Err), 512)
		}
		var t1, t2 bool
		rec.Before, t1 = encodeAuditObject(ev.Before)
		rec.After, t2 = encodeAuditObject(ev.After)
		rec.Truncated = t1 || t2
		if ev.Patch != nil {
			if raw, err := json.Marshal(ev.Patch); err == nil && len(raw) <= k8scontract.MaxAuditObjectBytes {
				rec.Patch = raw
			}
		}
		if err := a.repo.Insert(ctx, rec); err != nil {
			a.obs.AuditFailed()
			a.log.Error("write audit failed", "cluster", c.Name, "action", ev.Action, "resource", ev.Ref.Resource,
				"namespace", ev.Ref.Namespace, "name", ev.Ref.Name, "user_id", id.UserID, "error", err)
		}
	}
}

// AuditErrorText 审计里的失败原因：业务错误取其 message（集群返回的原文），其余取错误文本。
func AuditErrorText(err error) string {
	if be, ok := err.(*Error); ok { //nolint:errorlint // 只认最外层：包装过的错误取完整文本更有用
		return be.Message
	}
	return err.Error()
}

func encodeAuditObject(obj map[string]any) (raw []byte, truncated bool) {
	if obj == nil {
		return nil, false
	}
	raw, err := json.Marshal(SanitizeObject(obj))
	if err != nil || len(raw) > k8scontract.MaxAuditObjectBytes {
		return nil, true
	}
	return raw, false
}

// SanitizeObject 返回对象的清洗副本（不修改入参）：去掉 metadata.managedFields 与 status；
// Secret 的 data / stringData 的值替换为掩码；last-applied-configuration 注解（可能内嵌 Secret 明文）去掉。
func SanitizeObject(obj map[string]any) map[string]any {
	if obj == nil {
		return nil
	}
	out := maps.Clone(obj)
	delete(out, "status")
	if meta, ok := out["metadata"].(map[string]any); ok {
		meta = maps.Clone(meta)
		delete(meta, "managedFields")
		if ann, ok := meta["annotations"].(map[string]any); ok {
			if _, has := ann[LastAppliedAnnotation]; has {
				ann = maps.Clone(ann)
				delete(ann, LastAppliedAnnotation)
				meta["annotations"] = ann
			}
		}
		out["metadata"] = meta
	}
	if IsSecret(out) {
		out = MaskSecret(out)
	}
	return out
}

// LastAppliedAnnotation kubectl apply 写入的注解：内容是整个对象，体积大且可能含敏感数据。
const LastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// IsSecret 报告对象是否为核心组的 Secret。
func IsSecret(obj map[string]any) bool {
	kind, _ := obj["kind"].(string)
	api, _ := obj["apiVersion"].(string)
	return kind == "Secret" && api == "v1"
}

// MaskSecret 返回 Secret 的副本，data / stringData 的每个值替换为掩码（键保留）。
func MaskSecret(obj map[string]any) map[string]any {
	out := maps.Clone(obj)
	for _, field := range []string{"data", "stringData"} {
		m, ok := out[field].(map[string]any)
		if !ok {
			continue
		}
		masked := make(map[string]any, len(m))
		for k := range m {
			masked[k] = k8scontract.MaskedValue
		}
		out[field] = masked
	}
	return out
}
