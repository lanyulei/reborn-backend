package kube

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/jsonmergepatch"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

var dryRunAll = []string{metav1.DryRunAll}

// parseObject 解析单个对象的 YAML。
func parseObject(text string) (*unstructured.Unstructured, error) {
	var obj map[string]any
	if err := yaml.Unmarshal([]byte(text), &obj); err != nil {
		return nil, biz.ErrInvalid("invalid YAML: %s", err.Error())
	}
	if obj == nil {
		return nil, biz.ErrInvalid("YAML is empty")
	}
	u := &unstructured.Unstructured{Object: obj}
	if u.GetAPIVersion() == "" || u.GetKind() == "" {
		return nil, biz.ErrInvalid("apiVersion and kind are required")
	}
	return u, nil
}

// change 一次创建或更新的准备结果。
type change struct {
	ref     k8scontract.Ref
	kind    string
	rc      resourceClient
	obj     *unstructured.Unstructured
	current *unstructured.Unstructured // nil = 创建
}

// prepare 校验 YAML 与目标一致，并读出当前对象。ref.Name 为空 = 创建；allowUpsert 时名称取自 YAML，
// 对象存在即更新、不存在即创建（多文档 YAML）。
func (u *userClients) prepare(ref k8scontract.Ref, obj *unstructured.Unstructured, resourceVersion string, allowUpsert bool) (*change, error) {
	if err := checkRef(ref, false); err != nil {
		return nil, err
	}
	res, err := u.c.resolve(u.ctx, ref.GVR())
	if err != nil {
		return nil, err
	}
	gv := schema.GroupVersion{Group: ref.Group, Version: ref.Version}.String()
	if obj.GetAPIVersion() != gv || obj.GetKind() != res.Kind {
		return nil, biz.ErrInvalid("YAML is %s %s, but the target is %s %s", obj.GetAPIVersion(), obj.GetKind(), gv, res.Kind)
	}
	if res.Namespaced {
		switch {
		case obj.GetNamespace() == "" && ref.Namespace != "":
			obj.SetNamespace(ref.Namespace)
		case ref.Namespace == "":
			ref.Namespace = obj.GetNamespace()
		case obj.GetNamespace() != ref.Namespace:
			return nil, biz.ErrInvalid("namespace in YAML (%s) does not match the target (%s)", obj.GetNamespace(), ref.Namespace)
		}
		if ref.Namespace == "" {
			return nil, biz.ErrInvalid("namespace is required for %s", ref.Resource)
		}
	} else {
		obj.SetNamespace("")
		ref.Namespace = ""
	}
	create := ref.Name == "" && !allowUpsert
	if ref.Name == "" {
		ref.Name = obj.GetName()
	} else if obj.GetName() != ref.Name {
		return nil, biz.ErrInvalid("name in YAML (%s) does not match the target (%s); renaming is not supported", obj.GetName(), ref.Name)
	}
	if ref.Name == "" && obj.GetGenerateName() == "" {
		return nil, biz.ErrInvalid("metadata.name is required")
	}
	if ref.Name == "" {
		create = true // generateName：只能是创建
	}
	rc, _, err := u.resource(ref)
	if err != nil {
		return nil, err
	}
	// 服务端维护的字段不接受来自 YAML 的值。
	unstructured.RemoveNestedField(obj.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(obj.Object, "status")
	ch := &change{ref: ref, kind: res.Kind, rc: rc, obj: obj}
	if create {
		obj.SetResourceVersion("")
		obj.SetUID("")
		return ch, nil
	}
	cur, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err) && allowUpsert:
		obj.SetResourceVersion("")
		obj.SetUID("")
		return ch, nil
	case err != nil:
		return nil, err
	}
	ch.current = cur
	restoreMaskedSecret(obj.Object, cur.Object)
	// 乐观锁：以预览时读到的版本为准；未提供时用 YAML 自带的，再没有则用刚读到的。
	switch {
	case resourceVersion != "":
		obj.SetResourceVersion(resourceVersion)
	case obj.GetResourceVersion() == "":
		obj.SetResourceVersion(cur.GetResourceVersion())
	}
	obj.SetUID(cur.GetUID())
	return ch, nil
}

// restoreMaskedSecret 用户拿到的 Secret YAML 里 data 是掩码：提交时把仍为掩码的值还原成当前值，
// 否则会把真实内容覆盖成掩码文本。stringData 里的掩码值直接丢弃。
func restoreMaskedSecret(obj, current map[string]any) {
	if !biz.IsSecret(obj) {
		return
	}
	if data := nestedMap(obj, "data"); data != nil {
		cur := nestedMap(current, "data")
		for k, v := range data {
			if v == k8scontract.MaskedValue {
				if real, ok := cur[k]; ok {
					data[k] = real
				} else {
					delete(data, k)
				}
			}
		}
	}
	if sd := nestedMap(obj, "stringData"); sd != nil {
		for k, v := range sd {
			if v == k8scontract.MaskedValue {
				delete(sd, k)
			}
		}
	}
}

// run 执行创建或更新；dry = 预检。
func (u *userClients) run(ch *change, dry bool) (*unstructured.Unstructured, error) {
	if ch.current == nil {
		o := metav1.CreateOptions{}
		if dry {
			o.DryRun = dryRunAll
		}
		return ch.rc.Create(u.ctx, ch.obj, o)
	}
	o := metav1.UpdateOptions{}
	if dry {
		o.DryRun = dryRunAll
	}
	return ch.rc.Update(u.ctx, ch.obj, o)
}

// displayYAML 给人看的 YAML：去掉 managedFields，Secret 掩码。
func displayYAML(obj *unstructured.Unstructured) string {
	if obj == nil {
		return ""
	}
	view := obj.DeepCopy().Object
	stripNoise(view)
	if biz.IsSecret(view) {
		st := view["status"]
		view = biz.SanitizeObject(view)
		if st != nil {
			view["status"] = st
		}
	}
	return toYAML(view)
}

// changed 比较变更前后（忽略服务端每次都会变的字段）。
func changed(before, after *unstructured.Unstructured) bool {
	if before == nil {
		return true
	}
	norm := func(u *unstructured.Unstructured) map[string]any {
		o := u.DeepCopy().Object
		delete(o, "status")
		if meta := nestedMap(o, "metadata"); meta != nil {
			for _, k := range []string{"managedFields", "resourceVersion", "generation"} {
				delete(meta, k)
			}
		}
		return o
	}
	return !reflect.DeepEqual(norm(before), norm(after))
}

func (u *userClients) preview(req k8scontract.ApplyRequest) (k8scontract.PreviewResponse, error) {
	var out k8scontract.PreviewResponse
	obj, err := parseObject(req.YAML)
	if err != nil {
		return out, err
	}
	ch, err := u.prepare(req.Ref, obj, req.ResourceVersion, false)
	if err != nil {
		return out, err
	}
	after, err := u.run(ch, true)
	if err != nil {
		return out, err
	}
	out.Create = ch.current == nil
	out.BeforeYAML, out.AfterYAML = displayYAML(ch.current), displayYAML(after)
	out.Changed = changed(ch.current, after)
	if ch.current != nil {
		out.ResourceVersion = ch.current.GetResourceVersion()
	}
	return out, nil
}

// mergePatch 变更前后的 JSON Merge Patch（对清洗后的对象计算，Secret 已掩码）；算不出返回 nil。
func mergePatch(before, after map[string]any) any {
	if before == nil || after == nil {
		return nil
	}
	b, err1 := json.Marshal(biz.SanitizeObject(before))
	a, err2 := json.Marshal(biz.SanitizeObject(after))
	if err1 != nil || err2 != nil {
		return nil
	}
	raw, err := jsonmergepatch.CreateThreeWayJSONMergePatch(b, a, b)
	if err != nil {
		return nil
	}
	return json.RawMessage(raw)
}

func objMap(u *unstructured.Unstructured) map[string]any {
	if u == nil {
		return nil
	}
	return u.Object
}

func (u *userClients) apply(req k8scontract.ApplyRequest) (k8scontract.ApplyResponse, []*biz.AuditEvent, error) {
	var out k8scontract.ApplyResponse
	audit := &biz.AuditEvent{Ref: req.Ref, Action: k8scontract.ActionUpdate}
	if req.Ref.Name == "" {
		audit.Action = k8scontract.ActionCreate
	}
	fail := func(err error) (k8scontract.ApplyResponse, []*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return out, []*biz.AuditEvent{audit}, err
	}
	obj, err := parseObject(req.YAML)
	if err != nil {
		return fail(err)
	}
	ch, err := u.prepare(req.Ref, obj, req.ResourceVersion, false)
	if err != nil {
		return fail(err)
	}
	audit.Ref, audit.Kind, audit.Before = ch.ref, ch.kind, objMap(ch.current)
	after, err := u.run(ch, false)
	if err != nil {
		return fail(err)
	}
	audit.After = after.Object
	audit.Patch = mergePatch(audit.Before, audit.After)
	ch.ref.Name = after.GetName()
	audit.Ref = ch.ref
	out = k8scontract.ApplyResponse{Ref: ch.ref, Kind: ch.kind, UID: string(after.GetUID()), ResourceVersion: after.GetResourceVersion()}
	return out, []*biz.AuditEvent{audit}, nil
}

func (u *userClients) delete(req k8scontract.DeleteRequest) ([]*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{Ref: req.Ref, Action: k8scontract.ActionDelete}
	audits := []*biz.AuditEvent{audit}
	if err := checkRef(req.Ref, true); err != nil {
		audit.Err = err
		return audits, err
	}
	rc, res, err := u.resource(req.Ref)
	if err != nil {
		audit.Err = mapError(err)
		return audits, err
	}
	audit.Kind = res.Kind
	// 删除前的对象只用于审计：读不到（如只有 delete 权限）不阻止删除。
	if cur, gerr := rc.Get(u.ctx, req.Ref.Name, metav1.GetOptions{}); gerr == nil {
		audit.Before = cur.Object
	}
	policy := metav1.DeletePropagationBackground
	switch req.Propagation {
	case "", string(metav1.DeletePropagationBackground):
	case string(metav1.DeletePropagationForeground):
		policy = metav1.DeletePropagationForeground
	case string(metav1.DeletePropagationOrphan):
		policy = metav1.DeletePropagationOrphan
	default:
		err := biz.ErrInvalid("invalid propagation %q", req.Propagation)
		audit.Err = err
		return audits, err
	}
	opts := metav1.DeleteOptions{PropagationPolicy: &policy}
	if req.UID != "" {
		uid := types.UID(req.UID)
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	audit.Patch = map[string]any{"propagation": string(policy), "uid": req.UID}
	if err := rc.Delete(u.ctx, req.Ref.Name, opts); err != nil {
		audit.Err = mapError(err)
		return audits, err
	}
	return audits, nil
}

// splitManifest 拆分多文档 YAML（跳过空文档）。
func splitManifest(text string) ([]*unstructured.Unstructured, error) {
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader([]byte(text)), 4096)
	var out []*unstructured.Unstructured
	for {
		var obj map[string]any
		err := dec.Decode(&obj)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, biz.ErrInvalid("invalid YAML in document %d: %s", len(out)+1, err.Error())
		}
		if len(obj) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: obj}
		if u.GetAPIVersion() == "" || u.GetKind() == "" {
			return nil, biz.ErrInvalid("document %d: apiVersion and kind are required", len(out)+1)
		}
		if u.GetKind() == "List" || strings.HasSuffix(u.GetKind(), "List") && obj["items"] != nil {
			return nil, biz.ErrInvalid("document %d: List kinds are not supported, use separate documents", len(out)+1)
		}
		out = append(out, u)
		if len(out) > k8scontract.MaxManifestDocs {
			return nil, biz.ErrInvalid("too many objects: at most %d per manifest", k8scontract.MaxManifestDocs)
		}
	}
	if len(out) == 0 {
		return nil, biz.ErrInvalid("manifest contains no objects")
	}
	return out, nil
}

// manifest 多文档 YAML。预检：逐个 dry-run。提交：全部预检通过才顺序执行，遇错即停。
// 同一份清单里先建命名空间、再建其中的对象是常见写法（如「创建命名空间并设置配额」）：dry-run 不会真的
// 创建命名空间，其中的对象预检必然报命名空间不存在。对这类对象跳过 dry-run（只做解析与类型解析），
// 提交时由集群校验；失败则在该对象处停止。
func (u *userClients) manifest(req k8scontract.ManifestRequest, commit bool) (k8scontract.ManifestResponse, []*biz.AuditEvent, error) {
	out := k8scontract.ManifestResponse{OK: true, Items: []k8scontract.ManifestItem{}}
	docs, err := splitManifest(req.YAML)
	if err != nil {
		return out, nil, err
	}
	changes := make([]*change, len(docs))
	pendingNS := map[string]struct{}{}
	for i, doc := range docs {
		item := k8scontract.ManifestItem{Kind: doc.GetKind(), Ref: k8scontract.Ref{Namespace: doc.GetNamespace(), Name: doc.GetName()}}
		ch, after, err := u.manifestOne(doc, req.Namespace, pendingNS)
		if err == nil && ch.current == nil && ch.ref.Group == "" && ch.ref.Resource == "namespaces" {
			pendingNS[ch.ref.Name] = struct{}{}
		}
		if ch != nil {
			item.Ref, item.Create = ch.ref, ch.current == nil
			item.BeforeYAML = displayYAML(ch.current)
		}
		if err != nil {
			item.Error = biz.AuditErrorText(mapError(err))
			out.OK = false
		} else {
			item.AfterYAML = displayYAML(after)
			changes[i] = ch
		}
		out.Items = append(out.Items, item)
	}
	if !commit || !out.OK {
		return out, nil, nil
	}
	var audits []*biz.AuditEvent
	failed := false
	for i, ch := range changes {
		item := &out.Items[i]
		item.BeforeYAML, item.AfterYAML = "", ""
		if failed {
			item.Skipped = true
			continue
		}
		audit := &biz.AuditEvent{Ref: ch.ref, Kind: ch.kind, Action: k8scontract.ActionUpdate, Before: objMap(ch.current)}
		if ch.current == nil {
			audit.Action = k8scontract.ActionCreate
		}
		audits = append(audits, audit)
		after, err := u.run(ch, false)
		if err != nil {
			audit.Err = mapError(err)
			item.Error = biz.AuditErrorText(audit.Err)
			out.OK, failed = false, true
			continue
		}
		audit.After = after.Object
		audit.Patch = mergePatch(audit.Before, audit.After)
	}
	return out, audits, nil
}

func (u *userClients) manifestOne(doc *unstructured.Unstructured, defaultNS string, pendingNS map[string]struct{}) (*change, *unstructured.Unstructured, error) {
	res, err := u.c.resolveKind(u.ctx, doc.GetAPIVersion(), doc.GetKind())
	if err != nil {
		return nil, nil, err
	}
	ref := k8scontract.Ref{Group: res.Group, Version: res.Version, Resource: res.Resource}
	if res.Namespaced {
		if doc.GetNamespace() == "" {
			doc.SetNamespace(defaultNS)
		}
		ref.Namespace = doc.GetNamespace()
	}
	ch, err := u.prepare(ref, doc, "", true)
	if err != nil {
		return nil, nil, err
	}
	if _, pending := pendingNS[ref.Namespace]; pending && res.Namespaced {
		return ch, doc, nil
	}
	after, err := u.run(ch, true)
	return ch, after, err
}
