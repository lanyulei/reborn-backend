package kube

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// dispatchWrite 写操作：每个都返回审计事件（成功与失败）。
func (u *userClients) dispatchWrite(op string, body []byte) (any, []*biz.AuditEvent, error) {
	empty := k8scontract.Empty{}
	switch op {
	case k8scontract.OpPreview:
		req, err := decode[k8scontract.ApplyRequest](body)
		if err != nil {
			return nil, nil, err
		}
		r, err := u.preview(req)
		return r, nil, err
	case k8scontract.OpApply:
		req, err := decode[k8scontract.ApplyRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.apply(req)
	case k8scontract.OpDelete:
		req, err := decode[k8scontract.DeleteRequest](body)
		if err != nil {
			return nil, nil, err
		}
		a, err := u.delete(req)
		return empty, a, err
	case k8scontract.OpManifestPreview, k8scontract.OpManifestApply:
		req, err := decode[k8scontract.ManifestRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.manifest(req, op == k8scontract.OpManifestApply)
	case k8scontract.OpScale:
		req, err := decode[k8scontract.ScaleRequest](body)
		if err != nil {
			return nil, nil, err
		}
		if req.Replicas < 0 {
			return nil, nil, biz.ErrInvalid("replicas must not be negative")
		}
		return u.patchOp(req.Ref, k8scontract.ActionScale, map[string]any{"replicas": req.Replicas},
			types.MergePatchType, map[string]any{"spec": map[string]any{"replicas": req.Replicas}},
			[]k8scontract.GVR{k8scontract.GVRDeployment, k8scontract.GVRStatefulSet, k8scontract.GVRReplicaSet}, "scale")
	case k8scontract.OpRestart:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		at := time.Now().UTC().Format(time.RFC3339)
		return u.patchOp(req.Ref, k8scontract.ActionRestart, map[string]any{"restartedAt": at}, types.MergePatchType,
			map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{
				"annotations": map[string]any{"kubectl.kubernetes.io/restartedAt": at},
			}}}}, workloadGVRs)
	case k8scontract.OpSetImage:
		req, err := decode[k8scontract.SetImageRequest](body)
		if err != nil {
			return nil, nil, err
		}
		a, err := u.setImage(req)
		return empty, a, err
	case k8scontract.OpRollback:
		req, err := decode[k8scontract.RollbackRequest](body)
		if err != nil {
			return nil, nil, err
		}
		a, err := u.rollback(req)
		return empty, a, err
	case k8scontract.OpPause:
		req, err := decode[k8scontract.PauseRequest](body)
		if err != nil {
			return nil, nil, err
		}
		action := k8scontract.ActionResume
		if req.Paused {
			action = k8scontract.ActionPause
		}
		return u.patchOp(req.Ref, action, nil, types.MergePatchType,
			map[string]any{"spec": map[string]any{"paused": req.Paused}}, []k8scontract.GVR{k8scontract.GVRDeployment})
	case k8scontract.OpCronSuspend:
		req, err := decode[k8scontract.SuspendRequest](body)
		if err != nil {
			return nil, nil, err
		}
		action := k8scontract.ActionUnsuspend
		if req.Suspend {
			action = k8scontract.ActionSuspend
		}
		return u.patchOp(req.Ref, action, nil, types.MergePatchType,
			map[string]any{"spec": map[string]any{"suspend": req.Suspend}}, []k8scontract.GVR{k8scontract.GVRCronJob})
	case k8scontract.OpCronTrigger:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.cronTrigger(req.Ref)
	case k8scontract.OpPodEvict:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		audit := &biz.AuditEvent{Ref: podRef(req.Ref.Namespace, req.Ref.Name), Kind: "Pod", Action: k8scontract.ActionEvict}
		if req.Ref.Namespace == "" || req.Ref.Name == "" {
			return nil, nil, biz.ErrInvalid("namespace and name are required")
		}
		err = u.evict(req.Ref.Namespace, req.Ref.Name, nil)
		audit.Err = mapError(err)
		return empty, []*biz.AuditEvent{audit}, err
	case k8scontract.OpPodResize:
		req, err := decode[k8scontract.ResizeRequest](body)
		if err != nil {
			return nil, nil, err
		}
		a, err := u.resize(req)
		return empty, a, err
	case k8scontract.OpNodeCordon:
		req, err := decode[k8scontract.CordonRequest](body)
		if err != nil {
			return nil, nil, err
		}
		a, err := u.cordon(req.Name, req.Unschedulable)
		return empty, []*biz.AuditEvent{a}, err
	case k8scontract.OpSecretReveal:
		req, err := decode[k8scontract.ObjectRequest](body)
		if err != nil {
			return nil, nil, err
		}
		return u.secretReveal(req.Ref)
	}
	return nil, nil, biz.ErrInvalid("unknown operation %q", op)
}

var workloadGVRs = []k8scontract.GVR{k8scontract.GVRDeployment, k8scontract.GVRStatefulSet, k8scontract.GVRDaemonSet}

func podRef(ns, name string) k8scontract.Ref {
	return k8scontract.Ref{Version: "v1", Resource: "pods", Namespace: ns, Name: name}
}

func supports(ref k8scontract.Ref, allowed []k8scontract.GVR) error {
	for _, g := range allowed {
		if ref.GVR() == g {
			return nil
		}
	}
	return biz.ErrUnsupported("%s does not support this operation", ref.Resource)
}

// patchOp 以一次 Patch 完成的快捷操作。
func (u *userClients) patchOp(ref k8scontract.Ref, action string, params any, pt types.PatchType, patch any,
	allowed []k8scontract.GVR, sub ...string,
) (any, []*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{Ref: ref, Action: action, Patch: params}
	if params == nil {
		audit.Patch = patch
	}
	audits := []*biz.AuditEvent{audit}
	fail := func(err error) (any, []*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return nil, audits, err
	}
	if err := checkRef(ref, true); err != nil {
		return fail(err)
	}
	if err := supports(ref, allowed); err != nil {
		return fail(err)
	}
	rc, res, err := u.resource(ref)
	if err != nil {
		return fail(err)
	}
	audit.Kind = res.Kind
	raw, err := json.Marshal(patch)
	if err != nil {
		return fail(err)
	}
	if _, err := rc.Patch(u.ctx, ref.Name, pt, raw, metav1.PatchOptions{}, sub...); err != nil {
		return fail(err)
	}
	return k8scontract.Empty{}, audits, nil
}

func (u *userClients) setImage(req k8scontract.SetImageRequest) ([]*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{Ref: req.Ref, Action: k8scontract.ActionSetImage, Patch: map[string]any{"container": req.Container, "image": req.Image}}
	audits := []*biz.AuditEvent{audit}
	fail := func(err error) ([]*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return audits, err
	}
	if err := checkRef(req.Ref, true); err != nil {
		return fail(err)
	}
	if req.Container == "" || req.Image == "" {
		return fail(biz.ErrInvalid("container and image are required"))
	}
	if err := supports(req.Ref, workloadGVRs); err != nil {
		return fail(err)
	}
	rc, res, err := u.resource(req.Ref)
	if err != nil {
		return fail(err)
	}
	audit.Kind = res.Kind
	cur, err := rc.Get(u.ctx, req.Ref.Name, metav1.GetOptions{})
	if err != nil {
		return fail(err)
	}
	field := ""
	for _, f := range []string{"containers", "initContainers"} {
		for _, c := range nestedList(cur.Object, "spec", "template", "spec", f) {
			if m := asMap(c); str(m, "name") == req.Container {
				field = f
				audit.Patch = map[string]any{"container": req.Container, "image": req.Image, "from": str(m, "image")}
			}
		}
	}
	if field == "" {
		return fail(biz.ErrInvalid("container %q not found in %s", req.Container, req.Ref.Name))
	}
	// 策略合并：容器列表按名称合并，只改这一个容器的镜像。
	raw, _ := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
		field: []any{map[string]any{"name": req.Container, "image": req.Image}},
	}}}})
	if _, err := rc.Patch(u.ctx, req.Ref.Name, types.StrategicMergePatchType, raw, metav1.PatchOptions{}); err != nil {
		return fail(err)
	}
	return audits, nil
}

// owned 列出命名空间里由 owner 控制的某类对象。
func (u *userClients) owned(owner *unstructured.Unstructured, gvr k8scontract.GVR) ([]unstructured.Unstructured, error) {
	sel := ""
	if raw := nestedMap(owner.Object, "spec", "selector"); raw != nil {
		var ls metav1.LabelSelector
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &ls); err == nil {
			if s, err := metav1.LabelSelectorAsSelector(&ls); err == nil {
				sel = s.String()
			}
		}
	}
	list, err := u.dyn.Resource(toSchema(gvr)).Namespace(owner.GetNamespace()).List(u.ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return nil, err
	}
	var out []unstructured.Unstructured
	for _, item := range list.Items {
		for _, o := range item.GetOwnerReferences() {
			if o.UID == owner.GetUID() {
				out = append(out, item)
				break
			}
		}
	}
	return out, nil
}

var gvrControllerRevision = k8scontract.GVR{Group: "apps", Version: "v1", Resource: "controllerrevisions"}

// history 工作负载的历史版本及其来源对象。
type history struct {
	rev k8scontract.Revision
	src unstructured.Unstructured
}

func (u *userClients) history(ref k8scontract.Ref) (*unstructured.Unstructured, resourceClient, []history, error) {
	if err := checkRef(ref, true); err != nil {
		return nil, nil, nil, err
	}
	if err := supports(ref, workloadGVRs); err != nil {
		return nil, nil, nil, err
	}
	rc, _, err := u.resource(ref)
	if err != nil {
		return nil, nil, nil, err
	}
	w, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	var out []history
	if ref.GVR() == k8scontract.GVRDeployment {
		sets, err := u.owned(w, k8scontract.GVRReplicaSet)
		if err != nil {
			return nil, nil, nil, err
		}
		current := w.GetAnnotations()[revisionAnnotation]
		for _, rs := range sets {
			ann := rs.GetAnnotations()
			n, err := strconv.ParseInt(ann[revisionAnnotation], 10, 64)
			if err != nil {
				continue
			}
			tpl := nestedMap(rs.Object, "spec", "template")
			out = append(out, history{src: rs, rev: k8scontract.Revision{
				Revision: n, CreateTime: rs.GetCreationTimestamp().Time, Images: templateImages(rs.Object),
				Current: ann[revisionAnnotation] == current, ChangeCause: ann["kubernetes.io/change-cause"], TemplateYAML: toYAML(tpl),
			}})
		}
	} else {
		revs, err := u.owned(w, gvrControllerRevision)
		if err != nil {
			return nil, nil, nil, err
		}
		var max int64
		for _, cr := range revs {
			if n := numOr(cr.Object, 0, "revision"); n > max {
				max = n
			}
		}
		updateRev := str(w.Object, "status", "updateRevision")
		for _, cr := range revs {
			n := numOr(cr.Object, 0, "revision")
			tpl := nestedMap(cr.Object, "data", "spec", "template")
			delete(tpl, "$patch")
			cur := n == max
			if updateRev != "" {
				cur = cr.GetName() == updateRev
			}
			out = append(out, history{src: cr, rev: k8scontract.Revision{
				Revision: n, CreateTime: cr.GetCreationTimestamp().Time, Images: containerImages(tpl, "spec"),
				Current: cur, ChangeCause: cr.GetAnnotations()["kubernetes.io/change-cause"], TemplateYAML: toYAML(tpl),
			}})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rev.Revision > out[j].rev.Revision })
	return w, rc, out, nil
}

func (u *userClients) revisions(ref k8scontract.Ref) (k8scontract.RevisionsResponse, error) {
	out := k8scontract.RevisionsResponse{Items: []k8scontract.Revision{}}
	_, _, hs, err := u.history(ref)
	if err != nil {
		return out, err
	}
	for _, h := range hs {
		out.Items = append(out.Items, h.rev)
	}
	return out, nil
}

func (u *userClients) rollback(req k8scontract.RollbackRequest) ([]*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{Ref: req.Ref, Action: k8scontract.ActionRollback, Patch: map[string]any{"revision": req.Revision}}
	audits := []*biz.AuditEvent{audit}
	fail := func(err error) ([]*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return audits, err
	}
	w, rc, hs, err := u.history(req.Ref)
	if err != nil {
		return fail(err)
	}
	audit.Kind = w.GetKind()
	var target *history
	for i := range hs {
		if hs[i].rev.Revision == req.Revision {
			target = &hs[i]
		}
	}
	if target == nil {
		return fail(biz.ErrNotFound("revision %d not found", req.Revision))
	}
	if target.rev.Current {
		return fail(biz.ErrInvalid("revision %d is already the current revision", req.Revision))
	}
	audit.Before = w.Object
	var after *unstructured.Unstructured
	if req.Ref.GVR() == k8scontract.GVRDeployment {
		// 把目标 ReplicaSet 的 Pod 模板写回（去掉控制器加的 pod-template-hash 标签）。用 JSON 补丁整体替换
		// spec.template（kubectl rollout undo 同此做法）：目标版本里没有的字段必须消失，所以不能用合并补丁；
		// 也不能读出对象改完再 Update——Deployment 控制器随时在更新它的状态与注解，带着读到的版本号写回
		// 会偶发 409，而回滚并不依赖对象的其他部分。
		src, _ := nested(target.src.Object, "spec", "template")
		tpl, ok := runtime.DeepCopyJSONValue(src).(map[string]any)
		if !ok {
			return fail(biz.ErrInvalid("revision %d has no pod template", req.Revision))
		}
		if l := nestedMap(tpl, "metadata", "labels"); l != nil {
			delete(l, "pod-template-hash")
		}
		raw, merr := json.Marshal([]map[string]any{{"op": "replace", "path": "/spec/template", "value": tpl}})
		if merr != nil {
			return fail(merr)
		}
		after, err = rc.Patch(u.ctx, req.Ref.Name, types.JSONPatchType, raw, metav1.PatchOptions{})
	} else {
		// ControllerRevision.data 本身就是一份带 $patch: replace 的策略合并补丁（kubectl rollout undo 同此做法）。
		raw, merr := json.Marshal(target.src.Object["data"])
		if merr != nil {
			return fail(merr)
		}
		after, err = rc.Patch(u.ctx, req.Ref.Name, types.StrategicMergePatchType, raw, metav1.PatchOptions{})
	}
	if err != nil {
		return fail(err)
	}
	audit.After = after.Object
	return audits, nil
}

func (u *userClients) cronTrigger(ref k8scontract.Ref) (any, []*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{Ref: ref, Kind: "CronJob", Action: k8scontract.ActionTrigger}
	audits := []*biz.AuditEvent{audit}
	fail := func(err error) (any, []*biz.AuditEvent, error) {
		audit.Err = mapError(err)
		return nil, audits, err
	}
	if err := checkRef(ref, true); err != nil {
		return fail(err)
	}
	if err := supports(ref, []k8scontract.GVR{k8scontract.GVRCronJob}); err != nil {
		return fail(err)
	}
	rc, _, err := u.resource(ref)
	if err != nil {
		return fail(err)
	}
	cj, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return fail(err)
	}
	spec, _ := nested(cj.Object, "spec", "jobTemplate", "spec")
	if spec == nil {
		return fail(biz.ErrInvalid("cronjob has no job template"))
	}
	name := ref.Name
	if len(name) > 41 { // Job 名上限 63：留出 "-manual-" 与时间戳
		name = name[:41]
	}
	if cid := strings.ReplaceAll(u.id.ChangeID, "-", ""); len(cid) >= 12 {
		// 执行变更单：Job 名由变更单 ID 决定。同一张变更单万一被执行第二次，创建会因重名而失败，
		// 不会再触发一个任务。
		name = fmt.Sprintf("%s-chg-%s", name, cid[len(cid)-12:])
	} else {
		name = fmt.Sprintf("%s-manual-%d", name, time.Now().Unix())
	}
	meta := map[string]any{
		"name": name, "namespace": ref.Namespace,
		"annotations": map[string]any{"cronjob.kubernetes.io/instantiate": "manual"},
		"ownerReferences": []any{map[string]any{
			"apiVersion": "batch/v1", "kind": "CronJob", "name": cj.GetName(), "uid": string(cj.GetUID()),
			"controller": true, "blockOwnerDeletion": true,
		}},
	}
	if l := nestedMap(cj.Object, "spec", "jobTemplate", "metadata", "labels"); l != nil {
		meta["labels"] = runtime.DeepCopyJSONValue(l)
	}
	job := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "batch/v1", "kind": "Job", "metadata": meta, "spec": runtime.DeepCopyJSONValue(spec),
	}}
	created, err := u.dyn.Resource(toSchema(k8scontract.GVRJob)).Namespace(ref.Namespace).Create(u.ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fail(err)
	}
	audit.Patch = map[string]any{"job": created.GetName()}
	jobRef := k8scontract.Ref{Group: "batch", Version: "v1", Resource: "jobs", Namespace: ref.Namespace, Name: created.GetName()}
	return k8scontract.ApplyResponse{Ref: jobRef, Kind: "Job", UID: string(created.GetUID()), ResourceVersion: created.GetResourceVersion()}, audits, nil
}

// evict 经驱逐接口删除 Pod（遵守 PodDisruptionBudget；被拒绝时集群返回 429）。
func (u *userClients) evict(namespace, name string, grace *int64) error {
	ev := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if grace != nil && *grace >= 0 {
		ev.DeleteOptions = &metav1.DeleteOptions{GracePeriodSeconds: grace}
	}
	return u.kube.PolicyV1().Evictions(namespace).Evict(u.ctx, ev)
}

func (u *userClients) cordon(name string, unschedulable bool) (*biz.AuditEvent, error) {
	action := k8scontract.ActionUncordon
	if unschedulable {
		action = k8scontract.ActionCordon
	}
	audit := &biz.AuditEvent{Ref: k8scontract.Ref{Version: "v1", Resource: "nodes", Name: name}, Kind: "Node", Action: action}
	if name == "" {
		audit.Err = biz.ErrInvalid("node name is required")
		return audit, audit.Err
	}
	raw, _ := json.Marshal(map[string]any{"spec": map[string]any{"unschedulable": unschedulable}})
	_, err := u.dyn.Resource(toSchema(k8scontract.GVRNode)).Patch(u.ctx, name, types.MergePatchType, raw, metav1.PatchOptions{})
	audit.Err = mapError(err)
	return audit, err
}

// drainPlan 排空时各 Pod 的处置（与 kubectl drain 的规则一致）。
func (u *userClients) drainPlan(req k8scontract.DrainRequest) ([]k8scontract.DrainPod, bool, error) {
	if req.Name == "" {
		return nil, false, biz.ErrInvalid("node name is required")
	}
	if _, err := u.kube.CoreV1().Nodes().Get(u.ctx, req.Name, metav1.GetOptions{}); err != nil {
		return nil, false, err
	}
	list, err := u.kube.CoreV1().Pods(metav1.NamespaceAll).List(u.ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + req.Name})
	if err != nil {
		return nil, false, err
	}
	pods := make([]k8scontract.DrainPod, 0, len(list.Items))
	blocked := false
	for i := range list.Items {
		p := &list.Items[i]
		dp := k8scontract.DrainPod{Namespace: p.Namespace, Name: p.Name, Action: "evict"}
		controller := metav1.GetControllerOf(p)
		emptyDir := false
		for _, v := range p.Spec.Volumes {
			if v.EmptyDir != nil {
				emptyDir = true
			}
		}
		_, mirror := p.Annotations["kubernetes.io/config.mirror"]
		switch {
		case mirror:
			dp.Action, dp.Reason = "skip", "static pod"
		case controller != nil && controller.Kind == "DaemonSet":
			dp.Action, dp.Reason = "skip", "managed by DaemonSet"
		case p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed":
			dp.Reason = "already finished"
		case p.DeletionTimestamp != nil:
			dp.Action, dp.Reason = "skip", "already terminating"
		case controller == nil && !req.Force:
			dp.Action, dp.Reason = "block", "not managed by a controller: it will not be recreated (enable force to evict)"
		case emptyDir && !req.DeleteEmptyDir:
			dp.Action, dp.Reason = "block", "uses emptyDir: its data will be lost (enable delete emptyDir data to evict)"
		case controller == nil:
			dp.Reason = "not managed by a controller: it will not be recreated"
		case emptyDir:
			dp.Reason = "emptyDir data will be lost"
		}
		if dp.Action == "block" {
			blocked = true
		}
		pods = append(pods, dp)
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
	return pods, blocked, nil
}

func (u *userClients) drainPreview(req k8scontract.DrainRequest) (k8scontract.DrainPreviewResponse, error) {
	pods, blocked, err := u.drainPlan(req)
	if pods == nil {
		pods = []k8scontract.DrainPod{}
	}
	return k8scontract.DrainPreviewResponse{Pods: pods, Blocked: blocked}, err
}

// drainRetry 驱逐被 PodDisruptionBudget 拒绝后的重试间隔。
const drainRetry = 5 * time.Second

// drain 排空节点：先停止调度，再逐个驱逐，流式返回进度。受 PDB 限制的 Pod 持续重试到流结束。
func (u *userClients) drain(req k8scontract.DrainRequest, emit biz.Emit) ([]*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{
		Ref: k8scontract.Ref{Version: "v1", Resource: "nodes", Name: req.Name}, Kind: "Node", Action: k8scontract.ActionDrain,
	}
	audits := []*biz.AuditEvent{audit}
	plan, blocked, err := u.drainPlan(req)
	if err != nil {
		audit.Err = mapError(err)
		return audits, err
	}
	if blocked {
		err := biz.NewError(409, k8scontract.ReasonConflict, "some pods block the drain with the current options; see the drain preview")
		audit.Err = err
		return audits, err
	}
	if _, err := u.cordon(req.Name, true); err != nil {
		audit.Err = mapError(err)
		return audits, err
	}
	var targets []k8scontract.DrainPod
	for _, p := range plan {
		if p.Action == "evict" {
			targets = append(targets, p)
		}
	}
	var grace *int64
	if req.GracePeriodSeconds > 0 {
		grace = &req.GracePeriodSeconds
	}
	done, failed, gone := 0, 0, false
	evicted := make([]string, 0, len(targets))
	pending := targets
	for len(pending) > 0 && u.ctx.Err() == nil {
		var again []k8scontract.DrainPod
		for _, p := range pending {
			prog := k8scontract.DrainProgress{Namespace: p.Namespace, Name: p.Name, Total: len(targets)}
			err := u.evict(p.Namespace, p.Name, grace)
			switch {
			case err == nil || apierrors.IsNotFound(err):
				done++
				evicted = append(evicted, p.Namespace+"/"+p.Name)
				prog.Status = "evicted"
			case apierrors.IsTooManyRequests(err):
				again = append(again, p)
				prog.Status, prog.Message = "pending", biz.AuditErrorText(mapError(err))
			default:
				done++
				failed++
				prog.Status, prog.Message = "failed", biz.AuditErrorText(mapError(err))
			}
			prog.Done = done
			if emit(k8scontract.EventProgress, "", prog) != nil {
				gone = true
				break
			}
		}
		if gone { // 客户端已断开：停止驱逐剩余的 Pod（节点保持停止调度，由用户决定下一步）
			break
		}
		pending = again
		if len(pending) > 0 {
			select {
			case <-u.ctx.Done():
			case <-time.After(drainRetry):
			}
		}
	}
	audit.Patch = map[string]any{
		"deleteEmptyDir": req.DeleteEmptyDir, "force": req.Force, "evicted": evicted, "failed": failed, "unfinished": len(targets) - done,
	}
	end := k8scontract.StreamEnd{Reason: k8scontract.EndCompleted}
	if failed > 0 || done < len(targets) {
		end = k8scontract.StreamEnd{Reason: k8scontract.EndError, Message: fmt.Sprintf("%d failed, %d not finished", failed, len(targets)-done)}
		audit.Err = biz.NewError(409, k8scontract.ReasonConflict, "%s", end.Message)
	}
	if u.ctx.Err() == nil && !gone {
		_ = emit(k8scontract.EventEnd, "", end)
	}
	return audits, nil
}

func (u *userClients) secretReveal(ref k8scontract.Ref) (any, []*biz.AuditEvent, error) {
	audit := &biz.AuditEvent{
		Ref: k8scontract.Ref{Version: "v1", Resource: "secrets", Namespace: ref.Namespace, Name: ref.Name}, Kind: "Secret",
		Action: k8scontract.ActionSecretReveal,
	}
	audits := []*biz.AuditEvent{audit}
	if ref.Namespace == "" || ref.Name == "" {
		audit.Err = biz.ErrInvalid("namespace and name are required")
		return nil, audits, audit.Err
	}
	s, err := u.kube.CoreV1().Secrets(ref.Namespace).Get(u.ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		audit.Err = mapError(err)
		return nil, audits, err
	}
	out := k8scontract.SecretRevealResponse{Data: make(map[string]string, len(s.Data))}
	keys := make([]string, 0, len(s.Data))
	for k, v := range s.Data {
		keys = append(keys, k)
		if utf8.Valid(v) {
			out.Data[k] = string(v)
		} else {
			out.Data[k] = base64.StdEncoding.EncodeToString(v)
			out.Binary = append(out.Binary, k)
		}
	}
	sort.Strings(keys)
	sort.Strings(out.Binary)
	audit.Patch = map[string]any{"keys": keys} // 只记键名，不记内容
	return out, audits, nil
}
