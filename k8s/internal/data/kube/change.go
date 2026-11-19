package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/storage/driver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// 本文件是变更审批在数据面的两半：
//   - changePlan：提交时以发起人身份读取现状、预检，产出变更计划（展示形态 + 执行形态）；
//   - ExecuteChange：审批通过后以发起人身份执行——先把「涉及的字段」与现状逐个比对，再预检，再提交。
// 字段比对的纯逻辑在 biz/changefield.go。

var _ biz.ChangeExecutor = (*Manager)(nil)

// ---------------------------------------------------------------------------
// 计划
// ---------------------------------------------------------------------------

func (u *userClients) changePlan(req k8scontract.ChangePlanRequest) (k8scontract.ChangePlanResponse, error) {
	out := k8scontract.ChangePlanResponse{Exec: k8scontract.ChangeExec{Op: req.Op, Body: req.Body}}
	var err error
	switch req.Op {
	case k8scontract.OpApply:
		err = u.planApply(req.Body, &out)
	case k8scontract.OpManifestApply:
		err = u.planManifest(req.Body, &out)
	case k8scontract.OpDelete:
		err = u.planDelete(req.Body, &out)
	case k8scontract.OpHelmInstall, k8scontract.OpHelmUpgrade, k8scontract.OpHelmRollback, k8scontract.OpHelmUninstall:
		err = u.planHelm(req.Op, req.Body, &out)
	default:
		err = u.planQuick(req.Op, req.Body, &out)
	}
	if err != nil {
		return out, err
	}
	if len(out.Display.Items) > 0 && out.Display.Target.Resource == "" {
		out.Display.Target, out.Display.Kind = out.Display.Items[0].Ref, out.Display.Items[0].Kind
	}
	if out.Display.Items == nil {
		out.Display.Items = []k8scontract.ChangeItemDisplay{}
	}
	return out, nil
}

// planObject 一个对象的创建或更新：ch 已准备好，after 是预检结果（无法预检时为 nil）。
func planObject(ch *change, after *unstructured.Unstructured, skipDry bool) (k8scontract.ChangeItemDisplay, k8scontract.ChangeExecItem) {
	d := k8scontract.ChangeItemDisplay{Ref: ch.ref, Kind: ch.kind, Fields: []k8scontract.ChangeField{}}
	e := k8scontract.ChangeExecItem{Ref: ch.ref, Kind: ch.kind, SkipDryRun: skipDry}
	if after == nil {
		after = ch.obj
	}
	d.AfterYAML = displayYAML(after)
	if d.Ref.Name == "" {
		d.Ref.Name = after.GetName()
	}
	if ch.current == nil {
		d.Action, e.Mode = k8scontract.ActionCreate, k8scontract.ChangeModeCreate
		obj := runtime.DeepCopyJSON(ch.obj.Object)
		unstructured.RemoveNestedField(obj, "metadata", "resourceVersion")
		unstructured.RemoveNestedField(obj, "metadata", "uid")
		e.Object = obj
		return d, e
	}
	d.Action, e.Mode = k8scontract.ActionUpdate, k8scontract.ChangeModePatch
	d.BeforeYAML = displayYAML(ch.current)
	e.UID = string(ch.current.GetUID())
	base, desired := biz.ChangeBase(ch.current.Object), biz.ChangeBase(ch.obj.Object)
	e.Patch, e.Checks = biz.DiffFields(base, desired)
	for _, c := range e.Checks {
		secret := biz.SecretPath(ch.current.Object, c.Path)
		av, found := biz.LookupField(desired, c.Path)
		d.Fields = append(d.Fields, k8scontract.ChangeField{
			Path: biz.FieldPath(c.Path), Before: biz.FieldText(c.Value, c.Absent, secret), After: biz.FieldText(av, !found, secret),
		})
	}
	return d, e
}

func (u *userClients) planApply(body []byte, out *k8scontract.ChangePlanResponse) error {
	req, err := decode[k8scontract.ApplyRequest](body)
	if err != nil {
		return err
	}
	obj, err := parseObject(req.YAML)
	if err != nil {
		return err
	}
	ch, err := u.prepare(req.Ref, obj, req.ResourceVersion, false)
	if err != nil {
		return err
	}
	after, err := u.run(ch, true)
	if err != nil {
		return err
	}
	d, e := planObject(ch, after, false)
	if e.Mode == k8scontract.ChangeModePatch && len(e.Patch) == 0 {
		return biz.ErrInvalid("the submitted YAML does not change the object")
	}
	out.Display.Action, out.Display.Summary = d.Action, summarize(d)
	out.Display.Items = append(out.Display.Items, d)
	out.Exec.Items = append(out.Exec.Items, e)
	return nil
}

func summarize(d k8scontract.ChangeItemDisplay) string {
	who := d.Kind + " " + d.Ref.Name
	switch d.Action {
	case k8scontract.ActionCreate:
		return "创建 " + who
	case k8scontract.ActionDelete:
		return "删除 " + who
	}
	paths := make([]string, 0, 3)
	for i, f := range d.Fields {
		if i == 3 {
			paths = append(paths, fmt.Sprintf("等 %d 个字段", len(d.Fields)))
			break
		}
		paths = append(paths, f.Path)
	}
	return truncateText("更新 "+who+"："+strings.Join(paths, "、"), 400)
}

func (u *userClients) planManifest(body []byte, out *k8scontract.ChangePlanResponse) error {
	req, err := decode[k8scontract.ManifestRequest](body)
	if err != nil {
		return err
	}
	docs, err := splitManifest(req.YAML)
	if err != nil {
		return err
	}
	pendingNS := map[string]struct{}{}
	for i, doc := range docs {
		res, err := u.c.resolveKind(u.ctx, doc.GetAPIVersion(), doc.GetKind())
		if err != nil {
			return err
		}
		ref := k8scontract.Ref{Group: res.Group, Version: res.Version, Resource: res.Resource}
		if res.Namespaced {
			if doc.GetNamespace() == "" {
				doc.SetNamespace(req.Namespace)
			}
			ref.Namespace = doc.GetNamespace()
		}
		ch, err := u.prepare(ref, doc, "", true)
		if err != nil {
			return fmt.Errorf("document %d (%s %s): %w", i+1, doc.GetKind(), doc.GetName(), mapError(err))
		}
		var after *unstructured.Unstructured
		_, skip := pendingNS[ref.Namespace]
		skip = skip && res.Namespaced
		if !skip {
			if after, err = u.run(ch, true); err != nil {
				be := biz.AsError(mapError(err))
				return biz.NewError(be.Code, be.Reason, "document %d (%s %s): %s", i+1, doc.GetKind(), doc.GetName(), be.Message)
			}
		}
		if ch.current == nil && ref.Group == "" && ref.Resource == "namespaces" {
			pendingNS[ch.ref.Name] = struct{}{}
		}
		d, e := planObject(ch, after, skip)
		if e.Mode == k8scontract.ChangeModePatch && len(e.Patch) == 0 {
			continue // 与现状一致的对象不进变更单
		}
		out.Display.Items = append(out.Display.Items, d)
		out.Exec.Items = append(out.Exec.Items, e)
	}
	if len(out.Exec.Items) == 0 {
		return biz.ErrInvalid("the manifest does not change anything")
	}
	out.Display.Action = k8scontract.ActionUpdate
	if len(out.Exec.Items) == 1 {
		out.Display.Action = out.Display.Items[0].Action
		out.Display.Summary = summarize(out.Display.Items[0])
	} else {
		out.Display.Summary = fmt.Sprintf("YAML 清单：%d 个对象（%s 等）", len(out.Exec.Items), out.Display.Items[0].Kind+" "+out.Display.Items[0].Ref.Name)
	}
	return nil
}

func (u *userClients) planDelete(body []byte, out *k8scontract.ChangePlanResponse) error {
	req, err := decode[k8scontract.DeleteRequest](body)
	if err != nil {
		return err
	}
	cur, kind, err := u.current(req.Ref)
	if err != nil {
		return err
	}
	if req.UID != "" && req.UID != string(cur.GetUID()) {
		return biz.NewError(409, k8scontract.ReasonConflict, "the object was deleted and recreated; refresh and retry")
	}
	// 执行时以提交时的 UID 为前置条件：对象在审批期间被删除后重建则不删。
	req.UID = string(cur.GetUID())
	if out.Exec.Body, err = json.Marshal(req); err != nil {
		return err
	}
	d := k8scontract.ChangeItemDisplay{Ref: req.Ref, Kind: kind, Action: k8scontract.ActionDelete, BeforeYAML: displayYAML(cur), Fields: []k8scontract.ChangeField{}}
	out.Display.Action, out.Display.Summary = d.Action, summarize(d)
	out.Display.Items = append(out.Display.Items, d)
	out.Exec.Items = append(out.Exec.Items, k8scontract.ChangeExecItem{Ref: req.Ref, Kind: kind, Mode: k8scontract.ChangeModeCheck, UID: req.UID})
	return nil
}

// current 以用户身份读出对象。
func (u *userClients) current(ref k8scontract.Ref) (*unstructured.Unstructured, string, error) {
	if err := checkRef(ref, true); err != nil {
		return nil, "", err
	}
	rc, res, err := u.resource(ref)
	if err != nil {
		return nil, "", err
	}
	cur, err := rc.Get(u.ctx, ref.Name, metav1.GetOptions{})
	return cur, res.Kind, err
}

// quickPlan 一个快捷操作的计划：目标对象、动作、涉及的字段（路径 → 变更后的展示文本）与一句话说明。
type quickPlan struct {
	ref     k8scontract.Ref
	action  string
	allowed []k8scontract.GVR
	// fields 涉及的字段；after 是变更后的值（仅展示）。没有字段的操作只核对对象仍是原来那个（UID）。
	fields  []quickField
	summary func(cur map[string]any) string
}

type quickField struct {
	path  []string
	after any
}

// QuickTouched 各快捷操作涉及的字段（审批期间这些字段被改动即冲突）。container 仅 set-image 使用：
// 调用方已确认它在 containers 还是 initContainers 里（listField）。
func QuickTouched(op, listField, container string) [][]string {
	switch op {
	case k8scontract.OpScale:
		return [][]string{{"spec", "replicas"}}
	case k8scontract.OpSetImage:
		return [][]string{{"spec", "template", "spec", listField, "name=" + container, "image"}}
	case k8scontract.OpRollback:
		return [][]string{{"spec", "template"}}
	case k8scontract.OpPause:
		return [][]string{{"spec", "paused"}}
	case k8scontract.OpCronSuspend:
		return [][]string{{"spec", "suspend"}}
	case k8scontract.OpNodeCordon:
		return [][]string{{"spec", "unschedulable"}}
	}
	// restart / cronjob-trigger / pod-evict / node-drain：不改已有字段，只要求对象仍是原来那个。
	// pod-resize：涉及的字段随请求而定，读到 Pod 后由 planResize 得出（见 planQuick）。
	return nil
}

func (u *userClients) quickPlanFor(op string, body []byte) (*quickPlan, error) {
	nodeRef := func(name string) k8scontract.Ref {
		return k8scontract.Ref{Version: "v1", Resource: "nodes", Name: name}
	}
	field := func(after any, path ...string) []quickField { return []quickField{{path: path, after: after}} }
	touched := func(after any) []quickField {
		var out []quickField
		for _, p := range QuickTouched(op, "", "") {
			out = append(out, quickField{path: p, after: after})
		}
		return out
	}
	switch op {
	case k8scontract.OpScale:
		req, err := decode[k8scontract.ScaleRequest](body)
		if err != nil {
			return nil, err
		}
		if req.Replicas < 0 {
			return nil, biz.ErrInvalid("replicas must not be negative")
		}
		return &quickPlan{
			ref: req.Ref, action: k8scontract.ActionScale, fields: touched(req.Replicas),
			allowed: []k8scontract.GVR{k8scontract.GVRDeployment, k8scontract.GVRStatefulSet, k8scontract.GVRReplicaSet},
			summary: func(cur map[string]any) string {
				was, _ := nested(cur, "spec", "replicas")
				return fmt.Sprintf("副本数 %v → %d", was, req.Replicas)
			},
		}, nil
	case k8scontract.OpRestart:
		req, err := decode[k8scontract.ObjectRequest](body)
		return &quickPlan{ref: req.Ref, action: k8scontract.ActionRestart, allowed: workloadGVRs, summary: func(map[string]any) string { return "滚动重启" }}, err
	case k8scontract.OpSetImage:
		req, err := decode[k8scontract.SetImageRequest](body)
		if err != nil {
			return nil, err
		}
		if req.Container == "" || req.Image == "" {
			return nil, biz.ErrInvalid("container and image are required")
		}
		return &quickPlan{
			ref: req.Ref, action: k8scontract.ActionSetImage, allowed: workloadGVRs,
			// 字段路径在读到对象后确定（containers 或 initContainers），见 planQuick。
			fields: field(req.Image, "container", req.Container),
			summary: func(map[string]any) string {
				return fmt.Sprintf("容器 %s 的镜像改为 %s", req.Container, req.Image)
			},
		}, nil
	case k8scontract.OpRollback:
		req, err := decode[k8scontract.RollbackRequest](body)
		if err != nil {
			return nil, err
		}
		return &quickPlan{
			ref: req.Ref, action: k8scontract.ActionRollback, allowed: workloadGVRs, fields: touched(fmt.Sprintf("revision %d", req.Revision)),
			summary: func(map[string]any) string { return fmt.Sprintf("回滚到版本 %d", req.Revision) },
		}, nil
	case k8scontract.OpPause:
		req, err := decode[k8scontract.PauseRequest](body)
		act, text := k8scontract.ActionResume, "恢复发布"
		if req.Paused {
			act, text = k8scontract.ActionPause, "暂停发布"
		}
		return &quickPlan{
			ref: req.Ref, action: act, allowed: []k8scontract.GVR{k8scontract.GVRDeployment}, fields: touched(req.Paused),
			summary: func(map[string]any) string { return text },
		}, err
	case k8scontract.OpCronSuspend:
		req, err := decode[k8scontract.SuspendRequest](body)
		act, text := k8scontract.ActionUnsuspend, "恢复调度"
		if req.Suspend {
			act, text = k8scontract.ActionSuspend, "暂停调度"
		}
		return &quickPlan{
			ref: req.Ref, action: act, allowed: []k8scontract.GVR{k8scontract.GVRCronJob}, fields: touched(req.Suspend),
			summary: func(map[string]any) string { return text },
		}, err
	case k8scontract.OpCronTrigger:
		req, err := decode[k8scontract.ObjectRequest](body)
		return &quickPlan{
			ref: req.Ref, action: k8scontract.ActionTrigger, allowed: []k8scontract.GVR{k8scontract.GVRCronJob},
			summary: func(map[string]any) string { return "手动触发一次" },
		}, err
	case k8scontract.OpPodEvict:
		req, err := decode[k8scontract.ObjectRequest](body)
		return &quickPlan{ref: podRef(req.Ref.Namespace, req.Ref.Name), action: k8scontract.ActionEvict, summary: func(map[string]any) string { return "驱逐 Pod" }}, err
	case k8scontract.OpPodResize:
		req, err := decode[k8scontract.ResizeRequest](body)
		// 涉及的字段与说明在读到 Pod 后确定（要与现状比对），见 planQuick。
		return &quickPlan{ref: podRef(req.Ref.Namespace, req.Ref.Name), action: k8scontract.ActionResize, summary: func(map[string]any) string { return "调整资源" }}, err
	case k8scontract.OpNodeCordon:
		req, err := decode[k8scontract.CordonRequest](body)
		act, text := k8scontract.ActionUncordon, "恢复调度"
		if req.Unschedulable {
			act, text = k8scontract.ActionCordon, "停止调度"
		}
		return &quickPlan{ref: nodeRef(req.Name), action: act, fields: touched(req.Unschedulable), summary: func(map[string]any) string { return text }}, err
	case k8scontract.StreamDrain:
		req, err := decode[k8scontract.DrainRequest](body)
		return &quickPlan{ref: nodeRef(req.Name), action: k8scontract.ActionDrain, summary: func(map[string]any) string { return "排空节点" }}, err
	}
	return nil, biz.ErrUnsupported("operation %q cannot be submitted for approval", op)
}

func (u *userClients) planQuick(op string, body []byte, out *k8scontract.ChangePlanResponse) error {
	qp, err := u.quickPlanFor(op, body)
	if err != nil {
		return err
	}
	if qp.allowed != nil {
		if err := supports(qp.ref, qp.allowed); err != nil {
			return err
		}
	}
	cur, kind, err := u.current(qp.ref)
	if err != nil {
		return err
	}
	if op == k8scontract.OpSetImage {
		container, listField := qp.fields[0].path[1], ""
		for _, f := range []string{"containers", "initContainers"} {
			for _, c := range nestedList(cur.Object, "spec", "template", "spec", f) {
				if str(asMap(c), "name") == container {
					listField = f
				}
			}
		}
		if listField == "" {
			return biz.ErrInvalid("container %q not found in %s", container, qp.ref.Name)
		}
		qp.fields[0].path = QuickTouched(op, listField, container)[0]
	}
	if op == k8scontract.OpPodResize {
		req, _ := decode[k8scontract.ResizeRequest](body)
		changes, err := planResize(cur.Object, req)
		if err != nil {
			return err
		}
		for _, c := range changes {
			qp.fields = append(qp.fields, quickField{path: c.path(), after: c.after})
		}
		qp.summary = func(map[string]any) string { return resizeSummary(changes) }
	}
	if op == k8scontract.StreamDrain {
		req, _ := decode[k8scontract.DrainRequest](body)
		if _, blocked, err := u.drainPlan(req); err != nil {
			return err
		} else if blocked {
			return biz.NewError(409, k8scontract.ReasonConflict, "some pods block the drain with the current options; see the drain preview")
		}
	}
	d := k8scontract.ChangeItemDisplay{Ref: qp.ref, Kind: kind, Action: qp.action, Fields: []k8scontract.ChangeField{}}
	e := k8scontract.ChangeExecItem{Ref: qp.ref, Kind: kind, Mode: k8scontract.ChangeModeCheck, UID: string(cur.GetUID())}
	for _, f := range qp.fields {
		v, found := biz.LookupField(cur.Object, f.path)
		found = found && v != nil
		c := k8scontract.FieldCheck{Path: f.path, Absent: !found}
		if found {
			c.Value = v
		}
		// 展示用：整段 Pod 模板太长，只给出说明。
		before := biz.FieldText(v, !found, false)
		if op == k8scontract.OpRollback {
			before = "(current pod template)"
		}
		e.Checks = append(e.Checks, c)
		d.Fields = append(d.Fields, k8scontract.ChangeField{Path: biz.FieldPath(f.path), Before: before, After: biz.FieldText(f.after, false, false)})
	}
	out.Display.Action = qp.action
	out.Display.Summary = kind + " " + qp.ref.Name + "：" + qp.summary(cur.Object)
	out.Display.Items = append(out.Display.Items, d)
	out.Exec.Items = append(out.Exec.Items, e)
	return nil
}

// helmRevision Release 当前的版本号；不存在返回 0。
func (u *userClients) helmRevision(namespace, name string) (int, error) {
	if err := checkReleaseRef(namespace, name); err != nil {
		return 0, err
	}
	cfg, err := u.helmConfig(namespace)
	if err != nil {
		return 0, err
	}
	rel, err := action.NewGet(cfg).Run(name)
	if errors.Is(err, driver.ErrReleaseNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, helmError(err)
	}
	return rel.Version, nil
}

func (u *userClients) planHelm(op string, body []byte, out *k8scontract.ChangePlanResponse) error {
	if u.c.m.apps == nil {
		return biz.ErrUnsupported("helm is not enabled on this data plane")
	}
	var ns, name, summary, act string
	var (
		pin     k8scontract.HelmCheck
		display k8scontract.ChangeItemDisplay
	)
	fields := []k8scontract.ChangeField{}
	switch op {
	case k8scontract.OpHelmInstall, k8scontract.OpHelmUpgrade:
		req, err := decode[k8scontract.HelmApplyRequest](body)
		if err != nil {
			return err
		}
		req.Upgrade = op == k8scontract.OpHelmUpgrade
		// 预检：渲染并做服务端 dry-run（校验 Chart、values 与权限），并把 Chart 包与渲染结果固定下来：
		// 执行时两者须与此刻一致，审批人看到的就是将要执行的。
		in, err := u.helmInputs(req, req.Upgrade)
		if err != nil {
			return err
		}
		rel, err := u.helmRender(req, in)
		if err != nil {
			return err
		}
		pin.ChartDigest = in.digest
		// 同样的输入再渲染一次：结果不同说明模板里有随机内容（随机口令等），这类 Chart 无法固定渲染结果。
		if again, rerr := u.helmRender(req, in); rerr == nil && again.Manifest == rel.Manifest {
			pin.ManifestHash = manifestHash(rel.Manifest)
		}
		// 展示：values 与清单（Secret 内容掩码），供审批人查看将要执行的内容。
		if in.current != nil {
			display.BeforeYAML = "# values\n" + valuesYAML(biz.MaskValues(in.current.Config)) + "\n# manifest\n" + maskManifestSecrets(in.current.Manifest)
		}
		display.AfterYAML = "# values\n" + valuesYAML(biz.MaskValues(in.vals)) + "\n# manifest\n" + maskManifestSecrets(rel.Manifest)
		// 落进计划的请求体里，values 用已换回现值的那一份：执行时不再依赖「掩码 = 保持现值」的解释
		// （审批期间 Release 若被改动，版本号比对会先失败）。
		if raw, merr := yaml.Marshal(in.vals); merr == nil {
			req.Values = string(raw)
			if body, merr = json.Marshal(req); merr == nil {
				out.Exec.Body = body
			}
		}
		ns, name = req.Namespace, req.Name
		act, summary = k8scontract.ActionHelmInstall, fmt.Sprintf("安装 %s %s", req.Chart, req.Version)
		if req.Upgrade {
			act, summary = k8scontract.ActionHelmUpgrade, fmt.Sprintf("升级到 %s %s", req.Chart, req.Version)
		}
		fields = append(fields, k8scontract.ChangeField{Path: "chart", After: req.Chart + " " + req.Version})
	case k8scontract.OpHelmRollback:
		req, err := decode[k8scontract.HelmRollbackRequest](body)
		if err != nil {
			return err
		}
		if req.Revision < 1 {
			return biz.ErrInvalid("revision must be positive")
		}
		ns, name, act, summary = req.Namespace, req.Name, k8scontract.ActionHelmRollback, fmt.Sprintf("回滚到版本 %d", req.Revision)
	default:
		req, err := decode[k8scontract.HelmReleaseRequest](body)
		if err != nil {
			return err
		}
		ns, name, act, summary = req.Namespace, req.Name, k8scontract.ActionHelmUninstall, "卸载"
	}
	rev, err := u.helmRevision(ns, name)
	if err != nil {
		return err
	}
	switch {
	case op == k8scontract.OpHelmInstall && rev != 0:
		return biz.NewError(409, k8scontract.ReasonConflict, "release %s already exists", name)
	case op != k8scontract.OpHelmInstall && rev == 0:
		return biz.ErrNotFound("release %s not found", name)
	}
	fields = append(fields, k8scontract.ChangeField{Path: "revision", Before: fmt.Sprint(rev)})
	ref := k8scontract.Ref{Group: k8scontract.HelmReleaseGroup, Version: k8scontract.HelmReleaseVersion, Resource: k8scontract.HelmReleaseResource, Namespace: ns, Name: name}
	pin.Namespace, pin.Name, pin.Revision = ns, name, rev
	out.Exec.Helm = &pin
	out.Display.Action, out.Display.Target, out.Display.Kind = act, ref, k8scontract.HelmReleaseKind
	out.Display.Summary = "Release " + name + "：" + summary
	display.Ref, display.Kind, display.Action, display.Fields = ref, k8scontract.HelmReleaseKind, act, fields
	out.Display.Items = append(out.Display.Items, display)
	return nil
}

// ---------------------------------------------------------------------------
// 执行
// ---------------------------------------------------------------------------

// ExecuteChange implements biz.ChangeExecutor：解密执行计划并执行。
// 写操作以 id（发起人）的身份执行，集群据此重新判定权限；申请命名空间以平台身份执行（审批即授权）。
func (m *Manager) ExecuteChange(ctx context.Context, cl *biz.Cluster, id biz.Identity, kind int32, sealed []byte, started func() error) (k8scontract.ChangeResult, []*biz.AuditEvent, error) {
	var res k8scontract.ChangeResult
	plain, err := m.opener.Open(sealed)
	if err != nil {
		return res, nil, biz.NewError(500, k8scontract.ReasonUnspecified, "the change plan cannot be decrypted: %s", err.Error())
	}
	var exec k8scontract.ChangeExec
	if err := json.Unmarshal([]byte(plain), &exec); err != nil {
		return res, nil, biz.NewError(500, k8scontract.ReasonUnspecified, "the change plan is malformed")
	}
	// 计划与变更单、集群绑定：一份加密的计划被挪到别的变更单或别的集群上时拒绝执行。
	if exec.ChangeID != id.ChangeID || exec.ClusterID != cl.ID {
		return res, nil, biz.NewError(409, k8scontract.ReasonConflict, "the change plan does not belong to this change order or cluster")
	}
	c, err := m.conn(cl.ID)
	if err != nil {
		return res, nil, mapError(err)
	}
	if kind == k8scontract.ChangeKindNamespace {
		// 不记「已开始」：创建是幂等的（见 createNamespace），中断后可以安全地重试。
		audits, err := c.createNamespace(ctx, exec.Namespace, id.ChangeID)
		return res, audits, mapError(err)
	}
	u, err := c.asUser(ctx, id)
	if err != nil {
		return res, nil, mapError(err)
	}
	u.started = started
	// Helm：等到真正的结果（不转后台）；安装 / 升级核对审批时固定的 Chart 与渲染结果。
	u.helmWait = k8scontract.HelmOpTimeout + 30*time.Second
	if exec.Op == k8scontract.OpHelmInstall || exec.Op == k8scontract.OpHelmUpgrade {
		u.helmPin = exec.Helm
	}
	audits, err := u.executeChange(&exec, &res)
	if err != nil && len(audits) == 0 {
		// 在比对 / 预检阶段就失败（字段冲突、发起人已无权限等）：集群没有被改动，但这次尝试要留痕。
		audits = []*biz.AuditEvent{preflightAudit(&exec, mapError(err))}
	}
	return res, audits, mapError(err)
}

// preflightAudit 变更在动手之前就失败时的审计：记在第一个目标对象上。
func preflightAudit(exec *k8scontract.ChangeExec, err error) *biz.AuditEvent {
	a := &biz.AuditEvent{Action: k8scontract.ActionUpdate, Err: err, Patch: map[string]any{"op": exec.Op, "stage": "verify"}}
	if act, ok := opActions[exec.Op]; ok {
		a.Action = act
	}
	switch {
	case len(exec.Items) > 0:
		a.Ref, a.Kind = exec.Items[0].Ref, exec.Items[0].Kind
		if exec.Items[0].Mode == k8scontract.ChangeModeCreate {
			a.Action = k8scontract.ActionCreate
			a.Ref.Name, _, _ = unstructured.NestedString(exec.Items[0].Object, "metadata", "name")
		}
	case exec.Helm != nil:
		a.Kind = k8scontract.HelmReleaseKind
		a.Ref = k8scontract.Ref{
			Group: k8scontract.HelmReleaseGroup, Version: k8scontract.HelmReleaseVersion, Resource: k8scontract.HelmReleaseResource,
			Namespace: exec.Helm.Namespace, Name: exec.Helm.Name,
		}
	}
	return a
}

func conflictError(format string, args ...any) error {
	return biz.NewError(409, k8scontract.ReasonConflict, format, args...)
}

// verifyItem 把一个对象的前置条件与现状比对，返回现状（创建时为 nil）。
func (u *userClients) verifyItem(it *k8scontract.ChangeExecItem) (*unstructured.Unstructured, resourceClient, error) {
	rc, _, err := u.resource(it.Ref)
	if err != nil {
		return nil, nil, err
	}
	who := it.Kind + " " + it.Ref.Name
	if it.Mode == k8scontract.ChangeModeCreate {
		name, _, _ := unstructured.NestedString(it.Object, "metadata", "name")
		if name == "" {
			return nil, rc, nil // generateName：不会重名
		}
		if _, err := rc.Get(u.ctx, name, metav1.GetOptions{}); err == nil {
			return nil, nil, conflictError("%s %s was created by someone else during approval", it.Kind, name)
		} else if !apierrors.IsNotFound(err) {
			return nil, nil, err
		}
		return nil, rc, nil
	}
	live, err := rc.Get(u.ctx, it.Ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil, conflictError("%s was deleted during approval", who)
	}
	if err != nil {
		return nil, nil, err
	}
	if it.UID != "" && string(live.GetUID()) != it.UID {
		return nil, nil, conflictError("%s was deleted and recreated during approval", who)
	}
	if err := biz.VerifyChecks(live.Object, it.Checks); err != nil {
		return nil, nil, conflictError("%s: %s", who, err.Error())
	}
	return live, rc, nil
}

// patched 现状合并补丁后的对象（带现状的 resourceVersion：提交时有人并发修改即 409）。
func patched(live *unstructured.Unstructured, patch map[string]any) *unstructured.Unstructured {
	next := &unstructured.Unstructured{Object: biz.ApplyPatch(runtime.DeepCopyJSON(live.Object), patch)}
	unstructured.RemoveNestedField(next.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(next.Object, "status")
	next.SetResourceVersion(live.GetResourceVersion())
	next.SetUID(live.GetUID())
	return next
}

// writeItem 创建或更新一个对象；dry = 预检。
func (u *userClients) writeItem(it *k8scontract.ChangeExecItem, live *unstructured.Unstructured, rc resourceClient, dry bool) (*unstructured.Unstructured, error) {
	var dr []string
	if dry {
		dr = dryRunAll
	}
	if it.Mode == k8scontract.ChangeModeCreate {
		return rc.Create(u.ctx, &unstructured.Unstructured{Object: runtime.DeepCopyJSON(it.Object)}, metav1.CreateOptions{DryRun: dr})
	}
	return rc.Update(u.ctx, patched(live, it.Patch), metav1.UpdateOptions{DryRun: dr})
}

func (u *userClients) executeChange(exec *k8scontract.ChangeExec, res *k8scontract.ChangeResult) ([]*biz.AuditEvent, error) {
	objects := exec.Op == k8scontract.OpApply || exec.Op == k8scontract.OpManifestApply
	// 1. 全部对象先比对；对象变更再逐个预检——任何一个不过，一个都不执行。
	for i := range exec.Items {
		it := &exec.Items[i]
		live, rc, err := u.verifyItem(it)
		if err != nil {
			return nil, err
		}
		if objects && !it.SkipDryRun {
			if _, err := u.writeItem(it, live, rc, true); err != nil {
				be := biz.AsError(mapError(err))
				return nil, biz.NewError(be.Code, be.Reason, "%s %s: %s", it.Kind, it.Ref.Name, be.Message)
			}
		}
	}
	if h := exec.Helm; h != nil {
		rev, err := u.helmRevision(h.Namespace, h.Name)
		if err != nil {
			return nil, err
		}
		if rev != h.Revision {
			if h.Revision == 0 {
				return nil, conflictError("release %s was installed by someone else during approval", h.Name)
			}
			return nil, conflictError("release %s was changed during approval: it was at revision %d when the change was submitted and is at revision %d now", h.Name, h.Revision, rev)
		}
	}
	if !objects {
		return u.replay(exec)
	}
	// 2. 逐个提交；遇到并发修改（409）从比对重新开始，至多 ChangeMaxAttempts 次。中途失败即停。
	if err := u.markStarted(); err != nil {
		return nil, err
	}
	var audits []*biz.AuditEvent
	for i := range exec.Items {
		it := &exec.Items[i]
		audit := &biz.AuditEvent{Ref: it.Ref, Kind: it.Kind, Action: k8scontract.ActionUpdate}
		if it.Mode == k8scontract.ChangeModeCreate {
			audit.Action = k8scontract.ActionCreate
		}
		audits = append(audits, audit)
		var after *unstructured.Unstructured
		var err error
		for attempt := 1; ; attempt++ {
			var live *unstructured.Unstructured
			var rc resourceClient
			if live, rc, err = u.verifyItem(it); err != nil {
				break
			}
			audit.Before = objMap(live)
			after, err = u.writeItem(it, live, rc, false)
			if err == nil || !apierrors.IsConflict(err) || attempt >= k8scontract.ChangeMaxAttempts {
				break
			}
			res.Attempts++
		}
		if err != nil {
			audit.Err = mapError(err)
			be := biz.AsError(audit.Err)
			return audits, biz.NewError(be.Code, be.Reason, "%s %s: %s", it.Kind, it.Ref.Name, be.Message)
		}
		audit.After = after.Object
		audit.Patch = mergePatch(audit.Before, audit.After)
		audit.Ref.Name = after.GetName()
		res.Applied = append(res.Applied, audit.Ref)
	}
	return audits, nil
}

// markStarted 开始向集群提交之前记下「已开始」（仅执行变更单时有 started）。
func (u *userClients) markStarted() error {
	if u.started == nil {
		return nil
	}
	return u.started()
}

// replay 比对通过后重放原操作（快捷操作、删除、Helm、排空）：走与直接执行相同的代码路径。
// 这些操作多数不可重入（触发任务、驱逐、重启、Helm）：先记「已开始」，此后中断即视为结果未知，不再重跑。
func (u *userClients) replay(exec *k8scontract.ChangeExec) ([]*biz.AuditEvent, error) {
	if exec.Op == k8scontract.StreamDrain {
		req, err := decode[k8scontract.DrainRequest](exec.Body)
		if err != nil {
			return nil, err
		}
		if err := u.markStarted(); err != nil {
			return nil, err
		}
		audits, err := u.drain(req, func(string, string, any) error { return nil })
		if err == nil && len(audits) > 0 && audits[0].Err != nil {
			err = audits[0].Err // 有 Pod 驱逐失败或未完成：变更单不能记为成功
		}
		return audits, err
	}
	if exec.Helm == nil { // Helm 在 helmApply 核对完 Chart 与渲染结果之后才算开始（见 helmAsyncWait 之前的调用点）
		if err := u.markStarted(); err != nil {
			return nil, err
		}
	}
	resp, audits, err := u.dispatch(exec.Op, exec.Body)
	if err != nil {
		return audits, err
	}
	if r, ok := resp.(k8scontract.HelmApplyResponse); ok && r.Pending {
		// 等满了时限仍未结束：不能记为成功。操作仍在后台运行并会自行落审计。
		return audits, biz.NewError(504, k8scontract.ReasonUpstream, "the helm operation did not finish within %s; check the release state", u.helmWait)
	}
	return audits, nil
}

// changeAnnotation 平台写在自己创建的命名空间上的注解：创建它的变更单。重试时据此认出「这是我上次建的」。
const changeAnnotation = "reborn.io/change-id"

// reservedNamespace 不可申请的命名空间名（与 core 的校验一致；执行器不信任落库的内容）。
func reservedNamespace(name string) bool {
	return strings.HasPrefix(name, "kube-") || name == "default" || name == "reborn-system"
}

// createNamespace 申请命名空间：以平台身份创建 Namespace 与可选的 ResourceQuota / LimitRange。
// 对象完全由结构化字段生成（见 k8scontract.NamespaceRequest），申请人无法借此创建其他资源。
// 幂等：命名空间若已存在且带着本变更单的注解（上一次执行建到一半），继续补齐其余对象；
// 否则视为被别人占用。
func (c *conn) createNamespace(ctx context.Context, req *k8scontract.NamespaceRequest, changeID string) ([]*biz.AuditEvent, error) {
	if req == nil {
		return nil, biz.ErrInvalid("the change has no namespace request")
	}
	if reservedNamespace(req.Name) {
		return nil, biz.ErrInvalid("namespace name %q is reserved", req.Name)
	}
	if len(req.Labels) > 0 {
		return nil, biz.ErrInvalid("labels cannot be requested for a namespace")
	}
	objs, err := NamespaceObjects(req)
	if err != nil {
		return nil, err
	}
	nsRef := k8scontract.Ref{Version: "v1", Resource: "namespaces", Name: req.Name}
	resume := false
	if cur, err := c.platDyn.Resource(toSchema(k8scontract.GVRNamespace)).Get(ctx, req.Name, metav1.GetOptions{}); err == nil {
		if changeID == "" || cur.GetAnnotations()[changeAnnotation] != changeID || cur.GetDeletionTimestamp() != nil {
			err := conflictError("namespace %s already exists", req.Name)
			return []*biz.AuditEvent{{Ref: nsRef, Kind: "Namespace", Action: k8scontract.ActionCreate, Err: err}}, err
		}
		resume = true
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	var audits []*biz.AuditEvent
	for _, o := range objs {
		gvr := k8scontract.GVRNamespace
		switch o.GetKind() {
		case "ResourceQuota":
			gvr = k8scontract.GVRQuota
		case "LimitRange":
			gvr = k8scontract.GVRLimitRange
		}
		if gvr == k8scontract.GVRNamespace {
			if resume {
				continue
			}
			if changeID != "" {
				o.SetAnnotations(map[string]string{changeAnnotation: changeID})
			}
		}
		audit := &biz.AuditEvent{Ref: k8scontract.Ref{Version: "v1", Resource: gvr.Resource, Namespace: o.GetNamespace(), Name: o.GetName()}, Kind: o.GetKind(), Action: k8scontract.ActionCreate}
		ri := c.platDyn.Resource(toSchema(gvr))
		var after *unstructured.Unstructured
		if o.GetNamespace() != "" {
			after, err = ri.Namespace(o.GetNamespace()).Create(ctx, o, metav1.CreateOptions{})
		} else {
			after, err = ri.Create(ctx, o, metav1.CreateOptions{})
		}
		if resume && apierrors.IsAlreadyExists(err) {
			continue // 上一次执行已建好
		}
		audits = append(audits, audit)
		if err != nil {
			audit.Err = mapError(err)
			return audits, err
		}
		audit.After = after.Object
	}
	return audits, nil
}

// NamespaceObjects 由申请内容生成要创建的对象：恒为 Namespace，按需加 ResourceQuota 与 LimitRange。
// 配额项与数量在此再校验一次（core 已校验过；执行器不信任落库的内容）。
func NamespaceObjects(req *k8scontract.NamespaceRequest) ([]*unstructured.Unstructured, error) {
	if !dns1123Label.MatchString(req.Name) || len(req.Name) > 63 {
		return nil, biz.ErrInvalid("invalid namespace name %q", req.Name)
	}
	quantities := func(in map[string]string, allowed map[string]struct{}, what string) (map[string]any, error) {
		out := map[string]any{}
		for k, v := range in {
			if _, ok := allowed[k]; !ok {
				return nil, biz.ErrInvalid("%s %q is not allowed in a namespace request", what, k)
			}
			q, err := resource.ParseQuantity(v)
			if err != nil || q.Sign() < 0 {
				return nil, biz.ErrInvalid("%s %s: %q is not a valid quantity", what, k, v)
			}
			out[k] = q.String()
		}
		return out, nil
	}
	labels := map[string]any{k8scontract.ManagedByLabel: k8scontract.ManagedByValue}
	for k, v := range req.Labels {
		if strings.Contains(k, "kubernetes.io/") || strings.Contains(k, "k8s.io/") || len(k) > 253 || len(v) > 63 {
			return nil, biz.ErrInvalid("label %q is not allowed in a namespace request", k)
		}
		labels[k] = v
	}
	meta := func(name, ns string) map[string]any {
		m := map[string]any{"name": name, "labels": map[string]any{k8scontract.ManagedByLabel: k8scontract.ManagedByValue}}
		if ns != "" {
			m["namespace"] = ns
		}
		return m
	}
	nsMeta := meta(req.Name, "")
	nsMeta["labels"] = labels
	objs := []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": nsMeta}}}
	hard, err := quantities(req.Quota, k8scontract.NamespaceQuotaKeys, "quota item")
	if err != nil {
		return nil, err
	}
	if len(hard) > 0 {
		objs = append(objs, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ResourceQuota", "metadata": meta("default", req.Name), "spec": map[string]any{"hard": hard},
		}})
	}
	def, err := quantities(req.LimitDefault, k8scontract.NamespaceLimitKeys, "default limit")
	if err != nil {
		return nil, err
	}
	defReq, err := quantities(req.LimitDefaultRequest, k8scontract.NamespaceLimitKeys, "default request")
	if err != nil {
		return nil, err
	}
	if len(def)+len(defReq) > 0 {
		limit := map[string]any{"type": "Container"}
		if len(def) > 0 {
			limit["default"] = def
		}
		if len(defReq) > 0 {
			limit["defaultRequest"] = defReq
		}
		objs = append(objs, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "LimitRange", "metadata": meta("default", req.Name), "spec": map[string]any{"limits": []any{limit}},
		}})
	}
	return objs, nil
}

// dns1123Label 命名空间名（DNS 标签）。
var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// opActions 操作名 → 审计动作（比对阶段失败时用；暂停 / 恢复这类成对的动作取其一）。
var opActions = map[string]string{
	k8scontract.OpDelete: k8scontract.ActionDelete, k8scontract.OpScale: k8scontract.ActionScale,
	k8scontract.OpRestart: k8scontract.ActionRestart, k8scontract.OpSetImage: k8scontract.ActionSetImage,
	k8scontract.OpRollback: k8scontract.ActionRollback, k8scontract.OpPause: k8scontract.ActionPause,
	k8scontract.OpCronTrigger: k8scontract.ActionTrigger, k8scontract.OpCronSuspend: k8scontract.ActionSuspend,
	k8scontract.OpPodEvict: k8scontract.ActionEvict, k8scontract.OpNodeCordon: k8scontract.ActionCordon,
	k8scontract.OpPodResize:   k8scontract.ActionResize,
	k8scontract.StreamDrain:   k8scontract.ActionDrain,
	k8scontract.OpHelmInstall: k8scontract.ActionHelmInstall, k8scontract.OpHelmUpgrade: k8scontract.ActionHelmUpgrade,
	k8scontract.OpHelmRollback: k8scontract.ActionHelmRollback, k8scontract.OpHelmUninstall: k8scontract.ActionHelmUninstall,
}
