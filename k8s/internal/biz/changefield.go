package biz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// 本文件是变更审批的字段级比对：纯函数，不访问集群。
//
// 提交时：DiffFields(提交时的对象, 期望的对象) 得到一份 JSON Merge Patch 与「涉及的字段在提交时的值」。
// 执行时：VerifyChecks(现状, 那些值) 全部一致，才把补丁 ApplyPatch 到现状上更新。
// 审批期间别人改动了其余字段（或控制器更新了状态、自动扩缩容调整了副本数）不影响执行；涉及的字段
// 被改动则失败并指出是哪个字段。列表作为一个整体：合并补丁对列表是整体替换，无法只改其中一项。

// ChangeBase 返回对象用于比对的副本：去掉状态与服务端每次都会变的元数据。
func ChangeBase(obj map[string]any) map[string]any {
	if obj == nil {
		return nil
	}
	out := maps.Clone(obj)
	delete(out, "status")
	if meta, ok := out["metadata"].(map[string]any); ok {
		meta = maps.Clone(meta)
		for _, k := range []string{"managedFields", "resourceVersion", "uid", "generation", "creationTimestamp", "selfLink", "deletionTimestamp", "deletionGracePeriodSeconds"} {
			delete(meta, k)
		}
		if ann, ok := meta["annotations"].(map[string]any); ok {
			if _, has := ann[LastAppliedAnnotation]; has {
				ann = maps.Clone(ann)
				delete(ann, LastAppliedAnnotation)
				if len(ann) == 0 {
					delete(meta, "annotations")
				} else {
					meta["annotations"] = ann
				}
			}
		}
		out["metadata"] = meta
	}
	return out
}

// jsonEqual 按 JSON 编码比较两个值（int64 与 float64、nil 切片与空切片的差异不算不同）。
func jsonEqual(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

// DiffFields 计算 base → desired 的 JSON Merge Patch，以及补丁涉及的每个字段在 base 中的值。
// 两个对象都是 map 的位置逐键递归；其余（标量、列表、类型不同）整体替换。没有差异时 patch 为空。
func DiffFields(base, desired map[string]any) (patch map[string]any, checks []k8scontract.FieldCheck) {
	patch = map[string]any{}
	diffMap(base, desired, nil, patch, &checks)
	sort.SliceStable(checks, func(i, j int) bool { return FieldPath(checks[i].Path) < FieldPath(checks[j].Path) })
	return patch, checks
}

func diffMap(base, desired map[string]any, path []string, patch map[string]any, checks *[]k8scontract.FieldCheck) {
	for k, dv := range desired {
		p := append(append([]string(nil), path...), k)
		bv, ok := base[k]
		if !ok {
			if dv == nil {
				continue
			}
			patch[k] = dv
			*checks = append(*checks, k8scontract.FieldCheck{Path: p, Absent: true})
			continue
		}
		bm, bok := bv.(map[string]any)
		dm, dok := dv.(map[string]any)
		if bok && dok {
			sub := map[string]any{}
			diffMap(bm, dm, p, sub, checks)
			if len(sub) > 0 {
				patch[k] = sub
			}
			continue
		}
		if !jsonEqual(bv, dv) {
			patch[k] = dv
			*checks = append(*checks, k8scontract.FieldCheck{Path: p, Value: bv})
		}
	}
	for k, bv := range base {
		if _, ok := desired[k]; ok {
			continue
		}
		patch[k] = nil
		*checks = append(*checks, k8scontract.FieldCheck{Path: append(append([]string(nil), path...), k), Value: bv})
	}
}

// ApplyPatch 把 JSON Merge Patch 合并到 obj 的副本上并返回（RFC 7396：null 删除键，map 递归，其余替换）。
func ApplyPatch(obj, patch map[string]any) map[string]any {
	out := maps.Clone(obj)
	if out == nil {
		out = map[string]any{}
	}
	for k, pv := range patch {
		if pv == nil {
			delete(out, k)
			continue
		}
		pm, pok := pv.(map[string]any)
		om, ook := out[k].(map[string]any)
		switch {
		case pok && ook:
			out[k] = ApplyPatch(om, pm)
		case pok:
			out[k] = ApplyPatch(nil, pm)
		default:
			out[k] = pv
		}
	}
	return out
}

// LookupField 按路径取值。形如 "name=web" 的段在列表里按 name 选元素。
func LookupField(obj map[string]any, path []string) (any, bool) {
	var cur any = obj
	for _, seg := range path {
		switch t := cur.(type) {
		case map[string]any:
			v, ok := t[seg]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			name, ok := strings.CutPrefix(seg, "name=")
			if !ok {
				return nil, false
			}
			found := false
			for _, item := range t {
				if m, isMap := item.(map[string]any); isMap && m["name"] == name {
					cur, found = m, true
					break
				}
			}
			if !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return cur, true
}

// FieldPath 路径的展示形式：spec.template.spec.containers[name=web].image；含点的键用方括号。
func FieldPath(path []string) string {
	var b strings.Builder
	for i, seg := range path {
		switch {
		case strings.HasPrefix(seg, "name="):
			b.WriteString("[" + seg + "]")
		case strings.ContainsAny(seg, "./"):
			b.WriteString(`["` + seg + `"]`)
		default:
			if i > 0 {
				b.WriteByte('.')
			}
			b.WriteString(seg)
		}
	}
	return b.String()
}

// FieldText 字段值的展示文本（JSON，过长截断）；secret 为 true 时一律掩码。
func FieldText(v any, absent, secret bool) string {
	if absent {
		return ""
	}
	if secret {
		return k8scontract.MaskedValue
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return truncate(string(raw), 300)
}

// SecretPath 报告路径是否指向 Secret 的内容（展示与错误信息里必须掩码）。
func SecretPath(obj map[string]any, path []string) bool {
	return IsSecret(obj) && len(path) > 0 && (path[0] == "data" || path[0] == "stringData")
}

// FieldConflict 涉及的字段在审批期间被改动。
type FieldConflict struct {
	Path string
	Was  string
	Now  string
}

// Error implements error.
func (c *FieldConflict) Error() string {
	was, now := c.Was, c.Now
	if was == "" {
		was = "(not set)"
	}
	if now == "" {
		now = "(not set)"
	}
	return fmt.Sprintf("field %s was changed during approval: it was %s when the change was submitted and is %s now", c.Path, was, now)
}

// VerifyChecks 逐个比对涉及的字段：现状与提交时一致返回 nil，否则返回第一个 *FieldConflict。
func VerifyChecks(live map[string]any, checks []k8scontract.FieldCheck) error {
	for _, c := range checks {
		v, found := LookupField(live, c.Path)
		if found && v == nil {
			found = false
		}
		absent := c.Absent || c.Value == nil
		if absent && !found || (!absent && found && jsonEqual(v, c.Value)) {
			continue
		}
		secret := SecretPath(live, c.Path)
		return &FieldConflict{Path: FieldPath(c.Path), Was: FieldText(c.Value, absent, secret), Now: FieldText(v, !found, secret)}
	}
	return nil
}
