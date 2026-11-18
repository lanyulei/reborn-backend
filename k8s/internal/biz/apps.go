package biz

import (
	"context"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// GlobalExecutor 执行与集群无关的操作（Helm 仓库测试、Chart 目录与详情；data/kube 实现）。
type GlobalExecutor interface {
	ExecuteGlobal(ctx context.Context, id Identity, op string, body []byte) (any, error)
}

// AuditSink 事后补记审计：异步完成的写操作（Helm）在结束时经它落库。
type AuditSink func(ctx context.Context, c *Cluster, id Identity, events []*AuditEvent)

// AuditSinkSetter Executor 的可选能力：接收事后审计的落库入口。
type AuditSinkSetter interface {
	SetAuditSink(sink AuditSink)
}

// CallGlobal 执行一个与集群无关的操作：不判定集群归属，任一实例都可执行。
func (o *Ops) CallGlobal(ctx context.Context, id Identity, op string, body []byte) (any, error) {
	g, ok := o.exec.(GlobalExecutor)
	if !ok {
		return nil, ErrUnsupported("operation %s is not supported by this data plane", op)
	}
	return g.ExecuteGlobal(ctx, id, op, body)
}

// sensitiveKeyParts values 脱敏的键名片段（大小写不敏感，子串匹配）；口径同 core 的助手工具与
// kit/httpx 的日志脱敏，另加 key / credential / passwd：宁可误打码不可漏打码。
var sensitiveKeyParts = []string{"password", "passwd", "secret", "token", "authorization", "api_key", "apikey", "credential", "privatekey", "private_key", "accesskey", "access_key"}

func sensitiveKey(k string) bool {
	lk := strings.ToLower(k)
	for _, p := range sensitiveKeyParts {
		if strings.Contains(lk, p) {
			return true
		}
	}
	return false
}

// MaskValues 返回 Helm values 的脱敏深拷贝（不修改入参）：键名含敏感片段的值整体替换为掩码。
func MaskValues(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	return maskAny(v).(map[string]any) //nolint:forcetypeassert // map 入 map 出
}

func maskAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		// 形如 {name: DB_PASSWORD, value: xxx} 的条目（环境变量列表）：敏感的是 name，要掩的是 value。
		named := false
		if n, ok := t["name"].(string); ok && sensitiveKey(n) {
			named = true
		}
		for k, val := range t {
			if (sensitiveKey(k) || named && k == "value") && val != nil && val != "" {
				out[k] = k8scontract.MaskedValue
				continue
			}
			out[k] = maskAny(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = maskAny(val)
		}
		return out
	}
	return v
}

// RestoreMasked 把 values 里等于掩码的标量换回 current（当前 Release 的 values）同一路径上的值，原地修改 vals。
// 界面上看到的已保存 values 是掩码过的；用户原样提交回来时，掩码表示「保持现值」。current 里没有该路径
// （新装、或用户把掩码填到了新字段上）时报错并指出路径——掩码绝不能作为真实的值落进 Release。
// 列表元素若带 name，则按 name 对应（环境变量列表增删、调序后仍对得上），否则按下标。
func RestoreMasked(vals, current map[string]any) error {
	_, err := restoreAny(vals, current, current != nil, "")
	return err
}

func restoreAny(v, cur any, has bool, path string) (any, error) {
	switch t := v.(type) {
	case string:
		if t != k8scontract.MaskedValue {
			return v, nil
		}
		if !has || cur == nil || cur == k8scontract.MaskedValue {
			return nil, errInvalid("values: %q is the mask placeholder but the current release has no value at that path; enter the real value", strings.TrimPrefix(path, "."))
		}
		return cur, nil
	case map[string]any:
		cm, _ := cur.(map[string]any)
		for k, val := range t {
			cv, ok := cm[k]
			nv, err := restoreAny(val, cv, has && ok, path+"."+k)
			if err != nil {
				return nil, err
			}
			t[k] = nv
		}
		return t, nil
	case []any:
		cl, _ := cur.([]any)
		for i, val := range t {
			var (
				cv any
				ok bool
			)
			seg := "[" + itoa(i) + "]"
			if m, isMap := val.(map[string]any); isMap {
				if name, named := m["name"].(string); named {
					seg = "[name=" + name + "]"
					for _, c := range cl {
						if c2, isMap2 := c.(map[string]any); isMap2 && c2["name"] == name {
							cv, ok = c, true
							break
						}
					}
				} else if i < len(cl) {
					cv, ok = cl[i], true
				}
			} else if i < len(cl) {
				cv, ok = cl[i], true
			}
			nv, err := restoreAny(val, cv, has && ok, path+seg)
			if err != nil {
				return nil, err
			}
			t[i] = nv
		}
		return t, nil
	}
	return v, nil
}

func itoa(i int) string { return strconv.Itoa(i) }
