package monitorpipeline

import (
	"slices"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// CI 一个 CMDB 资源在监控侧的只读视图（monitor_ci_index 的一行，仅有效的资源）。
// Attrs 是可用于匹配的属性（属性名 → 取值列表）；UpstreamIDs 是它的上游资源（上游故障会影响它）。
type CI struct {
	ID          string
	ModelCode   string
	Name        string
	Env         string
	Status      string
	Owner       string
	Tags        []string
	Attrs       map[string][]string
	UpstreamIDs []string
}

// Maintenance 报告资源是否处于维护态（关联到它的告警只屏蔽通知）。nil 为 false。
func (c *CI) Maintenance() bool { return c != nil && c.Status == monitorcontract.CIStatusMaintenance }

// CIMatchStatus 一次关联的结论（模拟触发逐阶段展示；引擎只看是否关联到）。
type CIMatchStatus int8

// 关联结论。
const (
	CIMatched     CIMatchStatus = iota + 1 // 关联到唯一一个资源
	CIDisabled                             // 规则关闭了关联
	CINoLabel                              // 事件没有用于关联的标签
	CINotFound                             // 标签有值，但没有对应的资源
	CIAmbiguous                            // 对应到多个资源（可用模型限定来消歧）：不关联，宁缺毋错
	CIUnavailable                          // 资源索引尚未加载
)

// String 关联结论的英文短名（日志与模拟触发的阶段说明用）。
func (s CIMatchStatus) String() string {
	switch s {
	case CIMatched:
		return "matched"
	case CIDisabled:
		return "disabled"
	case CINoLabel:
		return "no_label"
	case CINotFound:
		return "not_found"
	case CIAmbiguous:
		return "ambiguous"
	case CIUnavailable:
		return "unavailable"
	}
	return "unknown"
}

// CIIndex 资源索引：按名称与可匹配属性查资源。创建后只读，可并发使用；nil 表示索引尚未加载。
type CIIndex struct {
	cis    []CI
	byID   map[string]int32
	byName map[string][]int32
	byAttr map[string]map[string][]int32
}

// NewCIIndex 建立索引并接管 cis（调用方之后不得再修改它）。名称与属性值按 CIKey 归一后入索引；
// ID 重复的只保留先出现的。
func NewCIIndex(cis []CI) *CIIndex {
	x := &CIIndex{
		cis: cis, byID: make(map[string]int32, len(cis)), byName: make(map[string][]int32, len(cis)),
		byAttr: map[string]map[string][]int32{},
	}
	for i := range cis {
		c := &cis[i]
		if _, dup := x.byID[c.ID]; dup || c.ID == "" {
			continue
		}
		at := int32(i) // #nosec G115 -- 资源数远小于 int32 上限
		x.byID[c.ID] = at
		if key := CIKey(c.Name); key != "" {
			x.byName[key] = append(x.byName[key], at)
		}
		for attr, values := range c.Attrs {
			m := x.byAttr[attr]
			if m == nil {
				m = map[string][]int32{}
				x.byAttr[attr] = m
			}
			for _, v := range values {
				key := CIKey(v)
				if key == "" || slices.Contains(m[key], at) {
					continue
				}
				m[key] = append(m[key], at)
			}
		}
	}
	return x
}

// Len 索引里的资源数。
func (x *CIIndex) Len() int {
	if x == nil {
		return 0
	}
	return len(x.byID)
}

// Get 按 ID 取资源；不存在返回 nil。返回值只读。
func (x *CIIndex) Get(id string) *CI {
	if x == nil {
		return nil
	}
	if at, ok := x.byID[id]; ok {
		return &x.cis[at]
	}
	return nil
}

// CIKey 把标签值 / 资源名称 / 属性值归一成索引键：去首尾空白、转小写，形如 host:port 的去掉端口
// （"10.0.0.1:9100" → "10.0.0.1"，"[fe80::1]:9100" → "fe80::1"，"web-01.example.com:9100" →
// "web-01.example.com"）；不带方括号的 IPv6 地址（多个冒号）原样保留。
func CIKey(value string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return ""
	}
	if v[0] == '[' {
		if end := strings.IndexByte(v, ']'); end > 0 && (end == len(v)-1 || isPort(v[end+1:])) {
			v = v[1:end]
		}
	} else if i := strings.IndexByte(v, ':'); i > 0 && strings.IndexByte(v[i+1:], ':') < 0 && isDigits(v[i+1:]) {
		v = v[:i]
	}
	return strings.ToLower(v)
}

// isPort 报告 s 是否形如 ":<数字>"。
func isPort(s string) bool { return len(s) >= 2 && s[0] == ':' && isDigits(s[1:]) }

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// defaultCIMatches monitorcontract.DefaultCIMatches 的包内只读副本（热路径不重复分配）。
var defaultCIMatches = monitorcontract.DefaultCIMatches()

// Lookup 按规则的关联配置为事件找资源。m.Label 为空时依次尝试全局缺省（ident → 名称，instance → ip
// 属性），第一项找到即止；m.ModelCodes 对缺省匹配同样生效。对应到多个资源时不关联（CIAmbiguous）。
// 返回的资源只读。
func (x *CIIndex) Lookup(m monitorcontract.CIMatch, labels map[string]string) (*CI, CIMatchStatus) {
	if m.Disabled {
		return nil, CIDisabled
	}
	if x == nil {
		return nil, CIUnavailable
	}
	if m.Label != "" {
		return x.lookupOne(m.Label, m.By, m.Attr, m.ModelCodes, labels)
	}
	status := CINoLabel
	for i := range defaultCIMatches {
		d := &defaultCIMatches[i]
		ci, st := x.lookupOne(d.Label, d.By, d.Attr, m.ModelCodes, labels)
		if st == CIMatched || st == CIAmbiguous {
			return ci, st
		}
		if st == CINotFound {
			status = CINotFound
		}
	}
	return nil, status
}

func (x *CIIndex) lookupOne(label, by, attr string, models []string, labels map[string]string) (*CI, CIMatchStatus) {
	key := CIKey(labels[label])
	if key == "" {
		return nil, CINoLabel
	}
	var candidates []int32
	if by == monitorcontract.CIMatchByName {
		candidates = x.byName[key]
	} else {
		if attr == "" {
			attr = monitorcontract.DefaultCIMatchAttr
		}
		candidates = x.byAttr[attr][key]
	}
	var found *CI
	for _, at := range candidates {
		c := &x.cis[at]
		if len(models) > 0 && !slices.Contains(models, c.ModelCode) {
			continue
		}
		if found != nil {
			return nil, CIAmbiguous
		}
		found = c
	}
	if found == nil {
		return nil, CINotFound
	}
	return found, CIMatched
}

// 富化标签名（monitorcontract.CILabelPrefix 开头）。
const (
	labelCIEnv   = monitorcontract.CILabelPrefix + "env"
	labelCIOwner = monitorcontract.CILabelPrefix + "owner"
	labelCIModel = monitorcontract.CILabelPrefix + "model"
	labelCITag   = monitorcontract.CILabelPrefix + "tag_"
)

// Enrich 把资源的环境、负责人、模型，以及 tags 中资源确实带有的标签，就地写入事件标签：
// ci_env / ci_owner / ci_model = 取值，TagLabel(标签) = "true"。
// 这几个标签名由富化独占：资源上没有对应内容时，事件上同名的标签被删除（序列自带的同名标签不能冒充
// CMDB 的结论）。ci 为 nil 时不动标签。富化在指纹计算之后调用：富化标签不参与指纹，资源属性变化
// 不会让告警重建。
func Enrich(labels map[string]string, ci *CI, tags []string) {
	if ci == nil {
		return
	}
	setOrDelete(labels, labelCIEnv, ci.Env)
	setOrDelete(labels, labelCIOwner, ci.Owner)
	setOrDelete(labels, labelCIModel, ci.ModelCode)
	for _, tag := range tags {
		name := TagLabel(tag)
		if !monitorcontract.ValidLabelName(name) {
			continue // Compile 已拒绝这样的配置；直接调用时不写出不合法的标签名
		}
		if slices.Contains(ci.Tags, tag) {
			labels[name] = "true"
		} else {
			delete(labels, name)
		}
	}
}

func setOrDelete(labels map[string]string, name, value string) {
	if value = clipLabelValue(value); value == "" {
		delete(labels, name)
		return
	}
	labels[name] = value
}

// clipLabelValue 把取值收进标签值的上限（monitorcontract.MaxLabelValueLen 个字符）并去掉 NUL。
func clipLabelValue(v string) string {
	v = clipValue(v)
	n := 0
	for i := range v {
		if n == monitorcontract.MaxLabelValueLen {
			return v[:i]
		}
		n++
	}
	return v
}

// TagLabel 资源标签 tag 对应的富化标签名：ci_tag_ 加上标签本身；标签里不能用于标签名的字符（ASCII 字母、
// 数字、下划线以外的）逐个换成 _x<十六进制码点>_，不同的标签不会落到同一个名称上——
// "core" → ci_tag_core，"dmz zone" → ci_tag_dmz_x20_zone，"核心" → ci_tag__x6838__x5fc3_。
// 转义会变长：结果超过标签名长度上限（monitorcontract.MaxLabelNameLen）的资源标签不能用于富化（Compile 拒绝）。
func TagLabel(tag string) string {
	b := make([]byte, 0, len(labelCITag)+len(tag)+8)
	b = append(b, labelCITag...)
	for _, r := range tag {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b = append(b, byte(r)) // #nosec G115 -- 分支内 r 为 ASCII
			continue
		}
		b = append(b, "_x"...)
		b = strconv.AppendInt(b, int64(r), 16)
		b = append(b, '_')
	}
	return string(b)
}

// InScope 报告事件是否在规则限定的资源范围内。scope 未启用恒为 true；启用后事件必须关联到资源，
// 且资源的模型在 ModelCodes 内、环境在 Envs 内、带有 Tags 中的任意一个（各项为空 = 不限）。
func InScope(scope monitorcontract.ResourceScope, ci *CI) bool {
	if !scope.Enabled {
		return true
	}
	if ci == nil {
		return false
	}
	if len(scope.ModelCodes) > 0 && !slices.Contains(scope.ModelCodes, ci.ModelCode) {
		return false
	}
	if len(scope.Envs) > 0 && !slices.Contains(scope.Envs, ci.Env) {
		return false
	}
	if len(scope.Tags) == 0 {
		return true
	}
	for _, tag := range scope.Tags {
		if slices.Contains(ci.Tags, tag) {
			return true
		}
	}
	return false
}
