package biz

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 文本类参数取值的长度上限（字符）：参数会固化进执行记录并写进变量文件，不接受超长文本。
const (
	maxStringParamLen int32 = 4096
	maxTextParamLen   int32 = 65536
)

// paramFields 把执行参数定义转成 formschema 字段（取值校验与归一化复用同一套实现）。
func paramFields(params []jc.Param) []formschema.Field {
	fields := make([]formschema.Field, 0, len(params))
	for _, p := range params {
		f := formschema.Field{Code: p.Code, Required: p.Required}
		switch p.Type {
		case jc.ParamText:
			f.Type, f.Rule.MaxLen = formschema.TypeText, new(maxTextParamLen)
		case jc.ParamInt:
			f.Type = formschema.TypeInt
		case jc.ParamFloat:
			f.Type = formschema.TypeFloat
		case jc.ParamBool:
			f.Type = formschema.TypeBool
		case jc.ParamEnum:
			f.Type, f.EnumValues = formschema.TypeEnum, p.Options
		default:
			f.Type, f.Rule.MaxLen = formschema.TypeString, new(maxStringParamLen)
		}
		fields = append(fields, f)
	}
	return fields
}

// parseDefault 把缺省值的文本形式解析为该类型的原始值。
func parseDefault(p jc.Param) (any, error) {
	switch p.Type {
	case jc.ParamInt:
		n, err := strconv.ParseInt(p.Default, 10, 64)
		if err != nil {
			return nil, errors.New("is not an integer")
		}
		return float64(n), nil
	case jc.ParamFloat:
		n, err := strconv.ParseFloat(p.Default, 64)
		if err != nil {
			return nil, errors.New("is not a number")
		}
		return n, nil
	case jc.ParamBool:
		switch p.Default {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, errors.New("must be true or false")
	}
	return p.Default, nil
}

// validateParams 校验执行参数定义：变量名合法且不重复、类型受支持、枚举有可选值、缺省值能按类型解析。
func validateParams(params []jc.Param) error {
	if len(params) > jc.MaxParams {
		return fmt.Errorf("at most %d params", jc.MaxParams)
	}
	seen := make(map[string]struct{}, len(params))
	for i := range params {
		p := &params[i]
		if !jc.ValidVarName(p.Code) {
			return fmt.Errorf("param %q: not a valid variable name (lowercase letters, digits and underscores; must not start with ansible_ or cmdb_, must not be an Ansible reserved name)", p.Code)
		}
		if _, dup := seen[p.Code]; dup {
			return fmt.Errorf("param %q is defined more than once", p.Code)
		}
		seen[p.Code] = struct{}{}
		if p.Name == "" {
			return fmt.Errorf("param %q: name must not be blank", p.Code)
		}
		if !jc.ValidParamType(p.Type) {
			return fmt.Errorf("param %q: unknown type", p.Code)
		}
		if p.Type == jc.ParamEnum {
			if len(p.Options) == 0 {
				return fmt.Errorf("param %q: an enum param needs at least one option", p.Code)
			}
		} else {
			p.Options = nil
		}
		if p.Default == "" {
			continue
		}
		raw, err := parseDefault(*p)
		if err != nil {
			return fmt.Errorf("param %q: default value %s", p.Code, err.Error())
		}
		if _, err := formschema.Validate(paramFields([]jc.Param{*p}), map[string]any{p.Code: raw}, true); err != nil {
			return fmt.Errorf("param %q: default value is not acceptable: %s", p.Code, err.Error())
		}
	}
	return nil
}

// resolveParams 按参数定义校验一次执行的取值并补缺省值，返回归一化后的 extra vars。
// 未定义的键一律拒绝：用户不能借参数覆盖连接变量或注入任意变量。
func resolveParams(params []jc.Param, values map[string]any) (map[string]any, error) {
	if hasNUL(values) {
		return nil, detail(ErrParamsInvalid, "param values must not contain NUL characters")
	}
	merged := make(map[string]any, len(params))
	for k, v := range values {
		if v != nil {
			merged[k] = v
		}
	}
	for _, p := range params {
		if _, ok := merged[p.Code]; ok {
			continue
		}
		if p.Default == "" {
			// 开关不填就是「关」：Playbook 里总能拿到一个确定的布尔值，而不是未定义变量。
			if p.Type == jc.ParamBool {
				merged[p.Code] = false
			}
			continue
		}
		raw, err := parseDefault(p)
		if err != nil {
			return nil, detail(ErrParamsInvalid, "param %q: default value %s", p.Code, err.Error())
		}
		merged[p.Code] = raw
	}
	out, err := formschema.Validate(paramFields(params), merged, false)
	if err != nil {
		return nil, detail(ErrParamsInvalid, "%s", err.Error())
	}
	return out, nil
}

// checkPartialParams 校验一份不要求齐全的参数取值（定时作业以外的固定参数：触发器）：
// 给出的键须是模板定义的参数、取值合乎类型；返回归一化后的取值。
func checkPartialParams(params []jc.Param, values map[string]any) (map[string]any, error) {
	if hasNUL(values) {
		return nil, detail(ErrParamsInvalid, "param values must not contain NUL characters")
	}
	given := make(map[string]any, len(values))
	for k, v := range values {
		if v != nil {
			given[k] = v
		}
	}
	out, err := formschema.Validate(paramFields(params), given, true)
	if err != nil {
		return nil, detail(ErrParamsInvalid, "%s", err.Error())
	}
	return out, nil
}

// hasNUL 报告取值里是否有带 NUL 字符的字符串：PostgreSQL 的 jsonb 不接受它（落库时才报错会变成 500）。
func hasNUL(v any) bool {
	switch x := v.(type) {
	case string:
		return strings.ContainsRune(x, 0)
	case []any:
		for _, e := range x {
			if hasNUL(e) {
				return true
			}
		}
	case map[string]any:
		for k, e := range x {
			if strings.ContainsRune(k, 0) || hasNUL(e) {
				return true
			}
		}
	}
	return false
}
