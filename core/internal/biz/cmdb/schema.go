package biz

import (
	"errors"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"
)

// compilePattern 字段正则编译（带缓存）；字段定义校验与取值校验共用同一实现。
var compilePattern = formschema.CompilePattern

// schemaFields 把 CMDB 字段定义转换为通用字段校验定义（只取参与取值校验的属性；
// Owner / Indexed / Sensitive 等域专有属性不参与）。单次分配，逐字段零额外分配。
func schemaFields(fields []*Field) []formschema.Field {
	out := make([]formschema.Field, len(fields))
	for i, f := range fields {
		out[i] = formschema.Field{
			Code:       f.Code,
			Type:       f.Type,
			Required:   f.Required,
			EnumValues: f.EnumValues,
			Rule:       formschema.Rule(f.Rule),
			Deprecated: f.Deprecated,
		}
	}
	return out
}

// attrError 把通用校验错误映射为 CMDB 业务错误（ErrCIInvalid，文案原样透出）。
func attrError(err error) error {
	var fe *formschema.Error
	if errors.As(err, &fe) {
		return detail(ErrCIInvalid, "%s", fe.Error())
	}
	return err
}

// ValidateAttrs 按字段定义校验并归一化属性：
//   - 未定义或已废弃的键拒绝（防脏数据）；
//   - partial=false 时必填字段必须出现且非空；
//   - 各类型归一：int → int64、float → float64、datetime → RFC3339 字符串、ip → 规范化文本；
//   - value 为 nil 表示清空该键（返回值中不包含）。
//
// 校验实现收敛在 internal/pkg/formschema（与工单表单等共用），此处只做字段
// 定义转换与错误码映射。
func ValidateAttrs(fields []*Field, attrs map[string]any, partial bool) (map[string]any, error) {
	out, err := formschema.Validate(schemaFields(fields), attrs, partial)
	if err != nil {
		return nil, attrError(err)
	}
	return out, nil
}

// checkRequired 合并后的属性须包含全部启用的必填字段。只看键是否存在且非空：
// 存量值不重校验（字段规则 / 枚举值收紧后不阻塞无关更新），已废弃字段的遗留键忽略。
func checkRequired(fields []*Field, attrs map[string]any) error {
	if err := formschema.CheckRequired(schemaFields(fields), attrs); err != nil {
		return attrError(err)
	}
	return nil
}
