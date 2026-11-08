package server

import (
	"buf.build/go/protovalidate"
	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/protobuf/proto"
)

// protoValidate 是边界校验钩子：proto.Message 统一按 .proto 中
// (buf.validate.field) 注解定义的规则校验，规则集中在契约层（AGENTS.md §5）。
func protoValidate(v any) error {
	if msg, ok := v.(proto.Message); ok {
		if err := protovalidate.Validate(msg); err != nil {
			return err
		}
	}
	return nil
}

// errBadRequest 将解码/校验错误包装为带 code 的结构化错误。
func errBadRequest(err error) error {
	return errors.BadRequest("CODEC", err.Error())
}
