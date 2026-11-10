package server

import (
	"buf.build/go/protovalidate"
	"github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/protobuf/proto"
)

// protoValidate 边界校验钩子：按 proto 的 (buf.validate.field) 规则校验。
func protoValidate(v any) error {
	if msg, ok := v.(proto.Message); ok {
		if err := protovalidate.Validate(msg); err != nil {
			return err
		}
	}
	return nil
}

// errBadRequest 解码/校验错误包装为带 code 的结构化错误。
func errBadRequest(err error) error {
	return errors.BadRequest("CODEC", err.Error())
}
