package testfake

import "context"

// BizScopeStub 是 biznotify.BizScopeResolver 的内存桩：固定返回给定范围。
type BizScopeStub struct {
	Unrestricted bool
	Scope        []string
}

// BizScope implements biznotify.BizScopeResolver。
func (s *BizScopeStub) BizScope(context.Context, string) (bool, []string, error) {
	return s.Unrestricted, s.Scope, nil
}

// Unrestricted 不限范围桩（现有测试缺省口径）。
func Unrestricted() *BizScopeStub { return &BizScopeStub{Unrestricted: true} }

// Scoped 受限范围桩。
func Scoped(bizs ...string) *BizScopeStub { return &BizScopeStub{Scope: bizs} }
