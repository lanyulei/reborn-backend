package biz

import "github.com/google/wire"

// ruleP2Providers 规则第二、三期的 biz providers（由包的 ProviderSet 引用）：带协作者的规则用例
// （代替 NewRuleUsecase）、规则配置候选项、模拟触发。SimulateDeps 由 wire 按字段注入。
var ruleP2Providers = wire.NewSet(
	ProvideRuleUsecase, NewRuleOptionUsecase, NewSimulateUsecase, wire.Struct(new(SimulateDeps), "*"),
)
