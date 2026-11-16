package biz

import (
	"fmt"
	"sync"

	"cel.dev/cel-go/cel"
)

// condCostLimit 单次条件求值的成本上限（CEL 运行时成本单位）：表达式来自流程
// 设计者，必须有界，超限按求值失败处理。
const condCostLimit = 10_000

// condition 已编译的分支条件。不可变，可并发求值。
type condition struct {
	program cel.Program
}

// condEnv 条件表达式的求值环境（惰性构造一次，只读）：
//   - form：流程变量（表单数据），map<string, dyn>；
//   - initiator / initiatorDept：发起人用户 ID、发起部门 ID。
//
// 仅启用标准库（无 IO、无副作用）。
var condEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("form", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("initiator", cel.StringType),
		cel.Variable("initiatorDept", cel.StringType),
	)
})

// compileMu 串行化编译：cel-go 的类型检查器在共享 Env 上并发编译时，其报错路径
// 存在数据竞争。编译只发生在发布校验与定义首次加载（低频），串行无性能顾虑；
// 已编译的 Program 并发求值是安全的，不经此锁。
var compileMu sync.Mutex

// compileCondition 编译并类型检查分支条件；结果类型须为 bool（或动态类型，运行期再判）。
func compileCondition(expr string) (*condition, error) {
	env, err := condEnv()
	if err != nil {
		return nil, fmt.Errorf("build condition env: %w", err)
	}
	compileMu.Lock()
	defer compileMu.Unlock()
	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	if t := ast.OutputType(); !t.IsExactType(cel.BoolType) && !t.IsExactType(cel.DynType) {
		return nil, fmt.Errorf("condition must evaluate to a boolean, got %s", t)
	}
	program, err := env.Program(ast, cel.CostLimit(condCostLimit), cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return nil, err
	}
	return &condition{program: program}, nil
}

// eval 求值条件。变量缺失、类型不符、成本超限等运行期错误原样返回，由调用方决定
// 走默认分支还是中止（引擎的口径：条件求值出错视为不命中，继续判下一分支）。
func (c *condition) eval(vars map[string]any) (bool, error) {
	out, _, err := c.program.Eval(vars)
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("condition evaluated to %T, want bool", out.Value())
	}
	return b, nil
}
