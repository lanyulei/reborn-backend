package monitorcontract

import (
	"errors"
	"fmt"
	"text/template/parse"
)

// 模板契约（附加信息与恢复取值 PromQL，设计 §4.3）。core 保存规则时与引擎渲染前调用同一个
// ValidateTemplate：通过校验的模板执行成本有界（与源文本长度 × 标签数成正比），不通过的模板
// 引擎一律不执行。
const (
	// MaxTemplateBytes 模板源文本上限（执行成本与源文本长度成正比）。
	MaxTemplateBytes = 8 << 10
	// TemplatePrefix 执行方解析模板时必须前置的声明，兼容 Prometheus 写法
	// {{ $labels.instance }} / {{ $value }}；执行上下文须有 Labels（map[string]string）与 Value 两个字段。
	TemplatePrefix = "{{$labels := .Labels}}{{$value := .Value}}"
	// MaxPrintfWidth printf 的宽度 / 精度上限：fmt 允许到 10^6，一次调用就能分配 1MB。
	MaxPrintfWidth = 256

	templateName = "t"
	printfName   = "printf"
	labelsVar    = "$labels"
)

// templateFuncs 模板可调用的函数：执行方必须提供的自定义函数（TemplateFuncs）加上 if 判断所需的
// 比较 / 逻辑内建。text/template 的其余内建（call / index / slice / len / html / js / urlquery /
// print / println）不在表内，解析时即被拒绝。值只是占位：parse 包只看键是否存在且非 nil。
var templateFuncs = map[string]any{
	printfName: true, "humanize": true, "humanizePercentage": true, "humanizeDuration": true,
	"toUpper": true, "toLower": true, "title": true,
	"eq": true, "ne": true, "lt": true, "le": true, "gt": true, "ge": true, "and": true, "or": true, "not": true,
}

// TemplateFuncs 执行方必须提供的自定义函数名（其余可用函数是 text/template 的内建）。
// printf 必须是先经 CheckPrintfFormat 检查并限制实参总长的版本。
func TemplateFuncs() []string {
	return []string{printfName, "humanize", "humanizePercentage", "humanizeDuration", "toUpper", "toLower", "title"}
}

// reservedVars 不允许声明或赋值的变量：前置声明的两个，以及根上下文 $。
var reservedVars = map[string]struct{}{labelsVar: {}, "$value": {}, "$": {}}

// ValidateTemplate 解析并校验一段模板源文本（不含 TemplatePrefix）。规则：
//   - 源文本不超过 MaxTemplateBytes；
//   - 只能调用 templateFuncs 里的函数；
//   - 不得声明或赋值 $labels / $value / $，任何变量都不得用 = 重新赋值；
//   - range 只能写成 {{range $labels}} / {{range $k, $v := $labels}}（遍历真实的标签 map），且不得嵌套；
//   - printf 必须在调用位置、格式串为字符串字面量并通过 CheckPrintfFormat；
//   - 不允许 template / block 调用；{{define}} 的子模板按同样规则检查。
//
// 失败返回包装了 ErrInvalid 的错误。
func ValidateTemplate(src string) error {
	if len(src) > MaxTemplateBytes {
		return invalidf("template longer than %d bytes", MaxTemplateBytes)
	}
	trees, err := parse.Parse(templateName, TemplatePrefix+src, "", "", templateFuncs)
	if err != nil {
		return invalidf("%v", err)
	}
	for name, tree := range trees {
		if tree == nil || tree.Root == nil {
			continue
		}
		nodes := tree.Root.Nodes
		if name == templateName {
			// 主模板的前两个节点是 TemplatePrefix 的声明，不属于用户输入。
			if len(nodes) < 2 {
				return invalidf("template: malformed")
			}
			nodes = nodes[2:]
		}
		for _, n := range nodes {
			if err := checkTemplateNode(n, false); err != nil {
				return invalidf("template: %v", err)
			}
		}
	}
	return nil
}

// checkTemplateNode 递归校验语法树；inRange 表示当前位于某个 range 体内。不认识的节点一律拒绝。
func checkTemplateNode(node parse.Node, inRange bool) error {
	switch n := node.(type) {
	case nil, *parse.TextNode, *parse.CommentNode, *parse.BreakNode, *parse.ContinueNode:
		return nil
	case *parse.ListNode:
		if n == nil {
			return nil
		}
		for _, c := range n.Nodes {
			if err := checkTemplateNode(c, inRange); err != nil {
				return err
			}
		}
		return nil
	case *parse.ActionNode:
		return checkTemplatePipe(n.Pipe)
	case *parse.IfNode:
		return checkTemplateBranch(&n.BranchNode, inRange)
	case *parse.WithNode:
		return checkTemplateBranch(&n.BranchNode, inRange)
	case *parse.RangeNode:
		if inRange {
			return errors.New("nested range is not allowed")
		}
		if !rangesOverLabels(n.Pipe) {
			return errors.New("range is only allowed over $labels")
		}
		return checkTemplateBranch(&n.BranchNode, true)
	case *parse.TemplateNode:
		return errors.New("template calls are not allowed")
	}
	return fmt.Errorf("node type %d is not allowed", node.Type())
}

func checkTemplateBranch(b *parse.BranchNode, inRange bool) error {
	if err := checkTemplatePipe(b.Pipe); err != nil {
		return err
	}
	if err := checkTemplateNode(b.List, inRange); err != nil {
		return err
	}
	if b.ElseList == nil {
		return nil
	}
	return checkTemplateNode(b.ElseList, inRange)
}

func checkTemplatePipe(p *parse.PipeNode) error {
	if p == nil {
		return nil
	}
	if p.IsAssign {
		return errors.New("variable assignment is not allowed")
	}
	for _, v := range p.Decl {
		if len(v.Ident) == 0 {
			return errors.New("malformed variable declaration")
		}
		if _, reserved := reservedVars[v.Ident[0]]; reserved {
			return fmt.Errorf("variable %s cannot be redeclared", v.Ident[0])
		}
	}
	for _, cmd := range p.Cmds {
		if err := checkTemplateCommand(cmd); err != nil {
			return err
		}
	}
	return nil
}

func checkTemplateCommand(cmd *parse.CommandNode) error {
	for i, arg := range cmd.Args {
		if err := checkTemplateArg(arg, i == 0); err != nil {
			return err
		}
	}
	if len(cmd.Args) == 0 {
		return nil
	}
	if id, ok := cmd.Args[0].(*parse.IdentifierNode); ok && id.Ident == printfName {
		if len(cmd.Args) < 2 {
			return errors.New("printf requires a literal format string")
		}
		format, ok := cmd.Args[1].(*parse.StringNode)
		if !ok {
			return errors.New("printf requires a literal format string")
		}
		return CheckPrintfFormat(format.Text)
	}
	return nil
}

func checkTemplateArg(arg parse.Node, callee bool) error {
	switch n := arg.(type) {
	case *parse.IdentifierNode:
		if _, ok := templateFuncs[n.Ident]; !ok {
			return fmt.Errorf("function %q is not allowed", n.Ident)
		}
		if n.Ident == printfName && !callee {
			return errors.New("printf requires a literal format string")
		}
		return nil
	case *parse.PipeNode:
		return checkTemplatePipe(n)
	case *parse.ChainNode:
		return checkTemplateArg(n.Node, false)
	case *parse.FieldNode, *parse.VariableNode, *parse.DotNode, *parse.NilNode, *parse.BoolNode,
		*parse.NumberNode, *parse.StringNode:
		return nil
	}
	return fmt.Errorf("node type %d is not allowed", arg.Type())
}

// rangesOverLabels 报告 range 的管道是否恰为 $labels（可带 := 声明的循环变量）。$labels 不可被
// 重新声明或赋值，所以它恒为真实的标签 map；.Labels 不放行——点可以被 with 改绑到别的值上。
func rangesOverLabels(p *parse.PipeNode) bool {
	if p == nil || len(p.Cmds) != 1 || len(p.Cmds[0].Args) != 1 {
		return false
	}
	v, ok := p.Cmds[0].Args[0].(*parse.VariableNode)
	return ok && len(v.Ident) == 1 && v.Ident[0] == labelsVar
}

// CheckPrintfFormat 校验 printf 的格式串：拒绝 `*`（宽度 / 精度取自实参）、显式参数索引 `[n]`，
// 以及超过 MaxPrintfWidth 的宽度 / 精度。执行方的 printf 在运行时须再调用一次（纵深防御）。
func CheckPrintfFormat(format string) error {
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		n := 0
	verb:
		for i++; i < len(format); i++ {
			switch c := format[i]; {
			case c == '*':
				return errors.New("printf: '*' width is not allowed")
			case c == '[':
				return errors.New("printf: explicit argument indexes are not allowed")
			case c >= '0' && c <= '9':
				if n = n*10 + int(c-'0'); n > MaxPrintfWidth {
					return errors.New("printf: width or precision too large")
				}
			case c == '.':
				n = 0
			case c == '+' || c == '-' || c == '#' || c == ' ':
			default:
				break verb
			}
		}
	}
	return nil
}
