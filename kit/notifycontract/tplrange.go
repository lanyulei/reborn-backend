package notifycontract

import (
	"errors"
	htmltemplate "html/template"
	"text/template"
	"text/template/parse"
)

// ErrNestedRange 模板里出现了嵌套的 range。
var ErrNestedRange = errors.New("nested range is not allowed in a template")

// CheckRangeNesting 拒绝嵌套的 range，core 的模板写入校验与管道的模板解析共用这一条规则。
//
// text/template 对 range 的迭代次数没有上限：嵌套 k 层、每层 n 个元素就是 n^k 次迭代，一段几百字节的
// 模板配上几十个键的载荷，就能让渲染 goroutine 指数级空转——既不产出输出（输出上限拦不住），也只在
// 写出时才响应取消，超时返回后 goroutine 仍在占满 CPU。只允许不嵌套的 range（此时迭代次数以载荷
// 大小为界）。trees 传模板及其全部关联模板（define）的解析树，见 TextTrees / HTMLTrees。
func CheckRangeNesting(trees []*parse.Tree) error {
	for _, t := range trees {
		if t != nil && hasNestedRange(t.Root, 0) {
			return ErrNestedRange
		}
	}
	return nil
}

// TextTrees 返回 text 模板及其全部关联模板的解析树。
func TextTrees(t *template.Template) []*parse.Tree {
	if t == nil {
		return nil
	}
	associated := t.Templates()
	out := make([]*parse.Tree, 0, len(associated))
	for _, at := range associated {
		out = append(out, at.Tree)
	}
	return out
}

// HTMLTrees 返回 html 模板及其全部关联模板的解析树。
func HTMLTrees(t *htmltemplate.Template) []*parse.Tree {
	if t == nil {
		return nil
	}
	associated := t.Templates()
	out := make([]*parse.Tree, 0, len(associated))
	for _, at := range associated {
		out = append(out, at.Tree)
	}
	return out
}

// hasNestedRange 报告节点树里是否出现嵌套的 range：depth 是外层已经进入的 range 层数。
func hasNestedRange(n parse.Node, depth int) bool {
	switch x := n.(type) {
	case *parse.ListNode:
		if x == nil {
			return false
		}
		for _, child := range x.Nodes {
			if hasNestedRange(child, depth) {
				return true
			}
		}
	case *parse.IfNode:
		return branchHasNestedRange(&x.BranchNode, depth)
	case *parse.WithNode:
		return branchHasNestedRange(&x.BranchNode, depth)
	case *parse.RangeNode:
		return depth >= 1 || branchHasNestedRange(&x.BranchNode, depth+1)
	}
	return false
}

// branchHasNestedRange 检查分支节点的主分支与 else 分支（都可能为空）。
func branchHasNestedRange(b *parse.BranchNode, depth int) bool {
	return hasNestedRange(b.List, depth) || hasNestedRange(b.ElseList, depth)
}
