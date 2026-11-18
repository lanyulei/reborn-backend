package biz

import "time"

// 测试专用的导出（只在 go test 时编译）：给后台用例换时钟，以及暴露几个纯函数。

// SetNow 替换资源索引同步用例的时钟。
func (uc *CISyncUsecase) SetNow(now func() time.Time) { uc.now = now }

// SetNow 替换事件动作用例的时钟。
func (uc *ActionUsecase) SetNow(now func() time.Time) { uc.now = now }

// 资源索引同步与事件动作的内部参数、纯函数。
var (
	CloseUpstreams = closeUpstreams
	MatchAttrs     = matchAttrs
	AIPrompt       = aiPrompt
	ActionBackoff  = actionBackoff
)

const (
	CIPageSize          = ciPageSize
	CIIncrementalPages  = ciIncrementalPages
	CIReconcileInterval = ciReconcileInterval
	MaxUpstreams        = maxUpstreams
	ActionLease         = actionLease
)
