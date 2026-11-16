package data

import (
	"context"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/operator"

	"github.com/lanyulei/reborn/backend/kit/dbx"
)

// OperatorArg 审计列实参（create_user / update_user / delete_user，uuid 列）：
// 请求上下文里的操作者 ID；未注入（匿名接口、内部任务、测试）落 NULL。
// 各域 data 子包共用，避免逐域复制。
func OperatorArg(ctx context.Context) any { return dbx.NilIfEmpty(operator.FromContext(ctx)) }
