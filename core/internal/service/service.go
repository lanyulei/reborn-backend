// Package service 实现 proto 生成的服务接口：只做参数编排、模型转换与错误透传，
// 业务规则全部收敛在 biz（禁止 import internal/data 与任何驱动）。
//
// 分域结构：各业务域在 internal/service/<domain>/ 子包实现（子包包名保持
// service）；本包只保留跨域聚合的 ProviderSet。新增域时在此追加。
package service

import (
	"github.com/google/wire"

	serviceagent "github.com/lanyulei/reborn/backend/core/internal/service/agent"
	serviceassistant "github.com/lanyulei/reborn/backend/core/internal/service/assistant"
	servicecmdb "github.com/lanyulei/reborn/backend/core/internal/service/cmdb"
	servicefile "github.com/lanyulei/reborn/backend/core/internal/service/file"
	serviceincident "github.com/lanyulei/reborn/backend/core/internal/service/incident"
	servicejob "github.com/lanyulei/reborn/backend/core/internal/service/job"
	servicek8s "github.com/lanyulei/reborn/backend/core/internal/service/k8s"
	serviceknowledge "github.com/lanyulei/reborn/backend/core/internal/service/knowledge"
	servicemonitor "github.com/lanyulei/reborn/backend/core/internal/service/monitor"
	servicenotify "github.com/lanyulei/reborn/backend/core/internal/service/notify"
	serviceoncall "github.com/lanyulei/reborn/backend/core/internal/service/oncall"
	servicesystem "github.com/lanyulei/reborn/backend/core/internal/service/system"
	serviceticket "github.com/lanyulei/reborn/backend/core/internal/service/ticket"
	serviceworkflow "github.com/lanyulei/reborn/backend/core/internal/service/workflow"
)

// ProviderSet 聚合各业务域的 service providers。
var ProviderSet = wire.NewSet(servicesystem.ProviderSet, servicenotify.ProviderSet, serviceassistant.ProviderSet, servicecmdb.ProviderSet, serviceoncall.ProviderSet, servicefile.ProviderSet, serviceworkflow.ProviderSet, serviceticket.ProviderSet, servicemonitor.ProviderSet, servicejob.ProviderSet, servicek8s.ProviderSet, serviceknowledge.ProviderSet, serviceincident.ProviderSet, serviceagent.ProviderSet)
