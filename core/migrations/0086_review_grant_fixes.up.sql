-- 0086 全量代码审查修复：补对象授权 + 修复权限数据（不改任何表结构）。
--
-- 1) notify_ingress_replay 建表时（0048）漏了授权：notify_pipe_role 下每个签名请求的防重放登记
--    都报 42501，通知接入被整体拒绝。开发环境以属主 / 超级用户连接，所以发现不了。
-- 2) notify_pipe_role 缺 schema 的 CREATE：分区维护要 CREATE TABLE（PG 15 起 public schema 对
--    非属主不再默认授予 CREATE；cmdb / monitor / k8s 三个数据面角色都已补过，唯独 notify 漏了）。
-- 3) 0040 新建的三张表漏了 cmdb_core_role 的授权（模型分组 / 收藏 / 结构变更审计）。
-- 4) 权限数据修复：
--    a. 0049–0053 给角色「补授」接口时，system_permission 那一半按 proto 方法名原文匹配（库里存的是
--       md5），从未匹配到任何行；system_grant 那一半（md5）写成功了 → 授权页显示已勾选，调用却 403。
--    b. 0082 给内置角色「运维 Agent」只写了物化策略（system_permission），没写直接授权
--       （system_grant）。授权页回显的是后者；保存时按它全量重建物化策略 → 该角色一被编辑保存，
--       51 条接口策略就被清空。
--
-- 幂等：全部语句可重复执行。

-- 1)
GRANT SELECT, INSERT, DELETE ON notify_ingress_replay TO notify_pipe_role;
-- 2)
GRANT CREATE ON SCHEMA public TO notify_pipe_role;
-- 3)
GRANT SELECT, INSERT, UPDATE, DELETE ON cmdb_model_group, cmdb_model_favorite, cmdb_schema_audit TO cmdb_core_role;

-- 4b「运维 Agent」角色：补上直接授权，清单与 0082 完全一致（被编辑保存过的环境，物化策略已被清空，
-- 这里连同 4a 一并补回；管理员如需收回个别接口，修好后再在授权页操作）。
INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), r.id, 'api', md5(op.name)
FROM system_role r
CROSS JOIN (VALUES
    ('/incident.v1.IncidentService/ListIncidents'),
    ('/incident.v1.IncidentService/GetIncident'),
    ('/incident.v1.IncidentService/SearchIncidents'),
    ('/incident.v1.IncidentService/ListTimeline'),
    ('/incident.v1.IncidentService/ListIncidentAlerts'),
    ('/incident.v1.IncidentService/GetImpact'),
    ('/incident.v1.IncidentService/ListChanges'),
    ('/incident.v1.IncidentService/GetIncidentContext'),
    ('/incident.v1.IncidentService/AddFinding'),
    ('/incident.v1.IncidentService/AddComment'),
    ('/incident.v1.IncidentService/MarkChange'),
    ('/incident.v1.IncidentService/AttachAlerts'),
    ('/incident.v1.IncidentService/AcquireAgentLease'),
    ('/incident.v1.IncidentService/ReleaseAgentLease'),
    ('/incident.v1.IncidentProposalService/ListProposals'),
    ('/incident.v1.IncidentProposalService/CreateProposal'),
    ('/incident.v1.IncidentPostmortemService/GetPostmortem'),
    ('/incident.v1.IncidentPostmortemService/ListPostmortemVersions'),
    ('/incident.v1.IncidentConfigService/ListPlaybooks'),
    ('/incident.v1.IncidentConfigService/GetPlaybook'),
    ('/monitor.v1.MonitorAlertService/ListAlerts'),
    ('/monitor.v1.MonitorAlertService/GetAlert'),
    ('/monitor.v1.MonitorDatasourceService/ListDatasources'),
    ('/monitor.v1.MonitorDatasourceService/QueryDatasource'),
    ('/monitor.v1.MonitorDatasourceService/QueryDatasourceRange'),
    ('/cmdb.v1.CmdbCIService/ListCIs'),
    ('/cmdb.v1.CmdbCIService/GetCI'),
    ('/cmdb.v1.CmdbCIService/SearchCIs'),
    ('/cmdb.v1.CmdbCIService/ListCIHistory'),
    ('/cmdb.v1.CmdbRelationService/ListRelated'),
    ('/cmdb.v1.CmdbRelationService/Neighbors'),
    ('/k8s.v1.K8sInsightService/GetOverview'),
    ('/k8s.v1.K8sInsightService/ListEvents'),
    ('/k8s.v1.K8sInsightService/QueryMetrics'),
    ('/k8s.v1.K8sInsightService/GetRelations'),
    ('/k8s.v1.K8sInsightService/DiagnosePod'),
    ('/k8s.v1.K8sInsightService/DiagnoseWorkload'),
    ('/k8s.v1.K8sResourceService/ListResources'),
    ('/k8s.v1.K8sResourceService/GetObject'),
    ('/k8s.v1.K8sResourceService/ListObjectEvents'),
    ('/job.v1.JobExecutionService/ListJobExecutions'),
    ('/job.v1.JobExecutionService/GetJobExecution'),
    ('/job.v1.JobExecutionService/GetJobExecutionLog'),
    ('/workflow.v1.WorkflowInstanceService/GetInstance'),
    ('/knowledge.v1.KnowledgeService/Search'),
    ('/knowledge.v1.KnowledgeService/GetDocument'),
    ('/knowledge.v1.KnowledgeService/ListRelated'),
    ('/knowledge.v1.KnowledgeService/ListDocuments'),
    ('/knowledge.v1.KnowledgeSpaceService/ListSpaces'),
    ('/knowledge.v1.KnowledgeSpaceService/GetSpace'),
    ('/knowledge.v1.KnowledgeService/CreateDocument')
) AS op(name)
WHERE r.code = 'ops-agent' AND r.delete_time IS NULL
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;

-- 4a 物化策略与直接授权对齐：所有「已直接授权、但物化策略缺失」的接口补上物化行
--（应用保存授权时两张表总是同事务全量替换，正常数据下本语句不产生任何行）。
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), g.role_id, g.resource_code
FROM system_grant g
WHERE g.resource_type = 'api'
  AND NOT EXISTS (SELECT 1 FROM system_permission p WHERE p.role_id = g.role_id AND p.operation = g.resource_code)
ON CONFLICT (role_id, operation) DO NOTHING;
