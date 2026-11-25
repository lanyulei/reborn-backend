-- 0089 查询索引补充（只加索引，不改表结构）。
--
-- 1) cmdb_ci：不带 model_code 的资源列表 / 回收站按 (update_time DESC, id DESC) 排序分页，现有索引都以
--    model_code / owner 打头，只能全量 top-N 排序。
-- 2) incident_proposal：自动处置的三类查询此前都是顺序扫描——
--    a. 同一预案步骤上一次自动执行（LastAutoExec：auto AND playbook_id AND step_index）；
--    b. 预案在时间窗内的执行次数（CountPlaybookExecs / CountPlaybookExecsMany：playbook_id AND create_time）；
--    c. 进行中的自动处置数（CountAutoRunning：auto AND proposer_type = 3 AND status IN (2, 4)）。
--
-- 3) oncall_shift：按分组 / 按人查班次（公共日历、ListRangeScoped）此前没有可用索引（0054 的注释写了、DDL 没建）。
-- 4) workflow：按业务对象查最近一次流程（变更单审批证据；原唯一索引只含进行中的实例）；作业领取按类别
--    拆成两条语句后各配一个部分索引。
-- 5) monitor：引擎事件清理、按屏蔽规则 / 抑制规则统计告警、环境下拉选项。
-- 6) job：触发记录不带触发器的列表排序（此前只有 BRIN）、滞留触发记录清理。
-- 7) incident_proposal：近期自动处置结果（Agent 判断是否允许自动执行）。
--
-- 大表上建索引期间会短暂阻塞对应表的写入（非 CONCURRENTLY）：数据量很大的环境建议在低峰执行迁移。
-- 幂等：IF NOT EXISTS。

CREATE INDEX IF NOT EXISTS idx_cmdb_ci_update ON cmdb_ci (update_time DESC, id DESC) WHERE delete_time IS NULL;
CREATE INDEX IF NOT EXISTS idx_incident_proposal_auto_step ON incident_proposal (playbook_id, step_index) WHERE auto;
CREATE INDEX IF NOT EXISTS idx_incident_proposal_playbook_time ON incident_proposal (playbook_id, create_time) WHERE playbook_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_incident_proposal_auto_running ON incident_proposal (incident_id) WHERE auto AND proposer_type = 3 AND status IN (2, 4);
CREATE INDEX IF NOT EXISTS idx_oncall_shift_group_start ON oncall_shift (group_id, start_time);
CREATE INDEX IF NOT EXISTS idx_oncall_shift_user_start ON oncall_shift (user_id, start_time);
CREATE INDEX IF NOT EXISTS idx_workflow_instance_business_all ON workflow_instance (business_id, business_type) WHERE business_type <> '';
CREATE INDEX IF NOT EXISTS idx_workflow_job_due_service ON workflow_job (due_time) WHERE status = 1 AND kind = 'service';
CREATE INDEX IF NOT EXISTS idx_workflow_job_due_internal ON workflow_job (due_time) WHERE status = 1 AND kind <> 'service';
CREATE INDEX IF NOT EXISTS idx_monitor_engine_event_done ON monitor_engine_event (done_time) WHERE status <> 1;
CREATE INDEX IF NOT EXISTS idx_monitor_alert_mute ON monitor_alert (mute_id) WHERE muted;
CREATE INDEX IF NOT EXISTS idx_monitor_alert_inhibit ON monitor_alert (inhibit_id) WHERE inhibited;
CREATE INDEX IF NOT EXISTS idx_monitor_ci_index_env ON monitor_ci_index (env) WHERE valid AND env <> '';
CREATE INDEX IF NOT EXISTS idx_job_trigger_fire_list ON job_trigger_fire (create_time DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_job_trigger_fire_stranded ON job_trigger_fire (create_time) WHERE result = 1 AND execution_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_incident_proposal_auto_outcome ON incident_proposal (create_time) WHERE auto AND proposer_type = 3 AND status IN (5, 6);
