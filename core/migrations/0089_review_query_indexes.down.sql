-- 0089 回滚：去掉本迁移补的全部索引。

DROP INDEX IF EXISTS idx_incident_proposal_auto_outcome;
DROP INDEX IF EXISTS idx_job_trigger_fire_stranded;
DROP INDEX IF EXISTS idx_job_trigger_fire_list;
DROP INDEX IF EXISTS idx_monitor_ci_index_env;
DROP INDEX IF EXISTS idx_monitor_alert_inhibit;
DROP INDEX IF EXISTS idx_monitor_alert_mute;
DROP INDEX IF EXISTS idx_monitor_engine_event_done;
DROP INDEX IF EXISTS idx_workflow_job_due_internal;
DROP INDEX IF EXISTS idx_workflow_job_due_service;
DROP INDEX IF EXISTS idx_workflow_instance_business_all;
DROP INDEX IF EXISTS idx_oncall_shift_user_start;
DROP INDEX IF EXISTS idx_oncall_shift_group_start;
DROP INDEX IF EXISTS idx_incident_proposal_auto_running;
DROP INDEX IF EXISTS idx_incident_proposal_playbook_time;
DROP INDEX IF EXISTS idx_incident_proposal_auto_step;
DROP INDEX IF EXISTS idx_cmdb_ci_update;
