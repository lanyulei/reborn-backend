-- 0090 回滚：去掉本迁移补的四个索引。

DROP INDEX IF EXISTS idx_oncall_shift_schedule;
DROP INDEX IF EXISTS idx_cmdb_sync_task_finished;
DROP INDEX IF EXISTS idx_cmdb_sync_run_finished;
DROP INDEX IF EXISTS idx_knowledge_ingest_job_finished;
