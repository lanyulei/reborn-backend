-- 0090 保留期清理的取行索引（只加索引，不改表结构）。
--
-- 保留期清理按下述条件分批取行；这些条件此前都没有索引，每批都要全表扫描。索引谓词与清理语句的谓词
-- 逐字相同（部分索引只有在查询条件能推出索引谓词时才会被使用）。
-- 1) knowledge_ingest_job：已结束（成功 / 失败）且更新时间早于保留期。
-- 2) cmdb_sync_run：已结束且结束时间早于保留期（每个采集源最近一次运行除外，由清理语句自行排除）。
-- 3) cmdb_sync_task：已结束且结束时间早于保留期。
-- 4) oncall_shift：已删除值班表的班次，按值班表取行。
--
-- 大表上建索引期间会短暂阻塞对应表的写入（非 CONCURRENTLY）：数据量很大的环境建议在低峰执行迁移。
-- 幂等：IF NOT EXISTS。

CREATE INDEX IF NOT EXISTS idx_knowledge_ingest_job_finished ON knowledge_ingest_job (update_time) WHERE status IN (3, 4);
CREATE INDEX IF NOT EXISTS idx_cmdb_sync_run_finished ON cmdb_sync_run (finish_time) WHERE status IN ('succeeded', 'failed');
CREATE INDEX IF NOT EXISTS idx_cmdb_sync_task_finished ON cmdb_sync_task (finish_time) WHERE status IN ('succeeded', 'failed');
CREATE INDEX IF NOT EXISTS idx_oncall_shift_schedule ON oncall_shift (schedule_id);
