-- 0075 down：删除作业中心二期 / 三期的表、列与内置的通知路由 / 模板。
-- 一期（0070）的约束不容纳快速执行与审批状态：这些执行记录（连同主机与日志）一并删除。

DELETE FROM notify_route WHERE id = '018f3e1a-0d11-7000-8000-000000000001';
DELETE FROM notify_template WHERE id = '018f3e1a-0d12-7000-8000-000000000001';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

DROP TABLE IF EXISTS job_trigger_fire;
DROP TABLE IF EXISTS job_trigger;
DROP TABLE IF EXISTS job_schedule;
DROP TABLE IF EXISTS job_policy;
DROP TABLE IF EXISTS job_risk_rule;
DROP TABLE IF EXISTS job_script;
DROP TABLE IF EXISTS job_execution_payload;

DELETE FROM job_execution_log WHERE execution_id IN (SELECT id FROM job_execution WHERE kind <> 1 OR status IN (9, 10) OR trigger <> 1);
DELETE FROM job_execution WHERE kind <> 1 OR status IN (9, 10) OR trigger <> 1;

DROP INDEX IF EXISTS idx_job_execution_trigger_ref;
DROP INDEX IF EXISTS idx_job_execution_notify;
DROP INDEX IF EXISTS idx_job_execution_group;
DROP INDEX IF EXISTS idx_job_execution_active;
DROP INDEX IF EXISTS uk_job_execution_exclusive;
ALTER TABLE job_execution
    DROP CONSTRAINT ck_job_execution_risk_hits,
    DROP CONSTRAINT ck_job_execution_finish,
    DROP CONSTRAINT ck_job_execution_status,
    DROP CONSTRAINT ck_job_execution_trigger,
    DROP CONSTRAINT ck_job_execution_template,
    DROP CONSTRAINT ck_job_execution_kind,
    DROP COLUMN notify_time,
    DROP COLUMN risk_hits,
    DROP COLUMN approval_instance_id,
    DROP COLUMN approval_reason,
    DROP COLUMN slice_total,
    DROP COLUMN slice_no,
    DROP COLUMN group_id,
    DROP COLUMN trigger_ref,
    DROP COLUMN kind,
    ALTER COLUMN template_id SET NOT NULL,
    ADD CONSTRAINT ck_job_execution_trigger CHECK (trigger IN (1)),
    ADD CONSTRAINT ck_job_execution_status CHECK (status BETWEEN 1 AND 8),
    ADD CONSTRAINT ck_job_execution_finish CHECK ((status IN (1, 2)) = (finish_time IS NULL));
CREATE UNIQUE INDEX uk_job_execution_exclusive ON job_execution (template_id) WHERE exclusive AND status IN (1, 2);

ALTER TABLE job_template
    DROP CONSTRAINT ck_job_template_slice_exclusive,
    DROP CONSTRAINT ck_job_template_slice_size,
    DROP COLUMN slice_size,
    DROP COLUMN require_approval;
