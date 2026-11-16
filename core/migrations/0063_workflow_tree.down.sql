-- 0063 down
-- 流程通知的缺省路由与模板：只删除本迁移写入的行（固定 ID），不动用户自建的配置。
DELETE FROM notify_route WHERE id = '018f3e1a-0cb1-7000-8000-000000000001';
DELETE FROM notify_template WHERE id::text LIKE '018f3e1a-0cb2-7000-8000-00000000000_';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');
DROP INDEX IF EXISTS idx_workflow_instance_definition_running;
DROP INDEX IF EXISTS idx_workflow_instance_title_trgm;
DROP INDEX IF EXISTS idx_workflow_instance_start;
DROP INDEX IF EXISTS idx_workflow_instance_root;
ALTER TABLE workflow_instance
    DROP COLUMN auto_skips,
    DROP COLUMN root_business_id,
    DROP COLUMN root_business_type,
    DROP COLUMN root_id;
