-- 0060 down
DROP TABLE IF EXISTS workflow_webhook;
DROP TABLE IF EXISTS workflow_calendar;
DROP TABLE IF EXISTS workflow_delegation;
DROP TABLE IF EXISTS workflow_job;
DROP INDEX IF EXISTS idx_workflow_task_assignee_behalf;
-- 回滚前把只存在于新版本的席位状态归并为「已取消」，否则旧约束加不回去。
UPDATE workflow_task_assignee SET status = 5 WHERE status = 9;
ALTER TABLE workflow_task_assignee DROP CONSTRAINT ck_workflow_task_assignee_status;
ALTER TABLE workflow_task_assignee ADD CONSTRAINT ck_workflow_task_assignee_status CHECK (status IN (1, 2, 3, 4, 5, 6, 7, 8));
ALTER TABLE workflow_task_assignee DROP COLUMN on_behalf, DROP COLUMN owner_id, DROP COLUMN origin;
ALTER TABLE workflow_task DROP COLUMN due_time;
ALTER TABLE workflow_token DROP COLUMN scope;
DROP INDEX IF EXISTS idx_workflow_instance_parent;
ALTER TABLE workflow_instance DROP COLUMN depth, DROP COLUMN parent_token_id, DROP COLUMN parent_node_id, DROP COLUMN parent_instance_id;
