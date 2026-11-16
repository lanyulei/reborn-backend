-- 0062 down
ALTER TABLE workflow_instance DROP COLUMN client_id;
DROP TABLE IF EXISTS workflow_client;
