-- 0082 运维 Agent 域回滚：删表与三处只增列；内置角色模板「运维 Agent」及其授权一并删除（物理删除，与 0025 的回滚口径一致）。

DROP TABLE IF EXISTS agent_budget;
DROP TABLE IF EXISTS agent_run;
DROP TABLE IF EXISTS agent_task;
DROP TABLE IF EXISTS agent_profile_history;
DROP TABLE IF EXISTS agent_profile;

ALTER TABLE incident_proposal DROP COLUMN IF EXISTS auto_eligible;

COMMENT ON COLUMN assistant_tool_call.status IS 'pending / approved / rejected / denied / succeeded / failed / cancelled';
ALTER TABLE assistant_conversation DROP COLUMN IF EXISTS kind;

DELETE FROM system_permission WHERE role_id IN (SELECT id FROM system_role WHERE code = 'ops-agent');
DELETE FROM system_user_role WHERE role_id IN (SELECT id FROM system_role WHERE code = 'ops-agent');
DELETE FROM system_role WHERE code = 'ops-agent';

ALTER TABLE "system_user" DROP CONSTRAINT IF EXISTS ck_system_user_kind;
ALTER TABLE "system_user" DROP COLUMN IF EXISTS kind;
