-- 回滚 0025：先解绑再删角色行，最后去掉内置标记列。
DELETE FROM system_user_role
WHERE role_id IN (SELECT id FROM system_role WHERE code = 'default' AND builtin);
DELETE FROM system_grant
WHERE role_id IN (SELECT id FROM system_role WHERE code = 'default' AND builtin);
DELETE FROM system_permission
WHERE role_id IN (SELECT id FROM system_role WHERE code = 'default' AND builtin);
DELETE FROM system_role WHERE code = 'default' AND builtin;
ALTER TABLE system_role DROP COLUMN IF EXISTS builtin;
