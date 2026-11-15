-- 0052 CMDB 解除同步托管：新接口授予已持有"更新资源"接口的角色（接口权限逐条物化，不会自动获得）。
INSERT INTO system_permission (id, role_id, operation)
SELECT gen_random_uuid(), p.role_id, '/cmdb.v1.CmdbCIService/UnclaimCI'
FROM system_permission p WHERE p.operation = '/cmdb.v1.CmdbCIService/UpdateCI'
ON CONFLICT (role_id, operation) DO NOTHING;

INSERT INTO system_grant (id, role_id, resource_type, resource_code)
SELECT gen_random_uuid(), g.role_id, 'api', md5('/cmdb.v1.CmdbCIService/UnclaimCI')
FROM system_grant g WHERE g.resource_type = 'api' AND g.resource_code = md5('/cmdb.v1.CmdbCIService/UpdateCI')
ON CONFLICT (role_id, resource_type, resource_code) DO NOTHING;
