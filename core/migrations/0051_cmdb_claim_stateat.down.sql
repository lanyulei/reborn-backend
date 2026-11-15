-- 0051 down
DELETE FROM system_grant WHERE resource_type = 'api' AND resource_code IN (
    md5('/cmdb.v1.CmdbSyncService/ClaimConflict'), md5('/cmdb.v1.CmdbCIService/GetCIAt'));
DELETE FROM system_permission WHERE operation IN (
    '/cmdb.v1.CmdbSyncService/ClaimConflict', '/cmdb.v1.CmdbCIService/GetCIAt');
REVOKE DELETE ON cmdb_sync_conflict FROM cmdb_core_role;
