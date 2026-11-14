-- 0050 down
DELETE FROM system_grant WHERE resource_type = 'api' AND resource_code IN (
    md5('/cmdb.v1.CmdbCIService/BatchUpdateCIs'), md5('/cmdb.v1.CmdbCIService/BatchTransitionCIs'),
    md5('/cmdb.v1.CmdbCIService/BatchDeleteCIs'), md5('/cmdb.v1.CmdbCIService/RestoreCI'),
    md5('/cmdb.v1.CmdbCIService/ListOwnerCandidates'), md5('/cmdb.v1.CmdbInsightService/GetQuality'),
    md5('/cmdb.v1.CmdbSyncService/ListConflicts'));
DELETE FROM system_permission WHERE operation IN (
    '/cmdb.v1.CmdbCIService/BatchUpdateCIs', '/cmdb.v1.CmdbCIService/BatchTransitionCIs',
    '/cmdb.v1.CmdbCIService/BatchDeleteCIs', '/cmdb.v1.CmdbCIService/RestoreCI',
    '/cmdb.v1.CmdbCIService/ListOwnerCandidates', '/cmdb.v1.CmdbInsightService/GetQuality',
    '/cmdb.v1.CmdbSyncService/ListConflicts');
DROP INDEX IF EXISTS idx_cmdb_ci_owner;
DROP INDEX IF EXISTS idx_cmdb_ci_deleted;
DROP TABLE IF EXISTS cmdb_sync_conflict;
