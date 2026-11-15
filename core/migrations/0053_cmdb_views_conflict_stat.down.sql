-- 0053 down
DELETE FROM system_grant WHERE resource_type = 'api' AND resource_code IN (
    md5('/cmdb.v1.CmdbInsightService/ListCIViews'), md5('/cmdb.v1.CmdbInsightService/SaveCIView'),
    md5('/cmdb.v1.CmdbInsightService/DeleteCIView'));
DELETE FROM system_permission WHERE operation IN (
    '/cmdb.v1.CmdbInsightService/ListCIViews', '/cmdb.v1.CmdbInsightService/SaveCIView',
    '/cmdb.v1.CmdbInsightService/DeleteCIView');
DROP TABLE IF EXISTS cmdb_sync_conflict_stat;
DROP TABLE IF EXISTS cmdb_ci_view;
