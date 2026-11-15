-- 0052 down
DELETE FROM system_grant WHERE resource_type = 'api' AND resource_code = md5('/cmdb.v1.CmdbCIService/UnclaimCI');
DELETE FROM system_permission WHERE operation = '/cmdb.v1.CmdbCIService/UnclaimCI';
