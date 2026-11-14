-- 0049 down：回到"全部 unique 字段拼成一个唯一键"的旧口径。
-- 注意：若新口径下已建立多条规则，旧口径会把这些字段合并成一个联合键（约束变宽松），
-- 重建唯一索引通常不会失败；仅当旧口径的空值处理（跳过空字段）导致重复时失败，需先清理数据。
-- 旧唯一键列为 varchar(512)：新口径下允许更长的唯一值，拼接超长的资源回退后唯一键置空
-- （不参与旧约束），而不是让回退整体失败。

DELETE FROM system_grant
WHERE resource_type = 'api' AND resource_code IN (
    md5('/cmdb.v1.CmdbModelService/CreateUniqueRule'), md5('/cmdb.v1.CmdbModelService/UpdateUniqueRule'),
    md5('/cmdb.v1.CmdbModelService/DeleteUniqueRule'), md5('/cmdb.v1.CmdbModelService/CheckUniqueRule'));
DELETE FROM system_permission
WHERE operation IN (
    '/cmdb.v1.CmdbModelService/CreateUniqueRule', '/cmdb.v1.CmdbModelService/UpdateUniqueRule',
    '/cmdb.v1.CmdbModelService/DeleteUniqueRule', '/cmdb.v1.CmdbModelService/CheckUniqueRule');

ALTER TABLE cmdb_sync_staging OWNER TO CURRENT_USER;
GRANT SELECT, INSERT, DELETE ON cmdb_sync_staging TO cmdb_sync_role;

DROP TRIGGER IF EXISTS trg_cmdb_ci_unique_update ON cmdb_ci;
DROP TRIGGER IF EXISTS trg_cmdb_ci_unique_insert ON cmdb_ci;
DROP FUNCTION IF EXISTS cmdb_ci_unique_sync();

-- 字段标记与规则对齐（正常情况下已一致，此处兜底），再按旧口径重算唯一键。
UPDATE cmdb_model_field f
SET "unique" = EXISTS (
    SELECT 1 FROM cmdb_model_unique_rule r WHERE r.model_code = f.model_code AND f.code = ANY (r.field_codes)) AND NOT f.deprecated;

DROP TABLE IF EXISTS cmdb_ci_unique;
DROP TABLE IF EXISTS cmdb_model_unique_rule;
DROP FUNCTION IF EXISTS cmdb_unique_key(jsonb, text[]);

UPDATE cmdb_ci c SET unique_key = k.key
FROM (SELECT x.id, CASE WHEN length(x.key) > 512 THEN '' ELSE x.key END AS key FROM (
    SELECT c2.id, COALESCE((
        SELECT string_agg(f.code || '=' || (c2.attrs ->> f.code), '|' ORDER BY f.code COLLATE "C")
        FROM cmdb_model_field f
        WHERE f.model_code = c2.model_code AND f."unique" AND NOT f.deprecated
          AND jsonb_typeof(c2.attrs -> f.code) <> 'null'), '') AS key
    FROM cmdb_ci c2 WHERE c2.delete_time IS NULL
) x) k
WHERE c.id = k.id AND c.unique_key <> k.key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_cmdb_ci_model_unique_key ON cmdb_ci (model_code, unique_key) WHERE delete_time IS NULL AND unique_key <> '';
COMMENT ON COLUMN cmdb_ci.unique_key IS '按类型唯一字段拼接（code=value|...），空串不参与唯一约束';
COMMENT ON COLUMN cmdb_model_field."unique" IS NULL;
