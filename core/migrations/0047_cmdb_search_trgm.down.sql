DROP INDEX IF EXISTS idx_cmdb_ci_external_id_trgm;
DROP INDEX IF EXISTS idx_cmdb_ci_name_trgm;
DROP INDEX IF EXISTS idx_cmdb_ci_search_trgm;
DROP FUNCTION IF EXISTS cmdb_ci_search_text(text, text, text[], jsonb);
-- 扩展不回收：可能已被其它对象使用。
