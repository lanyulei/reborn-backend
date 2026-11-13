-- 0047 CMDB 全局搜索 / 关键字筛选的三元组索引。
-- 此前全局搜索对每一行展开 jsonb 再 ILIKE（全表扫描）；列表关键字筛选同样无索引可用。
-- pg_trgm 为 PostgreSQL 自带的可信扩展（13+ 普通库属主即可创建）。
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 搜索文本投影：名称 / 外部标识 / 标签 / 全部属性值（与搜索语句匹配的范围一致，
-- 敏感属性值也在其中——它只作索引预筛，命中判定与敏感值排除仍由查询本身完成）。
-- 声明 IMMUTABLE 以便建表达式索引：结果只取决于入参。
CREATE OR REPLACE FUNCTION cmdb_ci_search_text(name text, external_id text, tags text[], attrs jsonb)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT coalesce(name, '') || E'\n' || coalesce(external_id, '') || E'\n'
        || coalesce(array_to_string(tags, E'\n'), '') || E'\n'
        || coalesce((SELECT string_agg(kv.value, E'\n') FROM jsonb_each_text(coalesce(attrs, '{}'::jsonb)) kv), '')
$$;

CREATE INDEX IF NOT EXISTS idx_cmdb_ci_search_trgm ON cmdb_ci
    USING gin (cmdb_ci_search_text(name, external_id, tags, attrs) gin_trgm_ops) WHERE delete_time IS NULL;
-- 列表关键字筛选：name ILIKE / external_id ILIKE（两个索引可 BitmapOr）。
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_name_trgm ON cmdb_ci USING gin (name gin_trgm_ops) WHERE delete_time IS NULL;
CREATE INDEX IF NOT EXISTS idx_cmdb_ci_external_id_trgm ON cmdb_ci USING gin (external_id gin_trgm_ops) WHERE delete_time IS NULL;
