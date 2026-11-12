-- 回滚 0040。cmdb_model.category 列未变，自定义分组下的模型保留其编码（旧版本按字符串读取）。
DROP TABLE IF EXISTS cmdb_schema_audit;
DROP TABLE IF EXISTS cmdb_model_favorite;
DROP TABLE IF EXISTS cmdb_model_group;
