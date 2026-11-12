-- 0032_cmdb.down.sql 回滚 0032：删除 CMDB 域全部表（分区随父表删除）。

DROP TABLE IF EXISTS cmdb_ci_history;
DROP TABLE IF EXISTS cmdb_relation;
DROP TABLE IF EXISTS cmdb_ci;
DROP TABLE IF EXISTS cmdb_relation_type;
DROP TABLE IF EXISTS cmdb_model_field;
DROP TABLE IF EXISTS cmdb_model;
