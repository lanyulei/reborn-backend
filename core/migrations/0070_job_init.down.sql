-- 0070 down：删除作业中心的全部表（表级授权随 DROP TABLE 一并清除）。
-- 角色 job_engine_role / job_core_role 保留：角色是集群级对象，可能被其它库引用（同 0021 / 0036 / 0069）。

DROP TABLE IF EXISTS job_execution_log;
DROP TABLE IF EXISTS job_execution_host;
DROP TABLE IF EXISTS job_execution;
DROP TABLE IF EXISTS job_runner;
DROP TABLE IF EXISTS job_template;
DROP TABLE IF EXISTS job_project;
DROP TABLE IF EXISTS job_credential;
