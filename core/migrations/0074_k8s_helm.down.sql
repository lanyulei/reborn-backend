-- 0074 down：删除 Helm 仓库表（表级授权随 DROP TABLE 一并清除）。
DROP TABLE IF EXISTS k8s_helm_repo;
