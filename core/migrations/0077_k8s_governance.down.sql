-- 0077 down：删除限时 kubeconfig 令牌、巡检表、终端录制的补充列与巡检通知模板。

DELETE FROM notify_template WHERE id = '018f3e1a-0cd2-7000-8000-000000000077';
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

ALTER TABLE k8s_exec_session DROP COLUMN IF EXISTS record_truncated;
DROP TABLE IF EXISTS k8s_inspection_finding;
DROP TABLE IF EXISTS k8s_inspection_run;
DROP TABLE IF EXISTS k8s_kube_token;
