-- 0073 down：删除 Warning 事件表与内置的事件通知模板。

DELETE FROM notify_template
WHERE biz_id = 'default' AND channel_type = '*' AND create_user IS NULL
  AND event_type IN ('k8s.event.nodenotready', 'k8s.event.oomkilling', 'k8s.event.oomkilled', 'k8s.event.failedscheduling',
                     'k8s.event.failed', 'k8s.event.errimagepull', 'k8s.event.imagepullbackoff', 'k8s.event.failedmount',
                     'k8s.event.failedattachvolume', 'k8s.event.backoff');
UPDATE notify_cfg_version SET version = version + 1, update_time = now() WHERE table_name IN ('notify_route', 'notify_template');

DROP TABLE IF EXISTS k8s_event;
