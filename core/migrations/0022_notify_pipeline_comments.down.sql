-- 回滚 0022：恢复原注释文案。
COMMENT ON COLUMN notify_event.publish_state IS 'NATS 发布状态：0 pending / 1 published（relay 退避推进）';
COMMENT ON COLUMN notify_event.publish_attempts IS 'NATS 发布尝试次数（失败退避累加）';
COMMENT ON COLUMN notify_event.next_publish_time IS '下次发布时间（退避后）';
COMMENT ON COLUMN notify_dispatch.template_id IS '渲染用模板版本（可空=无模板纯文本）';
COMMENT ON COLUMN notify_dispatch.next_retry_time IS '下次重试时间（指数退避后；可空=无待重试）';
