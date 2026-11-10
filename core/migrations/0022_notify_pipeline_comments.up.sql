-- 0022 notify 域：运行表列注释对齐管道 P1 实际语义（PG 队列骨干，无 relay/NATS；
-- NATS 作为后续可插拔骨干，语义保持兼容）。仅注释，无结构变更。
COMMENT ON COLUMN notify_event.publish_state IS '交付状态：0 pending（待 router 领取）/ 1 published（已交付路由处理，与事件终态同事务翻转；NATS 骨干下为已发布）';
COMMENT ON COLUMN notify_event.publish_attempts IS '路由瞬时错误重试次数（如订阅者查询超时；失败退避累加，超限置 failed）';
COMMENT ON COLUMN notify_event.next_publish_time IS '下次领取/发布时间（退避后）';
COMMENT ON COLUMN notify_dispatch.template_id IS '渲染用模板（可空=无模板走内置默认）；首次尝试渲染时回填，版本号在 content.template_version';
COMMENT ON COLUMN notify_dispatch.next_retry_time IS 'pending/failed：到期可领取时间；sending：租约到期时间（过期由回收器转重试）；终态 NULL';
