-- 0031 控制台能力（通知优化第三批）：路由/静默可读名称与说明；core 只读渠道共享熔断状态。
-- 全部幂等（IF NOT EXISTS / GRANT 可重复），手工 psql 与 golang-migrate 均可安全重放。
ALTER TABLE notify_route ADD COLUMN IF NOT EXISTS name varchar(64) NOT NULL DEFAULT '';
ALTER TABLE notify_route ADD COLUMN IF NOT EXISTS description varchar(255) NOT NULL DEFAULT '';
COMMENT ON COLUMN notify_route.name IS '可读名称（控制台列表展示；空 = 未命名，列表回退显示 event_pattern）';
COMMENT ON COLUMN notify_route.description IS '说明';
ALTER TABLE notify_silence ADD COLUMN IF NOT EXISTS name varchar(64) NOT NULL DEFAULT '';
ALTER TABLE notify_silence ADD COLUMN IF NOT EXISTS description varchar(255) NOT NULL DEFAULT '';
COMMENT ON COLUMN notify_silence.name IS '可读名称（控制台列表展示）';
COMMENT ON COLUMN notify_silence.description IS '说明';
-- 渠道健康（共享熔断）只读视图：控制台展示打开/探测中/失败率；写仍仅 notify_pipe_role。
GRANT SELECT ON notify_channel_health TO notify_core_role;
