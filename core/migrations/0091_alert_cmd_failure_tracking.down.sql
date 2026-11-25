-- 0091 回滚：去掉两个失败时刻列（列级授权随列一并消失）。

ALTER TABLE monitor_alert_cmd
    DROP COLUMN IF EXISTS last_failed_at,
    DROP COLUMN IF EXISTS first_failed_at;
