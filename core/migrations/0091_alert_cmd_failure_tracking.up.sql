-- 0091 事件指令：记录失败的起止时间（只加两个可空列，不回填、不改既有数据）。
--
-- 背景：引擎执行一条指令失败时它保持「待处理」，每个评估器实例每轮都会重新领取并重试，恒失败的指令
-- （约束冲突、行损坏……）会永远停在待处理，用户在事件上一直看到「待关闭」。现在每次失败记下「首次失败」
-- 与「最近一次失败」两个时刻，引擎据此判定「连续失败超过时限」后把指令置为未执行并写明原因。
-- 两个时刻都取数据库时钟；「最近一次失败」还用来识别中断——两次失败之间隔得太久（引擎宕机、指令长期
-- 没被领取）就从下一段失败重新计时，不会因为宕机前失败过一次、重启后刚好又抖一下就被放弃。
--
-- 评估引擎对该表的 UPDATE 权限是按列授予的（0076），新列要补授。
-- 幂等：IF NOT EXISTS / GRANT 重复执行无副作用。

ALTER TABLE monitor_alert_cmd ADD COLUMN IF NOT EXISTS first_failed_at timestamptz NULL;
ALTER TABLE monitor_alert_cmd ADD COLUMN IF NOT EXISTS last_failed_at timestamptz NULL;
COMMENT ON COLUMN monitor_alert_cmd.first_failed_at IS '当前这一段连续失败的起点；成功执行或被置为终态后不再有意义';
COMMENT ON COLUMN monitor_alert_cmd.last_failed_at IS '最近一次执行失败的时刻（数据库时钟）；为空表示从未失败';
GRANT UPDATE (status, result, done_time, first_failed_at, last_failed_at) ON monitor_alert_cmd TO monitor_engine_role;
