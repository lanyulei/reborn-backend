-- 0019 notify 域：死信表（运行表，管道写 / core 读写 replay_state，设计 §5.2）
-- 语义：一行 = 一个重试超限转死信的发送任务（dispatch 重试 5 次超限后由
-- sender 落入本表，§4.7）；core 控制台经 ListDeadLetters 只读查询、经
-- ReplayDeadLetter 标记重放（replay_state: none → pending），dlq worker
--（管道 P1）轮询 pending 行重新生成 dispatch 后置 replayed。
-- 权限边界（§5.3 列级 GRANT）：core 角色对本表仅 SELECT + UPDATE
-- (replay_state, update_time)——不能改错误详情、不能删除；本地/当前部署
-- 未建 DB 角色（角色矩阵属管道 P1 部署清单），代码层以条件 UPDATE 保证
-- 状态机（WHERE replay_state = 1）。
-- 分区：不分区（死信量小，§5.2「量小不分区」）；保留期 180 天，清理属
-- 管道 sweeper（§5.4）。
CREATE TABLE notify_dlq (
    id            uuid        NOT NULL,           -- 死信行标识（= 转死信的 dispatch id）
    dispatch_id   uuid        NOT NULL,           -- 死信来源发送任务（notify_dispatch.id）
    event_id      uuid        NOT NULL,           -- 源事件（重放时引用它生成新 dispatch）
    channel_id    uuid        NOT NULL,           -- 投递渠道实例（notify_channel.id）
    recipient     varchar(255) NOT NULL,          -- 收件目标（邮箱/群标识/webhook url/用户ID）
    last_error    varchar(512),                   -- 转死信时的最近失败原因（可空）
    attempts      int         NOT NULL,           -- 累计尝试次数（= 重试上限 5 + 1）
    replay_state  smallint    NOT NULL DEFAULT 1, -- 1 none / 2 pending（已标记待重放）/ 3 replayed（worker 已重放）
    create_time   timestamptz NOT NULL DEFAULT now(),
    update_time   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

COMMENT ON TABLE notify_dlq IS '通知死信表（运行表）：sender 写入 / core 仅标记重放；量小不分区，保留期 180 天（清理属管道 sweeper）';
COMMENT ON COLUMN notify_dlq.id IS '死信行标识（UUIDv7）';
COMMENT ON COLUMN notify_dlq.dispatch_id IS '死信来源发送任务标识（notify_dispatch.id）';
COMMENT ON COLUMN notify_dlq.event_id IS '源事件标识（重放时 dlq worker 引用它生成新 dispatch）';
COMMENT ON COLUMN notify_dlq.channel_id IS '投递渠道实例标识（notify_channel.id）';
COMMENT ON COLUMN notify_dlq.recipient IS '收件目标：邮箱/群标识/webhook url/用户ID';
COMMENT ON COLUMN notify_dlq.last_error IS '转死信时的最近失败原因（可空）';
COMMENT ON COLUMN notify_dlq.attempts IS '累计尝试次数（重试上限 5 次后转入）';
COMMENT ON COLUMN notify_dlq.replay_state IS '重放状态：1 none / 2 pending（控制台已标记）/ 3 replayed（dlq worker 已重放）';
COMMENT ON COLUMN notify_dlq.create_time IS '转入死信时间';
COMMENT ON COLUMN notify_dlq.update_time IS '最近状态变更时间（core 标记重放 / worker 重放均推进）';

-- dlq worker 轮询部分索引：仅 pending 行（§4.7 轮询 WHERE replay_state=2）；
-- 对齐 event/dispatch 的 pending 部分索引先例，量小近似零成本
CREATE INDEX idx_notify_dlq_pending ON notify_dlq (update_time) WHERE replay_state = 2;
