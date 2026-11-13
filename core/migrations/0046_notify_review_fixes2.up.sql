-- 0046 通知评审修复（二）：relay 认领租约列 + 生产者密钥列注释。
-- 1) relay_claim_until：nats 骨干下 relay 发布一批行时，此前在一个事务里 FOR UPDATE
--    SKIP LOCKED 锁行并等待 JetStream ack（至多 5s），行锁充当多副本互斥——事务与连接
--    在 NATS 等待期间一直占用。改为短语句盖认领租约（relay_claim_until = now() + 租约），
--    发布与等 ack 不在任何事务内，成功后第二条短语句置 publish_state = 1 并清租约；
--    租约到期仍未落账的行（relay 崩溃 / 发布失败退避）可被再次认领。NULL = 未被认领。
--    可空无默认值的列是纯元数据变更（不重写表）；在分区父表上 ADD COLUMN 自动下发到
--    全部分区（含 DEFAULT 与后续 ATTACH 的分区须同构，PARTITION OF 建的分区自动继承）。
--    不另建索引：认领扫描仍走 idx_notify_event_pending / idx_notify_dispatch_unpublished
--    （publish_state = 0 的在途行），租约只是其上的过滤条件，被认领的行至多一批 × 副本数。
ALTER TABLE notify_event ADD COLUMN IF NOT EXISTS relay_claim_until timestamptz;
ALTER TABLE notify_dispatch ADD COLUMN IF NOT EXISTS relay_claim_until timestamptz;
COMMENT ON COLUMN notify_event.relay_claim_until IS 'relay 认领租约到期时刻（nats 骨干）：未到期 = 某 relay 副本正在发布该行；NULL = 未认领';
COMMENT ON COLUMN notify_dispatch.relay_claim_until IS 'relay 认领租约到期时刻（nats 骨干）：未到期 = 某 relay 副本正在发布该行或处于发布失败退避；NULL = 未认领';
-- 2) notify_producer.token_hash 不再只存裸 SHA-256：配置了主密钥（notify.master.key）时
--    存 'sb1:' || AES-256-GCM 封存值（64 字节），存量 32 字节裸哈希仍被接受并在
--    创建 / 轮换 / 更新时换成封存形态。列类型不变（bytea），仅更新注释。
COMMENT ON COLUMN notify_producer.token_hash IS 'ingress HMAC 密钥 = SHA-256(token 明文)：''sb1:'' 前缀 = 主密钥封存（AES-256-GCM，64 字节）；32 字节裸哈希 = 存量/未配置主密钥；任何读接口不回显';
