-- 回滚前须先回退 notify 管道与 core（新版 relay 依赖 relay_claim_until；已封存的
-- token_hash 旧版管道无法识别——回退前请轮换或保留新版管道）。
COMMENT ON COLUMN notify_producer.token_hash IS 'token 的 SHA-256（32 字节）；token 明文仅签发响应返回一次，任何读接口不回显';
ALTER TABLE notify_dispatch DROP COLUMN IF EXISTS relay_claim_until;
ALTER TABLE notify_event DROP COLUMN IF EXISTS relay_claim_until;
