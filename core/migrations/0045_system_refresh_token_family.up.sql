-- 0045_system_refresh_token_family.up.sql 认证域：刷新令牌族 + 轮换墓碑（重放检测）。
-- 轮换不再物理删除旧行，而是置 rotated_at 保留为墓碑直至原过期时间；同一次
-- 登录派生的全部令牌共享 family_id。出示宽限期之外的墓碑 = 令牌被盗后重放，
-- 整族吊销（攻击者换发的后继令牌一并失效）。
-- family_id 可空：NULL 等价于「族 = 自身 id」（存量行与迁移窗口内旧版本进程
-- 写入的行），读取处统一 COALESCE(family_id, id)；存量行在此回填。

ALTER TABLE system_refresh_token ADD COLUMN IF NOT EXISTS family_id uuid;
ALTER TABLE system_refresh_token ADD COLUMN IF NOT EXISTS rotated_at timestamptz;

UPDATE system_refresh_token SET family_id = id WHERE family_id IS NULL;

COMMENT ON TABLE system_refresh_token IS '刷新令牌表：SHA-256 hex 落库；轮换置 rotated_at 留墓碑（重放检测），登出/删号/过期物理删除';
COMMENT ON COLUMN system_refresh_token.family_id IS '令牌族 ID：登录签发时等于自身 id，轮换后继承；NULL 视同自身 id';
COMMENT ON COLUMN system_refresh_token.rotated_at IS '轮换时间；非空即墓碑（已失效，仅用于重放检测），保留至 expire_time';

CREATE INDEX IF NOT EXISTS idx_system_refresh_token_family_id ON system_refresh_token (family_id);
