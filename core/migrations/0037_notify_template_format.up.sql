-- 0037 模板输出格式：text（缺省，现有模板全部落此）/ markdown / html / card。
-- 渠道按能力降级（kit/notifycontract.EffectiveFormat）：email 只认 html（其余按纯文本）；
-- 群机器人 card→卡片、markdown/html→markdown、text→text；webhook 原样透传 format 字段。
ALTER TABLE notify_template ADD COLUMN IF NOT EXISTS format varchar(16) NOT NULL DEFAULT 'text';
DO $$ BEGIN
    ALTER TABLE notify_template ADD CONSTRAINT ck_notify_template_format CHECK (format IN ('text', 'markdown', 'html', 'card'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
COMMENT ON COLUMN notify_template.format IS '输出格式 text/markdown/html/card；渠道按能力降级（email 仅 html 生效；机器人 card/markdown；webhook 透传）';
