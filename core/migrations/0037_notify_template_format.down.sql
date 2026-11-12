ALTER TABLE notify_template DROP CONSTRAINT IF EXISTS ck_notify_template_format;
ALTER TABLE notify_template DROP COLUMN IF EXISTS format;
