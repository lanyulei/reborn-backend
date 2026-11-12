REVOKE SELECT ON notify_channel_health FROM notify_core_role;
ALTER TABLE notify_silence DROP COLUMN IF EXISTS description;
ALTER TABLE notify_silence DROP COLUMN IF EXISTS name;
ALTER TABLE notify_route DROP COLUMN IF EXISTS description;
ALTER TABLE notify_route DROP COLUMN IF EXISTS name;
