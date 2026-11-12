DROP INDEX IF EXISTS idx_notify_dispatch_unpublished;
ALTER TABLE notify_dispatch DROP COLUMN IF EXISTS publish_state;
