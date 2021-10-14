BEGIN;
	ALTER TABLE person_notification_subscription ADD COLUMN language TEXT;
COMMIT;
