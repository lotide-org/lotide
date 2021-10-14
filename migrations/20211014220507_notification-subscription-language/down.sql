BEGIN;
	ALTER TABLE person_notification_subscription DROP COLUMN language;
COMMIT;
