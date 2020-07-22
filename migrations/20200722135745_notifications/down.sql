BEGIN;
	ALTER TABLE person DROP COLUMN last_checked_notifications;
	DROP TABLE notification;
COMMIT;
