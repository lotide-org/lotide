BEGIN;
	DROP TABLE person_notification_subscription;
	ALTER TABLE site DROP COLUMN vapid_private_key;
COMMIT;
