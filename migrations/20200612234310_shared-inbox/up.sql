BEGIN;
	ALTER TABLE person ADD COLUMN ap_shared_inbox TEXT;
COMMIT;
