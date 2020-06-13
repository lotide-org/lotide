BEGIN;
	ALTER TABLE community ADD COLUMN ap_shared_inbox TEXT;
COMMIT;
