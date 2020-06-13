BEGIN;
	ALTER TABLE community DROP COLUMN ap_shared_inbox;
COMMIT;
