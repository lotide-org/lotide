BEGIN;
	ALTER TABLE community DROP COLUMN ap_outbox;
COMMIT;
