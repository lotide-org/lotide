BEGIN;
	ALTER TABLE community ADD COLUMN ap_outbox TEXT;
COMMIT;
