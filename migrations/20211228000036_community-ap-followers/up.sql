BEGIN;
	ALTER TABLE community ADD COLUMN ap_followers TEXT;
COMMIT;
