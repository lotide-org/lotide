BEGIN;
	ALTER TABLE media ADD COLUMN created TIMESTAMPTZ;
COMMIT;
