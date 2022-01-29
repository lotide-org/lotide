BEGIN;
	ALTER TABLE post ADD COLUMN updated_local TIMESTAMPTZ;
	UPDATE post SET updated_local='epoch';
	ALTER TABLE post ALTER COLUMN updated_local SET NOT NULL;
COMMIT;
