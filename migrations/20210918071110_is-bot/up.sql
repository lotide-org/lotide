BEGIN;
	ALTER TABLE person ADD COLUMN is_bot BOOLEAN NOT NULL DEFAULT (FALSE);
COMMIT;
