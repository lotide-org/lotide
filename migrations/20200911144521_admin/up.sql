BEGIN;
	ALTER TABLE person ADD COLUMN is_site_admin BOOLEAN NOT NULL DEFAULT (FALSE);
COMMIT;
