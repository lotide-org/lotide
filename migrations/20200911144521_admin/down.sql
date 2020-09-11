BEGIN;
	ALTER TABLE person DROP COLUMN is_site_admin;
COMMIT;
