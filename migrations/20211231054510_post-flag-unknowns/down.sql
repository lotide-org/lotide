BEGIN;
	ALTER TABLE post_flag ALTER COLUMN to_site_admin SET NOT NULL;
COMMIT;
