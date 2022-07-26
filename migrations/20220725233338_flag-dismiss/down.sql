BEGIN;
	ALTER TABLE flag DROP COLUMN to_community_dismissed;
	ALTER TABLE flag DROP COLUMN to_site_admin_dismissed;
COMMIT;
