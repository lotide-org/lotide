BEGIN;
	ALTER TABLE flag ADD COLUMN to_community_dismissed BOOLEAN NOT NULL DEFAULT FALSE;
	ALTER TABLE flag ADD COLUMN to_site_admin_dismissed BOOLEAN NOT NULL DEFAULT FALSE;
COMMIT;
