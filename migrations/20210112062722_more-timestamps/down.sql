BEGIN;
	ALTER TABLE community DROP COLUMN created_local;
	ALTER TABLE community_moderator DROP COLUMN created_local;
COMMIT;
