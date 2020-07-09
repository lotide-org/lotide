BEGIN;
	ALTER TABLE community_follow DROP COLUMN accepted;
COMMIT;
