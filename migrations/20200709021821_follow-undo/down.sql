BEGIN;
	DROP TABLE local_community_follow_undo;
	ALTER TABLE community_follow DROP COLUMN ap_id;
	ALTER TABLE community_follow DROP COLUMN local;
COMMIT;
