BEGIN;
	ALTER TABLE community_follow ADD COLUMN accepted BOOLEAN;
	UPDATE community_follow SET accepted=TRUE; -- Assume that all existing follows are accepted, since we don't know
	ALTER TABLE community_follow ALTER COLUMN accepted SET NOT NULL;
COMMIT;
