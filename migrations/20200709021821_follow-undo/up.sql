BEGIN;
	ALTER TABLE community_follow ADD COLUMN local BOOLEAN;
	UPDATE community_follow SET local=(SELECT local FROM person WHERE id=community_follow.follower);
	ALTER TABLE community_follow ALTER COLUMN local SET NOT NULL;

	ALTER TABLE community_follow ADD COLUMN ap_id TEXT;

	CREATE TABLE local_community_follow_undo (
		id UUID PRIMARY KEY,
		community BIGINT NOT NULL REFERENCES community ON DELETE CASCADE,
		follower BIGINT NOT NULL REFERENCES person ON DELETE CASCADE
	);
COMMIT;
