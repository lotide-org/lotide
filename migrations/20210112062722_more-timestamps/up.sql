BEGIN;
	ALTER TABLE community ADD COLUMN created_local TIMESTAMPTZ;
	ALTER TABLE community_moderator ADD COLUMN created_local TIMESTAMPTZ;
COMMIT;
