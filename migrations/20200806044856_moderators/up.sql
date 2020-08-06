BEGIN;
	CREATE TABLE community_moderator (
		community BIGINT REFERENCES community,
		person BIGINT REFERENCES person,

		PRIMARY KEY (community, person)
	);

	INSERT INTO community_moderator (community, person) SELECT id, created_by FROM community WHERE created_by IS NOT NULL;
COMMIT;
