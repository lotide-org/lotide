BEGIN;
	DROP INDEX community_lower_idx;
	CREATE UNIQUE INDEX community_lower_idx ON community (LOWER(name)) WHERE local AND NOT deleted;
COMMIT;
