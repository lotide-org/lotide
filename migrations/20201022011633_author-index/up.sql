BEGIN;
	CREATE INDEX ON post (author);
	CREATE INDEX ON reply (author);
COMMIT;
