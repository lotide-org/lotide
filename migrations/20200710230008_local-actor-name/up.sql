BEGIN;
	CREATE TABLE local_actor_name (
		name TEXT NOT NULL
	);
	CREATE UNIQUE INDEX ON local_actor_name (LOWER(name));

	INSERT INTO local_actor_name (name) SELECT username FROM person WHERE local;
	INSERT INTO local_actor_name (name) SELECT name FROM community WHERE local;
COMMIT;
