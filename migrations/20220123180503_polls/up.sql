BEGIN;
	CREATE TABLE poll (
		id BIGSERIAL PRIMARY KEY,
		multiple BOOLEAN NOT NULL
	);
	CREATE TABLE poll_option (
		id BIGSERIAL PRIMARY KEY,
		poll_id BIGINT NOT NULL REFERENCES poll ON DELETE CASCADE,
		name TEXT NOT NULL,
		position INTEGER,
		remote_vote_count INTEGER,
		UNIQUE (poll_id, name)
	);
	CREATE TABLE poll_vote (
		poll_id BIGINT REFERENCES poll ON DELETE CASCADE,
		option_id BIGINT REFERENCES poll_option ON DELETE CASCADE,
		person BIGINT REFERENCES person ON DELETE CASCADE,
		PRIMARY KEY (poll_id, option_id, person)
	);

	ALTER TABLE post ADD COLUMN poll_id BIGINT REFERENCES poll;
COMMIT;
