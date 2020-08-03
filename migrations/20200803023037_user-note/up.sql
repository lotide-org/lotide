BEGIN;
	CREATE TABLE person_note (
		author BIGINT REFERENCES person,
		target BIGINT REFERENCES person,
		content_text TEXT NOT NULL,
		PRIMARY KEY (author, target)
	);
COMMIT;
