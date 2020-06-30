BEGIN;
	CREATE TABLE post_like (
		post	BIGINT NOT NULL REFERENCES post ON DELETE CASCADE,
		person BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		local BOOLEAN NOT NULL,
		ap_id TEXT,
		PRIMARY KEY (post, person)
	);

	CREATE TABLE reply_like (
		reply BIGINT NOT NULL REFERENCES reply ON DELETE CASCADE,
		person BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		local BOOLEAN NOT NULL,
		ap_id TEXT,
		PRIMARY KEY (reply, person)
	);
COMMIT;