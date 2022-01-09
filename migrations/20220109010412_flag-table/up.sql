BEGIN;
	CREATE TABLE flag (
		id BIGSERIAL PRIMARY KEY,
		kind TEXT NOT NULL,
		person BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		post BIGINT REFERENCES post ON DELETE CASCADE,
		content_text TEXT,
		to_community BOOLEAN NOT NULL,
		to_site_admin BOOLEAN,
		to_remote_site_admin BOOLEAN NOT NULL,
		created_local TIMESTAMPTZ NOT NULL,
		local BOOLEAN NOT NULL,
		ap_id TEXT UNIQUE
	);
	INSERT INTO flag SELECT id, 'post', person, post, content_text, to_community, to_site_admin, to_remote_site_admin, created_local, local, ap_id FROM post_flag;
	DROP TABLE post_flag;
COMMIT;
