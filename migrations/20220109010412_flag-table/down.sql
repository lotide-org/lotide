BEGIN;
	CREATE TABLE post_flag (
		id BIGSERIAL PRIMARY KEY,
		person BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		post BIGINT NOT NULL REFERENCES post ON DELETE CASCADE,
		content_text TEXT,
		to_community BOOLEAN NOT NULL,
		to_site_admin BOOLEAN,
		to_remote_site_admin BOOLEAN NOT NULL,
		created_local TIMESTAMPTZ NOT NULL,
		local BOOLEAN NOT NULL,
		ap_id TEXT UNIQUE
	);
	INSERT INTO post_flag SELECT id, person, post, content_text, to_community, to_site_admin, to_remote_site_admin, created_local, local, ap_id FROM flag WHERE kind='post';
	DROP TABLE flag;
COMMIT;
