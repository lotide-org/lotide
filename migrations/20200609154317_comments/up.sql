CREATE TABLE reply (
	id BIGSERIAL PRIMARY KEY,
	post BIGINT NOT NULL REFERENCES post ON DELETE CASCADE,
	parent BIGINT REFERENCES reply ON DELETE CASCADE,
	author BIGINT REFERENCES person ON DELETE CASCADE,
	content_text TEXT NOT NULL,
	created TIMESTAMPTZ NOT NULL,
	local BOOLEAN NOT NULL,
	ap_id TEXT UNIQUE
);
