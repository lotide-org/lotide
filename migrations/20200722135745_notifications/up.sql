BEGIN;
	CREATE TABLE notification (
		kind TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		to_user BIGINT NOT NULL REFERENCES person,
		reply BIGINT REFERENCES reply,
		parent_reply BIGINT REFERENCES reply,
		parent_post BIGINT REFERENCES post
	);

	ALTER TABLE person ADD COLUMN last_checked_notifications TIMESTAMPTZ NOT NULL DEFAULT ('epoch');
COMMIT;
