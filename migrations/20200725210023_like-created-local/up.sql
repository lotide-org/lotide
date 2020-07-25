BEGIN;
	ALTER TABLE post_like ADD COLUMN created_local TIMESTAMPTZ DEFAULT (current_timestamp);
	ALTER TABLE reply_like ADD COLUMN created_local TIMESTAMPTZ DEFAULT (current_timestamp);
COMMIT;
