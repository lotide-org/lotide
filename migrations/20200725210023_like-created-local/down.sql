BEGIN;
	ALTER TABLE post_like DROP COLUMN created_local;
	ALTER TABLE reply_like DROP COLUMN created_local;
COMMIT;
