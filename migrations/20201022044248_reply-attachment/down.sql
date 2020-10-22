BEGIN;
	ALTER TABLE reply DROP COLUMN attachment_href;
COMMIT;
