BEGIN;
	ALTER TABLE post ADD COLUMN content_markdown TEXT;
	ALTER TABLE reply ADD COLUMN content_markdown TEXT;
COMMIT;
