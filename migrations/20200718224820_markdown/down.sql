BEGIN;
	ALTER TABLE post DROP COLUMN content_markdown;
	ALTER TABLE reply DROP COLUMN content_markdown;
COMMIT;
