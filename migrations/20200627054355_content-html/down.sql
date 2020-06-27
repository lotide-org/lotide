BEGIN;
	ALTER TABLE post DROP COLUMN content_html;

	ALTER TABLE reply DROP COLUMN content_html;
	ALTER TABLE reply ALTER COLUMN content_text SET NOT NULL;
COMMIT;
