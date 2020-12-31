BEGIN;
	ALTER TABLE person DROP COLUMN description_html;
	ALTER TABLE community DROP COLUMN description_html;
COMMIT;
