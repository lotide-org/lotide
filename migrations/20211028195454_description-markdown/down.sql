BEGIN;
	ALTER TABLE community DROP COLUMN description_markdown;
	ALTER TABLE community ALTER COLUMN description SET NOT NULL;

	ALTER TABLE site DROP COLUMN description_markdown;
	ALTER TABLE site DROP COLUMN description_html;
	ALTER TABLE site ALTER COLUMN description SET NOT NULL;

	ALTER TABLE person DROP COLUMN description_markdown;
	ALTER TABLE person ALTER COLUMN description SET NOT NULL;
COMMIT;
