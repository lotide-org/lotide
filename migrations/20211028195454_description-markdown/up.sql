BEGIN;
	ALTER TABLE community ADD COLUMN description_markdown TEXT;
	ALTER TABLE community ALTER COLUMN description DROP NOT NULL;

	ALTER TABLE site ADD COLUMN description_markdown TEXT;
	ALTER TABLE site ADD COLUMN description_html TEXT;
	ALTER TABLE site ALTER COLUMN description DROP NOT NULL;

	ALTER TABLE person ADD COLUMN description_markdown TEXT;
	ALTER TABLE person ALTER COLUMN description DROP NOT NULL;
COMMIT;
