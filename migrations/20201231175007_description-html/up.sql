BEGIN;
	ALTER TABLE person ADD COLUMN description_html TEXT;
	ALTER TABLE community ADD COLUMN description_html TEXT;
COMMIT;
