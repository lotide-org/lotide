BEGIN;
	ALTER TABLE community DROP COLUMN hide_posts_from_aggregates;
COMMIT;
