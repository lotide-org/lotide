BEGIN;
	ALTER TABLE post DROP COLUMN cached_likes_for_sort;
COMMIT;
