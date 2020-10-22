BEGIN;
	DROP INDEX post_author_idx;
	DROP INDEX reply_author_idx;
COMMIT;
