BEGIN;
	ALTER TABLE post ADD COLUMN cached_likes_for_sort INTEGER NOT NULL DEFAULT 0;
	UPDATE post SET cached_likes_for_sort=(SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author);
COMMIT;
