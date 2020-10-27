BEGIN;
	CREATE INDEX post_fts ON post USING gin(to_tsvector('english', title || ' ' || COALESCE(content_text, content_markdown, content_html, '')));
COMMIT;
