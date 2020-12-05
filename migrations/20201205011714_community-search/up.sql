BEGIN;
	CREATE FUNCTION community_fts(community community) RETURNS tsvector AS $$
		BEGIN
			RETURN setweight(to_tsvector('english', community.name), 'A') ||
				setweight(to_tsvector('english', community.description), 'D');
		END;
	$$ LANGUAGE plpgsql IMMUTABLE;
	CREATE INDEX community_fts ON community USING gin(community_fts(community));
COMMIT;
