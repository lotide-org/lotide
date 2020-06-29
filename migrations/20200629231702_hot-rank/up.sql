CREATE FUNCTION hot_rank(score BIGINT, created TIMESTAMPTZ) RETURNS FLOAT AS $$
	BEGIN
		RETURN (1000000 * (score + 1) / ((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM created)) ^ 1.8));
	END;
$$ LANGUAGE plpgsql;
