BEGIN;
	CREATE TYPE lt_task_state AS ENUM ('pending', 'running', 'completed', 'failed');

	CREATE TABLE task (
		id BIGSERIAL PRIMARY KEY,
		state lt_task_state NOT NULL DEFAULT ('pending'),
		kind TEXT NOT NULL,
		params JSON NOT NULL,
		max_attempts SMALLINT NOT NULL,
		attempts SMALLINT NOT NULL DEFAULT (0),
		latest_error TEXT,
		created_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ
	);
COMMIT;
