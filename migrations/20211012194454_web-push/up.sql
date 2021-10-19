BEGIN;
	CREATE TABLE person_notification_subscription (
		id BIGSERIAL PRIMARY KEY,
		person BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
		endpoint TEXT NOT NULL,
		p256dh_key TEXT NOT NULL,
		auth_key TEXT NOT NULL
	);
	CREATE INDEX ON person_notification_subscription (person);

	ALTER TABLE site ADD COLUMN vapid_private_key BYTEA;
COMMIT;
