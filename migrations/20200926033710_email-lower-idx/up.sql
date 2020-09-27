BEGIN;
	CREATE UNIQUE INDEX person_lower_email_address_idx ON person (LOWER(email_address)) WHERE local;
	ALTER TABLE person DROP CONSTRAINT person_email_address_key;
COMMIT;
