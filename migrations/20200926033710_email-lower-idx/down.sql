BEGIN;
	DROP INDEX person_lower_email_address_idx;
	ALTER TABLE person ADD CONSTRAINT person_email_address_key UNIQUE (email_address);
COMMIT;
