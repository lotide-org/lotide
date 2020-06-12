BEGIN;
	ALTER TABLE person ADD COLUMN private_key BYTEA;
	ALTER TABLE person ADD COLUMN public_key BYTEA;
	ALTER TABLE community ADD COLUMN private_key BYTEA;
	ALTER TABLE community ADD COLUMN public_key BYTEA;
COMMIT;
