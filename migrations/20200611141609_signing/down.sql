BEGIN;
	ALTER TABLE community DROP COLUMN public_key;
	ALTER TABLE community DROP COLUMN private_key;
	ALTER TABLE person DROP COLUMN public_key;
	ALTER TABLE person DROP COLUMN private_key;
COMMIT;
