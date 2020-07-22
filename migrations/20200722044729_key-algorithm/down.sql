BEGIN;
	ALTER TABLE person DROP COLUMN public_key_sigalg;
	ALTER TABLE community DROP COLUMN public_key_sigalg;
COMMIT;
