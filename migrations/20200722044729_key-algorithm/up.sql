BEGIN;
	ALTER TABLE person ADD COLUMN public_key_sigalg TEXT;
	ALTER TABLE community ADD COLUMN public_key_sigalg TEXT;
COMMIT;
