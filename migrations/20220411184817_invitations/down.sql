BEGIN;
	DROP TABLE invitation;
	
	ALTER TABLE site DROP COLUMN allow_invitations;
	ALTER TABLE site DROP COLUMN users_create_invitations;
COMMIT;
