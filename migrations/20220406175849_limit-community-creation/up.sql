BEGIN;
	ALTER TABLE site ADD COLUMN community_creation_requirement TEXT;
COMMIT;
