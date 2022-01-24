BEGIN;
	ALTER TABLE post DROP COLUMN poll_id;
	DROP TABLE poll_vote;
	DROP TABLE poll_option;
	DROP TABLE poll;
COMMIT;
