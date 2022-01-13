BEGIN;
	ALTER TABLE post_flag DROP CONSTRAINT post_flag_ap_id_key;
COMMIT;
