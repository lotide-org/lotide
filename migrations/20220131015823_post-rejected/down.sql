BEGIN;
	ALTER TABLE post DROP COLUMN rejected;
	ALTER TABLE post DROP COLUMN rejected_ap_id;
COMMIT;
