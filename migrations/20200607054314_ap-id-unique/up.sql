BEGIN;
	ALTER TABLE post ADD UNIQUE (ap_id);
	ALTER TABLE community ADD UNIQUE (ap_id);
	ALTER TABLE person ADD UNIQUE (ap_id);
COMMIT;
