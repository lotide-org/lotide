BEGIN;
	ALTER TABLE post DROP CONSTRAINT post_ap_id_key;
	ALTER TABLE community DROP CONSTRAINT community_ap_id_key;
	ALTER TABLE person DROP CONSTRAINT person_ap_id_key;
COMMIT;
