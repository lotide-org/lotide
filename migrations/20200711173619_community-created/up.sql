ALTER TABLE community ADD COLUMN created_by BIGINT REFERENCES person;
