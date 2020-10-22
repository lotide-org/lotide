BEGIN;
	ALTER TABLE community_follow DROP CONSTRAINT community_follow_follower_fkey;
	ALTER TABLE community_follow ADD FOREIGN KEY (follower) REFERENCES person;

	ALTER TABLE community_moderator DROP CONSTRAINT community_moderator_person_fkey;
	ALTER TABLE community_moderator ADD FOREIGN KEY (person) REFERENCES person;

	ALTER TABLE forgot_password_key DROP CONSTRAINT forgot_password_key_person_fkey;
	ALTER TABLE forgot_password_key ADD FOREIGN KEY (person) REFERENCES person;

	ALTER TABLE login DROP CONSTRAINT login_person_fkey;
	ALTER TABLE login ADD FOREIGN KEY (person) REFERENCES person;

	ALTER TABLE media DROP CONSTRAINT media_person_fkey;
	ALTER TABLE media ADD FOREIGN KEY (person) REFERENCES person;

	ALTER TABLE notification DROP CONSTRAINT notification_to_user_fkey;
	ALTER TABLE notification ADD FOREIGN KEY (to_user) REFERENCES person;
COMMIT;
