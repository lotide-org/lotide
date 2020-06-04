CREATE TABLE community_follow (
	community BIGINT REFERENCES community,
	follower BIGINT REFERENCES person,

	PRIMARY KEY (community, follower)
);
