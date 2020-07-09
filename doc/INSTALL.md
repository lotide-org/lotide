# lotide installation
## For production
Requirements: rustc, cargo, openssl, [Migrant](https://github.com/jaemk/migrant), and a PostgreSQL database

Set these environment variables:
 - PGDATABASE, PGHOST, PGUSER, PGPASSWORD - Credentials for database connection, used by Migrant
 - DATABASE_URL - Same credentials again, should be equivalent to `postgres://$PGUSER:$PGPASSWORD@$PGHOST/$PGDATABASE`. Used by lotide itself.
 - HOST_URL_ACTIVITYPUB - If using the recommended proxy setup, set this to your root address (`https://example.com`)
 - HOST_URL_API - e.g. `https://example.com/api`
 - APUB_PROXY_REWRITES - Set to `true` to make signatures work with the proxy setup.
 - BACKEND_HOST - (for hitide only) Set this to a URL which hitide can use to reach lotide

To build lotide, run `cargo build --release` in the lotide directory. A `lotide` binary will appear in `./target/release`.

hitide can be built the same way, except that it currently requires a nightly version of Rust.

To set up the database, run `migrant setup`, then `migrant apply -a`.

### Recommended proxy setup:
This is written for Nginx, but it should be possible to adapt it to other proxy servers. Replace `c_backend_1` and `c_hitide_1` with your actual hostnames.

```
location /api {
	proxy_pass http://c_backend_1:3333;
}
location /apub {
	proxy_pass http://c_backend_1:3333;
	proxy_set_header Host $host;
	proxy_set_header X-Forwarded-Path $request_uri;
}
location /.well-known {
	proxy_pass http://c_backend_1:3333;
}
location / {
	if ($http_accept = application/activity+json) {
		rewrite ^(.*)$ /apub$1;
	}
	if ($http_content_type = application/activity+json) {
		rewrite ^(.*)$ /apub$1;
	}
	proxy_pass http://c_hitide_1:4333;
}
```

## For development
Requirements: rustc, cargo, openssl, [Migrant](https://github.com/jaemk/migrant), and a PostgreSQL database

The following environment variables are required (*An environment variable manager like [direnv](https://direnv.net/) is recommended in order to avoid these variables interfering with other programs.*)
 - PGDATABASE, PGHOST, PGUSER, PGPASSWORD - Credentials for database connection, used by Migrant
 - DATABASE_URL - Same credentials again, should be equivalent to `postgres://$PGUSER:$PGPASSWORD@$PGHOST/$PGDATABASE`. Used by lotide itself.
 - HOST_URL_ACTIVITYPUB - Typically `http://localhost:3333/apub` for dev instances
 - HOST_URL_API - Typically `http://localhost:3333/api` for dev instances

Run `migrant setup`, then `migrant apply -a` to update the database schema.

To build and run lotide, run `cargo run` in the lotide directory.
