# lotide installation
## For production
Requirements: rustc (>=1.45.0), cargo, openssl, and a PostgreSQL database

Set these environment variables:
 - DATABASE_URL - Credentials for database connection.
 - HOST_URL_ACTIVITYPUB - If using the recommended proxy setup, set this to your root address (`https://example.com`)
 - HOST_URL_API - e.g. `https://example.com/api`
 - APUB_PROXY_REWRITES - Set to `true` to make signatures work with the proxy setup.
 - ALLOW_FORWARDED - Set to `true` to make ratelimiting work with the proxy setup.
 - BACKEND_HOST - (for hitide only) Set this to a URL which hitide can use to reach lotide

Optionally (but recommended):
 - SMTP_URL - URL used to access SMTP server, required for sending email (e.g. `smtps://username:password@smtp.example.com`)
 - SMTP_FROM - From value used in sent emails, required for sending email
 - MEDIA_LOCATION - Directory on disk used for storing uploaded images. If not set, image uploads will be disabled.

To build lotide, run `cargo build --release` in the lotide directory. A `lotide` binary will appear in `./target/release`.

hitide can be built in the same way.

To set up the database, run `lotide migrate setup`, then `lotide migrate`.

### Recommended proxy setup:
This is written for Nginx, but it should be possible to adapt it to other proxy servers. Replace `c_backend_1` and `c_hitide_1` with your actual hostnames.

```
client_max_body_size 1G;
proxy_set_header X-Forwarded-For $remote_addr;

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
	if ($http_accept ~* "application/activity\+json") {
		rewrite ^(.*)$ /apub$1;
	}
	if ($http_content_type = application/activity+json) {
		rewrite ^(.*)$ /apub$1;
	}
	proxy_pass http://c_hitide_1:4333;
}
```

## For development
Requirements: rustc, cargo, openssl, and a PostgreSQL database

The following environment variables are required (*An environment variable manager like [direnv](https://direnv.net/) is recommended in order to avoid these variables interfering with other programs.*)
 - DATABASE_URL - Credentials for database connection.
 - HOST_URL_ACTIVITYPUB - Typically `http://localhost:3333/apub` for dev instances
 - HOST_URL_API - Typically `http://localhost:3333/api` for dev instances

Run `cargo run -- migrate setup`, then `cargo run -- migrate` to update the database schema.

To build and run lotide, run `cargo run` in the lotide directory.

Note that lotide itself does not contain a frontend, so you probably want to also setup [hitide](https://git.sr.ht/~vpzom/hitide).
