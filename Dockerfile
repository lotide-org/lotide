FROM alpine:3.12 AS builder
RUN apk add --no-cache cargo openssl-dev
WORKDIR /usr/src/lotide
COPY Cargo.* ./
COPY build.rs ./
COPY src ./src
COPY res ./res
COPY migrations ./migrations
RUN cargo build --release

FROM alpine:3.12
RUN apk add --no-cache libgcc openssl
COPY --from=builder /usr/src/lotide/target/release/lotide /usr/bin/
CMD ["lotide"]
