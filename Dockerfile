FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /app

# Cache dependency compilation — only re-runs when Cargo.toml/Cargo.lock changes
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main(){}" > src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && rm -rf src

# Build the real binary
COPY src ./src
RUN cargo build --release --target x86_64-unknown-linux-musl

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM alpine:3.21

# ca-certificates is required for TLS (rediss://) connections
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 1000 bench

COPY --from=builder \
    /app/target/x86_64-unknown-linux-musl/release/sidekiq-bench \
    /usr/local/bin/sidekiq-bench

USER bench
ENTRYPOINT ["sidekiq-bench"]
