FROM rustlang/rust:nightly AS builder
WORKDIR /app
RUN rustup default nightly-2025-11-11
RUN apt-get update && apt-get install -y lld && rm -rf /var/lib/apt/lists/*
COPY src/ src/
COPY Cargo.toml .
COPY Cargo.lock .

RUN RUSTFLAGS="-C target-cpu=native -C link-arg=-fuse-ld=lld" cargo build --release

FROM gcr.io/distroless/cc-debian12
COPY --from=builder /app/target/release/server /minimarket-server

ENTRYPOINT ["/minimarket-server", "0.0.0.0:8000"]
