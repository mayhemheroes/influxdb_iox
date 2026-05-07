FROM rustlang/rust:nightly AS builder

RUN cargo install cargo-fuzz

ADD . /src
RUN printf '[toolchain]\nchannel = "nightly"\n' > /src/rust-toolchain.toml

WORKDIR /src/influxdb_line_protocol/fuzz

RUN head -11 /src/workspace-hack/Cargo.toml > /tmp/wh.toml && mv /tmp/wh.toml /src/workspace-hack/Cargo.toml

RUN mkdir -p .cargo &&     cargo vendor vendor/ 2>/dev/null > .cargo/config.toml

RUN python3 /src/fix_ahash.py

RUN cargo fuzz build parse_lines &&     mv target/x86_64-unknown-linux-gnu/release/parse_lines /parse_lines

RUN export RUSTFLAGS="--cfg fuzzing -Clink-dead-code -Cdebug-assertions -C codegen-units=1" &&     cargo build --release --bin parse_lines &&     mv target/release/parse_lines /parse_lines_no_inst

FROM rustlang/rust:nightly
COPY --from=builder /parse_lines /parse_lines_no_inst /
