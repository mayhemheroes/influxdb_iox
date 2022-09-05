# Build Stage
FROM ghcr.io/evanrichter/cargo-fuzz:latest as builder

## Add source code to the build stage.
ADD . /src
WORKDIR /src

RUN echo building instrumented harnesses && \
    bash -c "pushd influxdb_line_protocol/fuzz && cargo +nightly -Z sparse-registry fuzz build && popd" && \
    mv influxdb_line_protocol/fuzz/target/x86_64-unknown-linux-gnu/release/parse_lines /parse_lines && \
    echo done

RUN echo building non-instrumented harnesses && \
    export RUSTFLAGS="--cfg fuzzing -Clink-dead-code -Cdebug-assertions -C codegen-units=1" && \
    bash -c "pushd influxdb_line_protocol/fuzz && cargo +nightly -Z sparse-registry build --release && popd" && \
    mv influxdb_line_protocol/fuzz/target/release/parse_lines /parse_lines_no_inst && \
    echo done

# Package Stage
FROM rustlang/rust:nightly

COPY --from=builder /parse_lines /parse_lines_no_inst /
