#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| { for _ in influxdb_line_protocol::parse_lines(data) {} });
