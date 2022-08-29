# Event Emitter
Tooling to emit InfluxDB-style events.

This crate should be used to emit fine-grained events that can be filtered. The application is mostly statistical
monitoring. You MUST NOT rely on events being delivered!

## Usage

```no_run
# async fn f() {
use std::sync::Arc;

use backoff::BackoffConfig;
use event_emitter::{
    data_provider::process::ProcessEventDataProvider,
    driver::EventDriver,
    emitter::{
        influxdb::InfluxDBEventEmitter,
    },
    measurement,
};
use iox_time::SystemProvider;
use tokio::runtime::Handle;

// create typed measurement
measurement!(RequestMeasurement, request);

// set up event driver
let time_provider = SystemProvider::new();
let driver = EventDriver::new(
    // data providers to add default tags/fields to events
    vec![
        Box::new(ProcessEventDataProvider::new(
            // git hash
            "c3e531aac0e8839ce8187e46de777cb021ce2adb",
            // process UUID
            "ec6734e1-c763-4e8a-92ca-b5cabe42cf0f",
            // time provider
            &time_provider,
        )),
    ],
    // event emitters (i.e. where the events are sent to)
    Box::new(
        InfluxDBEventEmitter::new(
            "https://my-influx.local:1234",
            "my_db".to_owned(),
            BackoffConfig::default(),
        )
        .await
        .unwrap()
    ),
    // time provider
    Arc::new(time_provider),
    // handle to current tokio runtime
    &Handle::current(),
);

// record event
driver.record::<RequestMeasurement>()
    .add_tag_mut("method", "GET")
    .add_tag_mut("path", "/query")
    .add_field_mut("request_size_bytes", 1024u64);
# }
```
