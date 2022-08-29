//! Event emission to InfluxDB.
use std::{ops::ControlFlow, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use influxdb_iox_client::{connection::Builder, error::Error as ClientError, write::Client};
use influxdb_line_protocol::LineProtocolBuilder;
use observability_deps::tracing::{debug, warn};
use snafu::{ResultExt, Snafu};

pub use influxdb_iox_client::connection::Error as ConnectionError;
use tokio::sync::Mutex;

use crate::event::{Event, FieldValue};

use super::EventEmitter;

/// Error for InfluxDB event emitter.
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Cannot create emitter: {source}"))]
    Creation { source: ConnectionError },
}

/// Emitter that sends events to InfluxDB.
#[derive(Debug)]
pub struct InfluxDBEventEmitter {
    // technically the client isn't shared, but the retry logic requires an `FnMut` which cannot return references, so
    // we need to trick a bit
    client: Arc<Mutex<Client>>,
    db_name: String,
    backoff_config: BackoffConfig,
}

impl InfluxDBEventEmitter {
    /// Create new emitter for the given endpoint and DB name.
    pub async fn new(
        endpoint: &str,
        db_name: String,
        backoff_config: BackoffConfig,
    ) -> Result<Self, Error> {
        let connection = Builder::new()
            .build(endpoint)
            .await
            .context(CreationSnafu)?;
        let client = Arc::new(Mutex::new(Client::new(connection)));
        Ok(Self {
            client,
            db_name,
            backoff_config,
        })
    }
}

#[async_trait]
impl EventEmitter for InfluxDBEventEmitter {
    async fn emit(&mut self, events: Vec<Event<&'static str>>) {
        let mut builder = LineProtocolBuilder::new();
        for event in events {
            if event.fields().next().is_none() {
                debug!("Ignoring event w/o fields");
                continue;
            }

            let mut builder_inner = builder.measurement(*event.measurement());

            for (k, v) in event.tags() {
                builder_inner = builder_inner.tag(k, v);
            }
            let mut field_it = event.fields();

            let mut builder_inner = match field_it.next() {
                Some((k, v)) => builder_inner.field(k, v),
                None => {
                    panic!("Just checked that there is at least 1 field");
                }
            };

            for (k, v) in field_it {
                builder_inner = builder_inner.field(k, v);
            }

            let builder_inner = builder_inner.timestamp(event.time().timestamp_nanos());

            builder = builder_inner.close_line();
        }

        let lp = builder.build();
        let lp = Arc::from(String::from_utf8(lp).expect("LP builder produces valid string"));

        let res = Backoff::new(&self.backoff_config)
            .retry_with_backoff("send LP to InfluxDB", || {
                let client = Arc::clone(&self.client);
                let db_name = self.db_name.clone();
                let lp = Arc::clone(&lp);
                async move {
                    let mut client = client.lock().await;
                    match client.write_lp(&db_name, lp, 0).await {
                        Ok(_) => ControlFlow::Break(Ok(())),
                        Err(e @ ClientError::Aborted(_)) => ControlFlow::Continue(e),
                        Err(e) => ControlFlow::Break(Err(e)),
                    }
                }
            })
            .await;
        if let Err(e) = res {
            warn!(
                %e,
                "Cannot emit events to InfluxDB",
            );
        }
    }
}

impl influxdb_line_protocol::builder::FieldValue for &FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldValue::I64(v) => <i64 as influxdb_line_protocol::builder::FieldValue>::fmt(v, f),
            FieldValue::U64(v) => <u64 as influxdb_line_protocol::builder::FieldValue>::fmt(v, f),
            FieldValue::F64(v) => <f64 as influxdb_line_protocol::builder::FieldValue>::fmt(v, f),
            FieldValue::Bool(v) => <bool as influxdb_line_protocol::builder::FieldValue>::fmt(v, f),
            FieldValue::String(v) => {
                <&str as influxdb_line_protocol::builder::FieldValue>::fmt(&v.as_ref(), f)
            }
        }
    }
}
