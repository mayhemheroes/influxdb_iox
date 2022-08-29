//! Event emission to different storage backends.
use async_trait::async_trait;
use std::fmt::Debug;

use crate::event::Event;

pub mod influxdb;
pub mod testing;

/// Emit emitter interface.
#[async_trait]
pub trait EventEmitter: Debug + Send + 'static {
    /// Emit given batch of events.
    ///
    /// This method cannot fail. Retries must be handled by the emitter. If the emission fails, the event will be
    /// dropped (although the emitter is encouraged to issue a warning-level log in this case).
    async fn emit(&mut self, events: Vec<Event<&'static str>>);
}
