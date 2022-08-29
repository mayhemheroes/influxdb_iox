//! Provider for default tags and fields for events.

use crate::event::Event;
use std::fmt::Debug;

pub mod function;
pub mod process;

/// Provider for tags and fields on an event.
pub trait EventDataProvider: Debug + Send + Sync + 'static {
    /// Add tags or fields to the given event.
    fn enrich(&self, event: &mut Event<&'static str>);
}
