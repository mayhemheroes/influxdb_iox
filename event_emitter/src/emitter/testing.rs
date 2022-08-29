//! Special emitter for testing purposes.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::event::Event;

use super::EventEmitter;

type SharedEvents = Arc<Mutex<Vec<Event<&'static str>>>>;

/// Test event emitter.
#[derive(Debug)]
pub struct TestEventEmitter {
    events: SharedEvents,
}

impl TestEventEmitter {
    /// Create new emitter and a receiver that can be used to read the events.
    pub fn create() -> (Self, TestEventReceiver) {
        let events: SharedEvents = Default::default();

        let this = Self {
            events: Arc::clone(&events),
        };
        let receiver = TestEventReceiver { events };

        (this, receiver)
    }
}

#[async_trait]
impl EventEmitter for TestEventEmitter {
    async fn emit(&mut self, mut events: Vec<Event<&'static str>>) {
        self.events.lock().append(&mut events);
    }
}

/// Receiver for [`TestEventEmitter`].
#[derive(Debug, Clone)]
pub struct TestEventReceiver {
    events: SharedEvents,
}

impl TestEventReceiver {
    /// Read all "emitted" events.
    ///
    /// This does NOT clear the events. So this vector will only grow over time.
    pub fn read(&self) -> Vec<Event<&'static str>> {
        self.events.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use iox_time::Time;

    use super::*;

    #[tokio::test]
    async fn test() {
        let e1 = Event::new("m1", Time::from_timestamp_nanos(1))
            .add_tag_move("foo", "bar")
            .add_field_move("i", 1u64);
        let e2 = Event::new("m1", Time::from_timestamp_nanos(1))
            .add_tag_move("foo", "x")
            .add_field_move("i", 2u64);
        let e3 = Event::new("m2", Time::from_timestamp_nanos(2))
            .add_tag_move("hello", "world")
            .add_field_move("j", 1u64);

        let (mut emitter, receiver) = TestEventEmitter::create();
        assert_eq!(receiver.read(), vec![]);

        emitter.emit(vec![e1.clone(), e2.clone()]).await;
        emitter.emit(vec![]).await;
        emitter.emit(vec![e1.clone(), e3.clone()]).await;
        assert_eq!(
            receiver.read(),
            vec![e1.clone(), e2.clone(), e1.clone(), e3.clone()]
        );
    }
}
