//! Main driver that controls event emission.

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use iox_time::TimeProvider;
use observability_deps::tracing::log::warn;
use tokio::{
    runtime::Handle,
    sync::mpsc::{channel, error::TrySendError, Sender},
    task::JoinHandle,
};

use crate::{
    data_provider::EventDataProvider, emitter::EventEmitter, event::Event,
    measurement::TypedMeasurement,
};

/// Internal message from [`EventDriver`]/[`EventRecorder`] to its background worker.
#[derive(Debug)]
enum Message {
    /// Event for emitter.
    Event(Event<&'static str>),

    /// Flush all batched/queued events and trigger oneshot AFTERWARDS.
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Main driver for event emission.
#[derive(Debug)]
pub struct EventDriver {
    _join_handle: JoinHandle<()>,
    tx: Arc<Sender<Message>>,
    data_providers: Vec<Box<dyn EventDataProvider>>,
    time_provider: Arc<dyn TimeProvider>,
}

impl EventDriver {
    /// Create new driver.
    pub fn new(
        data_providers: Vec<Box<dyn EventDataProvider>>,
        mut emitter: Box<dyn EventEmitter>,
        time_provider: Arc<dyn TimeProvider>,
        handle: &Handle,
    ) -> Self {
        let (tx, mut rx) = channel(1_000);
        let join_handle = handle.spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    Message::Event(event) => {
                        // TODO: batch events
                        emitter.emit(vec![event]).await;
                    }
                    Message::Flush(flush_tx) => {
                        // We do NOT care if the receiver is gone (e.g. because the flush method was cancelled).
                        flush_tx.send(()).ok();
                    }
                }
            }
        });

        Self {
            _join_handle: join_handle,
            tx: Arc::new(tx),
            data_providers,
            time_provider,
        }
    }

    /// Record new event.
    pub fn record<M>(&self) -> EventRecorder<M>
    where
        M: TypedMeasurement,
    {
        let mut event = Event::<&'static str>::new(M::default().into(), self.time_provider.now());

        for provider in &self.data_providers {
            provider.enrich(&mut event);
        }

        EventRecorder {
            event: Some(event.typed()),
            do_not_send: false,
            tx: Arc::clone(&self.tx),
        }
    }

    /// Flushes queued events.
    ///
    /// Events that are recorded (via [`EventRecorder::drop`]) AFTER calling this function are NOT flushed.
    pub async fn flush(&self) {
        let (flush_tx, flush_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Message::Flush(flush_tx))
            .await
            .expect("background worker alive");
        flush_rx.await.expect("background worker alive");
    }
}

/// Event recorder.
///
/// Sends recorded event on drop except [`do_not_send`](Self::do_not_send) was called.
#[derive(Debug)]
pub struct EventRecorder<M>
where
    M: TypedMeasurement,
{
    event: Option<Event<M>>,
    do_not_send: bool,
    tx: Arc<Sender<Message>>,
}

impl<M> EventRecorder<M>
where
    M: TypedMeasurement,
{
    /// Do NOT send this event.
    pub fn do_not_send(mut self) {
        self.do_not_send = true;
    }
}

impl<M> Deref for EventRecorder<M>
where
    M: TypedMeasurement,
{
    type Target = Event<M>;

    fn deref(&self) -> &Self::Target {
        self.event.as_ref().expect("not yet dropped")
    }
}

impl<M> DerefMut for EventRecorder<M>
where
    M: TypedMeasurement,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.event.as_mut().expect("not yet dropped")
    }
}

impl<M> Drop for EventRecorder<M>
where
    M: TypedMeasurement,
{
    fn drop(&mut self) {
        if !self.do_not_send {
            let event = self.event.take().expect("not yet dropped").untyped();
            match self.tx.try_send(Message::Event(event)) {
                Ok(()) => {}
                Err(TrySendError::Closed(_)) => {
                    panic!("Background worker died");
                }
                Err(TrySendError::Full(_)) => {
                    warn!("Event buffer full");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use iox_time::{MockProvider, Time};

    use crate::{
        data_provider::function::FunctionEventDataProvider, emitter::testing::TestEventEmitter,
        measurement,
    };

    use super::*;

    #[tokio::test]
    async fn test_send() {
        let (emitter, receiver) = TestEventEmitter::create();
        let driver = EventDriver::new(
            vec![Box::new(FunctionEventDataProvider::new(|e| {
                e.add_tag_mut("foo", "bar");
            }))],
            Box::new(emitter),
            Arc::new(MockProvider::new(Time::MIN)),
            &Handle::current(),
        );
        driver.record::<TestM>().add_tag_mut("my_tag", "xxx");

        driver.flush().await;

        let expected = Event::new("m", Time::MIN)
            .add_tag_move("foo", "bar")
            .add_tag_move("my_tag", "xxx");
        assert_eq!(receiver.read(), vec![expected]);
    }

    #[tokio::test]
    async fn test_do_not_send() {
        let (emitter, receiver) = TestEventEmitter::create();
        let driver = EventDriver::new(
            vec![],
            Box::new(emitter),
            Arc::new(MockProvider::new(Time::MIN)),
            &Handle::current(),
        );
        driver.record::<TestM>().do_not_send();

        driver.flush().await;

        assert_eq!(receiver.read(), vec![]);
    }

    measurement!(TestM, m);
}
