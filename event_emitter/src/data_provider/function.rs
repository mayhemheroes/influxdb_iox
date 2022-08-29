//! Provide data to an event using a simple function.

use std::fmt::Debug;

use crate::event::Event;

use super::EventDataProvider;

type BoxedFun = Box<dyn for<'a> Fn(&'a mut Event<&'static str>) + Send + Sync>;

/// Event data provider based on a function.
pub struct FunctionEventDataProvider {
    f: BoxedFun,
}

impl FunctionEventDataProvider {
    /// Create new provider from a function.
    pub fn new<F>(f: F) -> Self
    where
        F: for<'a> Fn(&'a mut Event<&'static str>) + Send + Sync + 'static,
    {
        Self { f: Box::new(f) }
    }
}

impl Debug for FunctionEventDataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionEventDataProvider")
            .field("f", &"<function>")
            .finish()
    }
}

impl EventDataProvider for FunctionEventDataProvider {
    fn enrich(&self, event: &mut Event<&'static str>) {
        (self.f)(event);
    }
}

#[cfg(test)]
mod tests {
    use iox_time::Time;

    use super::*;

    #[test]
    fn test() {
        let provider = FunctionEventDataProvider::new(|event| {
            event.add_tag_mut("foo", "bar");
        });

        let mut event = Event::new("m", Time::from_timestamp_nanos(0));

        let expected = event.clone().add_tag_move("foo", "bar");
        provider.enrich(&mut event);
        assert_eq!(event, expected);
    }
}
