//! Process-level event data.

use iox_time::{Time, TimeProvider};

use crate::event::Event;

use super::EventDataProvider;

/// Event data provider for process-wide information.
#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ProcessEventDataProvider {
    t_start: Time,
    git_hash: &'static str,
    process_uuid: &'static str,
}

impl ProcessEventDataProvider {
    /// Create new provider.
    pub fn new(
        git_hash: &'static str,
        process_uuid: &'static str,
        time_provider: &dyn TimeProvider,
    ) -> Self {
        Self {
            t_start: time_provider.now(),
            git_hash,
            process_uuid,
        }
    }
}

impl EventDataProvider for ProcessEventDataProvider {
    fn enrich(&self, event: &mut Event<&'static str>) {
        event.add_tag_mut("process_git_hash", self.git_hash);
        event.add_tag_mut("process_uuid", self.process_uuid);
        event.add_field_mut(
            "process_uptime_seconds",
            (event.time() - self.t_start).as_secs_f64(),
        );
    }
}

#[cfg(test)]
mod tests {
    use iox_time::MockProvider;

    use super::*;

    #[test]
    fn test() {
        let time_provider = MockProvider::new(Time::from_timestamp_millis(0));
        let provider =
            ProcessEventDataProvider::new("foo_githash", "bar_processuuid", &time_provider);

        let mut e = Event::new("m", Time::from_timestamp_millis(1200));
        provider.enrich(&mut e);
        let expected = Event::new("m", Time::from_timestamp_millis(1200))
            .add_tag_move("process_git_hash", "foo_githash")
            .add_tag_move("process_uuid", "bar_processuuid")
            .add_field_move("process_uptime_seconds", 1.2f64);
        assert_eq!(e, expected);
    }
}
