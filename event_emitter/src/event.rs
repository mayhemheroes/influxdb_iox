//! Event handling.
use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use iox_time::Time;

use crate::measurement::TypedMeasurement;

/// Value type for [`Event`] fields.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Signed integer.
    I64(i64),

    /// Unsigned integer.
    U64(u64),

    /// Float.
    F64(f64),

    /// Bool.
    Bool(bool),

    /// String.
    String(Cow<'static, str>),
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<u64> for FieldValue {
    fn from(v: u64) -> Self {
        Self::U64(v)
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<&'static str> for FieldValue {
    fn from(v: &'static str) -> Self {
        Self::String(v.into())
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        Self::String(v.into())
    }
}

/// Typed InfluxDB-style event.
#[derive(Debug, Clone, PartialEq)]
pub struct Event<M>
where
    M: Into<&'static str>,
{
    measurement: M,
    time: Time,
    tags: HashMap<&'static str, Cow<'static, str>>,
    fields: HashMap<&'static str, FieldValue>,
}

impl<M> Event<M>
where
    M: Into<&'static str>,
{
    /// Create new measurement.
    pub fn new(measurement: M, time: Time) -> Self {
        Self {
            measurement,
            time,
            tags: HashMap::default(),
            fields: HashMap::default(),
        }
    }

    /// Measurement.
    pub fn measurement(&self) -> &M {
        &self.measurement
    }

    /// Timestamp.
    pub fn time(&self) -> Time {
        self.time
    }

    /// Get all tags.
    ///
    /// The order of the elements is undefined!
    pub fn tags(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.tags.iter().map(|(k, v)| (*k, v.as_ref()))
    }

    /// Get all fields.
    ///
    /// The order of the elements is undefined!
    pub fn fields(&self) -> impl Iterator<Item = (&'static str, &FieldValue)> {
        self.fields.iter().map(|(k, v)| (*k, v))
    }

    /// Add new tag.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a tag called `"time"` is used.
    pub fn add_tag<V>(&mut self, name: &'static str, value: V)
    where
        V: Into<Cow<'static, str>>,
    {
        if name == "time" {
            panic!("Cannot use a tag called 'time'.");
        }

        if self.fields.contains_key(name) {
            panic!(
                "Cannot use tag named '{name}' because a field with the same name already exists"
            );
        }

        match self.tags.entry(name) {
            Entry::Vacant(v) => {
                v.insert(value.into());
            }
            Entry::Occupied(_) => {
                panic!("Tag '{name}' already used.")
            }
        }
    }

    /// Add new tag.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a tag called `"time"` is used.
    pub fn with_tag<V>(mut self, name: &'static str, value: V) -> Self
    where
        V: Into<Cow<'static, str>>,
    {
        self.add_tag(name, value);
        self
    }

    /// Add a new field.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a field called `"time"` is used.
    pub fn add_field<V>(&mut self, name: &'static str, value: V)
    where
        V: Into<FieldValue>,
    {
        if name == "time" {
            panic!("Cannot use a field called 'time'.");
        }

        if self.tags.contains_key(name) {
            panic!(
                "Cannot use field named '{name}' because a tag with the same name already exists"
            );
        }

        match self.fields.entry(name) {
            Entry::Vacant(v) => {
                v.insert(value.into());
            }
            Entry::Occupied(_) => {
                panic!("Field '{name}' already used.")
            }
        }
    }

    /// Add a new field.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a field called `"time"` is used.
    pub fn with_field<V>(mut self, name: &'static str, value: V) -> Self
    where
        V: Into<FieldValue>,
    {
        self.add_field(name, value);
        self
    }
}

impl<M> Event<M>
where
    M: TypedMeasurement,
{
    /// Drop typing from event.
    pub(crate) fn untyped(self) -> Event<&'static str> {
        Event {
            measurement: self.measurement.into(),
            time: self.time,
            tags: self.tags,
            fields: self.fields,
        }
    }
}

impl Event<&'static str> {
    /// Add typing to event.
    ///
    /// # Panic
    /// Panics if the type and untyped measurement don't match.
    pub(crate) fn typed<M>(self) -> Event<M>
    where
        M: TypedMeasurement,
    {
        let measurement = M::default();
        assert_eq!(measurement.into(), self.measurement);
        Event {
            measurement,
            time: self.time,
            tags: self.tags,
            fields: self.fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::measurement;

    use super::*;

    measurement!(TestM, test);

    #[test]
    #[should_panic(expected = "Cannot use a tag called 'time'")]
    fn test_check_tag_time() {
        Event::new(TestM::default(), Time::MIN).with_tag("time", "1");
    }

    #[test]
    #[should_panic(expected = "Tag 'foo' already used.")]
    fn test_check_tag_override() {
        Event::new(TestM::default(), Time::MIN)
            .with_tag("foo", "1")
            .with_tag("foo", "1");
    }

    #[test]
    #[should_panic(expected = "Cannot use a field called 'time'")]
    fn test_check_field_time() {
        Event::new(TestM::default(), Time::MIN).with_field("time", 1u64);
    }

    #[test]
    #[should_panic(expected = "Field 'foo' already used.")]
    fn test_check_field_override() {
        Event::new(TestM::default(), Time::MIN)
            .with_field("foo", 1u64)
            .with_field("foo", 1u64);
    }

    #[test]
    #[should_panic(
        expected = "Cannot use field named 'foo' because a tag with the same name already exists"
    )]
    fn test_check_tag_field_collision() {
        Event::new(TestM::default(), Time::MIN)
            .with_tag("foo", "1")
            .with_field("foo", 1u64);
    }

    #[test]
    #[should_panic(
        expected = "Cannot use tag named 'foo' because a field with the same name already exists"
    )]
    fn test_check_field_tag_collision() {
        Event::new(TestM::default(), Time::MIN)
            .with_field("foo", 1u64)
            .with_tag("foo", "1");
    }
}
