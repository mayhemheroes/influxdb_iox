//! Event handling.
use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
};

use iox_time::Time;

use crate::measurement::TypedMeasurement;

/// Value type for [`Event`] fields.
#[derive(Debug, Clone)]
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

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::U64(l0), Self::U64(r0)) => l0 == r0,
            (Self::F64(l0), Self::F64(r0)) => l0 == r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl Eq for FieldValue {}

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event<M>
where
    M: Into<&'static str>,
{
    measurement: M,
    time: Time,
    tags: BTreeMap<&'static str, Cow<'static, str>>,
    fields: BTreeMap<&'static str, FieldValue>,
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
            tags: BTreeMap::default(),
            fields: BTreeMap::default(),
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
    /// The iterator elements are sorted by tag name.
    pub fn tags(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.tags.iter().map(|(k, v)| (*k, v.as_ref()))
    }

    /// Get all fields.
    ///
    /// The iterator elements are sorted by field name.
    pub fn fields(&self) -> impl Iterator<Item = (&'static str, &FieldValue)> {
        self.fields.iter().map(|(k, v)| (*k, v))
    }

    /// Add new tag.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a tag called `"time"` is used.
    pub fn add_tag_mut<V>(&mut self, name: &'static str, value: V) -> &mut Self
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

        self
    }

    /// Add new tag.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a tag called `"time"` is used.
    pub fn add_tag_move<V>(mut self, name: &'static str, value: V) -> Self
    where
        V: Into<Cow<'static, str>>,
    {
        self.add_tag_mut(name, value);
        self
    }

    /// Add a new field.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a field called `"time"` is used.
    pub fn add_field_mut<V>(&mut self, name: &'static str, value: V) -> &mut Self
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

        self
    }

    /// Add a new field.
    ///
    /// # Panic
    /// Panics if a tag or a field with the same name already exists. Also panics if a field called `"time"` is used.
    pub fn add_field_move<V>(mut self, name: &'static str, value: V) -> Self
    where
        V: Into<FieldValue>,
    {
        self.add_field_mut(name, value);
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
    use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

    use crate::measurement;

    use super::*;

    measurement!(TestM, test);

    #[test]
    #[should_panic(expected = "Cannot use a tag called 'time'")]
    fn test_check_tag_time() {
        Event::new(TestM::default(), Time::MIN).add_tag_move("time", "1");
    }

    #[test]
    #[should_panic(expected = "Tag 'foo' already used.")]
    fn test_check_tag_override() {
        Event::new(TestM::default(), Time::MIN)
            .add_tag_move("foo", "1")
            .add_tag_move("foo", "1");
    }

    #[test]
    #[should_panic(expected = "Cannot use a field called 'time'")]
    fn test_check_field_time() {
        Event::new(TestM::default(), Time::MIN).add_field_move("time", 1u64);
    }

    #[test]
    #[should_panic(expected = "Field 'foo' already used.")]
    fn test_check_field_override() {
        Event::new(TestM::default(), Time::MIN)
            .add_field_move("foo", 1u64)
            .add_field_move("foo", 1u64);
    }

    #[test]
    #[should_panic(
        expected = "Cannot use field named 'foo' because a tag with the same name already exists"
    )]
    fn test_check_tag_field_collision() {
        Event::new(TestM::default(), Time::MIN)
            .add_tag_move("foo", "1")
            .add_field_move("foo", 1u64);
    }

    #[test]
    #[should_panic(
        expected = "Cannot use tag named 'foo' because a field with the same name already exists"
    )]
    fn test_check_field_tag_collision() {
        Event::new(TestM::default(), Time::MIN)
            .add_field_move("foo", 1u64)
            .add_tag_move("foo", "1");
    }

    #[test]
    fn test_tags_iter_sorted() {
        let mut tags: Vec<_> = (0..100).map(|i| format!("tag_{i}")).collect();
        let mut rng = StdRng::seed_from_u64(1234);
        tags.shuffle(&mut rng);

        let mut event = Event::new("foo", Time::MIN);
        for tag in &tags {
            let tag = Box::from(tag.clone());
            let tag = Box::leak(tag);
            event.add_tag_mut(tag, "bar");
        }

        tags.sort();
        let actual: Vec<_> = event.tags().map(|(t, _)| t.to_owned()).collect();
        assert_eq!(actual, tags);
    }

    #[test]
    fn test_fields_iter_sorted() {
        let mut fields: Vec<_> = (0..100).map(|i| format!("field_{i}")).collect();
        let mut rng = StdRng::seed_from_u64(1234);
        fields.shuffle(&mut rng);

        let mut event = Event::new("foo", Time::MIN);
        for field in &fields {
            let tag = Box::from(field.clone());
            let tag = Box::leak(tag);
            event.add_field_mut(tag, 1u64);
        }

        fields.sort();
        let actual: Vec<_> = event.fields().map(|(t, _)| t.to_owned()).collect();
        assert_eq!(actual, fields);
    }
}
