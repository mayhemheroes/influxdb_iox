//! Typed measurement.
use std::fmt::Debug;

/// A typed measurement.
pub trait TypedMeasurement: Clone + Copy + Debug + Default + Into<&'static str> + 'static {}

/// Easily create new measurements.
///
/// # Example
/// ```
/// use event_emitter::measurement;
///
/// measurement!(MyMeasurement, my_measurement);
///
/// let name: &'static str = MyMeasurement.into();
/// assert_eq!(name, "my_measurement");
/// ```
#[macro_export]
macro_rules! measurement {
    ($type_name:ident, $influx_name:ident) => {
        #[derive(Debug, Clone, Copy, Default)]
        #[allow(missing_copy_implementations)]
        pub struct $type_name;

        impl From<$type_name> for &'static str {
            fn from(_: $type_name) -> Self {
                stringify!($influx_name)
            }
        }

        impl $crate::measurement::TypedMeasurement for $type_name {}
    };
}
