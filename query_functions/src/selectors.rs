//! Implementaton of InfluxDB "Selector" Functions
//!
//! Selector functions are similar to aggregate functions in that they
//! collapse down an input set of rows into just one.
//!
//! Selector functions are different than aggregate functions because
//! they also return multiple column values rather than a single
//! scalar. Selector functions return the entire row that was
//! "selected" from the timeseries (value and time pair).
//!
//! This module implements a selector as a DataFusion aggregate
//! function which returns multiple columns via a struct with fields
//! `value` and `time`
use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::{
        AccumulatorFunctionImplementation, AggregateState, Signature, TypeSignature, Volatility,
    },
    physical_plan::{udaf::AggregateUDF, Accumulator},
    scalar::ScalarValue, prelude::Expr,
};

// Internal implementations of the selector functions
mod internal;
use internal::{
    BooleanFirstSelector, BooleanLastSelector, BooleanMaxSelector, BooleanMinSelector,
    F64FirstSelector, F64LastSelector, F64MaxSelector, F64MinSelector, I64FirstSelector,
    I64LastSelector, I64MaxSelector, I64MinSelector, U64FirstSelector, U64LastSelector,
    U64MaxSelector, U64MinSelector, Utf8FirstSelector, Utf8LastSelector, Utf8MaxSelector,
    Utf8MinSelector,
};
use once_cell::sync::Lazy;
use schema::{TIME_DATA_TYPE, TIME_COLUMN_NAME};


/// Represents selecting one of the two output fields (`value`, or `time`)
#[derive(Debug, Clone, Copy)]
pub enum SelectorOutputField {
    /// Select the "value" field from the struct returned by a selector
    Value,
    /// Select the "time" field from the struct returned by a selector
    Time,
}

/// Generates an `Expr` that represents selecting one field from a
/// struct.
///
/// So for example, for `SelectorOutputField::Value`, and the input is
/// `selector_first(col, time)`, returns a function like
///
/// selector_first(col, time)['value']
///
pub fn select_output_field(expr: Expr, output_field: SelectorOutputField) -> Expr {
    let col_name = match output_field {
        SelectorOutputField::Value => "value",
        SelectorOutputField::Time => TIME_COLUMN_NAME,
    };

    Expr::GetIndexedField {
        expr: Box::new(expr),
        key: ScalarValue::Utf8(Some(col_name.to_string())),
    }
}



/// registers selector functions so they can be invoked via SQL
pub fn register_selector_aggregates(mut state: SessionState) -> SessionState {
    let first = selector_first();
    let last = selector_last();
    let min = selector_min();
    let max = selector_max();

    //TODO make a nicer api for this in DataFusion
    state
        .aggregate_functions
        .insert(first.name.to_string(), first);

    state
        .aggregate_functions
        .insert(last.name.to_string(), last);

    state.aggregate_functions.insert(min.name.to_string(), min);

    state.aggregate_functions.insert(max.name.to_string(), max);

    state
}



/// Returns a DataFusion user defined aggregate function for computing
/// the first(value, time) selector function, returning a struct:
///
/// first(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the minimum of the time column.
///   time: value of the minimum time column
/// }
/// ```
///
/// If there are multiple rows with the minimum timestamp value, the
/// value is arbitrary
pub fn selector_first() -> Arc<AggregateUDF> {
    Arc::clone(&SELECTOR_FIRST)
}

static SELECTOR_FIRST: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let udaf = make_selector_udaf(
        "selector_first",
        Arc::new(|return_type| {
            let value_type = get_value_datatype(return_type)?;
            let accumulator: Box<dyn Accumulator> = match value_type {
                DataType::Float64 => Box::new(SelectorAccumulator::<F64FirstSelector>::new()),
                DataType::Int64 => Box::new(SelectorAccumulator::<I64FirstSelector>::new()),
                DataType::UInt64 => Box::new(SelectorAccumulator::<U64FirstSelector>::new()),
                DataType::Utf8 => Box::new(SelectorAccumulator::<Utf8FirstSelector>::new()),
                DataType::Boolean => {
                    Box::new(SelectorAccumulator::<BooleanFirstSelector>::new())
                }
                t => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected return type. Expected value type of f64/i64/u64/string/bool, got {:?}",
                        t
                    )))
                }
            };
            Ok(accumulator)
        }),
    );
    Arc::new(udaf)
});


/// Returns a DataFusion user defined aggregate function for computing
/// the last(value, time) selector function, returning a struct:
///
/// last(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row of the maximum of the time column.
///   time: value of the maximum time column
/// }
/// ```
///
/// If there are multiple rows with the maximum timestamp value, the
/// value is arbitrary
pub fn selector_last() -> Arc<AggregateUDF> {
    Arc::clone(&SELECTOR_LAST)
}

static SELECTOR_LAST: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let udaf = make_selector_udaf(
        "selector_last",
        Arc::new(|return_type| {
            let value_type = get_value_datatype(return_type)?;
            let accumulator: Box<dyn Accumulator> = match value_type {
                DataType::Float64 => Box::new(SelectorAccumulator::<F64LastSelector>::new()),
                DataType::Int64 => Box::new(SelectorAccumulator::<I64LastSelector>::new()),
                DataType::UInt64 => Box::new(SelectorAccumulator::<U64LastSelector>::new()),
                DataType::Utf8 => Box::new(SelectorAccumulator::<Utf8LastSelector>::new()),
                DataType::Boolean => {
                    Box::new(SelectorAccumulator::<BooleanLastSelector>::new())
                }
                t => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected return type. Expected value type of f64/i64/u64/string/bool, got {:?}",
                        t
                    )))
                }
            };
            Ok(accumulator)
        }),
    );
    Arc::new(udaf)
});

/// Returns a DataFusion user defined aggregate function for computing
/// the min(value, time) selector function, returning a struct:
///
/// min(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with minimum value
///   time: value of time for row with minimum value
/// }
/// ```
///
/// If there are multiple rows with the same minimum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_min() -> Arc<AggregateUDF> {
    Arc::clone(&SELECTOR_MIN)
}

static SELECTOR_MIN: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let udaf = make_selector_udaf(
        "selector_min",
        Arc::new(|return_type| {
            let value_type = get_value_datatype(return_type)?;
            let accumulator: Box<dyn Accumulator> = match value_type {
                DataType::Float64 => Box::new(SelectorAccumulator::<F64MinSelector>::new()),
                DataType::Int64 => Box::new(SelectorAccumulator::<I64MinSelector>::new()),
                DataType::UInt64 => Box::new(SelectorAccumulator::<U64MinSelector>::new()),
                DataType::Utf8 => Box::new(SelectorAccumulator::<Utf8MinSelector>::new()),
                DataType::Boolean => {
                    Box::new(SelectorAccumulator::<BooleanMinSelector>::new())
                }
                t => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected return type. Expected value type of f64/i64/u64/string/bool, got {:?}",
                        t
                    )))
                }
            };
            Ok(accumulator)
        }),
    );
    Arc::new(udaf)
});

/// Returns a DataFusion user defined aggregate function for computing
/// the max(value, time) selector function, returning a struct:
///
/// max(value, time) -> struct { value, time }
///
/// ```text
/// {
///   value: value at the row with maximum value
///   time: value of time for row with maximum value
/// }
/// ```
///
/// If there are multiple rows with the same maximum value, the value
/// with the first (earliest/smallest) timestamp is chosen
pub fn selector_max() -> Arc<AggregateUDF> {
    Arc::clone(&SELECTOR_MAX)
}

static SELECTOR_MAX: Lazy<Arc<AggregateUDF>> = Lazy::new(|| {
    let udaf = make_selector_udaf(
        "selector_max",
        Arc::new(|return_type| {
            let value_type = get_value_datatype(return_type)?;
            let accumulator: Box<dyn Accumulator> = match value_type {
                DataType::Float64 => Box::new(SelectorAccumulator::<F64MaxSelector>::new()),
                DataType::Int64 => Box::new(SelectorAccumulator::<I64MaxSelector>::new()),
                DataType::UInt64 => Box::new(SelectorAccumulator::<U64MaxSelector>::new()),
                DataType::Utf8 => Box::new(SelectorAccumulator::<Utf8MaxSelector>::new()),
                DataType::Boolean => {
                    Box::new(SelectorAccumulator::<BooleanMaxSelector>::new())
                }
                t => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected return type. Expected value type of f64/i64/u64/string/bool, got {:?}",
                        t
                    )))
                }
            };
            Ok(accumulator)
        }),
    );
    Arc::new(udaf)
});

/// Implements the logic of the specific selector function (this is a
/// cutdown version of the Accumulator DataFusion trait, to allow
/// sharing between implementations)
trait Selector: Debug + Default + Send + Sync {
    /// What type of values does this selector function work with (time is
    /// always I64)
    fn value_data_type() -> DataType;

    /// return state in a form that DataFusion can store during execution
    fn datafusion_state(&self) -> DataFusionResult<Vec<AggregateState>>;

    /// produces the final value of this selector as `Vec<ScalarValue>`s
    fn evaluate_to_vec(&self) -> DataFusionResult<Vec<ScalarValue>>;

    /// Update this selector's state based on values in value_arr and time_arr
    fn update_batch(&mut self, value_arr: &ArrayRef, time_arr: &ArrayRef) -> DataFusionResult<()>;
}

/// Create a User Defined Aggregate Function (UDAF) for datafusion.
fn make_selector_udaf(
    name: &str,
    accumulator_factory: AccumulatorFunctionImplementation,
) -> AggregateUDF {
    // All selectors support the same input types / signatures
    let input_signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Float64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Int64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::UInt64, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Utf8, TIME_DATA_TYPE()]),
            TypeSignature::Exact(vec![DataType::Boolean, TIME_DATA_TYPE()]),
        ],
        Volatility::Stable,
    );

    // return type of the selector is based on the input arguments.
    //
    // The inputs are (value, time) and the output is a struct with a
    // 'value' and 'time' field of the same time.
    let return_type_func: ReturnTypeFunction = Arc::new(|arg_types| {
        assert_eq!(
            arg_types.len(),
            2,
            "selector expected exactly 2 arguments, got {}",
            arg_types.len()
        );
        let value_type = &arg_types[0];
        assert_eq!(&arg_types[1], &TIME_DATA_TYPE());

        Ok(Arc::new(make_output_datatype(value_type.clone())))
    });

    // state type given the return type
    let state_type_factory: AccumulatorFactory = Arc::new(|return_type| {
        let value_type = get_value_datatype(&return_type)?;

        Ok(Arc::new(make_state_datatypes(value_type.clone())))
    });

    AggregateUDF::new(
        name,
        &input_signature,
        &return_type_func,
        &accumulator_factory,
        &state_type_factory,
    )
}

/// Create the struct fields for a selector with DataType `value_type`
fn make_struct_fields(value_type: DataType) -> Vec<Field> {
    vec![
        Field::new("value", value_type.clone(), true),
        Field::new("time", TIME_DATA_TYPE(), true),
    ]
}

/// Return the output data type for a selector with DataType `value_type`
fn make_output_datatype(value_type: DataType) -> DataType {
    DataType::Struct(make_struct_fields(value_type))
}

/// Return the value type given the (struct)  output data type
fn get_value_datatype(output_type: &DataType) -> DataFusionResult<&DataType> {
    match output_type {
        DataType::Struct(fields) => Ok(fields[0].data_type()),
        t => Err(DataFusionError::Internal(format!(
            "Unexpected selector return type. Expected struct got {:?}",
            t
        ))),
    }
}

/// Return the state in which the arguments are stored
fn make_state_datatypes(value_type: DataType) -> Vec<DataType> {
    vec![value_type, TIME_DATA_TYPE()]
}

type ReturnTypeFunction = Arc<dyn Fn(&[DataType]) -> DataFusionResult<Arc<DataType>> + Send + Sync>;
type AccumulatorFactory =
    Arc<dyn Fn(&DataType) -> DataFusionResult<Arc<Vec<DataType>>> + Send + Sync>;

/// Structure that implements the Accumulator trait for DataFusion
/// and processes (value, timestamp) pair and computes values
///
/// TODO inline this into the selector implementations
#[derive(Debug)]
struct SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector,
{
    // The underlying implementation for the selector
    selector: SELECTOR,
}

impl<SELECTOR> SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector,
{
    pub fn new() -> Self {
        Self {
            selector: SELECTOR::default(),
        }
    }
}

impl<SELECTOR> Accumulator for SelectorAccumulator<SELECTOR>
where
    SELECTOR: Selector + 'static,
{
    // this function serializes our state to a vector of
    // `ScalarValue`s, which DataFusion passes between execution
    // stages.
    fn state(&self) -> DataFusionResult<Vec<AggregateState>> {
        self.selector.datafusion_state()
    }

    // Return the final value of this aggregator.
    fn evaluate(&self) -> DataFusionResult<ScalarValue> {
        Ok(ScalarValue::Struct(
            Some(self.selector.evaluate_to_vec()?),
            Box::new(make_struct_fields(SELECTOR::value_data_type())),
        ))
    }

    // receives one entry per argument of this accumulator and updates
    // the selector state function appropriately
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "Internal error: Expected 2 arguments passed to selector but got {}",
                values.len()
            )));
        }

        // invoke the actual worker function.
        self.selector.update_batch(&values[0], &values[1])?;
        Ok(())
    }

    // The input values and accumulator state are the same types for
    // selectors, and thus we can merge intermediate states with the
    // same function as inputs
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        // merge is the same operation as update for these selectors
        self.update_batch(states)
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
            UInt64Array,
        },
        datatypes::{Field, Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{datasource::MemTable, logical_plan::Expr, prelude::*};
    use schema::TIME_DATA_TIMEZONE;

    use super::*;

    // Begin `first`

    #[tokio::test]
    async fn test_selector_first_f64() {
        run_case(
            selector_first().call(vec![col("f64_value"), col("time")]),
            vec![
                "+--------------------------------------------------+",
                "| selector_first(t.f64_value,t.time)               |",
                "+--------------------------------------------------+",
                "| {\"value\": 2, \"time\": 1970-01-01 00:00:00.000001} |",
                "+--------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_first_i64() {
        run_case(
            selector_first().call(vec![col("i64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_first(t.i64_value,t.time)                |",
                "+---------------------------------------------------+",
                "| {\"value\": 20, \"time\": 1970-01-01 00:00:00.000001} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_first_u64() {
        run_case(
            selector_first().call(vec![col("u64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_first(t.u64_value,t.time)                |",
                "+---------------------------------------------------+",
                "| {\"value\": 20, \"time\": 1970-01-01 00:00:00.000001} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_first_string() {
        run_case(
            selector_first().call(vec![col("string_value"), col("time")]),
            vec![
                "+------------------------------------------------------+",
                "| selector_first(t.string_value,t.time)                |",
                "+------------------------------------------------------+",
                "| {\"value\": \"two\", \"time\": 1970-01-01 00:00:00.000001} |",
                "+------------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_first_bool() {
        run_case(
            selector_first().call(vec![col("bool_value"), col("time")]),
            vec![
                "+-----------------------------------------------------+",
                "| selector_first(t.bool_value,t.time)                 |",
                "+-----------------------------------------------------+",
                "| {\"value\": true, \"time\": 1970-01-01 00:00:00.000001} |",
                "+-----------------------------------------------------+",
            ],
        )
        .await;
    }

    // Begin `last`

    #[tokio::test]
    async fn test_selector_last_f64() {
        run_case(
            selector_last().call(vec![col("f64_value"), col("time")]),
            vec![
                "+--------------------------------------------------+",
                "| selector_last(t.f64_value,t.time)                |",
                "+--------------------------------------------------+",
                "| {\"value\": 3, \"time\": 1970-01-01 00:00:00.000006} |",
                "+--------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_last_i64() {
        run_case(
            selector_last().call(vec![col("i64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_last(t.i64_value,t.time)                 |",
                "+---------------------------------------------------+",
                "| {\"value\": 30, \"time\": 1970-01-01 00:00:00.000006} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_last_u64() {
        run_case(
            selector_last().call(vec![col("u64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_last(t.u64_value,t.time)                 |",
                "+---------------------------------------------------+",
                "| {\"value\": 30, \"time\": 1970-01-01 00:00:00.000006} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_last_string() {
        run_case(
            selector_last().call(vec![col("string_value"), col("time")]),
            vec![
                "+--------------------------------------------------------+",
                "| selector_last(t.string_value,t.time)                   |",
                "+--------------------------------------------------------+",
                "| {\"value\": \"three\", \"time\": 1970-01-01 00:00:00.000006} |",
                "+--------------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_last_bool() {
        run_case(
            selector_last().call(vec![col("bool_value"), col("time")]),
            vec![
                "+------------------------------------------------------+",
                "| selector_last(t.bool_value,t.time)                   |",
                "+------------------------------------------------------+",
                "| {\"value\": false, \"time\": 1970-01-01 00:00:00.000006} |",
                "+------------------------------------------------------+",
            ],
        )
        .await;
    }

    // Begin `min`

    #[tokio::test]
    async fn test_selector_min_f64() {
        run_case(
            selector_min().call(vec![col("f64_value"), col("time")]),
            vec![
                "+--------------------------------------------------+",
                "| selector_min(t.f64_value,t.time)                 |",
                "+--------------------------------------------------+",
                "| {\"value\": 1, \"time\": 1970-01-01 00:00:00.000004} |",
                "+--------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_min_i64() {
        run_case(
            selector_min().call(vec![col("i64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_min(t.i64_value,t.time)                  |",
                "+---------------------------------------------------+",
                "| {\"value\": 10, \"time\": 1970-01-01 00:00:00.000004} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_min_u64() {
        run_case(
            selector_min().call(vec![col("u64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_min(t.u64_value,t.time)                  |",
                "+---------------------------------------------------+",
                "| {\"value\": 10, \"time\": 1970-01-01 00:00:00.000004} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_min_string() {
        run_case(
            selector_min().call(vec![col("string_value"), col("time")]),
            vec![
                "+--------------------------------------------------------+",
                "| selector_min(t.string_value,t.time)                    |",
                "+--------------------------------------------------------+",
                "| {\"value\": \"a_one\", \"time\": 1970-01-01 00:00:00.000004} |",
                "+--------------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_min_bool() {
        run_case(
            selector_min().call(vec![col("bool_value"), col("time")]),
            vec![
                "+------------------------------------------------------+",
                "| selector_min(t.bool_value,t.time)                    |",
                "+------------------------------------------------------+",
                "| {\"value\": false, \"time\": 1970-01-01 00:00:00.000002} |",
                "+------------------------------------------------------+",
            ],
        )
        .await;
    }

    // Begin `max`

    #[tokio::test]
    async fn test_selector_max_f64() {
        run_case(
            selector_max().call(vec![col("f64_value"), col("time")]),
            vec![
                "+--------------------------------------------------+",
                "| selector_max(t.f64_value,t.time)                 |",
                "+--------------------------------------------------+",
                "| {\"value\": 5, \"time\": 1970-01-01 00:00:00.000005} |",
                "+--------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_max_i64() {
        run_case(
            selector_max().call(vec![col("i64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_max(t.i64_value,t.time)                  |",
                "+---------------------------------------------------+",
                "| {\"value\": 50, \"time\": 1970-01-01 00:00:00.000005} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_max_u64() {
        run_case(
            selector_max().call(vec![col("u64_value"), col("time")]),
            vec![
                "+---------------------------------------------------+",
                "| selector_max(t.u64_value,t.time)                  |",
                "+---------------------------------------------------+",
                "| {\"value\": 50, \"time\": 1970-01-01 00:00:00.000005} |",
                "+---------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_max_string() {
        run_case(
            selector_max().call(vec![col("string_value"), col("time")]),
            vec![
                "+---------------------------------------------------------+",
                "| selector_max(t.string_value,t.time)                     |",
                "+---------------------------------------------------------+",
                "| {\"value\": \"z_five\", \"time\": 1970-01-01 00:00:00.000005} |",
                "+---------------------------------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_selector_max_bool() {
        run_case(
            selector_max().call(vec![col("bool_value"), col("time")]),
            vec![
                "+-----------------------------------------------------+",
                "| selector_max(t.bool_value,t.time)                   |",
                "+-----------------------------------------------------+",
                "| {\"value\": true, \"time\": 1970-01-01 00:00:00.000001} |",
                "+-----------------------------------------------------+",
            ],
        )
        .await;
    }


    // Begin utility functions

    /// Runs the expr using `run_plan` and compares the result to `expected`
    async fn run_case(expr: Expr, expected: Vec<&'static str>) {
        println!("Running case for {}", expr);

        let actual = run_plan(expr.clone()).await;

        assert_eq!(
            expected, actual,
            "\n\nexpr: {}\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
            expr, expected, actual
        );
    }

    /// Run a plan against the following input table as "t"
    ///
    /// ```text
    /// +-----------+-----------+-----------+--------------+------------+----------------------------+,
    /// | f64_value | i64_value | u64_value | string_value | bool_value | time                       |,
    /// +-----------+-----------+--------------+------------+----------------------------+,
    /// | 2         | 20        | 20        | two          | true       | 1970-01-01 00:00:00.000001 |,
    /// | 4         | 40        | 40        | four         | false      | 1970-01-01 00:00:00.000002 |,
    /// |           |           |           |              |            | 1970-01-01 00:00:00.000003 |,
    /// | 1         | 10        | 10        | a_one        | true       | 1970-01-01 00:00:00.000004 |,
    /// | 5         | 50        | 50        | z_five       | false      | 1970-01-01 00:00:00.000005 |,
    /// | 3         | 30        | 30        | three        | false      | 1970-01-01 00:00:00.000006 |,
    /// +-----------+-----------+--------------+------------+----------------------------+,
    /// ```
    ///
    /// And returns the resulting RecordBatch as a formatted vec of strings
    async fn run_plan(agg: Expr) -> Vec<String> {
        // define a schema for input
        // (value) and timestamp
        let schema = Arc::new(Schema::new(vec![
            Field::new("f64_value", DataType::Float64, true),
            Field::new("i64_value", DataType::Int64, true),
            Field::new("u64_value", DataType::UInt64, true),
            Field::new("string_value", DataType::Utf8, true),
            Field::new("bool_value", DataType::Boolean, true),
            Field::new("time", TIME_DATA_TYPE(), true),
        ]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(2.0), Some(4.0), None])),
                Arc::new(Int64Array::from(vec![Some(20), Some(40), None])),
                Arc::new(UInt64Array::from(vec![Some(20), Some(40), None])),
                Arc::new(StringArray::from(vec![Some("two"), Some("four"), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![1000, 2000, 3000],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        )
        .unwrap();

        // No values in this batch
        let batch2 = match RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
                Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
                Arc::new(UInt64Array::from(vec![] as Vec<Option<u64>>)),
                Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
                Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        ) {
            Ok(a) => a,
            _ => unreachable!(),
        };

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.0), Some(5.0), Some(3.0)])),
                Arc::new(Int64Array::from(vec![Some(10), Some(50), Some(30)])),
                Arc::new(UInt64Array::from(vec![Some(10), Some(50), Some(30)])),
                Arc::new(StringArray::from(vec![
                    Some("a_one"),
                    Some("z_five"),
                    Some("three"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(false),
                ])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![4000, 5000, 6000],
                    TIME_DATA_TIMEZONE(),
                )),
            ],
        )
        .unwrap();

        let aggs = vec![agg];

        // Ensure the answer is the same regardless of the order of inputs
        let input = vec![batch1, batch2, batch3];
        let input_string = pretty_format_batches(&input).unwrap();
        let results = run_with_inputs(Arc::clone(&schema), aggs.clone(), input.clone()).await;

        use itertools::Itertools;
        // Get all permutations of the input
        for p in input.iter().permutations(3) {
            let p_batches = p.into_iter().cloned().collect::<Vec<_>>();
            let p_input_string = pretty_format_batches(&p_batches).unwrap();
            let p_results = run_with_inputs(Arc::clone(&schema), aggs.clone(), p_batches).await;
            assert_eq!(
                results, p_results,
                "Mismatch with permutation.\n\
                 Input1 \n\n\
                 {}\n\n\
                 produces output:\n\n\
                 {:#?}\n\n\
                 Input 2\n\n\
                 {}\n\n\
                 produces output:\n\n\
                 {:#?}\n\n",
                input_string, results, p_input_string, p_results
            );
        }

        results
    }

    async fn run_with_inputs(
        schema: SchemaRef,
        aggs: Vec<Expr>,
        inputs: Vec<RecordBatch>,
    ) -> Vec<String> {
        let provider = MemTable::try_new(Arc::clone(&schema), vec![inputs]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx.table("t").unwrap();
        let df = df.aggregate(vec![], aggs).unwrap();

        // execute the query
        let record_batches = df.collect().await.unwrap();

        pretty_format_batches(&record_batches)
            .unwrap()
            .to_string()
            .split('\n')
            .map(|s| s.to_owned())
            .collect()
    }
}
