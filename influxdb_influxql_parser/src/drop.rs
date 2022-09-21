use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::map;
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter};

/// Represents a `DROP MEASUREMENT` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMeasurementStatement {
    /// The name of the measurement to delete.
    name: Identifier,
}

impl Display for DropMeasurementStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP MEASUREMENT {}", self.name)?;
        Ok(())
    }
}

pub fn drop_statement(i: &str) -> ParseResult<&str, DropMeasurementStatement> {
    preceded(
        pair(tag_no_case("DROP"), multispace1),
        expect(
            "invalid DROP statement, must be followed by MEASUREMENT",
            drop_measurement,
        ),
    )(i)
}

fn drop_measurement(i: &str) -> ParseResult<&str, DropMeasurementStatement> {
    preceded(
        pair(tag_no_case("MEASUREMENT"), multispace1),
        map(
            expect(
                "invalid DROP MEASUREMENT statement, expected identifier",
                identifier,
            ),
            |name| DropMeasurementStatement { name },
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_drop_statement() {
        drop_statement("DROP MEASUREMENT foo").unwrap();

        // Fallible cases
        assert_expect_error!(
            drop_statement("DROP foo"),
            "invalid DROP statement, must be followed by MEASUREMENT"
        );
    }

    #[test]
    fn test_drop_measurement() {
        let (_, got) = drop_measurement("MEASUREMENT \"foo\"").unwrap();
        assert_eq!(got, DropMeasurementStatement { name: "foo".into() });
        // validate Display
        assert_eq!(format!("{}", got), "DROP MEASUREMENT foo");

        // Fallible cases
        assert_expect_error!(
            drop_measurement("MEASUREMENT 'foo'"),
            "invalid DROP MEASUREMENT statement, expected identifier"
        );
    }
}
