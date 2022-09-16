use crate::internal::{expect, ParseResult};
use crate::show_measurements::show_measurements;
use crate::Statement;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::{map, value};
use nom::sequence::{pair, preceded};

/// Parse a SHOW statement.
pub fn show_statement(i: &str) -> ParseResult<&str, Statement> {
    preceded(
        pair(tag_no_case("SHOW"), multispace1),
        expect(
            "invalid SHOW statement, expected MEASUREMENTS",
            // NOTE: This will become an alt(()) once more statements are added
            alt((
                // SHOW DATABASES
                show_databases,
                // SHOW MEASUREMENTS
                map(show_measurements, |v| {
                    Statement::ShowMeasurements(Box::new(v))
                }),
            )),
        ),
    )(i)
}

/// Parse a `SHOW DATABASES` statement.
fn show_databases(i: &str) -> ParseResult<&str, Statement> {
    value(Statement::ShowDatabases, tag_no_case("DATABASES"))(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_show_statement() {
        let (_, got) = show_statement("SHOW DATABASES").unwrap();
        assert_eq!(format!("{}", got), "SHOW DATABASES");

        let (_, got) = show_statement("SHOW MEASUREMENTS").unwrap();
        assert_eq!(format!("{}", got), "SHOW MEASUREMENTS");

        // Fallible case

        // Unsupported SHOW
        assert_expect_error!(
            show_statement("SHOW TAG KEYS"),
            "invalid SHOW statement, expected MEASUREMENTS"
        );
    }
}
