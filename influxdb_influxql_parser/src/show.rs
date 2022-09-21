use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::show_field_keys::show_field_keys;
use crate::show_measurements::show_measurements;
use crate::show_retention_policies::show_retention_policies;
use crate::show_tag_keys::show_tag_keys;
use crate::show_tag_values::show_tag_values;
use crate::Statement;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::{map, value};
use nom::sequence::{pair, preceded};
use std::fmt::{Display, Formatter};

/// Parse a SHOW statement.
pub fn show_statement(i: &str) -> ParseResult<&str, Statement> {
    preceded(
        pair(tag_no_case("SHOW"), multispace1),
        expect(
            "invalid SHOW statement, expected DATABASES, FIELD, MEASUREMENTS, TAG, or RETENTION following SHOW",
            alt((
                // SHOW DATABASES
                map(show_databases, |s| Statement::ShowDatabases(Box::new(s))),
                // SHOW FIELD KEYS
                map(show_field_keys, |s| Statement::ShowFieldKeys(Box::new(s))),
                // SHOW MEASUREMENTS
                map(show_measurements, |s| {
                    Statement::ShowMeasurements(Box::new(s))
                }),
                // SHOW RETENTION POLICIES
                map(show_retention_policies, |s| {
                    Statement::ShowRetentionPolicies(Box::new(s))
                }),
                // SHOW TAG
                show_tag,
            )),
        ),
    )(i)
}

/// Represents a `SHOW DATABASES` statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShowDatabasesStatement;

impl Display for ShowDatabasesStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SHOW DATABASES")?;
        Ok(())
    }
}

/// Parse a `SHOW DATABASES` statement.
fn show_databases(i: &str) -> ParseResult<&str, ShowDatabasesStatement> {
    value(ShowDatabasesStatement, tag_no_case("DATABASES"))(i)
}

/// Parse an `ON` clause for `SHOW TAG KEYS`, `SHOW TAG VALUES` and `SHOW FIELD KEYS`
/// statements.
pub fn on_clause(i: &str) -> ParseResult<&str, Identifier> {
    preceded(
        pair(tag_no_case("ON"), multispace1),
        expect("invalid ON clause, expected identifier", identifier),
    )(i)
}

/// Parse a `SHOW TAG (KEYS|VALUES)` statement.
fn show_tag(i: &str) -> ParseResult<&str, Statement> {
    preceded(
        pair(tag_no_case("TAG"), multispace1),
        expect(
            "invalid SHOW TAG statement, expected KEYS or VALUES following TAG",
            alt((
                map(show_tag_keys, |s| Statement::ShowTagKeys(Box::new(s))),
                map(show_tag_values, |s| Statement::ShowTagValues(Box::new(s))),
            )),
        ),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_show_statement() {
        // Validate each of the `SHOW` statements are accepted

        let (_, got) = show_statement("SHOW DATABASES").unwrap();
        assert_eq!(format!("{}", got), "SHOW DATABASES");

        let (_, got) = show_statement("SHOW FIELD KEYS").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS");

        let (_, got) = show_statement("SHOW MEASUREMENTS").unwrap();
        assert_eq!(format!("{}", got), "SHOW MEASUREMENTS");

        let (_, got) = show_statement("SHOW RETENTION POLICIES ON \"foo\"").unwrap();
        assert_eq!(format!("{}", got), "SHOW RETENTION POLICIES ON foo");

        let (_, got) = show_statement("SHOW TAG KEYS").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG KEYS");

        let (_, got) = show_statement("SHOW TAG VALUES WITH KEY = some_key").unwrap();
        assert_eq!(format!("{}", got), "SHOW TAG VALUES WITH KEY = some_key");

        // Fallible cases

        assert_expect_error!(
            show_statement("SHOW TAG FOO WITH KEY = some_key"),
            "invalid SHOW TAG statement, expected KEYS or VALUES following TAG"
        );

        // Unsupported SHOW
        assert_expect_error!(
            show_statement("SHOW FOO"),
            "invalid SHOW statement, expected DATABASES, FIELD, MEASUREMENTS, TAG, or RETENTION following SHOW"
        );
    }
}
