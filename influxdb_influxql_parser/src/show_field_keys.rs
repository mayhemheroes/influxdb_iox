use crate::common::{limit_clause, offset_clause};
use crate::identifier::Identifier;
use crate::internal::{expect, ParseResult};
use crate::show::{from_clause, on_clause, FromMeasurementClause};
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::opt;
use nom::sequence::{preceded, tuple};
use std::fmt;
use std::fmt::Formatter;

/// Represents a `SHOW FIELD KEYS` InfluxQL statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShowFieldKeysStatement {
    /// The name of the database to query. If `None`, a default
    /// database will be used.
    pub database: Option<Identifier>,

    /// The measurement or measurements to restrict which field keys
    /// are retrieved.
    pub from: Option<FromMeasurementClause>,

    /// A value to restrict the number of field keys returned.
    pub limit: Option<u64>,

    /// A value to specify an offset to start retrieving field keys.
    pub offset: Option<u64>,
}

impl fmt::Display for ShowFieldKeysStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW FIELD KEYS")?;

        if let Some(ref expr) = self.database {
            write!(f, " ON {}", expr)?;
        }

        if let Some(ref expr) = self.from {
            write!(f, " FROM {}", expr)?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }

        Ok(())
    }
}

/// Parse a `SHOW FIELD KEYS` statement, starting from the `FIELD` token.
pub fn show_field_keys(i: &str) -> ParseResult<&str, ShowFieldKeysStatement> {
    let (
        remaining_input,
        (
            _, // FIELD
            _, // whitespace
            _, // "KEYS"
            database,
            from,
            limit,
            offset,
        ),
    ) = tuple((
        tag_no_case("FIELD"),
        multispace1,
        expect(
            "invalid SHOW FIELD KEYS statement, expect KEYS following FIELD",
            tag_no_case("KEYS"),
        ),
        opt(preceded(multispace1, on_clause)),
        opt(preceded(multispace1, from_clause)),
        opt(preceded(multispace1, limit_clause)),
        opt(preceded(multispace1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowFieldKeysStatement {
            database,
            from,
            limit,
            offset,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;

    #[test]
    fn test_show_field_keys() {
        // No optional clauses
        let (_, got) = show_field_keys("FIELD KEYS").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS");

        let (_, got) = show_field_keys("FIELD KEYS ON db").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS ON db");

        // measurement selection using name
        let (_, got) = show_field_keys("FIELD KEYS FROM db..foo").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS FROM db..foo");

        // measurement selection using regex
        let (_, got) = show_field_keys("FIELD KEYS FROM /foo/").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS FROM /foo/");

        // measurement selection using list
        let (_, got) = show_field_keys("FIELD KEYS FROM /foo/ , bar, \"foo bar\"").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW FIELD KEYS FROM /foo/, bar, \"foo bar\""
        );

        let (_, got) = show_field_keys("FIELD KEYS LIMIT 1").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS LIMIT 1");

        let (_, got) = show_field_keys("FIELD KEYS OFFSET 2").unwrap();
        assert_eq!(format!("{}", got), "SHOW FIELD KEYS OFFSET 2");

        // all optional clauses
        let (_, got) = show_field_keys("FIELD KEYS ON db FROM /foo/ LIMIT 1 OFFSET 2").unwrap();
        assert_eq!(
            format!("{}", got),
            "SHOW FIELD KEYS ON db FROM /foo/ LIMIT 1 OFFSET 2"
        );

        // Fallible cases
        assert_expect_error!(
            show_field_keys("FIELD ON db"),
            "invalid SHOW FIELD KEYS statement, expect KEYS following FIELD"
        );
    }
}
