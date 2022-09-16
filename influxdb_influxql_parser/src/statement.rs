use crate::delete::{delete, DeleteStatement};
use crate::identifier::Identifier;
use crate::internal::ParseResult;
use crate::show::show_statement;
use crate::show_field_keys::ShowFieldKeysStatement;
use crate::show_measurements::ShowMeasurementsStatement;
use crate::show_tag_keys::ShowTagKeysStatement;
use crate::show_tag_values::ShowTagValuesStatement;
use nom::branch::alt;
use nom::combinator::map;
use std::fmt::{Display, Formatter};

/// An InfluxQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Represents a `DELETE` statement.
    Delete(Box<DeleteStatement>),
    /// Represents a `SHOW DATABASES` statement.
    ShowDatabases,
    /// Represents a `SHOW MEASUREMENTS` statement.
    ShowMeasurements(Box<ShowMeasurementsStatement>),
    /// Represents a `SHOW RETENTION POLICIES` statement.
    ShowRetentionPolicies {
        /// Name of the database to list the retention policies, or all if this is `None`.
        database: Option<Identifier>,
    },
    /// Represents a `SHOW TAG KEYS` statement.
    ShowTagKeys(Box<ShowTagKeysStatement>),
    /// Represents a `SHOW TAG VALUES` statement.
    ShowTagValues(Box<ShowTagValuesStatement>),
    /// Represents a `SHOW FIELD KEYS` statement.
    ShowFieldKeys(Box<ShowFieldKeysStatement>),
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delete(s) => Display::fmt(s, f)?,
            Self::ShowDatabases => f.write_str("SHOW DATABASES")?,
            Self::ShowMeasurements(s) => Display::fmt(s, f)?,
            Self::ShowRetentionPolicies { database } => {
                write!(f, "SHOW RETENTION POLICIES")?;
                if let Some(database) = database {
                    write!(f, " ON {}", database)?;
                }
            }
            Self::ShowTagKeys(s) => Display::fmt(s, f)?,
            Self::ShowTagValues(s) => Display::fmt(s, f)?,
            Self::ShowFieldKeys(s) => Display::fmt(s, f)?,
        };

        Ok(())
    }
}

/// Parse a single InfluxQL statement.
pub fn statement(i: &str) -> ParseResult<&str, Statement> {
    alt((
        map(delete, |s| Statement::Delete(Box::new(s))),
        show_statement,
    ))(i)
}

#[cfg(test)]
mod test {
    use crate::statement;

    #[test]
    fn test_statement() {
        // validate one of each statement parser is accepted

        statement("DELETE FROM foo").unwrap();

        statement("SHOW TAG KEYS").unwrap();
    }
}
