use crate::identifier::Identifier;
use crate::internal::ParseResult;
use crate::show::show_statement;
use crate::show_measurements::ShowMeasurementsStatement;
use std::fmt::{Display, Formatter};

/// An InfluxQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Represents a `SHOW DATABASES` statement.
    ShowDatabases,
    /// Represents a `SHOW MEASUREMENTS` statement.
    ShowMeasurements(Box<ShowMeasurementsStatement>),
    /// Represents a `SHOW RETENTION POLICIES` statement.
    ShowRetentionPolicies {
        /// Name of the database to list the retention policies, or all if this is `None`.
        database: Option<Identifier>,
    },
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ShowDatabases => f.write_str("SHOW DATABASES")?,
            Self::ShowMeasurements(s) => write!(f, "{}", s)?,
            Self::ShowRetentionPolicies { database } => {
                write!(f, "SHOW RETENTION POLICIES")?;
                if let Some(database) = database {
                    write!(f, " ON {}", database)?;
                }
            }
        };

        Ok(())
    }
}

/// Parse a single InfluxQL statement.
pub fn statement(i: &str) -> ParseResult<&str, Statement> {
    // NOTE: This will become an alt(()) once more statements are added
    show_statement(i)
}
