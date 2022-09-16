use crate::common::{measurement_name_expression, MeasurementNameExpression};
use crate::internal::{expect, ParseResult};
use crate::string::{regex, Regex};
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::{char, multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::multi::separated_list1;
use nom::sequence::{pair, preceded, tuple};
use std::fmt;
use std::fmt::Formatter;

pub trait Parser: Sized {
    fn parse(i: &str) -> ParseResult<&str, Self>;
}

/// Represents a single measurement selection found in a `FROM` measurement clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MeasurementSelection<T: Parser> {
    Name(T),
    Regex(Regex),
}

impl<T: fmt::Display + Parser> fmt::Display for MeasurementSelection<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(ref name) => fmt::Display::fmt(name, f)?,
            Self::Regex(ref re) => fmt::Display::fmt(re, f)?,
        };

        Ok(())
    }
}

/// Represents a `FROM` clause of a `DELETE` or `SHOW` statement.
///
/// A `FROM` clause for a `DELETE` can only accept [`Identifier`] or regular expressions
/// for measurements names.
///
/// A `FROM` clause for a number of `SHOW` statements can accept a 3-part measurement name or
/// regular expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FromMeasurementClause<T: Parser> {
    first: MeasurementSelection<T>,
    rest: Option<Vec<MeasurementSelection<T>>>,
}

impl<T: fmt::Display + Parser> fmt::Display for FromMeasurementClause<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.first, f)?;
        if let Some(ref rest) = self.rest {
            for arg in rest {
                write!(f, ", {}", arg)?;
            }
        }

        Ok(())
    }
}

fn measurement_selection<T: Parser>(i: &str) -> ParseResult<&str, MeasurementSelection<T>> {
    alt((
        map(T::parse, MeasurementSelection::Name),
        map(regex, MeasurementSelection::Regex),
    ))(i)
}

fn from_clause<T: Parser>(i: &str) -> ParseResult<&str, FromMeasurementClause<T>> {
    // NOTE: This combinator is optimised to parse
    map(
        preceded(
            pair(tag_no_case("FROM"), multispace1),
            expect(
                "invalid FROM clause, expected one or more identifiers or regexes",
                tuple((
                    measurement_selection,
                    opt(preceded(
                        pair(multispace0, char(',')),
                        expect(
                            "invalid FROM clause, expected identifier after ,",
                            separated_list1(
                                preceded(multispace0, char(',')),
                                preceded(multispace0, measurement_selection),
                            ),
                        ),
                    )),
                )),
            ),
        ),
        |(first, rest)| FromMeasurementClause { first, rest },
    )(i)
}

impl Parser for MeasurementNameExpression {
    fn parse(i: &str) -> ParseResult<&str, Self> {
        measurement_name_expression(i)
    }
}

/// Represents a `FROM` clause for various `SHOW` statements.
pub type ShowFromClause = FromMeasurementClause<MeasurementNameExpression>;

/// Parse a `FROM` clause for various `SHOW` statements.
pub fn show_from_clause(i: &str) -> ParseResult<&str, ShowFromClause> {
    from_clause(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_show_from_clause() {
        use crate::simple_from_clause::MeasurementSelection::*;

        let (_, from) = show_from_clause("FROM c").unwrap();
        assert_eq!(
            from,
            ShowFromClause {
                first: Name(MeasurementNameExpression {
                    database: None,
                    retention_policy: None,
                    name: "c".into()
                }),
                rest: None
            }
        );

        let (_, from) = show_from_clause("FROM a..c").unwrap();
        assert_eq!(
            from,
            ShowFromClause {
                first: Name(MeasurementNameExpression {
                    database: Some("a".into()),
                    retention_policy: None,
                    name: "c".into()
                }),
                rest: None
            }
        );

        let (_, from) = show_from_clause("FROM a.b.c").unwrap();
        assert_eq!(
            from,
            ShowFromClause {
                first: Name(MeasurementNameExpression {
                    database: Some("a".into()),
                    retention_policy: Some("b".into()),
                    name: "c".into()
                }),
                rest: None
            }
        );

        let (_, from) = show_from_clause("FROM /reg/").unwrap();
        assert_eq!(
            from,
            ShowFromClause {
                first: Regex("reg".into()),
                rest: None
            }
        );

        let (_, from) = show_from_clause("FROM c, /reg/").unwrap();
        assert_eq!(
            from,
            ShowFromClause {
                first: Name(MeasurementNameExpression {
                    database: None,
                    retention_policy: None,
                    name: "c".into()
                }),
                rest: Some(vec![Regex("reg".into())]),
            }
        );
    }
}
