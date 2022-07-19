use std::sync::Arc;

use crate::exec::context::IOxSessionContext;
use datafusion::{error::Result, physical_plan::ExecutionPlan};

/// This struct can create plans for running SQL queries against databases
#[derive(Debug, Default)]
pub struct SqlQueryPlanner {}

impl SqlQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plan a SQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query(
        &self,
        query: &str,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ctx.prepare_sql(query).await
    }

    /// Plan a SQL statement against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query_ast(
        &self,
        statement: datafusion::sql::sqlparser::ast::Statement,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ctx.prepare_ast(statement).await
    }
}
