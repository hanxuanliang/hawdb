mod binder;
mod catalog;
mod ir;
mod lowering;

pub use binder::{bind_postgres_graph_tables, PgqBindError, PgqBindErrorCode};
pub use catalog::{
    PgqBindingContext, PgqCatalog, PropertyGraphCatalog, PropertyGraphElementSchema,
    PropertyGraphSchema,
};
pub use ir::*;
pub use lowering::{
    lower_bound_pgq_graph_table, PgqLoweringError, PgqLoweringErrorCode, PgqLoweringParameters,
};

#[cfg(test)]
mod tests;
