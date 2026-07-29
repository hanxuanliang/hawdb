use super::{Database, DatabaseReadTransaction};
use skein_core::{
    build_graph_rag_schema_context, GraphRagSchemaContext, GraphRagSchemaContextOptions,
};

impl Database {
    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        build_graph_rag_schema_context(&self.catalog, &self.store.statistics(), options)
    }
}

impl DatabaseReadTransaction {
    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        build_graph_rag_schema_context(&self.catalog, &self.store.statistics(), options)
    }
}
