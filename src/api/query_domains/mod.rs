mod entities;
mod properties;

pub(super) use entities::{
    knowledge_entity_batch_via_query_runtime, knowledge_entity_via_query_runtime,
    knowledge_scoped_entity_batch_via_query_runtime, knowledge_scoped_entity_via_query_runtime,
    lookup_entities_via_query_runtime_strict,
};
pub(super) use properties::{
    knowledge_property_batch_via_query_runtime, knowledge_scoped_property_batch_via_query_runtime,
};
