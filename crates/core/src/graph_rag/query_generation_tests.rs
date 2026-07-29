use super::*;
use crate::schema::{PropertyType, TableKind};

fn schema_context() -> GraphRagSchemaContext {
    let mut catalog = Catalog::default();
    let memory = catalog.get_or_create_label("Memory");
    let entity = catalog.get_or_create_label("Entity");
    let mentions = catalog.get_or_create_rel_type("MENTIONS");
    let memory_table = catalog.get_or_create_table(TableKind::Node, "Memory");
    let entity_table = catalog.get_or_create_table(TableKind::Node, "Entity");
    let mentions_table = catalog.get_or_create_table(TableKind::Relationship, "MENTIONS");
    catalog.get_or_create_property(memory_table, "id", PropertyType::String, false);
    catalog.get_or_create_property(entity_table, "name", PropertyType::String, true);
    catalog.get_or_create_property(mentions_table, "confidence", PropertyType::Float, true);
    let statistics = GraphStatistics {
        computed_at_commit_epoch: 9,
        label_counts: BTreeMap::from([(memory, 4), (entity, 2)]),
        rel_type_counts: BTreeMap::from([(mentions, 3)]),
        path_counts: BTreeMap::from([((memory, mentions, entity), 3)]),
        ..GraphStatistics::default()
    };
    build_graph_rag_schema_context(
        &catalog,
        &statistics,
        GraphRagSchemaContextOptions::default(),
    )
}

#[test]
fn generates_bounded_parameterized_route_query() {
    let context = schema_context();
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    operator: GraphRagQueryPredicateOperator::Eq,
                    parameter: Some("memory_id".to_string()),
                },
                GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Relationship,
                    property: "confidence".to_string(),
                    operator: GraphRagQueryPredicateOperator::Gte,
                    parameter: Some("minimum_confidence".to_string()),
                },
            ],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "entity_name".to_string(),
            }],
            limit: 5,
        })
        .unwrap();

    assert_eq!(
        generated.cypher,
        "MATCH (n0:Memory)-[r0:MENTIONS]->(n1:Entity) \
         WHERE n0.id = $memory_id AND r0.confidence >= $minimum_confidence \
         RETURN n1.name AS entity_name LIMIT 5"
    );
    assert_eq!(
        generated.required_parameters,
        vec!["memory_id".to_string(), "minimum_confidence".to_string()]
    );
}

#[test]
fn rejects_stale_schema_and_invented_identifiers() {
    let context = schema_context();
    let mut draft = GraphRagQueryDraft {
        schema_fingerprint: context.fingerprint.wrapping_add(1),
        pattern: GraphRagQueryPattern::Node {
            label: "Memory".to_string(),
        },
        predicates: Vec::new(),
        projections: vec![GraphRagQueryProjection {
            binding: GraphRagQueryBinding::Source,
            property: "id".to_string(),
            alias: "id".to_string(),
        }],
        limit: 5,
    };
    assert!(matches!(
        context.generate_query(&draft),
        Err(GraphRagQueryGenerationError::SchemaFingerprintMismatch { .. })
    ));

    draft.schema_fingerprint = context.fingerprint;
    draft.pattern = GraphRagQueryPattern::Node {
        label: "InventedByModel".to_string(),
    };
    assert_eq!(
        context.generate_query(&draft).unwrap_err(),
        GraphRagQueryGenerationError::UnknownLabel("InventedByModel".to_string())
    );
}

#[test]
fn rejects_unavailable_bindings_and_unbounded_limits() {
    let context = schema_context();
    let draft = GraphRagQueryDraft {
        schema_fingerprint: context.fingerprint,
        pattern: GraphRagQueryPattern::Node {
            label: "Memory".to_string(),
        },
        predicates: Vec::new(),
        projections: vec![GraphRagQueryProjection {
            binding: GraphRagQueryBinding::Target,
            property: "name".to_string(),
            alias: "name".to_string(),
        }],
        limit: 5,
    };
    assert_eq!(
        context.generate_query(&draft).unwrap_err(),
        GraphRagQueryGenerationError::BindingUnavailable(GraphRagQueryBinding::Target)
    );

    let mut unbounded = draft;
    unbounded.limit = MAX_GRAPH_RAG_QUERY_LIMIT + 1;
    assert!(matches!(
        context.generate_query(&unbounded),
        Err(GraphRagQueryGenerationError::InvalidLimit { .. })
    ));
}
