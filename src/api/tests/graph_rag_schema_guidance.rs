use super::*;
use crate::{
    GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, GraphRagSchemaContextOptions,
};

#[test]
fn graph_rag_schema_context_guides_queries_through_the_read_runtime() {
    let mut db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        ..DatabaseConfig::default()
    });
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE NODE TABLE Entity").unwrap();
    db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Entity(name) TYPE STRING")
        .unwrap();
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(confidence) TYPE FLOAT")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory-1', private_payload: 'do-not-render'})\
         -[:MENTIONS {confidence: 0.9}]->(:Entity {id: 'entity-1', name: 'Skein'})",
    )
    .unwrap();

    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    assert_eq!(context.labels.len(), 2);
    assert_eq!(context.relationship_types.len(), 1);
    assert_eq!(context.routes.len(), 1);
    assert!(context
        .properties
        .iter()
        .any(|property| property.subject_name == "Memory" && property.name == "id"));
    let guidance = context.render_compact_cypher_guidance();
    assert!(guidance.contains("RULES read_only=true"));
    assert!(guidance.contains("ROUTE (Memory)-[:MENTIONS]->(Entity)"));
    assert!(!guidance.contains("do-not-render"));

    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                operator: GraphRagQueryPredicateOperator::Eq,
                parameter: Some("id".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "name".to_string(),
            }],
            limit: 5,
        })
        .unwrap();
    let slow_query_count = db.slow_query_log_snapshot().len();
    let output = db
        .query_with_params(
            &generated.cypher,
            &BTreeMap::from([("id".to_string(), Value::String("memory-1".to_string()))]),
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Skein".to_string()))
    );
    assert_eq!(db.slow_query_log_snapshot().len(), slow_query_count + 1);

    let mut read = db.begin_read_transaction();
    let error = read.query("CREATE (:InventedByModel)").unwrap_err();
    assert!(error
        .to_string()
        .contains("read transaction query must not be a mutation"));
}

#[test]
fn graph_rag_schema_context_is_pinned_to_the_read_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-1'})").unwrap();
    let read = db.begin_read_transaction();
    let pinned = read.graph_rag_schema_context(GraphRagSchemaContextOptions::default());

    db.query("CREATE (:Entity {id: 'entity-1'})").unwrap();
    let latest = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let pinned_again = read.graph_rag_schema_context(GraphRagSchemaContextOptions::default());

    assert_eq!(pinned, pinned_again);
    assert_ne!(pinned.fingerprint, latest.fingerprint);
    assert_eq!(pinned.labels.len(), 1);
    assert_eq!(latest.labels.len(), 2);
}

#[test]
fn graph_rag_generated_predicates_follow_the_cypher_parser_contract() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING")
        .unwrap();
    let context = db.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let operators = [
        GraphRagQueryPredicateOperator::Eq,
        GraphRagQueryPredicateOperator::NotEq,
        GraphRagQueryPredicateOperator::Lt,
        GraphRagQueryPredicateOperator::Lte,
        GraphRagQueryPredicateOperator::Gt,
        GraphRagQueryPredicateOperator::Gte,
        GraphRagQueryPredicateOperator::In,
        GraphRagQueryPredicateOperator::Contains,
        GraphRagQueryPredicateOperator::StartsWith,
        GraphRagQueryPredicateOperator::EndsWith,
        GraphRagQueryPredicateOperator::IsNull,
        GraphRagQueryPredicateOperator::IsNotNull,
    ];

    for operator in operators {
        let parameter = (!matches!(
            operator,
            GraphRagQueryPredicateOperator::IsNull | GraphRagQueryPredicateOperator::IsNotNull
        ))
        .then(|| "value".to_string());
        let generated = context
            .generate_query(&GraphRagQueryDraft {
                schema_fingerprint: context.fingerprint,
                pattern: GraphRagQueryPattern::Node {
                    label: "Memory".to_string(),
                },
                predicates: vec![GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "title".to_string(),
                    operator,
                    parameter,
                }],
                projections: vec![GraphRagQueryProjection {
                    binding: GraphRagQueryBinding::Source,
                    property: "title".to_string(),
                    alias: "title".to_string(),
                }],
                limit: 5,
            })
            .unwrap();

        skein_cypher::parse(&generated.cypher).unwrap();
    }
}
