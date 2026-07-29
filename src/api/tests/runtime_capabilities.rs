use super::*;
use crate::{RuntimeCapabilities, RuntimeCapability, SkeinError};

#[test]
fn disabled_query_capabilities_fail_before_planning_or_catalog_mutation() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::FullTextSearch, false)
            .with(RuntimeCapability::VectorSearch, false)
            .with(RuntimeCapability::GraphAnalytics, false),
        ..DatabaseConfig::default()
    });
    let plan_cache_before = db.plan_cache_stats();

    assert_capability_error(
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)")
            .unwrap_err(),
        RuntimeCapability::FullTextSearch,
    );
    assert_capability_error(
        db.query("CALL vector_search($embedding, topK := 1) RETURN id, score")
            .unwrap_err(),
        RuntimeCapability::VectorSearch,
    );
    assert_capability_error(
        db.query("CALL project_graph('EntityGraph', ['Entity'], ['MENTIONS'])")
            .unwrap_err(),
        RuntimeCapability::GraphAnalytics,
    );

    assert_eq!(db.plan_cache_stats(), plan_cache_before);
    assert!(db.property_indexes().is_empty());
}

#[test]
fn disabled_search_capability_does_not_fall_back_to_another_retriever() {
    let mut search = SearchIndex::in_memory();
    search.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::FullTextSearch, false),
    );

    let error = search
        .try_search_with_options(
            "skein",
            None,
            SearchMode::Text,
            crate::search::SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::FullTextSearch);
}

#[test]
fn disabled_background_capability_precedes_qos_admission() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::BackgroundMaintenance, false),
        ..DatabaseConfig::default()
    });

    let error = db
        .run_background_schema_maintenance(&LocalQosPolicy::default(), &LocalQosState::default(), 0)
        .unwrap_err();

    assert_capability_error(error, RuntimeCapability::BackgroundMaintenance);
}

#[test]
fn runtime_capabilities_cannot_exceed_compiled_availability() {
    let requested = RuntimeCapabilities::desktop_bound();
    let db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: requested,
        ..DatabaseConfig::default()
    });
    let mut search = SearchIndex::in_memory();
    search.set_runtime_capabilities(requested);

    assert_eq!(
        db.runtime_capabilities(),
        crate::compiled_runtime_capabilities()
    );
    assert_eq!(
        search.runtime_capabilities(),
        crate::compiled_runtime_capabilities()
    );
}

#[cfg(not(any(
    feature = "background-maintenance",
    feature = "full-text-search",
    feature = "graph-analytics",
    feature = "vector-search"
)))]
#[test]
fn minimal_build_fails_closed_for_every_optional_capability() {
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::desktop_bound(),
        ..DatabaseConfig::default()
    });

    assert_capability_error(
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)")
            .unwrap_err(),
        RuntimeCapability::FullTextSearch,
    );
    assert_capability_error(
        db.query("CALL vector_search($embedding, topK := 1) RETURN id, score")
            .unwrap_err(),
        RuntimeCapability::VectorSearch,
    );
    assert_capability_error(
        db.query("CALL project_graph('EntityGraph', ['Entity'], ['MENTIONS'])")
            .unwrap_err(),
        RuntimeCapability::GraphAnalytics,
    );
    assert_capability_error(
        db.run_background_schema_maintenance(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            0,
        )
        .unwrap_err(),
        RuntimeCapability::BackgroundMaintenance,
    );
}

fn assert_capability_error(error: SkeinError, expected: RuntimeCapability) {
    assert_eq!(
        error,
        SkeinError::CapabilityUnavailable {
            capability: expected
        }
    );
}
