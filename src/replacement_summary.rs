use crate::{
    graph_route_readiness::{
        NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL, NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
    },
    nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_specs_json,
    replacement_readiness_family_evidence_health_from_bundle,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use std::collections::{BTreeMap, BTreeSet};

const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-cli";
const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
const SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "skein_search_projection_segment_descriptor_missing";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "skein_search_projection_segment_descriptor_fields_missing";
pub fn nowledge_replacement_summary_usage() -> String {
    "nowledge-replacement-summary requires [--require-production-ready] [--compact] [--max-family-items <n>] [--max-blockers <n>] [--search-projection-evidence-json <path>] [--search-projection-shadow-evidence-json <path>] [--search-candidate-shadow-evidence-json <path>] [--bounded-read-evidence-json <path>] [--query-runtime-preflight-json <path>] [--query-family-evidence-json <path>] <migration-gate-json>"
        .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeReplacementSummaryOptions {
    pub include_family_details: bool,
    pub max_family_items: Option<usize>,
    pub include_blocker_details: bool,
    pub max_blockers: Option<usize>,
}

impl Default for NowledgeReplacementSummaryOptions {
    fn default() -> Self {
        Self {
            include_family_details: true,
            max_family_items: None,
            include_blocker_details: true,
            max_blockers: None,
        }
    }
}

pub fn nowledge_replacement_summary_json(bundle: &serde_json::Value) -> serde_json::Value {
    nowledge_replacement_summary_json_with_options(
        bundle,
        NowledgeReplacementSummaryOptions::default(),
    )
}

pub fn nowledge_replacement_summary_json_with_options(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> serde_json::Value {
    let covered_business_surface_per_million =
        json_get_u64_path(bundle, &["inventory_gate", "coverage_per_million"])
            .or_else(|| json_get_u64_path(bundle, &["coverage", "coverage_per_million"]));
    let shadow_parity_per_million = json_get_u64_path(bundle, &["cutover", "matched_per_million"])
        .or_else(|| json_get_u64_path(bundle, &["migration_gate", "shadow_matched_per_million"]));
    let replacement_readiness_per_million =
        json_get_u64_path(bundle, &["replacement_readiness_per_million"]);
    let migration_gate_decision = json_get_str_path(bundle, &["migration_gate", "decision"]);
    let cutover_decision = json_get_str_path(bundle, &["cutover", "decision"]);
    let cutover_evidence_eligible =
        json_get_bool_path(bundle, &["cutover_evidence", "eligible"]).unwrap_or(false);
    let shadow_evidence = shadow_evidence_summary(bundle);
    let shadow_evidence_ready = shadow_evidence.ready;
    let previous_wrapper_contract_ready =
        json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "ready"])
            .unwrap_or(false);
    let full_contract_evidence = full_contract_evidence_summary(bundle);
    let full_contract_evidence_ready = full_contract_evidence.ready;
    let dual_engine_evidence = dual_engine_evidence_summary(bundle);
    let dual_engine_evidence_present = dual_engine_evidence.present;
    let dual_engine_evidence_ready = dual_engine_evidence.ready;
    let dual_engine_evidence_consistent = dual_engine_evidence.consistent;
    let search_projection_evidence = search_projection_evidence_summary(bundle);
    let search_projection_evidence_ready = search_projection_evidence.ready;
    let search_projection_shadow_evidence = search_projection_shadow_evidence_summary(bundle);
    let search_projection_shadow_evidence_ready = search_projection_shadow_evidence.ready;
    let search_candidate_shadow_evidence = search_candidate_shadow_evidence_summary(bundle);
    let search_candidate_shadow_evidence_ready = search_candidate_shadow_evidence.ready;
    let bounded_read_evidence = bounded_read_evidence_summary(bundle);
    let bounded_read_evidence_ready = bounded_read_evidence.ready;
    let graph_route_readiness = nowledge_graph_route_readiness_summary_from_bundle(bundle);
    let graph_route_readiness_ready = graph_route_readiness.ready;
    let query_runtime_preflight = query_runtime_preflight_summary(bundle);
    let query_runtime_preflight_ready = query_runtime_preflight.ready;
    let background_graph_delta_evidence_missing =
        background_maintenance_graph_delta_evidence_missing(bundle);
    let family_health = replacement_readiness_family_evidence_health_from_bundle(bundle);
    let family_evidence_ready = family_health.present && family_health.ready;
    let production_cutover_ready = migration_gate_decision == Some("ready")
        && cutover_decision == Some("ready")
        && cutover_evidence_eligible
        && shadow_evidence_ready
        && previous_wrapper_contract_ready
        && full_contract_evidence_ready
        && dual_engine_evidence_present
        && dual_engine_evidence_ready == Some(true)
        && dual_engine_evidence_consistent
        && search_projection_evidence_ready
        && search_projection_shadow_evidence_ready
        && search_candidate_shadow_evidence_ready
        && bounded_read_evidence_ready
        && graph_route_readiness_ready
        && query_runtime_preflight_ready
        && !background_graph_delta_evidence_missing
        && family_evidence_ready
        && replacement_readiness_per_million == Some(1_000_000);
    let production_replacement_per_million = if production_cutover_ready {
        1_000_000
    } else {
        0
    };
    let blocking_categories = nowledge_replacement_blocking_categories(
        bundle,
        ReplacementReadinessInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            search_candidate_shadow_evidence_ready,
            bounded_read_evidence_ready,
            graph_route_readiness_ready,
            query_runtime_preflight_ready,
            background_graph_delta_evidence_missing,
            family_evidence_ready,
        },
    );
    let blockers = nowledge_replacement_blockers(bundle);
    let blocker_details = nowledge_replacement_blocker_details(&blockers, options);
    let missing_evidence = nowledge_replacement_missing_evidence(bundle);
    let family_details = replacement_readiness_family_details(bundle, options);
    let family_summary = replacement_readiness_family_summary(bundle, family_details.omitted_count);
    let next_actions = nowledge_replacement_next_actions(
        bundle,
        NextActionInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            search_candidate_shadow_evidence_ready,
            bounded_read_evidence_ready,
            graph_route_readiness_ready,
            query_runtime_preflight_ready,
            background_graph_delta_evidence_missing,
            family_evidence_ready,
            production_cutover_ready,
        },
    );

    serde_json::json!({
        "protocol": "skein-nowledge-replacement-summary",
        "business_surface": {
            "covered_per_million": covered_business_surface_per_million,
            "ready": covered_business_surface_per_million == Some(1_000_000),
        },
        "shadow_parity": {
            "matched_per_million": shadow_parity_per_million,
            "decision": cutover_decision,
            "ready": cutover_decision == Some("ready") && shadow_parity_per_million == Some(1_000_000),
        },
        "replacement_readiness_per_million": replacement_readiness_per_million,
        "production_replacement_per_million": production_replacement_per_million,
        "production_cutover_ready": production_cutover_ready,
        "migration_gate_decision": migration_gate_decision,
        "dual_engine_evidence": {
            "present": dual_engine_evidence.present,
            "ready": dual_engine_evidence.ready,
            "consistent": dual_engine_evidence.consistent,
            "primary_engine": dual_engine_evidence.primary_engine,
            "shadow_engine": dual_engine_evidence.shadow_engine,
            "primary_check_count": dual_engine_evidence.primary_check_count,
            "shadow_check_count": dual_engine_evidence.shadow_check_count,
            "matched_check_count": dual_engine_evidence.matched_check_count,
            "primary_only_check_count": dual_engine_evidence.primary_only_check_count,
            "matched_per_million": dual_engine_evidence.matched_per_million,
        },
        "search_projection_evidence": {
            "protocol": search_projection_evidence.protocol,
            "present": search_projection_evidence.present,
            "ready": search_projection_evidence.ready,
            "derived_projection": search_projection_evidence.derived_projection,
            "all_tables_covered": search_projection_evidence.all_tables_covered,
            "covered_table_count": search_projection_evidence.covered_table_count,
            "required_table_count": search_projection_evidence.required_table_count,
            "fts_ready": search_projection_evidence.fts_ready,
            "vector_ready": search_projection_evidence.vector_ready,
            "document_identity_ready": search_projection_evidence.document_identity_ready,
            "embedding_identity_ready": search_projection_evidence.embedding_identity_ready,
            "fail_soft_ready": search_projection_evidence.fail_soft_ready,
            "rebuild_marker_ready": search_projection_evidence.rebuild_marker_ready,
            "metadata_repair_marker_ready": search_projection_evidence.metadata_repair_marker_ready,
            "incremental_update_ready": search_projection_evidence.incremental_update_ready,
            "source_chunk_ready": search_projection_evidence.source_chunk_ready,
            "predicate_pushdown_ready": search_projection_evidence.predicate_pushdown_ready,
            "compressed_vector_projection_required": search_projection_evidence.compressed_vector_projection_required,
            "compressed_vector_projection_ready": search_projection_evidence.compressed_vector_projection_ready,
            "blocker_codes": search_projection_evidence.blocker_codes,
        },
        "search_projection_shadow_evidence": {
            "protocol": search_projection_shadow_evidence.protocol,
            "evidence_source": search_projection_shadow_evidence.evidence_source,
            "present": search_projection_shadow_evidence.present,
            "ready": search_projection_shadow_evidence.ready,
            "primary_ready": search_projection_shadow_evidence.primary_ready,
            "shadow_ready": search_projection_shadow_evidence.shadow_ready,
            "document_count_parity": search_projection_shadow_evidence.document_count_parity,
            "document_identity_parity": search_projection_shadow_evidence.document_identity_parity,
            "table_parity_ready": search_projection_shadow_evidence.table_parity_ready,
            "embedding_identity_parity": search_projection_shadow_evidence.embedding_identity_parity,
            "lifecycle_parity": search_projection_shadow_evidence.lifecycle_parity,
            "incremental_watermark_parity": search_projection_shadow_evidence.incremental_watermark_parity,
            "predicate_pushdown_parity": search_projection_shadow_evidence.predicate_pushdown_parity,
            "pushdown_evidence": search_projection_shadow_evidence.pushdown_evidence,
            "primary_engine": search_projection_shadow_evidence.primary_engine,
            "shadow_engine": search_projection_shadow_evidence.shadow_engine,
            "blocker_codes": search_projection_shadow_evidence.blocker_codes,
        },
        "search_candidate_shadow_evidence": {
            "protocol": search_candidate_shadow_evidence.protocol,
            "evidence_source": search_candidate_shadow_evidence.evidence_source,
            "route": search_candidate_shadow_evidence.route,
            "present": search_candidate_shadow_evidence.present,
            "ready": search_candidate_shadow_evidence.ready,
            "candidate_primary_engine": search_candidate_shadow_evidence.candidate_primary_engine,
            "primary_engine": search_candidate_shadow_evidence.primary_engine,
            "shadow_engine": search_candidate_shadow_evidence.shadow_engine,
            "request_count": search_candidate_shadow_evidence.request_count,
            "primary_candidate_count": search_candidate_shadow_evidence.primary_candidate_count,
            "shadow_candidate_count": search_candidate_shadow_evidence.shadow_candidate_count,
            "matched_candidate_count": search_candidate_shadow_evidence.matched_candidate_count,
            "primary_only_candidate_count": search_candidate_shadow_evidence.primary_only_candidate_count,
            "candidate_counts_ready": search_candidate_shadow_evidence.candidate_counts_ready,
            "candidate_identity_ready": search_candidate_shadow_evidence.candidate_identity_ready,
            "candidate_identity_parity": search_candidate_shadow_evidence.candidate_identity_parity,
            "row_count_parity": search_candidate_shadow_evidence.row_count_parity,
            "vector_top_k_overlap_ready": search_candidate_shadow_evidence.vector_top_k_overlap_ready,
            "fts_top_k_overlap_ready": search_candidate_shadow_evidence.fts_top_k_overlap_ready,
            "shadow_scan_filter_pushdown_ready": search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready,
            "shadow_scan_field_pruning_ready": search_candidate_shadow_evidence.shadow_scan_field_pruning_ready,
            "shadow_scan_field_summary_count": search_candidate_shadow_evidence.shadow_scan_field_summary_count,
            "filter_pushdown_ready": search_candidate_shadow_evidence.filter_pushdown_ready,
            "filter_pushdown_field_summary_count": search_candidate_shadow_evidence.filter_pushdown_field_summary_count,
            "filter_pushdown_missing_required_fields": search_candidate_shadow_evidence.filter_pushdown_missing_required_fields,
            "blocker_codes": search_candidate_shadow_evidence.blocker_codes,
        },
        "bounded_read_evidence": {
            "protocol": bounded_read_evidence.protocol,
            "present": bounded_read_evidence.present,
            "ready": bounded_read_evidence.ready,
            "mode": bounded_read_evidence.mode,
            "max_rows": bounded_read_evidence.max_rows,
            "execution_row_cap": bounded_read_evidence.execution_row_cap,
            "row_limit_enforced_before_output": bounded_read_evidence.row_limit_enforced_before_output,
            "operator_row_cap_enabled": bounded_read_evidence.operator_row_cap_enabled,
            "streaming": bounded_read_evidence.streaming,
            "blocking_operator_count": bounded_read_evidence.blocking_operator_count,
            "covered_routes": bounded_read_evidence.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": bounded_read_evidence.missing_covered_routes,
            "route_primary_ready": bounded_read_evidence.route_primary_ready,
            "primary_ready_routes": bounded_read_evidence.primary_ready_routes,
            "route_query_plan_evidence_ready": bounded_read_evidence.route_query_plan_evidence_ready,
            "route_query_profile_evidence_ready": bounded_read_evidence.route_query_profile_evidence_ready,
            "relationship_property_pruning_required_count": bounded_read_evidence.relationship_property_pruning_required_count,
            "relationship_property_pruning_report_count": bounded_read_evidence.relationship_property_pruning_report_count,
            "route_relationship_property_pruning_evidence_ready": bounded_read_evidence.route_relationship_property_pruning_evidence_ready,
            "blocker_codes": bounded_read_evidence.blocker_codes,
        },
        "graph_route_readiness": graph_route_readiness.json(),
        "query_runtime_preflight": {
            "protocol": query_runtime_preflight.protocol,
            "present": query_runtime_preflight.present,
            "ready": query_runtime_preflight.ready,
            "database_opened": query_runtime_preflight.database_opened,
            "probe_count": query_runtime_preflight.probe_count,
            "passed_probe_count": query_runtime_preflight.passed_probe_count,
            "failed_probe_count": query_runtime_preflight.failed_probe_count,
            "required_route_count": query_runtime_preflight.required_route_count,
            "covered_route_count": query_runtime_preflight.covered_route_count,
            "covered_routes": query_runtime_preflight.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": query_runtime_preflight.missing_required_routes,
            "unknown_routes": query_runtime_preflight.unknown_routes,
            "duplicate_routes": query_runtime_preflight.duplicate_routes,
            "required_routes_covered": query_runtime_preflight.required_routes_covered,
            "route_coverage_ready": query_runtime_preflight.route_coverage_ready,
            "probe_details_ready": query_runtime_preflight.probe_details_ready,
            "blocker_codes": query_runtime_preflight.blocker_codes,
        },
        "cutover_evidence": {
            "eligible": cutover_evidence_eligible,
            "evidence_kind": json_get_str_path(bundle, &["cutover_evidence", "evidence_kind"]),
            "ready_engine_kind": json_get_str_path(bundle, &["cutover_evidence", "ready_engine_kind"]),
            "ready_wrapper_identity": json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]),
            "storage_recovery_required": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]),
            "storage_recovery_ready": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]),
            "storage_recovery_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_protocol_matches"]),
            "storage_recovery_durable": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_durable"]),
            "storage_recovery_checkpoint_boundary_present": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_checkpoint_boundary_present"]),
            "storage_recovery_wal_replay_bounded": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_wal_replay_bounded"]),
            "storage_recovery_torn_tail_clean": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_torn_tail_clean"]),
            "storage_recovery_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "storage_recovery_blocker_codes"]),
            "background_maintenance_required": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_required"]),
            "background_maintenance_ready": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_ready"]),
            "background_maintenance_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_protocol_matches"]),
            "background_maintenance_executable_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_count"]),
            "background_maintenance_admitted_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_count"]),
            "background_maintenance_deferred_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_deferred_search_projection_graph_delta_count"]),
            "background_maintenance_rejected_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_rejected_search_projection_graph_delta_count"]),
            "background_maintenance_executable_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_operations"]),
            "background_maintenance_admitted_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_operations"]),
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"]),
            "background_maintenance_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "background_maintenance_blocker_codes"]),
            "replacement_readiness_min_per_million": json_get_u64_path(bundle, &["cutover_evidence", "replacement_readiness_min_per_million"]),
        },
        "shadow_evidence": {
            "ready": shadow_evidence.ready,
            "run_evidence_kind": shadow_evidence.run_evidence_kind,
            "ready_engine_kind": shadow_evidence.ready_engine_kind,
            "ready_wrapper_identity": shadow_evidence.ready_wrapper_identity,
            "contract_wrapper_identity": shadow_evidence.contract_wrapper_identity,
            "cutover_ready_wrapper_identity": shadow_evidence.cutover_ready_wrapper_identity,
        },
        "previous_wrapper_contract_evidence": {
            "ready": previous_wrapper_contract_ready,
            "evidence_kind": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "evidence_kind"]),
            "wrapper_identity": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "wrapper_identity"]),
            "requires_full_contract_ready": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_full_contract_ready"]),
            "requires_wrapper_identity": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_wrapper_identity"]),
            "blocker_codes": json_get_array_path(bundle, &["previous_wrapper_contract_evidence", "blocker_codes"]),
        },
        "full_contract_evidence": {
            "ready": full_contract_evidence.ready,
            "required_contract_ready": full_contract_evidence.required_contract_ready,
            "full_contract_checked": full_contract_evidence.full_contract_checked,
            "full_contract_ready": full_contract_evidence.full_contract_ready,
            "selected_checks": full_contract_evidence.selected_checks,
            "check_count": full_contract_evidence.check_count,
        },
        "blocking_categories": blocking_categories,
        "blocker_summary": {
            "total_count": blockers.len(),
            "omitted_count": blocker_details.omitted_count,
        },
        "blockers": blocker_details.blockers,
        "missing_evidence": missing_evidence,
        "next_actions": next_actions,
        "replacement_readiness_family_summary": family_summary,
        "replacement_readiness_by_query_family": family_details.families,
    })
}

struct NowledgeReplacementBlockerDetails {
    blockers: Vec<String>,
    omitted_count: usize,
}

fn nowledge_replacement_blocker_details(
    blockers: &[String],
    options: NowledgeReplacementSummaryOptions,
) -> NowledgeReplacementBlockerDetails {
    if !options.include_blocker_details {
        return NowledgeReplacementBlockerDetails {
            blockers: Vec::new(),
            omitted_count: blockers.len(),
        };
    }
    let limit = options.max_blockers.unwrap_or(blockers.len());
    NowledgeReplacementBlockerDetails {
        blockers: blockers.iter().take(limit).cloned().collect(),
        omitted_count: blockers.len().saturating_sub(limit),
    }
}

struct ReplacementReadinessFamilyDetails {
    families: serde_json::Value,
    omitted_count: usize,
}

fn replacement_readiness_family_details(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> ReplacementReadinessFamilyDetails {
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !options.include_family_details {
        return ReplacementReadinessFamilyDetails {
            families: serde_json::json!([]),
            omitted_count: families.len(),
        };
    }
    let limit = options.max_family_items.unwrap_or(families.len());
    let omitted_count = families.len().saturating_sub(limit);
    ReplacementReadinessFamilyDetails {
        families: serde_json::Value::Array(families.into_iter().take(limit).collect()),
        omitted_count,
    }
}

fn replacement_readiness_family_summary(
    bundle: &serde_json::Value,
    omitted_count: usize,
) -> serde_json::Value {
    let health = replacement_readiness_family_evidence_health_from_bundle(bundle);
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_count = families.len();
    let ready_count = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                == Some(1_000_000)
        })
        .count();
    let blocked_count = total_count.saturating_sub(ready_count);
    let min_replacement_readiness_per_million = families
        .iter()
        .filter_map(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
        })
        .min();
    let blocked_query_families = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                != Some(1_000_000)
        })
        .filter_map(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    serde_json::json!({
        "total_count": total_count,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "omitted_count": omitted_count,
        "min_replacement_readiness_per_million": min_replacement_readiness_per_million,
        "blocked_query_families": blocked_query_families,
        "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        "missing_required_query_families": health.missing_required_query_families,
    })
}

struct ReplacementReadinessInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    search_candidate_shadow_evidence_ready: bool,
    bounded_read_evidence_ready: bool,
    graph_route_readiness_ready: bool,
    query_runtime_preflight_ready: bool,
    background_graph_delta_evidence_missing: bool,
    family_evidence_ready: bool,
}

struct NextActionInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    search_candidate_shadow_evidence_ready: bool,
    bounded_read_evidence_ready: bool,
    graph_route_readiness_ready: bool,
    query_runtime_preflight_ready: bool,
    background_graph_delta_evidence_missing: bool,
    family_evidence_ready: bool,
    production_cutover_ready: bool,
}

struct DualEngineEvidenceSummary<'a> {
    present: bool,
    ready: Option<bool>,
    consistent: bool,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    primary_check_count: Option<u64>,
    shadow_check_count: Option<u64>,
    matched_check_count: Option<u64>,
    primary_only_check_count: Option<u64>,
    matched_per_million: Option<u64>,
}

struct ShadowEvidenceSummary<'a> {
    ready: bool,
    run_evidence_kind: Option<&'a str>,
    ready_engine_kind: Option<&'a str>,
    ready_wrapper_identity: Option<&'a str>,
    contract_wrapper_identity: Option<&'a str>,
    cutover_ready_wrapper_identity: Option<&'a str>,
}

struct SearchProjectionEvidenceSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    derived_projection: Option<bool>,
    all_tables_covered: Option<bool>,
    covered_table_count: Option<u64>,
    required_table_count: Option<u64>,
    fts_ready: Option<bool>,
    vector_ready: Option<bool>,
    document_identity_ready: Option<bool>,
    embedding_identity_ready: Option<bool>,
    fail_soft_ready: Option<bool>,
    rebuild_marker_ready: Option<bool>,
    metadata_repair_marker_ready: Option<bool>,
    incremental_update_ready: Option<bool>,
    source_chunk_ready: Option<bool>,
    predicate_pushdown_ready: Option<bool>,
    compressed_vector_projection_required: Option<bool>,
    compressed_vector_projection_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct SearchProjectionShadowEvidenceSummary<'a> {
    protocol: Option<String>,
    evidence_source: Option<String>,
    present: bool,
    ready: bool,
    primary_ready: Option<bool>,
    shadow_ready: Option<bool>,
    document_count_parity: Option<bool>,
    document_identity_parity: Option<bool>,
    table_parity_ready: Option<bool>,
    embedding_identity_parity: Option<bool>,
    lifecycle_parity: Option<bool>,
    incremental_watermark_parity: Option<bool>,
    predicate_pushdown_parity: Option<bool>,
    pushdown_evidence: serde_json::Value,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    blocker_codes: serde_json::Value,
}

struct SearchCandidateShadowEvidenceSummary {
    protocol: Option<String>,
    evidence_source: Option<String>,
    route: Option<String>,
    present: bool,
    ready: bool,
    candidate_primary_engine: Option<String>,
    primary_engine: Option<String>,
    shadow_engine: Option<String>,
    request_count: Option<u64>,
    primary_candidate_count: Option<u64>,
    shadow_candidate_count: Option<u64>,
    matched_candidate_count: Option<u64>,
    primary_only_candidate_count: Option<u64>,
    candidate_counts_ready: bool,
    candidate_identity_ready: Option<bool>,
    candidate_identity_parity: Option<bool>,
    row_count_parity: Option<bool>,
    vector_top_k_overlap_ready: Option<bool>,
    fts_top_k_overlap_ready: Option<bool>,
    shadow_scan_filter_pushdown_ready: Option<bool>,
    shadow_scan_field_pruning_ready: Option<bool>,
    shadow_scan_field_summary_count: Option<u64>,
    filter_pushdown_ready: Option<bool>,
    filter_pushdown_field_summary_count: Option<u64>,
    filter_pushdown_missing_required_fields: Vec<String>,
    blocker_codes: serde_json::Value,
}

struct BoundedReadEvidenceSummary<'a> {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    mode: Option<&'a str>,
    max_rows: Option<u64>,
    execution_row_cap: Option<u64>,
    row_limit_enforced_before_output: Option<bool>,
    operator_row_cap_enabled: Option<bool>,
    streaming: Option<bool>,
    blocking_operator_count: Option<u64>,
    covered_routes: Vec<String>,
    missing_covered_routes: Vec<&'static str>,
    route_primary_ready: Option<bool>,
    primary_ready_routes: Vec<String>,
    route_query_plan_evidence_ready: Option<bool>,
    route_query_profile_evidence_ready: Option<bool>,
    relationship_property_pruning_required_count: Option<u64>,
    relationship_property_pruning_report_count: Option<u64>,
    route_relationship_property_pruning_evidence_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRouteReadinessSummary {
    pub protocol: Option<String>,
    pub present: bool,
    pub ready: bool,
    pub evidence_protocol: Option<String>,
    pub evidence_ready: Option<bool>,
    pub required_route_count: Option<u64>,
    pub covered_route_count: Option<u64>,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<&'static str>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_coverage_ready: Option<bool>,
    pub evidence_route_coverage_present: Option<bool>,
    pub evidence_route_coverage_matches: Option<bool>,
    pub route_query_runtime_ready: Option<bool>,
    pub route_query_plan_evidence_ready: Option<bool>,
    pub route_query_profile_evidence_ready: Option<bool>,
    pub relationship_property_pruning_required_count: Option<u64>,
    pub relationship_property_pruning_report_count: Option<u64>,
    pub route_relationship_property_pruning_evidence_ready: Option<bool>,
    pub route_primary_ready: Option<bool>,
    pub primary_ready_route_count: Option<u64>,
    pub route_catalog_metadata_ready: bool,
    pub missing_route_catalog_metadata_routes: Vec<&'static str>,
    pub route_catalog_metadata_mismatch_routes: Vec<String>,
    pub blocker_codes: serde_json::Value,
}

impl GraphRouteReadinessSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "evidence_protocol": self.evidence_protocol,
            "evidence_ready": self.evidence_ready,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_coverage_ready": self.route_coverage_ready,
            "evidence_route_coverage_present": self.evidence_route_coverage_present,
            "evidence_route_coverage_matches": self.evidence_route_coverage_matches,
            "route_query_runtime_ready": self.route_query_runtime_ready,
            "route_query_plan_evidence_ready": self.route_query_plan_evidence_ready,
            "route_query_profile_evidence_ready": self.route_query_profile_evidence_ready,
            "relationship_property_pruning_required_count": self.relationship_property_pruning_required_count,
            "relationship_property_pruning_report_count": self.relationship_property_pruning_report_count,
            "route_relationship_property_pruning_evidence_ready": self.route_relationship_property_pruning_evidence_ready,
            "route_primary_ready": self.route_primary_ready,
            "primary_ready_route_count": self.primary_ready_route_count,
            "route_catalog": nowledge_mem_graph_read_route_specs_json(),
            "route_catalog_metadata_ready": self.route_catalog_metadata_ready,
            "missing_route_catalog_metadata_routes": self.missing_route_catalog_metadata_routes,
            "route_catalog_metadata_mismatch_routes": self.route_catalog_metadata_mismatch_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

struct QueryRuntimePreflightSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    database_opened: Option<bool>,
    probe_count: Option<u64>,
    passed_probe_count: Option<u64>,
    failed_probe_count: Option<u64>,
    required_route_count: Option<u64>,
    covered_route_count: Option<u64>,
    covered_routes: Vec<String>,
    missing_required_routes: Vec<&'static str>,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    required_routes_covered: Option<bool>,
    route_coverage_ready: bool,
    probe_details_ready: bool,
    blocker_codes: serde_json::Value,
}

fn shadow_evidence_summary(bundle: &serde_json::Value) -> ShadowEvidenceSummary<'_> {
    let run_evidence_kind = json_get_str_path(bundle, &["shadow_run", "evidence_kind"]);
    let ready_engine_kind = json_get_str_path(bundle, &["shadow_ready", "engine_kind"]);
    let ready_wrapper_identity = json_get_str_path(bundle, &["shadow_ready", "wrapper_identity"]);
    let contract_wrapper_identity = json_get_str_path(
        bundle,
        &["previous_wrapper_contract_evidence", "wrapper_identity"],
    );
    let cutover_ready_wrapper_identity =
        json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]);
    ShadowEvidenceSummary {
        ready: run_evidence_kind == Some("previous_wrapper")
            && ready_engine_kind == Some("previous_wrapper")
            && ready_wrapper_identity.is_some()
            && ready_wrapper_identity == contract_wrapper_identity
            && ready_wrapper_identity == cutover_ready_wrapper_identity,
        run_evidence_kind,
        ready_engine_kind,
        ready_wrapper_identity,
        contract_wrapper_identity,
        cutover_ready_wrapper_identity,
    }
}

struct FullContractEvidenceSummary {
    ready: bool,
    required_contract_ready: Option<bool>,
    full_contract_checked: Option<bool>,
    full_contract_ready: Option<bool>,
    selected_checks: Option<u64>,
    check_count: Option<u64>,
}

fn full_contract_evidence_summary(bundle: &serde_json::Value) -> FullContractEvidenceSummary {
    let path = if json_get_path(bundle, &["contract_evidence"]).is_some() {
        &["contract_evidence"][..]
    } else {
        &[][..]
    };
    let required_contract_ready =
        json_get_bool_path_from_dynamic(bundle, path, "required_contract_ready");
    let full_contract_checked =
        json_get_bool_path_from_dynamic(bundle, path, "full_contract_checked");
    let full_contract_ready = json_get_bool_path_from_dynamic(bundle, path, "full_contract_ready");
    let selected_checks = json_get_u64_path_from_dynamic(bundle, path, "selected_checks");
    let check_count = json_get_u64_path_from_dynamic(bundle, path, "check_count");
    FullContractEvidenceSummary {
        ready: required_contract_ready == Some(true)
            && full_contract_checked == Some(true)
            && full_contract_ready == Some(true)
            && check_count.is_some_and(|count| count > 0)
            && selected_checks == check_count,
        required_contract_ready,
        full_contract_checked,
        full_contract_ready,
        selected_checks,
        check_count,
    }
}

fn dual_engine_evidence_summary(bundle: &serde_json::Value) -> DualEngineEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["dual_engine_evidence"]).is_some() {
        &["dual_engine_evidence"][..]
    } else {
        &["cutover", "dual_engine_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let primary_check_count = json_get_u64_path_from_dynamic(bundle, path, "primary_check_count");
    let shadow_check_count = json_get_u64_path_from_dynamic(bundle, path, "shadow_check_count");
    let matched_check_count = json_get_u64_path_from_dynamic(bundle, path, "matched_check_count");
    let primary_only_check_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_only_check_count");
    let matched_per_million = json_get_u64_path_from_dynamic(bundle, path, "matched_per_million");
    DualEngineEvidenceSummary {
        present,
        ready: json_get_bool_path_from_dynamic(bundle, path, "ready"),
        consistent: primary_check_count.is_some_and(|count| count > 0)
            && primary_check_count == shadow_check_count
            && matched_check_count == shadow_check_count
            && primary_only_check_count == Some(0)
            && matched_per_million == Some(1_000_000),
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        primary_check_count,
        shadow_check_count,
        matched_check_count,
        primary_only_check_count,
        matched_per_million,
    }
}

fn search_projection_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionEvidenceSummary {
    let path = if json_get_path(bundle, &["search_projection_evidence"]).is_some() {
        &["search_projection_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some_and(|value| !value.is_null());
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let covered_table_count = json_get_u64_path_from_dynamic(bundle, path, "covered_table_count");
    let required_table_count = json_get_u64_path_from_dynamic(bundle, path, "required_table_count");
    let derived_projection = json_get_bool_path_from_dynamic(bundle, path, "derived_projection");
    let all_tables_covered = json_get_bool_path_from_dynamic(bundle, path, "all_tables_covered");
    let fts_ready = json_get_bool_path_from_dynamic(bundle, path, "fts_ready");
    let vector_ready = json_get_bool_path_from_dynamic(bundle, path, "vector_ready");
    let document_identity_ready =
        json_get_bool_path_from_dynamic(bundle, path, "document_identity_ready");
    let embedding_identity_ready =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_ready");
    let fail_soft_ready = json_get_bool_path_from_dynamic(bundle, path, "fail_soft_ready");
    let rebuild_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "rebuild_marker_ready");
    let metadata_repair_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "metadata_repair_marker_ready");
    let incremental_update_ready =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_update_ready");
    let source_chunk_ready = json_get_bool_path_from_dynamic(bundle, path, "source_chunk_ready");
    let predicate_pushdown_ready =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_ready");
    let compressed_vector_projection_required =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_required");
    let compressed_vector_projection_ready =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_ready");
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL)
        && derived_projection == Some(true)
        && all_tables_covered == Some(true)
        && covered_table_count.is_some_and(|count| count > 0)
        && covered_table_count == required_table_count
        && fts_ready == Some(true)
        && vector_ready == Some(true)
        && document_identity_ready == Some(true)
        && embedding_identity_ready == Some(true)
        && fail_soft_ready == Some(true)
        && rebuild_marker_ready == Some(true)
        && metadata_repair_marker_ready == Some(true)
        && incremental_update_ready == Some(true)
        && source_chunk_ready == Some(true)
        && predicate_pushdown_ready == Some(true)
        && compressed_vector_projection_ready.unwrap_or(true);
    SearchProjectionEvidenceSummary {
        protocol,
        present,
        ready,
        derived_projection,
        all_tables_covered,
        covered_table_count,
        required_table_count,
        fts_ready,
        vector_ready,
        document_identity_ready,
        embedding_identity_ready,
        fail_soft_ready,
        rebuild_marker_ready,
        metadata_repair_marker_ready,
        incremental_update_ready,
        source_chunk_ready,
        predicate_pushdown_ready,
        compressed_vector_projection_required,
        compressed_vector_projection_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn search_projection_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionShadowEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["search_projection_shadow_evidence"]).is_some() {
        &["search_projection_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source =
        json_get_str_path_from_dynamic(bundle, path, "evidence_source").map(str::to_string);
    let primary_ready = json_get_bool_path_from_dynamic(bundle, path, "primary_ready");
    let shadow_ready = json_get_bool_path_from_dynamic(bundle, path, "shadow_ready");
    let document_count_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_count_parity");
    let document_identity_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_identity_parity");
    let table_parity_ready = {
        let mut table_parity_path = path.to_vec();
        table_parity_path.extend(["table_parity", "ready"]);
        json_get_bool_path(bundle, &table_parity_path)
            .or_else(|| json_get_bool_path_from_dynamic(bundle, path, "table_parity_ready"))
    };
    let embedding_identity_parity =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_parity");
    let lifecycle_parity = json_get_bool_path_from_dynamic(bundle, path, "lifecycle_parity");
    let incremental_watermark_parity =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_watermark_parity");
    let predicate_pushdown_parity =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_parity");
    let pushdown_evidence = search_projection_shadow_pushdown_evidence_json(bundle, path);
    let pushdown_ready = json_get_bool_path(&pushdown_evidence, &["ready"]);
    let blocker_codes =
        search_projection_shadow_blocker_codes_with_pushdown(bundle, path, &pushdown_evidence);
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
        && evidence_source.as_deref() == Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
        && primary_ready == Some(true)
        && shadow_ready == Some(true)
        && document_count_parity == Some(true)
        && document_identity_parity == Some(true)
        && table_parity_ready == Some(true)
        && embedding_identity_parity == Some(true)
        && lifecycle_parity == Some(true)
        && incremental_watermark_parity == Some(true)
        && predicate_pushdown_parity == Some(true)
        && pushdown_ready == Some(true);
    SearchProjectionShadowEvidenceSummary {
        protocol,
        evidence_source,
        present,
        ready,
        primary_ready,
        shadow_ready,
        document_count_parity,
        document_identity_parity,
        table_parity_ready,
        embedding_identity_parity,
        lifecycle_parity,
        incremental_watermark_parity,
        predicate_pushdown_parity,
        pushdown_evidence,
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        blocker_codes,
    }
}

fn search_candidate_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchCandidateShadowEvidenceSummary {
    let path = if json_get_path(bundle, &["search_candidate_shadow_evidence"]).is_some() {
        &["search_candidate_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_candidate_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source =
        json_get_str_path_from_dynamic(bundle, path, "evidence_source").map(str::to_string);
    let route = json_get_str_path_from_dynamic(bundle, path, "route").map(str::to_string);
    let candidate_primary_engine =
        json_get_str_path_from_dynamic(bundle, path, "candidate_primary_engine")
            .map(str::to_string);
    let primary_engine =
        json_get_str_path_from_dynamic(bundle, path, "primary_engine").map(str::to_string);
    let shadow_engine =
        json_get_str_path_from_dynamic(bundle, path, "shadow_engine").map(str::to_string);
    let request_count = json_get_u64_path_from_dynamic(bundle, path, "request_count");
    let primary_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_candidate_count");
    let shadow_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "shadow_candidate_count");
    let matched_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "matched_candidate_count");
    let primary_only_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_only_candidate_count");
    let candidate_counts_ready = request_count.is_some_and(|count| count > 0)
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0);
    let candidate_identity_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_identity", "ready"])
            .collect::<Vec<_>>(),
    );
    let candidate_identity_parity = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_identity", "parity"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "ready"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_field_summary_count = json_get_u64_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "field_summary_count"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_required_fields = json_get_string_array_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_required_fields"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_required_fields_present = json_get_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_required_fields"])
            .collect::<Vec<_>>(),
    )
    .is_some_and(serde_json::Value::is_array);

    let row_count_parity = json_get_bool_path_from_dynamic(bundle, path, "row_count_parity");
    let vector_top_k_overlap_ready =
        json_get_bool_path_from_dynamic(bundle, path, "vector_top_k_overlap_ready");
    let fts_top_k_overlap_ready =
        json_get_bool_path_from_dynamic(bundle, path, "fts_top_k_overlap_ready");
    let shadow_scan_filter_pushdown_ready =
        json_get_bool_path_from_dynamic(bundle, path, "shadow_scan_filter_pushdown_ready");
    let shadow_scan_field_pruning_ready =
        json_get_bool_path_from_dynamic(bundle, path, "shadow_scan_field_pruning_ready");
    let shadow_scan_field_summary_count =
        json_get_u64_path_from_dynamic(bundle, path, "shadow_scan_field_summary_count");
    let blocker_codes = json_get_array_path_from_dynamic(bundle, path, "blocker_codes");

    let bridge_ready = evidence_source.as_deref()
        == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
        && candidate_primary_engine.as_deref()
            == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE)
        && candidate_counts_ready
        && candidate_identity_ready == Some(true)
        && candidate_identity_parity == Some(true)
        && filter_pushdown_ready == Some(true)
        && filter_pushdown_field_summary_count.is_some_and(|count| count > 0)
        && filter_pushdown_missing_required_fields_present
        && filter_pushdown_missing_required_fields.is_empty();
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
        && route.as_deref() == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE)
        && bridge_ready;
    SearchCandidateShadowEvidenceSummary {
        protocol,
        evidence_source,
        route,
        present,
        ready,
        candidate_primary_engine,
        primary_engine,
        shadow_engine,
        request_count,
        primary_candidate_count,
        shadow_candidate_count,
        matched_candidate_count,
        primary_only_candidate_count,
        candidate_counts_ready,
        candidate_identity_ready,
        candidate_identity_parity,
        row_count_parity,
        vector_top_k_overlap_ready,
        fts_top_k_overlap_ready,
        shadow_scan_filter_pushdown_ready,
        shadow_scan_field_pruning_ready,
        shadow_scan_field_summary_count,
        filter_pushdown_ready,
        filter_pushdown_field_summary_count,
        filter_pushdown_missing_required_fields,
        blocker_codes,
    }
}

fn search_projection_shadow_pushdown_evidence_json(
    bundle: &serde_json::Value,
    path: &[&str],
) -> serde_json::Value {
    let mut pushdown_path = path.to_vec();
    pushdown_path.push("pushdown_evidence");
    if let Some(pushdown) = json_get_path(bundle, &pushdown_path).filter(|value| value.is_object())
    {
        return search_projection_shadow_pushdown_evidence_with_recomputed_scan_fields(
            pushdown.clone(),
        );
    }

    let predicate_pushdown_parity =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_parity").unwrap_or(false);
    let primary_predicate_pushdown_ready = {
        let mut nested = path.to_vec();
        nested.extend(["primary_evidence", "predicate_pushdown_ready"]);
        json_get_bool_path(bundle, &nested)
    }
    .unwrap_or(false);
    let shadow_predicate_pushdown_ready = {
        let mut nested = path.to_vec();
        nested.extend(["shadow_evidence", "predicate_pushdown_ready"]);
        json_get_bool_path(bundle, &nested)
    }
    .unwrap_or(false);
    let shadow_persisted_segment_descriptor_ready = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "persisted_segment_descriptor_ready",
        ]);
        json_get_bool_path(bundle, &nested).unwrap_or(false)
    };
    let reported_shadow_segment_descriptor_scan_filter_fields_ready = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "segment_descriptor_scan_filter_fields_ready",
        ]);
        json_get_bool_path(bundle, &nested).unwrap_or(false)
    };
    let primary_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "primary_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_string_array_path(bundle, &nested)
    };
    let shadow_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_string_array_path(bundle, &nested)
    };
    let shadow_segment_descriptor_field_summaries = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "segment_descriptor_field_summaries",
        ]);
        json_get_path(bundle, &nested)
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]))
    };
    let shadow_segment_descriptor_scan_filter_fields_ready =
        reported_shadow_segment_descriptor_scan_filter_fields_ready
            && scan_filter_fields_cover_required(&primary_scan_filter_fields)
            && scan_filter_fields_cover_required(&shadow_scan_filter_fields)
            && segment_descriptor_summaries_cover_required(
                &shadow_segment_descriptor_field_summaries,
            );
    let ready = predicate_pushdown_parity
        && primary_predicate_pushdown_ready
        && shadow_predicate_pushdown_ready
        && shadow_persisted_segment_descriptor_ready
        && shadow_segment_descriptor_scan_filter_fields_ready;
    serde_json::json!({
        "ready": ready,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "primary_predicate_pushdown_ready": primary_predicate_pushdown_ready,
        "shadow_predicate_pushdown_ready": shadow_predicate_pushdown_ready,
        "shadow_persisted_segment_descriptor_ready": shadow_persisted_segment_descriptor_ready,
        "shadow_segment_descriptor_scan_filter_fields_ready": shadow_segment_descriptor_scan_filter_fields_ready,
        "primary_scan_filter_fields": primary_scan_filter_fields,
        "shadow_scan_filter_fields": shadow_scan_filter_fields,
        "shadow_segment_descriptor_field_summaries": shadow_segment_descriptor_field_summaries,
    })
}

fn search_projection_shadow_pushdown_evidence_with_recomputed_scan_fields(
    pushdown: serde_json::Value,
) -> serde_json::Value {
    let primary_scan_filter_fields =
        json_get_string_array_path(&pushdown, &["primary_scan_filter_fields"]);
    let shadow_scan_filter_fields =
        json_get_string_array_path(&pushdown, &["shadow_scan_filter_fields"]);
    let shadow_segment_descriptor_field_summaries =
        json_get_path(&pushdown, &["shadow_segment_descriptor_field_summaries"])
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
    let scan_filter_fields_ready = scan_filter_fields_cover_required(&primary_scan_filter_fields)
        && scan_filter_fields_cover_required(&shadow_scan_filter_fields)
        && segment_descriptor_summaries_cover_required(&shadow_segment_descriptor_field_summaries);
    let reported_descriptor_fields_ready = json_get_bool_path(
        &pushdown,
        &["shadow_segment_descriptor_scan_filter_fields_ready"],
    ) == Some(true);
    let descriptor_fields_ready = reported_descriptor_fields_ready && scan_filter_fields_ready;
    let ready = json_get_bool_path(&pushdown, &["ready"]) == Some(true) && descriptor_fields_ready;

    let mut object = pushdown.as_object().cloned().unwrap_or_default();
    object.insert("ready".to_string(), serde_json::json!(ready));
    object.insert(
        "required_scan_filter_fields".to_string(),
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS),
    );
    object.insert(
        "shadow_segment_descriptor_scan_filter_fields_ready".to_string(),
        serde_json::json!(descriptor_fields_ready),
    );
    object.insert(
        "scan_filter_fields_ready".to_string(),
        serde_json::json!(scan_filter_fields_ready),
    );
    serde_json::Value::Object(object)
}

fn scan_filter_fields_cover_required(scan_filter_fields: &[String]) -> bool {
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| scan_filter_fields.iter().any(|field| field == required))
}

fn segment_descriptor_summaries_cover_required(
    segment_descriptor_field_summaries: &serde_json::Value,
) -> bool {
    let Some(summaries) = segment_descriptor_field_summaries.as_array() else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let fields = summaries
        .iter()
        .filter_map(|summary| json_get_str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
}

fn search_projection_shadow_blocker_codes_with_pushdown(
    bundle: &serde_json::Value,
    path: &[&str],
    pushdown_evidence: &serde_json::Value,
) -> serde_json::Value {
    let mut blockers = json_get_string_array_path_from_dynamic(bundle, path, "blocker_codes")
        .into_iter()
        .collect::<BTreeSet<_>>();
    if json_get_bool_path(pushdown_evidence, &["ready"]) != Some(true) {
        blockers.insert(SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY.to_string());
    }
    if json_get_bool_path(
        pushdown_evidence,
        &["shadow_persisted_segment_descriptor_ready"],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING.to_string());
    }
    if json_get_bool_path(
        pushdown_evidence,
        &["shadow_segment_descriptor_scan_filter_fields_ready"],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    serde_json::json!(blockers.into_iter().collect::<Vec<_>>())
}

fn bounded_read_evidence_summary(bundle: &serde_json::Value) -> BoundedReadEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["bounded_read_evidence"]).is_some() {
        &["bounded_read_evidence"][..]
    } else {
        &["cutover_evidence", "bounded_read_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let mode = json_get_str_path_from_dynamic(bundle, path, "mode");
    let max_rows = json_get_u64_path_from_dynamic(bundle, path, "max_rows");
    let execution_row_cap = json_get_u64_path_from_dynamic(bundle, path, "execution_row_cap");
    let row_limit_enforced_before_output =
        json_get_bool_path_from_dynamic(bundle, path, "row_limit_enforced_before_output");
    let operator_row_cap_enabled =
        json_get_bool_path_from_dynamic(bundle, path, "operator_row_cap_enabled");
    let streaming = json_get_bool_path_from_dynamic(bundle, path, "streaming");
    let blocking_operator_count =
        json_get_u64_path_from_dynamic(bundle, path, "blocking_operator_count");
    let covered_routes = json_get_string_array_path_from_dynamic(bundle, path, "covered_routes");
    let primary_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, path, "primary_ready_routes");
    let primary_ready_route_set = primary_ready_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let covered_route_set = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_route_set.contains(route))
        .collect::<Vec<_>>();
    let route_primary_ready = json_get_bool_path_from_dynamic(bundle, path, "route_primary_ready");
    let route_query_plan_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_plan_evidence_ready");
    let route_query_profile_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_profile_evidence_ready");
    let relationship_property_pruning_required_count = json_get_u64_path_from_dynamic(
        bundle,
        path,
        "relationship_property_pruning_required_count",
    );
    let relationship_property_pruning_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "relationship_property_pruning_report_count");
    let route_relationship_property_pruning_evidence_ready = json_get_bool_path_from_dynamic(
        bundle,
        path,
        "route_relationship_property_pruning_evidence_ready",
    );
    let primary_ready_routes_cover_required = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| primary_ready_route_set.contains(route));
    let relationship_property_pruning_counts_match =
        relationship_property_pruning_required_count == relationship_property_pruning_report_count;
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL)
        && mode == Some("shadow_read_only")
        && max_rows.is_some_and(|value| value > 0)
        && execution_row_cap == max_rows.and_then(|value| value.checked_add(1))
        && row_limit_enforced_before_output == Some(true)
        && operator_row_cap_enabled == Some(true)
        && missing_covered_routes.is_empty()
        && route_primary_ready == Some(true)
        && primary_ready_routes_cover_required
        && route_query_plan_evidence_ready == Some(true)
        && route_query_profile_evidence_ready == Some(true)
        && route_relationship_property_pruning_evidence_ready == Some(true)
        && relationship_property_pruning_required_count.is_some()
        && relationship_property_pruning_counts_match;
    BoundedReadEvidenceSummary {
        protocol,
        present,
        ready,
        mode,
        max_rows,
        execution_row_cap,
        row_limit_enforced_before_output,
        operator_row_cap_enabled,
        streaming,
        blocking_operator_count,
        covered_routes,
        missing_covered_routes,
        route_primary_ready,
        primary_ready_routes,
        route_query_plan_evidence_ready,
        route_query_profile_evidence_ready,
        relationship_property_pruning_required_count,
        relationship_property_pruning_report_count,
        route_relationship_property_pruning_evidence_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

pub fn nowledge_graph_route_readiness_summary_from_bundle(
    bundle: &serde_json::Value,
) -> GraphRouteReadinessSummary {
    let path = if json_get_path(bundle, &["graph_route_readiness"]).is_some() {
        &["graph_route_readiness"][..]
    } else {
        &["cutover_evidence", "graph_route_readiness"][..]
    };
    graph_route_readiness_summary_at(bundle, path)
}

pub fn nowledge_graph_route_readiness_summary(
    value: &serde_json::Value,
) -> GraphRouteReadinessSummary {
    graph_route_readiness_summary_at(value, &[])
}

fn graph_route_readiness_summary_at(
    bundle: &serde_json::Value,
    path: &[&str],
) -> GraphRouteReadinessSummary {
    let present = json_get_path(bundle, path).is_some_and(|value| !value.is_null());
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_protocol =
        json_get_str_path_from_dynamic(bundle, path, "evidence_protocol").map(str::to_string);
    let evidence_ready = json_get_bool_path_from_dynamic(bundle, path, "evidence_ready");
    let required_route_count = json_get_u64_path_from_dynamic(bundle, path, "required_route_count");
    let covered_route_count = json_get_u64_path_from_dynamic(bundle, path, "covered_route_count");
    let covered_routes = json_get_string_array_path_from_dynamic(bundle, path, "covered_routes");
    let covered_route_set = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_route_set.contains(route))
        .collect::<Vec<_>>();
    let unknown_routes = covered_routes
        .iter()
        .map(String::as_str)
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = duplicate_strings(&covered_routes);
    let route_coverage_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_coverage_ready");
    let evidence_route_coverage_present =
        json_get_bool_path_from_dynamic(bundle, path, "evidence_route_coverage_present");
    let evidence_route_coverage_matches =
        json_get_bool_path_from_dynamic(bundle, path, "evidence_route_coverage_matches");
    let route_query_runtime_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_runtime_ready");
    let route_query_plan_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_plan_evidence_ready");
    let route_query_profile_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_profile_evidence_ready");
    let relationship_property_pruning_required_count = json_get_u64_path_from_dynamic(
        bundle,
        path,
        "relationship_property_pruning_required_count",
    );
    let relationship_property_pruning_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "relationship_property_pruning_report_count");
    let route_relationship_property_pruning_evidence_ready = json_get_bool_path_from_dynamic(
        bundle,
        path,
        "route_relationship_property_pruning_evidence_ready",
    );
    let route_primary_ready = json_get_bool_path_from_dynamic(bundle, path, "route_primary_ready");
    let primary_ready_route_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_ready_route_count");
    let required_route_len = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64;
    let relationship_property_pruning_counts_match =
        relationship_property_pruning_required_count == relationship_property_pruning_report_count;
    let route_catalog_metadata = graph_route_catalog_metadata_summary(bundle, path);
    let ready = present
        && protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL)
        && evidence_protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL)
        && evidence_ready == Some(true)
        && required_route_count == Some(required_route_len)
        && covered_route_count == Some(required_route_len)
        && missing_required_routes.is_empty()
        && unknown_routes.is_empty()
        && duplicate_routes.is_empty()
        && route_coverage_ready == Some(true)
        && evidence_route_coverage_present == Some(true)
        && evidence_route_coverage_matches == Some(true)
        && route_query_runtime_ready == Some(true)
        && route_query_plan_evidence_ready == Some(true)
        && route_query_profile_evidence_ready == Some(true)
        && route_relationship_property_pruning_evidence_ready == Some(true)
        && relationship_property_pruning_required_count.is_some()
        && relationship_property_pruning_counts_match
        && route_primary_ready == Some(true)
        && primary_ready_route_count == Some(required_route_len)
        && route_catalog_metadata.ready;
    GraphRouteReadinessSummary {
        protocol,
        present,
        ready,
        evidence_protocol,
        evidence_ready,
        required_route_count,
        covered_route_count,
        covered_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        route_coverage_ready,
        evidence_route_coverage_present,
        evidence_route_coverage_matches,
        route_query_runtime_ready,
        route_query_plan_evidence_ready,
        route_query_profile_evidence_ready,
        relationship_property_pruning_required_count,
        relationship_property_pruning_report_count,
        route_relationship_property_pruning_evidence_ready,
        route_primary_ready,
        primary_ready_route_count,
        route_catalog_metadata_ready: route_catalog_metadata.ready,
        missing_route_catalog_metadata_routes: route_catalog_metadata.missing_routes,
        route_catalog_metadata_mismatch_routes: route_catalog_metadata.mismatch_routes,
        blocker_codes: json_get_array_path_from_dynamic(
            bundle,
            path,
            "route_primary_blocker_codes",
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphRouteCatalogMetadataSummary {
    ready: bool,
    missing_routes: Vec<&'static str>,
    mismatch_routes: Vec<String>,
}

fn graph_route_catalog_metadata_summary(
    bundle: &serde_json::Value,
    path: &[&str],
) -> GraphRouteCatalogMetadataSummary {
    let catalog_matches = json_get_path_from_dynamic(bundle, path, "route_catalog")
        == Some(&nowledge_mem_graph_read_route_specs_json());
    let routes = json_get_path_from_dynamic(bundle, path, "routes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|route| json_get_str_path(route, &["route"]).map(|name| (name, route)))
        .collect::<BTreeMap<_, _>>();
    let mut missing_routes = Vec::new();
    let mut mismatch_routes = Vec::new();

    for route in REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES {
        let Some(entry) = routes.get(route) else {
            missing_routes.push(*route);
            continue;
        };
        match graph_route_catalog_metadata_entry_state(route, entry) {
            GraphRouteCatalogMetadataEntryState::Ready => {}
            GraphRouteCatalogMetadataEntryState::Missing => missing_routes.push(*route),
            GraphRouteCatalogMetadataEntryState::Mismatch => {
                mismatch_routes.push((*route).to_string())
            }
        }
    }
    if !catalog_matches {
        mismatch_routes.push("__route_catalog__".to_string());
    }

    GraphRouteCatalogMetadataSummary {
        ready: catalog_matches && missing_routes.is_empty() && mismatch_routes.is_empty(),
        missing_routes,
        mismatch_routes,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphRouteCatalogMetadataEntryState {
    Ready,
    Missing,
    Mismatch,
}

fn graph_route_catalog_metadata_entry_state(
    route: &str,
    entry: &serde_json::Value,
) -> GraphRouteCatalogMetadataEntryState {
    let Some(spec) = nowledge_mem_graph_read_route_spec(route) else {
        return GraphRouteCatalogMetadataEntryState::Mismatch;
    };
    let owner = json_get_str_path(entry, &["owner"]);
    let required_evidence_kind = json_get_str_path(entry, &["required_evidence_kind"]);
    let stale_on_catalog_change = json_get_bool_path(entry, &["stale_on_catalog_change"]);
    if owner.is_none() || required_evidence_kind.is_none() || stale_on_catalog_change.is_none() {
        return GraphRouteCatalogMetadataEntryState::Missing;
    }
    if owner == Some(spec.owner.as_str())
        && required_evidence_kind == Some(spec.required_evidence_kind.as_str())
        && stale_on_catalog_change == Some(spec.stale_on_catalog_change)
    {
        GraphRouteCatalogMetadataEntryState::Ready
    } else {
        GraphRouteCatalogMetadataEntryState::Mismatch
    }
}

fn query_runtime_preflight_summary(bundle: &serde_json::Value) -> QueryRuntimePreflightSummary {
    let path = if json_get_path(bundle, &["query_runtime_preflight"]).is_some() {
        &["query_runtime_preflight"][..]
    } else {
        &["cutover_evidence", "query_runtime_preflight"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let database_opened = json_get_bool_path_from_dynamic(bundle, path, "database_opened");
    let probe_count = json_get_u64_path_from_dynamic(bundle, path, "probe_count");
    let passed_probe_count = json_get_u64_path_from_dynamic(bundle, path, "passed_probe_count");
    let failed_probe_count = json_get_u64_path_from_dynamic(bundle, path, "failed_probe_count");
    let required_route_count = json_get_u64_path_from_dynamic(bundle, path, "required_route_count");
    let covered_route_count = json_get_u64_path_from_dynamic(bundle, path, "covered_route_count");
    let required_routes_covered =
        json_get_bool_path_from_dynamic(bundle, path, "required_routes_covered");
    let probe_routes = query_runtime_preflight_probe_routes(bundle, path);
    let probe_route_set = query_runtime_preflight_probe_route_set(bundle, path);
    let covered_routes = probe_route_set
        .iter()
        .map(String::to_string)
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !probe_route_set.contains(*route))
        .collect::<Vec<_>>();
    let unknown_routes = probe_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route.as_str()))
        .map(String::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = duplicate_probe_routes(&probe_routes);
    let required_route_len = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64;
    let route_coverage_ready = required_route_count == Some(required_route_len)
        && covered_route_count == Some(required_route_len)
        && required_routes_covered == Some(true)
        && missing_required_routes.is_empty()
        && unknown_routes.is_empty()
        && duplicate_routes.is_empty()
        && json_get_bool_path_from_dynamic(bundle, path, "route_coverage_ready")
            .is_none_or(|ready| ready);
    let probe_details_ready = query_runtime_preflight_probe_details_ready(bundle, path);
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL)
        && database_opened == Some(true)
        && probe_count.is_some_and(|count| count > 0)
        && passed_probe_count == probe_count
        && failed_probe_count == Some(0)
        && route_coverage_ready
        && probe_details_ready
        && json_get_bool_path_from_dynamic(bundle, path, "ready") == Some(true);
    QueryRuntimePreflightSummary {
        protocol,
        present,
        ready,
        database_opened,
        probe_count,
        passed_probe_count,
        failed_probe_count,
        required_route_count,
        covered_route_count,
        covered_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        required_routes_covered,
        route_coverage_ready,
        probe_details_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn duplicate_strings(values: &[String]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for value in values {
        *counts.entry(value.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(value, _)| value.to_string())
        .collect()
}

fn query_runtime_preflight_probe_route_set(
    bundle: &serde_json::Value,
    path: &[&str],
) -> BTreeSet<String> {
    query_runtime_preflight_probe_routes(bundle, path)
        .into_iter()
        .collect()
}

fn query_runtime_preflight_probe_routes(bundle: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path_from_dynamic(bundle, path, "probes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|probe| {
            probe
                .get("route")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn duplicate_probe_routes(routes: &[String]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(route.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

fn query_runtime_preflight_probe_details_ready(bundle: &serde_json::Value, path: &[&str]) -> bool {
    let Some(probes) =
        json_get_path_from_dynamic(bundle, path, "probes").and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    !probes.is_empty() && probes.iter().all(query_runtime_preflight_probe_ready)
}

fn query_runtime_preflight_probe_ready(probe: &serde_json::Value) -> bool {
    probe.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && probe.get("success").and_then(serde_json::Value::as_bool) == Some(true)
        && probe
            .get("selected_plan_fingerprint")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|fingerprint| !fingerprint.is_empty())
        && probe
            .get("physical_operator_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("physical_operator_class_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("optimizer_decision_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("plan_cache_bypassed")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        && probe
            .get("scan_pruning")
            .is_some_and(serde_json::Value::is_object)
}

fn json_get_string_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> Vec<String> {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_string_array_path(value, &path)
}

fn json_get_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> serde_json::Value {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_array_path(value, &path)
}

fn nowledge_replacement_blocking_categories(
    bundle: &serde_json::Value,
    inputs: ReplacementReadinessInputs<'_>,
) -> Vec<String> {
    let mut categories = BTreeSet::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        categories.insert("scanner_coverage".to_string());
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        categories.insert("shadow_parity".to_string());
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000) {
        categories.insert("query_family_readiness".to_string());
    }
    if !inputs.family_evidence_ready {
        categories.insert("query_family_readiness".to_string());
    }
    if inputs.migration_gate_decision != Some("ready") {
        categories.insert("migration_gate".to_string());
    }
    if !inputs.cutover_evidence_eligible {
        categories.insert("cutover_evidence".to_string());
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        categories.insert("previous_wrapper_contract".to_string());
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        categories.insert("dual_engine_evidence".to_string());
    }
    if !inputs.search_projection_evidence_ready {
        categories.insert("search_projection_evidence".to_string());
    }
    if !inputs.search_projection_shadow_evidence_ready {
        categories.insert("search_projection_shadow_evidence".to_string());
    }
    if !inputs.search_candidate_shadow_evidence_ready {
        categories.insert("search_candidate_shadow_evidence".to_string());
    }
    if !inputs.bounded_read_evidence_ready {
        categories.insert("bounded_read_evidence".to_string());
    }
    if !inputs.graph_route_readiness_ready {
        categories.insert("graph_route_readiness".to_string());
    }
    if !inputs.query_runtime_preflight_ready {
        categories.insert("query_runtime_preflight".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        categories.insert("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        categories.insert("background_maintenance".to_string());
    }
    if json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "replacement_readiness_invalid_family_count",
        ],
    )
    .unwrap_or(0)
        > 0
    {
        categories.insert("query_family_readiness".to_string());
    }
    categories.into_iter().collect()
}

fn nowledge_replacement_next_actions(
    bundle: &serde_json::Value,
    inputs: NextActionInputs<'_>,
) -> Vec<serde_json::Value> {
    if inputs.production_cutover_ready {
        return Vec::new();
    }

    let mut actions = Vec::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        actions.push(next_action(
            "refresh_nowledge_cypher_inventory",
            "scanner coverage is below full Nowledge business-surface coverage",
            [
                "inventory_gate.coverage_per_million",
                "coverage.coverage_per_million",
            ],
        ));
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000) || !inputs.family_evidence_ready
    {
        actions.push(next_action(
            "close_blocked_query_families",
            "one or more required query families are missing or below full replacement readiness",
            [
                "replacement_readiness_per_million",
                "replacement_readiness_by_query_family",
            ],
        ));
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || inputs.migration_gate_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        actions.push(next_action(
            "run_previous_wrapper_shadow_gate",
            "shadow parity or migration gate decision is not ready",
            [
                "cutover.matched_per_million",
                "cutover.decision",
                "migration_gate.decision",
                "shadow_run.evidence_kind",
                "shadow_ready.engine_kind",
                "shadow_ready.wrapper_identity",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
        ));
    }
    if !inputs.cutover_evidence_eligible {
        actions.push(next_action(
            "provide_eligible_cutover_evidence",
            "cutover evidence is missing or not eligible for production replacement",
            [
                "cutover_evidence.eligible",
                "cutover_evidence.evidence_kind",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
            ],
        ));
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        actions.push(next_action(
            "run_full_previous_wrapper_contract_check",
            "previous-wrapper contract evidence is missing or not ready",
            [
                "required_contract_ready",
                "full_contract_checked",
                "full_contract_ready",
                "selected_checks",
                "check_count",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
                "previous_wrapper_contract_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        actions.push(next_action(
            "rerun_dual_engine_shadow_gate",
            "side-by-side dual-engine evidence is missing or not ready",
            [
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "dual_engine_evidence.matched_per_million",
            ],
        ));
    }
    if !inputs.search_projection_evidence_ready {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence is missing or not ready",
            [
                "search_projection_evidence.protocol",
                "search_projection_evidence.present",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.required_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.document_identity_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
                "search_projection_evidence.compressed_vector_projection_required",
                "search_projection_evidence.compressed_vector_projection_ready",
                "search_projection_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_projection_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_projection_shadow_evidence",
            "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
            [
                "search_projection_shadow_evidence.protocol",
                "search_projection_shadow_evidence.evidence_source",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.document_identity_parity",
                "search_projection_shadow_evidence.table_parity.ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_candidate_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_candidate_shadow_evidence",
            "LanceDB/Skein search candidate side-by-side evidence is missing or not ready",
            [
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.request_count",
                "search_candidate_shadow_evidence.primary_candidate_count",
                "search_candidate_shadow_evidence.shadow_candidate_count",
                "search_candidate_shadow_evidence.matched_candidate_count",
                "search_candidate_shadow_evidence.primary_only_candidate_count",
                "search_candidate_shadow_evidence.candidate_identity.ready",
                "search_candidate_shadow_evidence.filter_pushdown.ready",
                "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields",
                "search_candidate_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.bounded_read_evidence_ready {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "bounded read execution profile is missing or not ready",
            [
                "bounded_read_evidence.protocol",
                "bounded_read_evidence.present",
                "bounded_read_evidence.ready",
                "bounded_read_evidence.mode",
                "bounded_read_evidence.max_rows",
                "bounded_read_evidence.execution_row_cap",
                "bounded_read_evidence.row_limit_enforced_before_output",
                "bounded_read_evidence.operator_row_cap_enabled",
                "bounded_read_evidence.blocking_operator_count",
                "bounded_read_evidence.covered_routes",
                "bounded_read_evidence.route_primary_ready",
                "bounded_read_evidence.primary_ready_routes",
                "bounded_read_evidence.route_query_plan_evidence_ready",
                "bounded_read_evidence.route_query_profile_evidence_ready",
                "bounded_read_evidence.relationship_property_pruning_required_count",
                "bounded_read_evidence.relationship_property_pruning_report_count",
                "bounded_read_evidence.route_relationship_property_pruning_evidence_ready",
                "bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.graph_route_readiness_ready {
        actions.push(next_action(
            "attach_graph_route_readiness_evidence",
            "graph route readiness evidence is missing, stale, or does not cover all required graph read routes",
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.present",
                "graph_route_readiness.ready",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.covered_route_count",
                "graph_route_readiness.covered_routes",
                "graph_route_readiness.route_coverage_ready",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_query_plan_evidence_ready",
                "graph_route_readiness.route_query_profile_evidence_ready",
                "graph_route_readiness.relationship_property_pruning_required_count",
                "graph_route_readiness.relationship_property_pruning_report_count",
                "graph_route_readiness.route_relationship_property_pruning_evidence_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
            ],
        ));
    }
    if !inputs.query_runtime_preflight_ready {
        actions.push(next_action(
            "attach_query_runtime_preflight_evidence",
            "query runtime preflight evidence is missing or does not cover all required graph read routes",
            [
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.present",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.required_routes_covered",
                "query_runtime_preflight.route_coverage_ready",
                "query_runtime_preflight.probe_details_ready",
                "query_runtime_preflight.blocker_codes",
                "query_runtime_preflight.probes",
            ],
        ));
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "required storage recovery evidence is missing or blocked",
            [
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
                "cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "required background maintenance QoS or graph-delta evidence is missing or blocked",
            [
                "cutover_evidence.background_maintenance_present",
                "cutover_evidence.background_maintenance_ready",
                "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_blocker_codes",
            ],
        ));
    }

    actions
}

fn next_action(
    action: &str,
    reason: &str,
    evidence_fields: impl IntoIterator<Item = &'static str>,
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "reason": reason,
        "evidence_fields": evidence_fields.into_iter().collect::<Vec<_>>(),
    })
}

fn nowledge_replacement_missing_evidence(bundle: &serde_json::Value) -> Vec<String> {
    let mut missing = Vec::new();
    if bundle.get("cutover_evidence").is_none() {
        missing.push("cutover_evidence".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_present"])
            != Some(true)
    {
        missing.push("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        missing.push("background_maintenance".to_string());
    }
    if background_maintenance_graph_delta_evidence_missing(bundle) {
        missing.push("background_maintenance_search_projection_graph_delta".to_string());
    }
    if bundle
        .get("replacement_readiness_by_query_family")
        .is_none()
    {
        missing.push("replacement_readiness_by_query_family".to_string());
    } else if !replacement_readiness_family_evidence_health_from_bundle(bundle).ready {
        missing.push("replacement_readiness_by_query_family_ready".to_string());
    }
    if bundle.get("previous_wrapper_contract_evidence").is_none() {
        missing.push("previous_wrapper_contract_evidence".to_string());
    }
    if !full_contract_evidence_summary(bundle).ready {
        missing.push("full_contract_evidence".to_string());
    }
    if bundle.get("dual_engine_evidence").is_none()
        && json_get_path(bundle, &["cutover", "dual_engine_evidence"]).is_none()
    {
        missing.push("dual_engine_evidence".to_string());
    }
    if bundle.get("search_projection_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "search_projection_evidence"]).is_none()
    {
        missing.push("search_projection_evidence".to_string());
    } else if !search_projection_evidence_summary(bundle).ready {
        missing.push("search_projection_evidence_ready".to_string());
    }
    if bundle.get("search_projection_shadow_evidence").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "search_projection_shadow_evidence"],
        )
        .is_none()
    {
        missing.push("search_projection_shadow_evidence".to_string());
    } else if !search_projection_shadow_evidence_summary(bundle).ready {
        missing.push("search_projection_shadow_evidence_ready".to_string());
    }
    if bundle.get("search_candidate_shadow_evidence").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "search_candidate_shadow_evidence"],
        )
        .is_none()
    {
        missing.push("search_candidate_shadow_evidence".to_string());
    } else if !search_candidate_shadow_evidence_summary(bundle).ready {
        missing.push("search_candidate_shadow_evidence_ready".to_string());
    }
    if bundle.get("bounded_read_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "bounded_read_evidence"]).is_none()
    {
        missing.push("bounded_read_evidence".to_string());
    } else if !bounded_read_evidence_summary(bundle).ready {
        missing.push("bounded_read_evidence_ready".to_string());
    }
    if bundle.get("graph_route_readiness").is_none()
        && json_get_path(bundle, &["cutover_evidence", "graph_route_readiness"]).is_none()
    {
        missing.push("graph_route_readiness".to_string());
    } else if !nowledge_graph_route_readiness_summary_from_bundle(bundle).ready {
        missing.push("graph_route_readiness_ready".to_string());
    }
    if bundle.get("query_runtime_preflight").is_none()
        && json_get_path(bundle, &["cutover_evidence", "query_runtime_preflight"]).is_none()
    {
        missing.push("query_runtime_preflight".to_string());
    } else if !query_runtime_preflight_summary(bundle).ready {
        missing.push("query_runtime_preflight_ready".to_string());
    }
    if bundle.get("shadow_run").is_none() {
        missing.push("shadow_run".to_string());
    }
    if bundle.get("shadow_ready").is_none() {
        missing.push("shadow_ready".to_string());
    }
    missing
}

fn background_maintenance_graph_delta_evidence_missing(bundle: &serde_json::Value) -> bool {
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) != Some(true)
        || json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        return false;
    }

    [
        "background_maintenance_executable_search_projection_graph_delta_count",
        "background_maintenance_admitted_search_projection_graph_delta_count",
        "background_maintenance_deferred_search_projection_graph_delta_count",
        "background_maintenance_rejected_search_projection_graph_delta_count",
        "background_maintenance_executable_search_projection_graph_delta_operations",
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
    ]
    .into_iter()
    .any(|field| json_get_u64_path_from_dynamic(bundle, &["cutover_evidence"], field).is_none())
}

fn nowledge_replacement_blockers(bundle: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    for path in [
        &["inventory_gate", "blockers"][..],
        &["cutover", "blockers"][..],
        &["migration_gate", "blockers"][..],
        &["cutover_evidence", "blockers"][..],
        &["cutover_evidence", "storage_recovery_blockers"][..],
        &["cutover_evidence", "background_maintenance_blockers"][..],
        &["cutover_evidence", "replacement_readiness_blockers"][..],
        &["previous_wrapper_contract_evidence", "blockers"][..],
        &["contract_evidence", "full_contract_blockers"][..],
        &["contract_evidence", "required_contract_blockers"][..],
        &["full_contract_blockers"][..],
        &["required_contract_blockers"][..],
        &["search_projection_shadow_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_projection_shadow_evidence",
            "blocker_codes",
        ][..],
        &["search_candidate_shadow_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_candidate_shadow_evidence",
            "blocker_codes",
        ][..],
        &["bounded_read_evidence", "blocker_codes"][..],
        &["cutover_evidence", "bounded_read_evidence", "blocker_codes"][..],
        &["graph_route_readiness", "route_coverage_blocker_codes"][..],
        &[
            "graph_route_readiness",
            "evidence_route_coverage_blocker_codes",
        ][..],
        &["graph_route_readiness", "route_primary_blocker_codes"][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "route_coverage_blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "evidence_route_coverage_blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "route_primary_blocker_codes",
        ][..],
        &["query_runtime_preflight", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "query_runtime_preflight",
            "blocker_codes",
        ][..],
        &["query_runtime_preflight", "failed_checks"][..],
        &[
            "cutover_evidence",
            "query_runtime_preflight",
            "failed_checks",
        ][..],
    ] {
        for blocker in json_get_string_array_path(bundle, path) {
            blockers.insert(blocker);
        }
    }
    blockers.into_iter().collect()
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_get_u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn json_get_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_bool_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<bool> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a str> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_str)
}

fn json_get_u64_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<u64> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_u64)
}

fn json_get_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in prefix {
        current = current.get(*key)?;
    }
    current.get(field)
}

fn json_get_array_path(value: &serde_json::Value, path: &[&str]) -> serde_json::Value {
    json_get_path(value, path)
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn json_get_string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
        nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
        nowledge_replacement_summary_usage, NowledgeReplacementSummaryOptions,
        NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL, NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES, SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY,
        SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING,
        SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING,
    };
    use crate::{
        nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_specs_json,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
    };

    #[test]
    fn replacement_summary_keeps_production_replacement_zero_without_cutover_evidence() {
        let bundle = serde_json::json!({
            "coverage": {
                "coverage_per_million": 1_000_000
            },
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["business_surface"]["ready"], true);
        assert_eq!(summary["shadow_parity"]["ready"], true);
        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!([
                "bounded_read_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "graph_route_readiness",
                "previous_wrapper_contract",
                "query_family_readiness",
                "query_runtime_preflight",
                "search_candidate_shadow_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity"
            ])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "cutover_evidence"));
    }

    #[test]
    fn replacement_summary_reports_production_ready_when_all_evidence_is_ready() {
        let bundle = production_ready_bundle();

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], true);
        assert_eq!(summary["production_replacement_per_million"], 1_000_000);
        assert_eq!(summary["blocking_categories"], serde_json::json!([]));
        assert_eq!(summary["missing_evidence"], serde_json::json!([]));
        assert_eq!(summary["next_actions"], serde_json::json!([]));
        assert_eq!(summary["bounded_read_evidence"]["present"], true);
        assert_eq!(summary["bounded_read_evidence"]["ready"], true);
        assert_eq!(summary["bounded_read_evidence"]["mode"], "shadow_read_only");
        assert_eq!(summary["bounded_read_evidence"]["execution_row_cap"], 513);
        assert_eq!(
            summary["bounded_read_evidence"]["route_primary_ready"],
            true
        );
        assert_eq!(
            summary["bounded_read_evidence"]["route_query_plan_evidence_ready"],
            true
        );
        assert_eq!(
            summary["bounded_read_evidence"]["route_query_profile_evidence_ready"],
            true
        );
        assert_eq!(
            summary["bounded_read_evidence"]["route_relationship_property_pruning_evidence_ready"],
            true
        );
        assert_eq!(summary["graph_route_readiness"]["present"], true);
        assert_eq!(summary["graph_route_readiness"]["ready"], true);
        assert_eq!(
            summary["graph_route_readiness"]["evidence_route_coverage_matches"],
            true
        );
        assert_eq!(
            summary["graph_route_readiness"]["route_primary_ready"],
            true
        );
        assert_eq!(summary["query_runtime_preflight"]["present"], true);
        assert_eq!(summary["query_runtime_preflight"]["ready"], true);
        assert_eq!(
            summary["query_runtime_preflight"]["required_routes_covered"],
            true
        );
        assert_eq!(
            summary["query_runtime_preflight"]["route_coverage_ready"],
            true
        );
        assert_eq!(
            summary["query_runtime_preflight"]["probe_details_ready"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_protocol_matches"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_durable"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_checkpoint_boundary_present"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_wal_replay_bounded"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_torn_tail_clean"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]["background_maintenance_protocol_matches"],
            true
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_count"],
            2
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_admitted_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_deferred_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_operations"],
            8
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_admitted_search_projection_graph_delta_operations"],
            3
        );
        assert_eq!(
            summary["cutover_evidence"]
                ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"],
            42
        );
        assert_eq!(summary["shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["shadow_evidence"]["ready_wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
        assert_eq!(summary["full_contract_evidence"]["ready"], true);
        assert_eq!(
            summary["full_contract_evidence"]["full_contract_checked"],
            true
        );
        assert_eq!(
            summary["full_contract_evidence"]["full_contract_ready"],
            true
        );
        assert_eq!(summary["full_contract_evidence"]["selected_checks"], 2);
        assert_eq!(summary["full_contract_evidence"]["check_count"], 2);
        assert_eq!(
            summary["previous_wrapper_contract_evidence"]["wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(summary["dual_engine_evidence"]["present"], true);
        assert_eq!(summary["dual_engine_evidence"]["ready"], true);
        assert_eq!(summary["dual_engine_evidence"]["consistent"], true);
        assert_eq!(summary["dual_engine_evidence"]["primary_engine"], "skein");
        assert_eq!(
            summary["dual_engine_evidence"]["shadow_engine"],
            "previous-wrapper"
        );
        assert_eq!(summary["search_projection_evidence"]["present"], true);
        assert_eq!(summary["search_projection_evidence"]["ready"], true);
        assert_eq!(
            summary["search_projection_evidence"]["covered_table_count"],
            6
        );
        assert_eq!(
            summary["search_projection_evidence"]["required_table_count"],
            6
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["present"],
            true
        );
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["evidence_source"],
            "skein-rust-cli"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["primary_engine"],
            "lancedb"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["shadow_engine"],
            "skein"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["table_parity_ready"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["document_identity_parity"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["predicate_pushdown_parity"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]
                ["shadow_persisted_segment_descriptor_ready"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]
                ["shadow_segment_descriptor_scan_filter_fields_ready"],
            true
        );
        assert_eq!(summary["search_candidate_shadow_evidence"]["present"], true);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["candidate_counts_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["candidate_identity_ready"],
            true
        );
        assert_eq!(
            summary["replacement_readiness_by_query_family"][0]["query_family"],
            "memory_lookup"
        );
    }

    #[test]
    fn graph_route_readiness_summary_parses_direct_library_value() {
        let bundle = production_ready_bundle();
        let direct =
            nowledge_graph_route_readiness_summary(bundle.get("graph_route_readiness").unwrap());
        let from_bundle = nowledge_graph_route_readiness_summary_from_bundle(&bundle);

        assert!(direct.ready);
        assert_eq!(direct, from_bundle);
        assert_eq!(
            direct.covered_route_count,
            Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        );
        assert_eq!(direct.missing_required_routes, Vec::<&'static str>::new());
        assert!(direct.route_catalog_metadata_ready);
        assert_eq!(
            direct.missing_route_catalog_metadata_routes,
            Vec::<&'static str>::new()
        );
        assert_eq!(
            direct.route_catalog_metadata_mismatch_routes,
            Vec::<String>::new()
        );
    }

    #[test]
    fn graph_route_readiness_summary_fails_closed_for_missing_library_value() {
        let direct = nowledge_graph_route_readiness_summary(&serde_json::Value::Null);

        assert!(!direct.present);
        assert!(!direct.ready);
        assert_eq!(direct.covered_routes, Vec::<String>::new());
    }

    #[test]
    fn graph_route_readiness_summary_fails_closed_for_stale_route_catalog_metadata() {
        let mut bundle = production_ready_bundle();
        bundle["graph_route_readiness"]
            .as_object_mut()
            .unwrap()
            .remove("route_catalog");
        bundle["graph_route_readiness"]["routes"][0]
            .as_object_mut()
            .unwrap()
            .remove("owner");

        let direct =
            nowledge_graph_route_readiness_summary(bundle.get("graph_route_readiness").unwrap());

        assert!(!direct.ready);
        assert!(!direct.route_catalog_metadata_ready);
        assert_eq!(
            direct.missing_route_catalog_metadata_routes,
            vec![REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]
        );
        assert_eq!(
            direct.route_catalog_metadata_mismatch_routes,
            vec!["__route_catalog__".to_string()]
        );
    }

    #[test]
    fn replacement_summary_reports_trace_candidate_evidence_but_blocks_production() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["primary_engine"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_engine"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["row_count_parity"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
            true
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence_ready"));
    }

    #[test]
    fn replacement_summary_rejects_weak_search_candidate_trace_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();
        bundle["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"] =
            serde_json::json!(0);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_field_pruning_missing"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"],
            0
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_required_query_families() {
        let mut bundle = production_ready_bundle();
        bundle["replacement_readiness_by_query_family"] = serde_json::json!([
            {
                "query_family": "read",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "mutation",
                "replacement_readiness_per_million": 1_000_000
            }
        ]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_family_readiness"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "replacement_readiness_by_query_family_ready"));
        assert_eq!(
            summary["replacement_readiness_family_summary"]["missing_required_query_families"],
            serde_json::json!([
                "memory_lookup",
                "graph_traversal",
                "projected_graph",
                "label_stats_read",
                "search_projection"
            ])
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "close_blocked_query_families"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_search_projection_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_projection_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_evidence"]["present"], false);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_search_projection_replacement_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_projection_evidence.fts_ready")
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_evidence"]["covered_table_count"] = serde_json::json!(5);
        bundle["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["missing_source_chunks_index"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_evidence"]["covered_table_count"],
            5
        );
        assert_eq!(
            summary["search_projection_evidence"]["blocker_codes"],
            serde_json::json!(["missing_source_chunks_index"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_projection_document_identity() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_evidence"]["document_identity_ready"] = serde_json::json!(false);
        bundle["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["document_identity_not_ready"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_evidence"]["document_identity_ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_projection_evidence_protocol() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_evidence"]["protocol"] = serde_json::json!("handwritten");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_projection_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_evidence"]["protocol"],
            "handwritten"
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_search_projection_replacement_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_projection_evidence.protocol")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_search_projection_shadow_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_projection_shadow_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["present"],
            false
        );
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_projection_shadow_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "search_projection_shadow_evidence.table_parity.ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_shadow_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["table_parity"]["ready"] =
            serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["table_parity_mismatch"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["table_parity_ready"],
            false
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["table_parity_mismatch"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_projection_shadow_document_identity() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["document_count_parity"] =
            serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["document_identity_parity"] =
            serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["document_identity_mismatch"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["document_count_parity"],
            true
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["document_identity_parity"],
            false
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["document_identity_mismatch"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_projection_shadow_pushdown_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_persisted_segment_descriptor_ready"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
            false
        );
        assert!(
            summary["search_projection_shadow_evidence"]["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY)
        );
        assert!(
            summary["search_projection_shadow_evidence"]["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING)
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_projection_shadow_descriptor_fields() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(false);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_field_summaries"] = serde_json::json!([]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]
                ["shadow_segment_descriptor_scan_filter_fields_ready"],
            false
        );
        assert!(
            summary["search_projection_shadow_evidence"]["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_projection_shadow_evidence_ready"));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_shadow_descriptor_field_coverage() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
            serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["primary_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_field_summaries"] =
            serde_json::json!([{ "field": "unit_type" }, { "field": "importance" }]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
            false
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]
                ["shadow_segment_descriptor_scan_filter_fields_ready"],
            false
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["pushdown_evidence"]
                ["scan_filter_fields_ready"],
            false
        );
        assert!(
            summary["search_projection_shadow_evidence"]["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
        );
    }

    #[test]
    fn replacement_summary_requires_search_projection_shadow_evidence_protocol() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["protocol"] = serde_json::json!("handwritten");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["protocol"],
            "handwritten"
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_projection_shadow_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_projection_shadow_evidence.protocol")
            }));
    }

    #[test]
    fn replacement_summary_requires_search_projection_shadow_evidence_source() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["evidence_source"] =
            serde_json::json!("manual-json");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["evidence_source"],
            "manual-json"
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_projection_shadow_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_projection_shadow_evidence.evidence_source")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_search_candidate_shadow_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_candidate_shadow_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["present"],
            false
        );
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_search_candidate_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_identity_parity() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["candidate_identity"]["ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["candidate_identity"]["parity"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_identity_mismatch"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["candidate_identity_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["candidate_identity_parity"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["search_candidate_identity_mismatch"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence_ready"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence"));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_filter_pushdown() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["field_summary_count"] =
            serde_json::json!(0);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["missing_required_fields"] =
            serde_json::json!(["unit_type"]);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_field_pruning_missing"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["filter_pushdown_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["filter_pushdown_field_summary_count"],
            0
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence_ready"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_candidate_shadow_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "search_candidate_shadow_evidence.filter_pushdown.ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_bounded_read_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("bounded_read_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["bounded_read_evidence"]["present"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn replacement_summary_requires_shadow_read_only_bounded_read_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["bounded_read_evidence"]["mode"] = serde_json::json!("writable_cutover");
        bundle["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["not_shadow_read_only"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["mode"], "writable_cutover");
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence_ready"));
    }

    #[test]
    fn replacement_summary_requires_bounded_read_route_coverage() {
        let mut bundle = production_ready_bundle();
        let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .copied()
            .filter(|route| *route != "/graph/explore")
            .collect::<Vec<_>>();
        bundle["bounded_read_evidence"]["covered_routes"] = serde_json::json!(covered_routes);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert_eq!(
            summary["bounded_read_evidence"]["missing_covered_routes"],
            serde_json::json!(["/graph/explore"])
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence_ready"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_bounded_read_profile"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "bounded_read_evidence.covered_routes")
            }));
    }

    #[test]
    fn replacement_summary_requires_bounded_read_graph_route_summary() {
        let mut bundle = production_ready_bundle();
        bundle["bounded_read_evidence"]
            .as_object_mut()
            .unwrap()
            .remove("route_relationship_property_pruning_evidence_ready");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence_ready"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_bounded_read_profile"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "bounded_read_evidence.route_relationship_property_pruning_evidence_ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_requires_bounded_read_evidence_protocol() {
        let mut bundle = production_ready_bundle();
        bundle["bounded_read_evidence"]["protocol"] = serde_json::json!("handwritten");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["protocol"], "handwritten");
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_bounded_read_profile"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "bounded_read_evidence.protocol")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_graph_route_readiness() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("graph_route_readiness");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["graph_route_readiness"]["present"], false);
        assert_eq!(summary["graph_route_readiness"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "graph_route_readiness"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "graph_route_readiness"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
    }

    #[test]
    fn replacement_summary_rejects_stale_graph_route_coverage() {
        let mut bundle = production_ready_bundle();
        let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .copied()
            .filter(|route| *route != "/graph/explore")
            .collect::<Vec<_>>();
        bundle["graph_route_readiness"]["covered_routes"] = serde_json::json!(covered_routes);
        bundle["graph_route_readiness"]["covered_route_count"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1);
        bundle["graph_route_readiness"]["evidence_route_coverage_matches"] =
            serde_json::json!(false);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["route_coverage_evidence_mismatch"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["graph_route_readiness"]["ready"], false);
        assert_eq!(
            summary["graph_route_readiness"]["missing_required_routes"],
            serde_json::json!(["/graph/explore"])
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "graph_route_readiness_ready"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_graph_route_readiness_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "graph_route_readiness.evidence_route_coverage_matches"
                        })
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_query_runtime_preflight() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("query_runtime_preflight");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["query_runtime_preflight"]["present"], false);
        assert_eq!(summary["query_runtime_preflight"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_query_runtime_preflight_evidence"));
    }

    #[test]
    fn replacement_summary_requires_query_runtime_preflight_route_coverage() {
        let mut bundle = production_ready_bundle();
        bundle["query_runtime_preflight"]["covered_route_count"] = serde_json::json!(14);
        bundle["query_runtime_preflight"]["required_routes_covered"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["missing_required_routes"] =
            serde_json::json!(["/graph/explore"]);
        bundle["query_runtime_preflight"]["covered_routes"] = serde_json::json!([
            "/graph/overview",
            "/graph/expand/{node_id}",
            "/graph/live-preview",
            "/graph/live-preview/{node_id}",
            "/graph/community-members/{community_id}",
            "/library/community/{community_id}/subgraph",
            "/library/community/{community_id}/recent-memories",
            "/library/community/{community_id}/related",
            "/graph/analysis",
            "/graph/augmentation/state",
            "/graph/augmentation/pagerank/plan",
            "/graph/node-details/{node_id}",
            "/graph/orphans",
            "/graph/shortest-path"
        ]);
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .retain(|probe| {
                probe.get("route").and_then(serde_json::Value::as_str) != Some("/graph/explore")
            });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["query_runtime_preflight"]["ready"], false);
        assert_eq!(
            summary["query_runtime_preflight"]["route_coverage_ready"],
            false
        );
        assert_eq!(
            summary["query_runtime_preflight"]["missing_required_routes"],
            serde_json::json!(["/graph/explore"])
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight_ready"));
    }

    #[test]
    fn replacement_summary_recomputes_query_runtime_route_coverage_from_probes() {
        let mut bundle = production_ready_bundle();
        bundle["query_runtime_preflight"]["covered_route_count"] = serde_json::json!(15);
        bundle["query_runtime_preflight"]["required_routes_covered"] = serde_json::json!(true);
        bundle["query_runtime_preflight"]["missing_required_routes"] = serde_json::json!([]);
        bundle["query_runtime_preflight"]["covered_routes"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .retain(|probe| {
                probe.get("route").and_then(serde_json::Value::as_str) != Some("/graph/explore")
            });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["query_runtime_preflight"]["ready"], false);
        assert_eq!(
            summary["query_runtime_preflight"]["route_coverage_ready"],
            false
        );
        assert_eq!(
            summary["query_runtime_preflight"]["missing_required_routes"],
            serde_json::json!(["/graph/explore"])
        );
        assert!(!summary["query_runtime_preflight"]["covered_routes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "/graph/explore"));
    }

    #[test]
    fn replacement_summary_rejects_query_runtime_unknown_route_probe() {
        let mut bundle = production_ready_bundle();
        let mut probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        probe["route"] = serde_json::json!("/graph/stale-route");
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["query_runtime_preflight"]["ready"], false);
        assert_eq!(
            summary["query_runtime_preflight"]["route_coverage_ready"],
            false
        );
        assert_eq!(
            summary["query_runtime_preflight"]["unknown_routes"],
            serde_json::json!(["/graph/stale-route"])
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight"));
    }

    #[test]
    fn replacement_summary_rejects_query_runtime_duplicate_route_probe() {
        let mut bundle = production_ready_bundle();
        let probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["query_runtime_preflight"]["ready"], false);
        assert_eq!(
            summary["query_runtime_preflight"]["route_coverage_ready"],
            false
        );
        assert_eq!(
            summary["query_runtime_preflight"]["duplicate_routes"],
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]])
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "query_runtime_preflight"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_graph_delta_aggregate_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["cutover_evidence"]
            .as_object_mut()
            .unwrap()
            .remove("background_maintenance_admitted_search_projection_graph_delta_count");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background_maintenance"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background_maintenance_search_projection_graph_delta"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["action"] == "attach_background_maintenance_report"
                    && item["evidence_fields"].as_array().unwrap().iter().any(|field| {
                        field
                            == "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
                    })
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_when_dual_engine_evidence_is_not_ready() {
        let mut bundle = production_ready_bundle();
        bundle["dual_engine_evidence"]["ready"] = serde_json::json!(false);
        bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["present"], true);
        assert_eq!(summary["dual_engine_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "rerun_dual_engine_shadow_gate"));
    }

    #[test]
    fn replacement_summary_blocks_production_when_dual_engine_counts_are_inconsistent() {
        let mut bundle = production_ready_bundle();
        bundle["dual_engine_evidence"]["ready"] = serde_json::json!(true);
        bundle["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(0);
        bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["ready"], true);
        assert_eq!(summary["dual_engine_evidence"]["consistent"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "rerun_dual_engine_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "dual_engine_evidence.consistent")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_dual_engine_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("dual_engine_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["dual_engine_evidence"]["present"], false);
        assert_eq!(
            summary["dual_engine_evidence"]["ready"],
            serde_json::Value::Null
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "dual_engine_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "rerun_dual_engine_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "dual_engine_evidence.present")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_previous_wrapper_contract_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("previous_wrapper_contract_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(
            summary["previous_wrapper_contract_evidence"]["ready"],
            false
        );
        assert_eq!(
            summary["missing_evidence"],
            serde_json::json!(["previous_wrapper_contract_evidence"])
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_full_previous_wrapper_contract_check"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "previous_wrapper_contract"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_shadow_wrapper_identity() {
        let mut bundle = production_ready_bundle();
        bundle["shadow_ready"]
            .as_object_mut()
            .unwrap()
            .remove("wrapper_identity");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["shadow_evidence"]["ready"], false);
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "shadow_parity"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_previous_wrapper_shadow_gate"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "shadow_ready.wrapper_identity")
            }));
    }

    #[test]
    fn replacement_summary_blocks_production_without_full_contract_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["full_contract_checked"] = serde_json::json!(false);
        bundle["full_contract_ready"] = serde_json::json!(false);
        bundle["selected_checks"] = serde_json::json!(1);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
        assert_eq!(summary["full_contract_evidence"]["ready"], false);
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "full_contract_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "previous_wrapper_contract"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_full_previous_wrapper_contract_check"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "full_contract_ready")
            }));
    }

    #[test]
    fn replacement_summary_can_omit_family_details_for_compact_output() {
        let bundle = production_ready_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: false,
                max_family_items: None,
                include_blocker_details: true,
                max_blockers: None,
            },
        );

        assert_eq!(
            summary["replacement_readiness_by_query_family"],
            serde_json::json!([])
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["total_count"],
            REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["ready_count"],
            REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["omitted_count"],
            REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
        );
        assert_eq!(summary["production_cutover_ready"], true);
    }

    #[test]
    fn replacement_summary_can_limit_family_details() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "cutover_evidence": {
                "eligible": true,
                "replacement_readiness_invalid_family_count": 0,
                "blockers": []
            },
            "shadow_run": {},
            "shadow_ready": {},
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 0
                }
            ]
        });

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: true,
                max_family_items: Some(1),
                include_blocker_details: true,
                max_blockers: None,
            },
        );

        assert_eq!(
            summary["replacement_readiness_by_query_family"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["total_count"],
            2
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["ready_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["blocked_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["omitted_count"],
            1
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["blocked_query_families"],
            serde_json::json!(["search_projection"])
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "close_blocked_query_families"));
    }

    #[test]
    fn replacement_summary_can_omit_blocker_details_for_compact_output() {
        let bundle = blocked_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: false,
                max_family_items: None,
                include_blocker_details: false,
                max_blockers: None,
            },
        );

        assert_eq!(summary["blockers"], serde_json::json!([]));
        assert_eq!(summary["blocker_summary"]["total_count"], 5);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 5);
        assert_eq!(
            summary["replacement_readiness_by_query_family"],
            serde_json::json!([])
        );
    }

    #[test]
    fn replacement_summary_can_limit_blocker_details() {
        let bundle = blocked_bundle();

        let summary = nowledge_replacement_summary_json_with_options(
            &bundle,
            NowledgeReplacementSummaryOptions {
                include_family_details: true,
                max_family_items: None,
                include_blocker_details: true,
                max_blockers: Some(2),
            },
        );

        assert_eq!(summary["blockers"].as_array().unwrap().len(), 2);
        assert_eq!(summary["blocker_summary"]["total_count"], 5);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 3);
    }

    #[test]
    fn replacement_summary_groups_storage_and_background_blockers() {
        let bundle = serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "blocked",
                "matched_per_million": 500_000,
                "blockers": ["primary-only projected graph"]
            },
            "migration_gate": {
                "decision": "blocked",
                "blockers": ["shadow parity blocked"]
            },
            "cutover_evidence": {
                "eligible": false,
                "storage_recovery_required": true,
                "storage_recovery_present": false,
                "storage_recovery_ready": false,
                "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
                "storage_recovery_blockers": [
                    "WAL replay was not opened with a configured entry bound"
                ],
                "background_maintenance_required": true,
                "background_maintenance_present": false,
                "background_maintenance_ready": false,
                "background_maintenance_blocker_codes": ["missing_evidence"],
                "background_maintenance_blockers": [
                    "background maintenance summary is missing"
                ],
                "replacement_readiness_min_per_million": 500_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [
                    "query family search is below full replacement readiness"
                ],
                "blockers": ["cutover evidence blocked"]
            },
            "replacement_readiness_per_million": 500_000,
            "replacement_readiness_by_query_family": []
        });

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(
            summary["blocking_categories"],
            serde_json::json!([
                "background_maintenance",
                "bounded_read_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "graph_route_readiness",
                "migration_gate",
                "previous_wrapper_contract",
                "query_family_readiness",
                "query_runtime_preflight",
                "search_candidate_shadow_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity",
                "storage_recovery"
            ])
        );
        assert_eq!(
            summary["missing_evidence"],
            serde_json::json!([
                "storage_recovery",
                "background_maintenance",
                "replacement_readiness_by_query_family_ready",
                "previous_wrapper_contract_evidence",
                "full_contract_evidence",
                "dual_engine_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "search_candidate_shadow_evidence",
                "bounded_read_evidence",
                "graph_route_readiness",
                "query_runtime_preflight",
                "shadow_run",
                "shadow_ready"
            ])
        );
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "background maintenance summary is missing"));
        assert_eq!(
            summary["cutover_evidence"]["storage_recovery_blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        assert_eq!(
            summary["cutover_evidence"]["background_maintenance_blocker_codes"],
            serde_json::json!(["missing_evidence"])
        );
        assert_eq!(
            summary["next_actions"],
            serde_json::json!([
                {
                    "action": "close_blocked_query_families",
                    "reason": "one or more required query families are missing or below full replacement readiness",
                    "evidence_fields": [
                        "replacement_readiness_per_million",
                        "replacement_readiness_by_query_family"
                    ]
                },
                {
                    "action": "run_previous_wrapper_shadow_gate",
                    "reason": "shadow parity or migration gate decision is not ready",
                    "evidence_fields": [
                        "cutover.matched_per_million",
                        "cutover.decision",
                        "migration_gate.decision",
                        "shadow_run.evidence_kind",
                        "shadow_ready.engine_kind",
                        "shadow_ready.wrapper_identity",
                        "cutover_evidence.ready_wrapper_identity",
                        "previous_wrapper_contract_evidence.wrapper_identity"
                    ]
                },
                {
                    "action": "provide_eligible_cutover_evidence",
                    "reason": "cutover evidence is missing or not eligible for production replacement",
                    "evidence_fields": [
                        "cutover_evidence.eligible",
                        "cutover_evidence.evidence_kind",
                        "cutover_evidence.ready_engine_kind",
                        "cutover_evidence.ready_wrapper_identity"
                    ]
                },
                {
                    "action": "run_full_previous_wrapper_contract_check",
                    "reason": "previous-wrapper contract evidence is missing or not ready",
                    "evidence_fields": [
                        "required_contract_ready",
                        "full_contract_checked",
                        "full_contract_ready",
                        "selected_checks",
                        "check_count",
                        "previous_wrapper_contract_evidence.ready",
                        "previous_wrapper_contract_evidence.wrapper_identity",
                        "previous_wrapper_contract_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "rerun_dual_engine_shadow_gate",
                    "reason": "side-by-side dual-engine evidence is missing or not ready",
                    "evidence_fields": [
                        "dual_engine_evidence.present",
                        "dual_engine_evidence.ready",
                        "dual_engine_evidence.consistent",
                        "dual_engine_evidence.primary_check_count",
                        "dual_engine_evidence.shadow_check_count",
                        "dual_engine_evidence.matched_check_count",
                        "dual_engine_evidence.primary_only_check_count",
                        "dual_engine_evidence.matched_per_million"
                    ]
                },
                {
                    "action": "attach_search_projection_replacement_evidence",
                    "reason": "LanceDB replacement evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_evidence.protocol",
                        "search_projection_evidence.present",
                        "search_projection_evidence.ready",
                        "search_projection_evidence.derived_projection",
                        "search_projection_evidence.all_tables_covered",
                        "search_projection_evidence.covered_table_count",
                        "search_projection_evidence.required_table_count",
                        "search_projection_evidence.fts_ready",
                        "search_projection_evidence.vector_ready",
                        "search_projection_evidence.document_identity_ready",
                        "search_projection_evidence.embedding_identity_ready",
                        "search_projection_evidence.fail_soft_ready",
                        "search_projection_evidence.rebuild_marker_ready",
                        "search_projection_evidence.metadata_repair_marker_ready",
                        "search_projection_evidence.incremental_update_ready",
                        "search_projection_evidence.source_chunk_ready",
                        "search_projection_evidence.predicate_pushdown_ready",
                        "search_projection_evidence.compressed_vector_projection_required",
                        "search_projection_evidence.compressed_vector_projection_ready",
                        "search_projection_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_search_projection_shadow_evidence",
                    "reason": "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_shadow_evidence.protocol",
                        "search_projection_shadow_evidence.evidence_source",
                        "search_projection_shadow_evidence.present",
                        "search_projection_shadow_evidence.ready",
                        "search_projection_shadow_evidence.primary_ready",
                        "search_projection_shadow_evidence.shadow_ready",
                        "search_projection_shadow_evidence.document_count_parity",
                        "search_projection_shadow_evidence.document_identity_parity",
                        "search_projection_shadow_evidence.table_parity.ready",
                        "search_projection_shadow_evidence.embedding_identity_parity",
                        "search_projection_shadow_evidence.lifecycle_parity",
                        "search_projection_shadow_evidence.incremental_watermark_parity",
                        "search_projection_shadow_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_search_candidate_shadow_evidence",
                    "reason": "LanceDB/Skein search candidate side-by-side evidence is missing or not ready",
                    "evidence_fields": [
                        "search_candidate_shadow_evidence.protocol",
                        "search_candidate_shadow_evidence.evidence_source",
                        "search_candidate_shadow_evidence.route",
                        "search_candidate_shadow_evidence.present",
                        "search_candidate_shadow_evidence.ready",
                        "search_candidate_shadow_evidence.candidate_primary_engine",
                        "search_candidate_shadow_evidence.request_count",
                        "search_candidate_shadow_evidence.primary_candidate_count",
                        "search_candidate_shadow_evidence.shadow_candidate_count",
                        "search_candidate_shadow_evidence.matched_candidate_count",
                        "search_candidate_shadow_evidence.primary_only_candidate_count",
                        "search_candidate_shadow_evidence.candidate_identity.ready",
                        "search_candidate_shadow_evidence.filter_pushdown.ready",
                        "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                        "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields",
                        "search_candidate_shadow_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "attach_bounded_read_profile",
                    "reason": "bounded read execution profile is missing or not ready",
                    "evidence_fields": [
                        "bounded_read_evidence.protocol",
                        "bounded_read_evidence.present",
                        "bounded_read_evidence.ready",
                        "bounded_read_evidence.mode",
                        "bounded_read_evidence.max_rows",
                        "bounded_read_evidence.execution_row_cap",
                        "bounded_read_evidence.row_limit_enforced_before_output",
                        "bounded_read_evidence.operator_row_cap_enabled",
                        "bounded_read_evidence.blocking_operator_count",
                        "bounded_read_evidence.covered_routes",
                        "bounded_read_evidence.route_primary_ready",
                        "bounded_read_evidence.primary_ready_routes",
                        "bounded_read_evidence.route_query_plan_evidence_ready",
                        "bounded_read_evidence.route_query_profile_evidence_ready",
                        "bounded_read_evidence.relationship_property_pruning_required_count",
                        "bounded_read_evidence.relationship_property_pruning_report_count",
                        "bounded_read_evidence.route_relationship_property_pruning_evidence_ready",
                        "bounded_read_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "attach_graph_route_readiness_evidence",
                    "reason": "graph route readiness evidence is missing, stale, or does not cover all required graph read routes",
                    "evidence_fields": [
                        "graph_route_readiness.protocol",
                        "graph_route_readiness.present",
                        "graph_route_readiness.ready",
                        "graph_route_readiness.evidence_protocol",
                        "graph_route_readiness.evidence_ready",
                        "graph_route_readiness.required_route_count",
                        "graph_route_readiness.covered_route_count",
                        "graph_route_readiness.covered_routes",
                        "graph_route_readiness.route_coverage_ready",
                        "graph_route_readiness.evidence_route_coverage_present",
                        "graph_route_readiness.evidence_route_coverage_matches",
                        "graph_route_readiness.route_query_runtime_ready",
                        "graph_route_readiness.route_query_plan_evidence_ready",
                        "graph_route_readiness.route_query_profile_evidence_ready",
                        "graph_route_readiness.relationship_property_pruning_required_count",
                        "graph_route_readiness.relationship_property_pruning_report_count",
                        "graph_route_readiness.route_relationship_property_pruning_evidence_ready",
                        "graph_route_readiness.route_primary_ready",
                        "graph_route_readiness.primary_ready_route_count",
                        "graph_route_readiness.route_primary_blocker_codes"
                    ]
                },
                {
                    "action": "attach_query_runtime_preflight_evidence",
                    "reason": "query runtime preflight evidence is missing or does not cover all required graph read routes",
                    "evidence_fields": [
                        "query_runtime_preflight.protocol",
                        "query_runtime_preflight.present",
                        "query_runtime_preflight.ready",
                        "query_runtime_preflight.database_opened",
                        "query_runtime_preflight.probe_count",
                        "query_runtime_preflight.passed_probe_count",
                        "query_runtime_preflight.failed_probe_count",
                        "query_runtime_preflight.required_routes_covered",
                        "query_runtime_preflight.route_coverage_ready",
                        "query_runtime_preflight.probe_details_ready",
                        "query_runtime_preflight.blocker_codes",
                        "query_runtime_preflight.probes"
                    ]
                },
                {
                    "action": "attach_storage_recovery_report",
                    "reason": "required storage recovery evidence is missing or blocked",
                    "evidence_fields": [
                        "cutover_evidence.storage_recovery_present",
                        "cutover_evidence.storage_recovery_ready",
                        "cutover_evidence.storage_recovery_blocker_codes"
                    ]
                },
                {
                    "action": "attach_background_maintenance_report",
                    "reason": "required background maintenance QoS or graph-delta evidence is missing or blocked",
                    "evidence_fields": [
                        "cutover_evidence.background_maintenance_present",
                        "cutover_evidence.background_maintenance_ready",
                        "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                        "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                        "cutover_evidence.background_maintenance_blocker_codes"
                    ]
                }
            ])
        );
        assert_eq!(summary["production_replacement_per_million"], 0);
    }

    #[test]
    fn validates_nowledge_replacement_summary_usage_text() {
        assert!(nowledge_replacement_summary_usage().contains("<migration-gate-json>"));
        assert!(nowledge_replacement_summary_usage().contains("--require-production-ready"));
        assert!(nowledge_replacement_summary_usage().contains("--compact"));
        assert!(nowledge_replacement_summary_usage().contains("--max-family-items"));
        assert!(nowledge_replacement_summary_usage().contains("--max-blockers"));
        assert!(nowledge_replacement_summary_usage().contains("--search-projection-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-projection-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-candidate-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--bounded-read-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--query-runtime-preflight-json"));
        assert!(nowledge_replacement_summary_usage().contains("--query-family-evidence-json"));
    }

    fn production_ready_bundle() -> serde_json::Value {
        let query_runtime_preflight = ready_query_runtime_preflight();
        serde_json::json!({
            "required_contract_ready": true,
            "full_contract_checked": true,
            "full_contract_ready": true,
            "selected_checks": 2,
            "check_count": 2,
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": []
            },
            "cutover": {
                "decision": "ready",
                "matched_per_million": 1_000_000,
                "blockers": []
            },
            "migration_gate": {
                "decision": "ready",
                "blockers": []
            },
            "cutover_evidence": {
                "eligible": true,
                "evidence_kind": "previous_wrapper",
                "ready_engine_kind": "previous_wrapper",
                "ready_wrapper_identity": "nowledge-previous-wrapper:test",
                "storage_recovery_required": true,
                "storage_recovery_present": true,
                "storage_recovery_ready": true,
                "storage_recovery_protocol_matches": true,
                "storage_recovery_durable": true,
                "storage_recovery_checkpoint_boundary_present": true,
                "storage_recovery_wal_replay_bounded": true,
                "storage_recovery_torn_tail_clean": true,
                "storage_recovery_blocker_codes": [],
                "storage_recovery_blockers": [],
                "background_maintenance_required": true,
                "background_maintenance_present": true,
                "background_maintenance_ready": true,
                "background_maintenance_protocol_matches": true,
                "background_maintenance_executable_search_projection_graph_delta_count": 2,
                "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                "background_maintenance_deferred_search_projection_graph_delta_count": 1,
                "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                "background_maintenance_executable_search_projection_graph_delta_operations": 8,
                "background_maintenance_admitted_search_projection_graph_delta_operations": 3,
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42,
                "background_maintenance_blocker_codes": [],
                "background_maintenance_blockers": [],
                "replacement_readiness_min_per_million": 1_000_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [],
                "blockers": []
            },
            "shadow_run": {
                "evidence_kind": "previous_wrapper"
            },
            "shadow_ready": {
                "engine_kind": "previous_wrapper",
                "wrapper_identity": "nowledge-previous-wrapper:test"
            },
            "dual_engine_evidence": {
                "ready": true,
                "primary_engine": "skein",
                "shadow_engine": "previous-wrapper",
                "primary_check_count": 1,
                "shadow_check_count": 1,
                "matched_check_count": 1,
                "primary_only_check_count": 0,
                "matched_per_million": 1_000_000
            },
            "search_projection_evidence": {
                "protocol": "skein-nowledge-search-projection-evidence",
                "ready": true,
                "derived_projection": true,
                "all_tables_covered": true,
                "covered_table_count": 6,
                "required_table_count": 6,
                "fts_ready": true,
                "vector_ready": true,
                "document_identity_ready": true,
                "embedding_identity_ready": true,
                "fail_soft_ready": true,
                "rebuild_marker_ready": true,
                "metadata_repair_marker_ready": true,
                "incremental_update_ready": true,
                "source_chunk_ready": true,
                "predicate_pushdown_ready": true,
                "compressed_vector_projection_required": true,
                "compressed_vector_projection_ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "protocol": "skein-nowledge-search-projection-shadow-evidence",
                "evidence_source": "skein-rust-cli",
                "ready": true,
                "primary_engine": "lancedb",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "document_count_parity": true,
                "document_identity_parity": true,
                "table_parity": {
                    "ready": true
                },
                "embedding_identity_parity": true,
                "lifecycle_parity": true,
                "incremental_watermark_parity": true,
                "predicate_pushdown_parity": true,
                "pushdown_evidence": {
                    "ready": true,
                    "predicate_pushdown_parity": true,
                    "primary_predicate_pushdown_ready": true,
                    "shadow_predicate_pushdown_ready": true,
                    "shadow_persisted_segment_descriptor_ready": true,
                    "shadow_segment_descriptor_scan_filter_fields_ready": true,
                    "primary_scan_filter_fields": scan_filter_fields_json(),
                    "shadow_scan_filter_fields": scan_filter_fields_json(),
                    "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
                },
                "blocker_codes": []
            },
            "search_candidate_shadow_evidence": {
                "protocol": "skein-nowledge-search-candidate-shadow-evidence",
                "route": "/search-index/skein-shadow/candidate-evidence",
                "evidence_source": "nmem-rust-bridge",
                "ready": true,
                "candidate_primary_engine": "skein",
                "request_count": 2,
                "primary_candidate_count": 3,
                "shadow_candidate_count": 3,
                "matched_candidate_count": 3,
                "primary_only_candidate_count": 0,
                "candidate_identity": {
                    "ready": true,
                    "id_space": "search_candidate_id",
                    "representation": "per_request_sorted_candidate_ids",
                    "primary_checksum": 123,
                    "shadow_checksum": 123,
                    "matched_checksum": 123,
                    "parity": true
                },
                "filter_pushdown_ready": true,
                "filter_pushdown": {
                    "ready": true,
                    "pushed_predicate_count": 1,
                    "shadow_scan_present": true,
                    "required_fields": scan_filter_fields_json(),
                    "missing_required_fields": [],
                    "field_summary_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
                    "field_summaries": scan_filter_field_summaries_json(),
                    "blocker_codes": []
                },
                "blocker_codes": []
            },
            "bounded_read_evidence": {
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "route_primary_ready": true,
                "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
                "route_query_plan_evidence_ready": true,
                "route_query_profile_evidence_ready": true,
                "relationship_property_pruning_required_count": 0,
                "relationship_property_pruning_report_count": 0,
                "route_relationship_property_pruning_evidence_ready": true,
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
                "blocker_codes": []
            },
            "graph_route_readiness": ready_graph_route_readiness(),
            "query_runtime_preflight": query_runtime_preflight,
            "previous_wrapper_contract_evidence": {
                "ready": true,
                "evidence_kind": "previous_wrapper_contract",
                "wrapper_identity": "nowledge-previous-wrapper:test",
                "requires_full_contract_ready": true,
                "requires_wrapper_identity": true,
                "blocker_codes": [],
                "blockers": []
            },
            "replacement_readiness_per_million": 1_000_000,
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "graph_traversal",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "projected_graph",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "label_stats_read",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
        })
    }

    fn scan_filter_fields_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
    }

    fn ready_search_candidate_trace_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
            "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
            "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
            "present": true,
            "ready": true,
            "reported_ready": true,
            "engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
            "primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
            "shadow_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
            "row_count_parity": true,
            "vector_top_k_overlap_ready": true,
            "fts_top_k_overlap_ready": true,
            "shadow_scan_present": true,
            "shadow_scan_filter_pushdown_ready": true,
            "shadow_scan_field_pruning_ready": true,
            "shadow_scan_field_summary_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
            "shadow_scan_input_predicate_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
            "shadow_scan_pushed_predicate_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
            "shadow_scan_residual_predicate_count": 0,
            "shadow_scan_filtered_out_count": 4,
            "shadow_scan_pruned_document_count": 2,
            "shadow_scan_scanned_document_count": 2,
            "shadow_scan_parse_error": null,
            "shadow_scan_unsatisfiable": false,
            "blocker_codes": []
        })
    }

    fn scan_filter_field_summaries_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| serde_json::json!({ "field": field }))
            .collect::<Vec<_>>())
    }

    fn ready_query_runtime_preflight() -> serde_json::Value {
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_query_runtime_preflight_probe(route))
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "probe_count": probes.len(),
            "passed_probe_count": probes.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "blocker_codes": [],
            "probes": probes,
        })
    }

    fn ready_graph_route_readiness() -> serde_json::Value {
        let routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                let spec = nowledge_mem_graph_read_route_spec(route).unwrap();
                serde_json::json!({
                    "route": route,
                    "owner": spec.owner.as_str(),
                    "required_evidence_kind": spec.required_evidence_kind.as_str(),
                    "stale_on_catalog_change": spec.stale_on_catalog_change,
                    "primary_ready": true
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
            "evidence_protocol": NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
            "evidence_ready": true,
            "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "evidence_route_coverage_present": true,
            "evidence_route_coverage_matches": true,
            "evidence_route_coverage_blocker_codes": [],
            "shadow_compare_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "primary_ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_plan_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_profile_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_failed_query_count": 0,
            "query_runtime_missing_plan_evidence_count": 0,
            "query_runtime_missing_profile_evidence_count": 0,
            "relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_report_count": 0,
            "missing_query_runtime_routes": [],
            "route_query_runtime_ready": true,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_relationship_property_pruning_evidence_ready": true,
            "route_primary_ready": true,
            "route_primary_blocker_codes": [],
            "route_catalog": nowledge_mem_graph_read_route_specs_json(),
            "routes": routes
        })
    }

    fn ready_query_runtime_preflight_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "route": route,
            "ready": true,
            "success": true,
            "selected_plan_fingerprint": format!("fixture:{route}"),
            "physical_operator_count": 2,
            "physical_operator_class_count": 2,
            "optimizer_decision_count": 1,
            "plan_cache_bypassed": false,
            "scan_pruning": {
                "ready": true,
                "report_count": 1
            }
        })
    }

    fn blocked_bundle() -> serde_json::Value {
        serde_json::json!({
            "inventory_gate": {
                "coverage_per_million": 1_000_000,
                "blockers": ["inventory blocked"]
            },
            "cutover": {
                "decision": "blocked",
                "matched_per_million": 500_000,
                "blockers": ["primary-only projected graph"]
            },
            "migration_gate": {
                "decision": "blocked",
                "blockers": ["shadow parity blocked"]
            },
            "cutover_evidence": {
                "eligible": false,
                "storage_recovery_required": true,
                "storage_recovery_ready": false,
                "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
                "storage_recovery_blockers": [
                    "WAL replay was not opened with a configured entry bound"
                ],
                "background_maintenance_required": true,
                "background_maintenance_ready": false,
                "background_maintenance_blocker_codes": ["missing_evidence"],
                "background_maintenance_blockers": [
                    "background maintenance summary is missing"
                ],
                "replacement_readiness_min_per_million": 500_000,
                "replacement_readiness_invalid_family_count": 0,
                "replacement_readiness_blockers": [],
                "blockers": []
            },
            "replacement_readiness_per_million": 500_000,
            "replacement_readiness_by_query_family": []
        })
    }
}
