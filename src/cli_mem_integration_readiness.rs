use skein::{
    Result, SkeinError, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL: &str =
    "nowledge-mem-skein-integration-bundle";
const SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL: &str = "skein-nowledge-replacement-summary";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-cli";
const SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
const SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
const SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/skein-shadow/candidate-evidence";
const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
const SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const NMEM_GRAPH_ROUTE_READINESS_PROTOCOL: &str = "nmem-graph-route-readiness-v1";
const NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-evidence-v1";
const SKEIN_NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";

pub fn nowledge_mem_integration_readiness_usage() -> String {
    "nowledge-mem-integration-readiness requires [--require-ready] <integration-bundle-json>"
        .to_string()
}

pub fn run_nowledge_mem_integration_readiness(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_mem_integration_readiness_usage(),
                    ));
                }
                let bundle = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_mem_integration_readiness_json(&bundle),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_mem_integration_readiness_usage(),
    ))
}

pub fn nowledge_mem_integration_readiness_json(bundle: &serde_json::Value) -> serde_json::Value {
    let checks = vec![
        check(
            "integration_bundle_protocol",
            [str_path(bundle, &["protocol"]) == Some(NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL)],
            ["protocol"],
            Vec::new(),
        ),
        check(
            "replacement_summary_protocol",
            [str_path(bundle, &["replacement_summary", "protocol"])
                == Some(SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL)],
            ["replacement_summary.protocol"],
            Vec::new(),
        ),
        check(
            "skein_submodule",
            [
                bool_path(bundle, &["submodule", "present"]) == Some(true),
                non_empty_str_path(bundle, &["submodule", "path"]),
                non_empty_str_path(bundle, &["submodule", "commit"]),
            ],
            [
                "submodule.present",
                "submodule.path",
                "submodule.commit",
            ],
            blocker_codes(bundle, &[&["submodule", "blocker_codes"][..]]),
        ),
        check(
            "legacy_coexistence",
            [
                bool_path(bundle, &["coexistence", "old_database_retained"]) == Some(true),
                coexistence_mode_is_safe(bundle),
                bool_path(bundle, &["coexistence", "old_database_deleted"]) != Some(true),
            ],
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
            blocker_codes(bundle, &[&["coexistence", "blocker_codes"][..]]),
        ),
        check(
            "content_store_boundary",
            [
                bool_path(bundle, &["content_store", "present"]) == Some(true),
                str_path(bundle, &["content_store", "engine"]) == Some("sqlite"),
                bool_path(bundle, &["content_store", "messages_available"]) == Some(true),
                bool_path(bundle, &["content_store", "source_chunks_available"]) == Some(true),
            ],
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
            blocker_codes(bundle, &[&["content_store", "blocker_codes"][..]]),
        ),
        check(
            "previous_wrapper_preflight",
            [bool_path(bundle, &["previous_wrapper_preflight", "ready"]) == Some(true)],
            ["previous_wrapper_preflight.ready"],
            blocker_codes(
                bundle,
                &[
                    &["previous_wrapper_preflight", "blocker_codes"][..],
                    &["previous_wrapper_preflight", "failed_checks"][..],
                ],
            ),
        ),
        check(
            "graph_replacement_evidence",
            [
                bool_path(bundle, &["replacement_summary", "production_cutover_ready"])
                    == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "present"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "consistent"],
                ) == Some(true),
            ],
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.shadow_evidence.ready",
                "replacement_summary.dual_engine_evidence.present",
                "replacement_summary.dual_engine_evidence.ready",
                "replacement_summary.dual_engine_evidence.consistent",
            ],
            blocker_codes(
                bundle,
                &[
                    &["replacement_summary", "blocking_categories"][..],
                    &["replacement_summary", "missing_evidence"][..],
                    &["replacement_summary", "dual_engine_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "query_family_replacement_evidence",
            [
                replacement_summary_required_query_families_present(bundle),
                string_array_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "missing_required_query_families",
                    ],
                )
                .is_empty(),
                string_array_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "blocked_query_families",
                    ],
                )
                .is_empty(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "min_replacement_readiness_per_million",
                    ],
                ) == Some(1_000_000),
            ],
            [
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families",
                "replacement_summary.replacement_readiness_family_summary.blocked_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million",
            ],
            blocker_codes(
                bundle,
                &[
                    &["replacement_summary", "blocking_categories"][..],
                    &["replacement_summary", "missing_evidence"][..],
                ],
            ),
        ),
        check(
            "search_projection_replacement_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "search_projection_evidence", "ready"],
                ) == Some(true),
                str_path(
                    bundle,
                    &["replacement_summary", "search_projection_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "fts_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "vector_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "incremental_update_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "predicate_pushdown_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "compressed_vector_projection_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "compressed_vector_projection_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "present",
                    ],
                ) == Some(true),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "protocol",
                    ],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "evidence_source",
                    ],
                ) == Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "document_count_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "table_parity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "embedding_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "incremental_watermark_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_segment_descriptor_scan_filter_fields_ready",
                    ],
                ) == Some(true),
                search_projection_scan_filter_fields_cover_required(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "primary_scan_filter_fields",
                    ],
                ),
                search_projection_scan_filter_fields_cover_required(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_scan_filter_fields",
                    ],
                ),
                search_projection_segment_descriptor_summaries_cover_required(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "pushdown_evidence",
                        "shadow_segment_descriptor_field_summaries",
                    ],
                ),
            ],
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_evidence.fts_ready",
                "replacement_summary.search_projection_evidence.vector_ready",
                "replacement_summary.search_projection_evidence.incremental_update_ready",
                "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
                "replacement_summary.search_projection_shadow_evidence.present",
                "replacement_summary.search_projection_shadow_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.evidence_source",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity",
                "replacement_summary.search_projection_shadow_evidence.table_parity_ready",
                "replacement_summary.search_projection_shadow_evidence.embedding_identity_parity",
                "replacement_summary.search_projection_shadow_evidence.incremental_watermark_parity",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "blocker_codes",
                    ][..],
                ],
            ),
        ),
        check(
            "search_candidate_primary_evidence",
            [
                str_path(bundle, &["search_candidate_shadow_evidence", "protocol"])
                    == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL),
                str_path(bundle, &["search_candidate_shadow_evidence", "evidence_source"])
                    == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_SOURCE),
                str_path(bundle, &["search_candidate_shadow_evidence", "route"])
                    == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_ROUTE),
                bool_path(bundle, &["search_candidate_shadow_evidence", "ready"]) == Some(true),
                str_path(
                    bundle,
                    &["search_candidate_shadow_evidence", "candidate_primary_engine"],
                ) == Some("skein"),
            ],
            [
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
            ],
            blocker_codes(
                bundle,
                &[&["search_candidate_shadow_evidence", "blocker_codes"][..]],
            ),
        ),
        check(
            "bounded_read_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "present"],
                ) == Some(true),
                str_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "ready"],
                ) == Some(true),
                u64_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "max_rows"],
                )
                .is_some_and(|value| value > 0),
                str_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "mode"],
                ) == Some("shadow_read_only"),
                bounded_read_execution_cap_matches(bundle),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "row_limit_enforced_before_output",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "operator_row_cap_enabled",
                    ],
                ) == Some(true),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "blocking_operator_count",
                    ],
                ) == Some(0),
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "streaming"],
                ) == Some(false),
                bounded_read_route_coverage_ready(bundle),
            ],
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.covered_routes",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "bounded_read_evidence",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "bounded_read_evidence_alignment",
            [
                bool_path(bundle, &["bounded_read_evidence", "ready"]) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "evidence_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "summary_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "protocol_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "readiness_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "mode_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "max_rows_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "streaming_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_bounded_read_alignment",
                        "covered_routes_matches",
                    ],
                ) == Some(true),
            ],
            [
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.summary_ready",
                "replacement_summary_bounded_read_alignment.protocol_matches",
                "replacement_summary_bounded_read_alignment.readiness_matches",
                "replacement_summary_bounded_read_alignment.mode_matches",
                "replacement_summary_bounded_read_alignment.max_rows_matches",
                "replacement_summary_bounded_read_alignment.streaming_matches",
                "replacement_summary_bounded_read_alignment.covered_routes_matches",
            ],
            blocker_codes(
                bundle,
                &[
                    &["bounded_read_evidence", "blocker_codes"][..],
                    &["replacement_summary_bounded_read_alignment", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "graph_route_readiness",
            [
                str_path(bundle, &["graph_route_readiness", "protocol"])
                    == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL),
                str_path(bundle, &["graph_route_readiness", "evidence_protocol"])
                    == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL),
                bool_path(bundle, &["graph_route_readiness", "evidence_ready"]) == Some(true),
                u64_path(bundle, &["graph_route_readiness", "route_count"])
                    .is_some_and(|value| value > 0),
                u64_path(bundle, &["graph_route_readiness", "required_route_count"])
                    == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64),
                graph_route_readiness_route_coverage_ready(bundle),
                string_array_path_is_empty(
                    bundle,
                    &["graph_route_readiness", "missing_required_routes"],
                ),
                u64_path(bundle, &["graph_route_readiness", "query_runtime_route_count"])
                    == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64),
                u64_path(bundle, &["graph_route_readiness", "query_runtime_report_count"])
                    .is_some_and(|value| value >= REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64),
                string_array_path_is_empty(
                    bundle,
                    &["graph_route_readiness", "missing_query_runtime_routes"],
                ),
                bool_path(bundle, &["graph_route_readiness", "route_query_runtime_ready"])
                    == Some(true),
                bool_path(bundle, &["graph_route_readiness", "route_primary_ready"]) == Some(true),
                graph_route_primary_ready_count_matches(bundle),
                string_array_path(bundle, &["graph_route_readiness", "route_primary_blocker_codes"])
                    .is_empty(),
                bool_path(bundle, &["graph_route_readiness", "evidence_route_coverage_present"])
                    == Some(true),
                bool_path(bundle, &["graph_route_readiness", "evidence_route_coverage_matches"])
                    == Some(true),
                string_array_path(
                    bundle,
                    &[
                        "graph_route_readiness",
                        "evidence_route_coverage_blocker_codes",
                    ],
                )
                .is_empty(),
                graph_route_query_profiles_ready(bundle),
            ],
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_count",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.route_coverage",
                "graph_route_readiness.missing_required_routes",
                "graph_route_readiness.query_runtime_route_count",
                "graph_route_readiness.query_runtime_report_count",
                "graph_route_readiness.missing_query_runtime_routes",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.evidence_route_coverage_blocker_codes",
                "graph_route_readiness.routes",
            ],
            blocker_codes(
                bundle,
                &[
                    &["graph_route_readiness", "blocker_codes"][..],
                    &["graph_route_readiness", "route_primary_blocker_codes"][..],
                    &[
                        "graph_route_readiness",
                        "evidence_route_coverage_blocker_codes",
                    ][..],
                ],
            ),
        ),
        check(
            "graph_route_readiness_alignment",
            [
                bool_path(
                    bundle,
                    &["replacement_summary_graph_route_alignment", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "evidence_protocol_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_graph_route_alignment", "evidence_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "evidence_route_primary_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "summary_route_primary_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "route_primary_ready_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "primary_ready_routes_match",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "evidence_required_routes_covered",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_graph_route_alignment",
                        "summary_required_routes_covered",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.evidence_protocol_matches",
                "replacement_summary_graph_route_alignment.evidence_ready",
                "replacement_summary_graph_route_alignment.evidence_route_primary_ready",
                "replacement_summary_graph_route_alignment.summary_route_primary_ready",
                "replacement_summary_graph_route_alignment.route_primary_ready_matches",
                "replacement_summary_graph_route_alignment.primary_ready_routes_match",
                "replacement_summary_graph_route_alignment.evidence_required_routes_covered",
                "replacement_summary_graph_route_alignment.summary_required_routes_covered",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary_graph_route_alignment",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "graph_route_parity_alignment",
            [
                bool_path(bundle, &["graph_route_parity_alignment", "ready"]) == Some(true),
                u64_path(bundle, &["graph_route_parity_alignment", "required_route_count"])
                    .is_some_and(|value| value > 0),
                u64_path(bundle, &["graph_route_parity_alignment", "ready_route_count"])
                    == u64_path(bundle, &["graph_route_parity_alignment", "required_route_count"]),
                string_array_path(bundle, &["graph_route_parity_alignment", "missing_routes"])
                    .is_empty(),
                string_array_path(bundle, &["graph_route_parity_alignment", "not_ready_routes"])
                    .is_empty(),
                string_array_path(
                    bundle,
                    &["graph_route_parity_alignment", "route_mismatch_routes"],
                )
                .is_empty(),
                string_array_path(
                    bundle,
                    &["graph_route_parity_alignment", "protocol_mismatch_routes"],
                )
                .is_empty(),
                string_array_path(bundle, &["graph_route_parity_alignment", "blocker_routes"])
                    .is_empty(),
            ],
            [
                "graph_route_parity_alignment.ready",
                "graph_route_parity_alignment.required_route_count",
                "graph_route_parity_alignment.ready_route_count",
                "graph_route_parity_alignment.missing_routes",
                "graph_route_parity_alignment.not_ready_routes",
                "graph_route_parity_alignment.route_mismatch_routes",
                "graph_route_parity_alignment.protocol_mismatch_routes",
                "graph_route_parity_alignment.blocker_routes",
            ],
            blocker_codes(
                bundle,
                &[&["graph_route_parity_alignment", "blocker_codes"][..]],
            ),
        ),
        check(
            "query_runtime_preflight",
            [
                str_path(bundle, &["query_runtime_preflight", "protocol"])
                    == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL),
                bool_path(bundle, &["query_runtime_preflight", "ready"]) == Some(true),
                bool_path(bundle, &["query_runtime_preflight", "database_opened"]) == Some(true),
                u64_path(bundle, &["query_runtime_preflight", "probe_count"])
                    .is_some_and(|value| value > 0),
                query_runtime_preflight_counts_match(bundle),
                u64_path(bundle, &["query_runtime_preflight", "failed_probe_count"]) == Some(0),
                query_runtime_preflight_route_coverage_ready(bundle),
                query_runtime_preflight_probe_details_ready(bundle),
            ],
            [
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.probes",
            ],
            blocker_codes(
                bundle,
                &[
                    &["query_runtime_preflight", "blocker_codes"][..],
                    &["query_runtime_preflight", "failed_checks"][..],
                    &["query_runtime_preflight", "route_coverage_blocker_codes"][..],
                ],
            ),
        ),
        check(
            "query_runtime_preflight_alignment",
            [
                bool_path(
                    bundle,
                    &["replacement_summary_query_runtime_alignment", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "evidence_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_query_runtime_alignment", "summary_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "protocol_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "readiness_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "database_opened_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "probe_count_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "passed_probe_count_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "failed_probe_count_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "required_route_count_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "covered_route_count_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "covered_routes_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "required_routes_covered_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_query_runtime_alignment",
                        "route_coverage_ready_matches",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.evidence_ready",
                "replacement_summary_query_runtime_alignment.summary_ready",
                "replacement_summary_query_runtime_alignment.protocol_matches",
                "replacement_summary_query_runtime_alignment.readiness_matches",
                "replacement_summary_query_runtime_alignment.database_opened_matches",
                "replacement_summary_query_runtime_alignment.probe_count_matches",
                "replacement_summary_query_runtime_alignment.passed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.failed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.required_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_routes_matches",
                "replacement_summary_query_runtime_alignment.required_routes_covered_matches",
                "replacement_summary_query_runtime_alignment.route_coverage_ready_matches",
            ],
            blocker_codes(
                bundle,
                &[
                    &["query_runtime_preflight", "blocker_codes"][..],
                    &[
                        "replacement_summary",
                        "query_runtime_preflight",
                        "blocker_codes",
                    ][..],
                    &["replacement_summary_query_runtime_alignment", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "library_readiness",
            [
                str_path(bundle, &["library_readiness", "protocol"])
                    == Some(NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL),
                bool_path(bundle, &["library_readiness", "present"]) == Some(true),
                bool_path(bundle, &["library_readiness", "ready"]) == Some(true),
                u64_path(bundle, &["library_readiness", "ready_area_count"])
                    .is_some_and(|value| value > 0),
                u64_path(bundle, &["library_readiness", "blocked_area_count"]) == Some(0),
                bool_path(bundle, &["library_readiness", "open_report", "graph_opened"])
                    == Some(true),
                bool_path(
                    bundle,
                    &["library_readiness", "open_report", "search_projection_opened"],
                ) == Some(true),
                library_readiness_area_ready(bundle, "graph"),
                library_readiness_area_ready(bundle, "query"),
                library_readiness_area_ready(bundle, "storage"),
                library_readiness_area_ready(bundle, "background"),
                library_readiness_area_ready(bundle, "query_family"),
                library_readiness_area_ready(bundle, "search_projection"),
                library_readiness_area_ready(bundle, "search_projection_shadow"),
            ],
            [
                "library_readiness.protocol",
                "library_readiness.present",
                "library_readiness.ready",
                "library_readiness.ready_area_count",
                "library_readiness.blocked_area_count",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area.graph.ready",
                "library_readiness.readiness_by_area.query.ready",
                "library_readiness.readiness_by_area.storage.ready",
                "library_readiness.readiness_by_area.background.ready",
                "library_readiness.readiness_by_area.query_family.ready",
                "library_readiness.readiness_by_area.search_projection.ready",
                "library_readiness.readiness_by_area.search_projection_shadow.ready",
            ],
            blocker_codes(
                bundle,
                &[
                    &["library_readiness", "blocker_codes"][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "graph",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "query",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "storage",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "background",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "query_family",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "search_projection",
                        "blocker_codes",
                    ][..],
                    &[
                        "library_readiness",
                        "readiness_by_area",
                        "search_projection_shadow",
                        "blocker_codes",
                    ][..],
                ],
            ),
        ),
        check(
            "background_maintenance_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_protocol_matches",
                    ],
                ) == Some(true),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_deferred_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_rejected_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                    ],
                )
                .is_some(),
            ],
            [
                "replacement_summary.cutover_evidence.background_maintenance_required",
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_blockers",
                    ][..],
                ],
            ),
        ),
        check(
            "storage_recovery_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_protocol_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_durable"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_checkpoint_boundary_present",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_wal_replay_bounded",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_torn_tail_clean",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
            ],
            blocker_codes(
                bundle,
                &[
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_blocker_codes",
                    ][..],
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_blockers",
                    ][..],
                ],
            ),
        ),
    ];
    let ready = checks
        .iter()
        .all(|check| bool_path(check, &["ready"]) == Some(true));
    let failed_checks = checks
        .iter()
        .filter(|check| bool_path(check, &["ready"]) != Some(true))
        .filter_map(|check| str_path(check, &["name"]))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let blocker_codes = checks
        .iter()
        .flat_map(|check| string_array_path(check, &["blocker_codes"]))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "skein-nowledge-mem-integration-readiness",
        "ready": ready,
        "failed_checks": failed_checks,
        "checks": checks,
        "blocker_codes": blocker_codes,
        "next_actions": next_actions(bundle, ready),
    })
}

fn check(
    name: &'static str,
    conditions: impl IntoIterator<Item = bool>,
    evidence_fields: impl IntoIterator<Item = &'static str>,
    blocker_codes: Vec<String>,
) -> serde_json::Value {
    let conditions = conditions.into_iter().collect::<Vec<_>>();
    let evidence_fields = evidence_fields.into_iter().collect::<Vec<_>>();
    let failed_evidence_fields = conditions
        .iter()
        .zip(evidence_fields.iter())
        .filter_map(|(condition, field)| (!*condition).then_some(*field))
        .collect::<Vec<_>>();
    serde_json::json!({
        "name": name,
        "ready": failed_evidence_fields.is_empty(),
        "evidence_fields": evidence_fields,
        "failed_evidence_fields": failed_evidence_fields,
        "blocker_codes": blocker_codes,
    })
}

fn next_actions(bundle: &serde_json::Value, ready: bool) -> Vec<serde_json::Value> {
    if ready {
        return Vec::new();
    }
    let mut actions = Vec::new();
    if str_path(bundle, &["protocol"]) != Some(NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL) {
        actions.push(next_action(
            "regenerate_skein_integration_bundle",
            "Nowledge Mem integration readiness requires the versioned integration bundle protocol",
            ["protocol"],
        ));
    }
    if bool_path(bundle, &["submodule", "present"]) != Some(true) {
        actions.push(next_action(
            "add_skein_submodule",
            "Nowledge Mem must depend on Skein as a submodule instead of copying sources",
            ["submodule.present", "submodule.path", "submodule.commit"],
        ));
    }
    if bool_path(bundle, &["coexistence", "old_database_retained"]) != Some(true)
        || !coexistence_mode_is_safe(bundle)
        || bool_path(bundle, &["coexistence", "old_database_deleted"]) == Some(true)
    {
        actions.push(next_action(
            "enable_side_by_side_coexistence",
            "Kuzu/Ladybug and LanceDB must remain available while Skein runs in shadow",
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
        ));
    }
    if bool_path(bundle, &["content_store", "present"]) != Some(true) {
        actions.push(next_action(
            "attach_content_store_evidence",
            "messages and source chunks still come from content.db during replacement validation",
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
        ));
    }
    if bool_path(bundle, &["previous_wrapper_preflight", "ready"]) != Some(true) {
        actions.push(next_action(
            "run_previous_wrapper_preflight",
            "the previous-wrapper release bundle must pass before Mem cutover",
            ["previous_wrapper_preflight.ready"],
        ));
    }
    if bool_path(bundle, &["replacement_summary", "production_cutover_ready"]) != Some(true) {
        actions.push(next_action(
            "produce_replacement_summary",
            "graph and search replacement evidence must be production-ready",
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.blocking_categories",
                "replacement_summary.missing_evidence",
            ],
        ));
    }
    if str_path(bundle, &["replacement_summary", "protocol"])
        != Some(SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL)
    {
        actions.push(next_action(
            "produce_replacement_summary",
            "replacement summary must use the Skein Nowledge replacement-summary protocol",
            ["replacement_summary.protocol"],
        ));
    }
    if !replacement_summary_required_query_families_present(bundle)
        || !string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "missing_required_query_families",
            ],
        )
        .is_empty()
        || !string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "blocked_query_families",
            ],
        )
        .is_empty()
        || u64_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "min_replacement_readiness_per_million",
            ],
        ) != Some(1_000_000)
    {
        actions.push(next_action(
            "close_required_query_families",
            "Nowledge Mem cutover requires explicit readiness for every required query family",
            [
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families",
                "replacement_summary.replacement_readiness_family_summary.blocked_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million",
            ],
        ));
    }
    if !replacement_summary_search_projection_ready(bundle) {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence must prove FTS, vector, incremental, predicate pushdown, compressed projection, and shadow parity",
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_evidence.fts_ready",
                "replacement_summary.search_projection_evidence.vector_ready",
                "replacement_summary.search_projection_evidence.incremental_update_ready",
                "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
                "replacement_summary.search_projection_shadow_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.evidence_source",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !search_candidate_primary_evidence_ready(bundle) {
        actions.push(next_action(
            "enable_skein_search_candidate_primary_reads",
            "LanceDB replacement must prove memory-hybrid candidate reads are served by Skein before Mem cutover",
            [
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !replacement_summary_bounded_read_ready(bundle) {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "Skein read replacement must prove bounded execution before Mem cutover",
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !bounded_read_alignment_ready(bundle) {
        actions.push(next_action(
            "regenerate_bounded_read_alignment",
            "live bounded-read evidence must match the replacement summary before Mem cutover",
            [
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.summary_ready",
                "replacement_summary_bounded_read_alignment.blocker_codes",
            ],
        ));
    }
    if !graph_route_readiness_ready(bundle) {
        actions.push(next_action(
            "attach_graph_route_readiness_evidence",
            "Nowledge Mem graph cutover requires route-level primary-read readiness evidence",
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_count",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.route_coverage",
                "graph_route_readiness.missing_required_routes",
                "graph_route_readiness.query_runtime_route_count",
                "graph_route_readiness.query_runtime_report_count",
                "graph_route_readiness.missing_query_runtime_routes",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.evidence_route_coverage_blocker_codes",
                "graph_route_readiness.routes",
            ],
        ));
    }
    if !graph_route_readiness_alignment_ready(bundle) {
        actions.push(next_action(
            "regenerate_graph_route_readiness_alignment",
            "live graph route primary-read readiness must match the replacement summary before Mem cutover",
            [
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.evidence_protocol_matches",
                "replacement_summary_graph_route_alignment.evidence_ready",
                "replacement_summary_graph_route_alignment.evidence_route_primary_ready",
                "replacement_summary_graph_route_alignment.summary_route_primary_ready",
                "replacement_summary_graph_route_alignment.route_primary_ready_matches",
                "replacement_summary_graph_route_alignment.primary_ready_routes_match",
                "replacement_summary_graph_route_alignment.blocker_codes",
            ],
        ));
    }
    if !graph_route_parity_alignment_ready(bundle) {
        actions.push(next_action(
            "attach_graph_route_parity_evidence",
            "Nowledge Mem graph cutover requires route-level shadow parity evidence for graph reads",
            [
                "graph_route_parity_alignment.ready",
                "graph_route_parity_alignment.required_route_count",
                "graph_route_parity_alignment.ready_route_count",
                "graph_route_parity_alignment.missing_routes",
                "graph_route_parity_alignment.not_ready_routes",
                "graph_route_parity_alignment.route_mismatch_routes",
                "graph_route_parity_alignment.protocol_mismatch_routes",
                "graph_route_parity_alignment.blocker_routes",
            ],
        ));
    }
    if !query_runtime_preflight_ready(bundle) {
        actions.push(next_action(
            "attach_query_runtime_preflight_evidence",
            "Nowledge Mem cutover requires read-only query runtime EXPLAIN ANALYZE preflight evidence",
            [
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.blocker_codes",
                "query_runtime_preflight.probes",
            ],
        ));
    }
    if !query_runtime_preflight_alignment_ready(bundle) {
        actions.push(next_action(
            "regenerate_query_runtime_preflight_alignment",
            "live query runtime preflight evidence must match the replacement summary before Mem cutover",
            [
                "query_runtime_preflight.ready",
                "replacement_summary.query_runtime_preflight.ready",
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.evidence_ready",
                "replacement_summary_query_runtime_alignment.summary_ready",
                "replacement_summary_query_runtime_alignment.covered_routes_matches",
                "replacement_summary_query_runtime_alignment.blocker_codes",
            ],
        ));
    }
    if !library_readiness_ready(bundle) {
        actions.push(next_action(
            "attach_library_readiness_evidence",
            "Nowledge Mem cutover requires the Skein Rust library to open graph, search projection, and required evidence areas",
            [
                "library_readiness.protocol",
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area",
            ],
        ));
    }
    if !replacement_summary_storage_recovery_ready(bundle) {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "storage recovery evidence must prove durable bounded WAL replay before Mem cutover",
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
                "replacement_summary.cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if !replacement_summary_background_maintenance_ready(bundle) {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "background maintenance QoS and search-projection graph-delta evidence must be ready before Mem cutover",
            [
                "replacement_summary.cutover_evidence.background_maintenance_required",
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                "replacement_summary.cutover_evidence.background_maintenance_blocker_codes",
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

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read Nowledge Mem integration bundle: {error}",
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse Nowledge Mem integration bundle: {error}",
        ))
    })
}

fn coexistence_mode_is_safe(bundle: &serde_json::Value) -> bool {
    matches!(
        str_path(bundle, &["coexistence", "mode"]),
        Some("shadow") | Some("side_by_side")
    )
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    paths
        .iter()
        .flat_map(|path| string_array_path(value, path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn replacement_summary_required_query_families_present(bundle: &serde_json::Value) -> bool {
    let families = string_array_path(
        bundle,
        &[
            "replacement_summary",
            "replacement_readiness_family_summary",
            "required_query_families",
        ],
    );
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
        .iter()
        .all(|required| families.iter().any(|family| family == required))
}

fn replacement_summary_search_projection_ready(bundle: &serde_json::Value) -> bool {
    if str_path(
        bundle,
        &[
            "replacement_summary",
            "search_projection_evidence",
            "protocol",
        ],
    ) != Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL)
        || str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "protocol",
            ],
        ) != Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
        || str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "evidence_source",
            ],
        ) != Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
    {
        return false;
    }
    [
        &["replacement_summary", "search_projection_evidence", "ready"][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "fts_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "vector_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "incremental_update_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "predicate_pushdown_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "compressed_vector_projection_required",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "compressed_vector_projection_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_shadow_evidence",
            "ready",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
}

fn search_candidate_primary_evidence_ready(bundle: &serde_json::Value) -> bool {
    str_path(bundle, &["search_candidate_shadow_evidence", "protocol"])
        == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
        && str_path(
            bundle,
            &["search_candidate_shadow_evidence", "evidence_source"],
        ) == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
        && str_path(bundle, &["search_candidate_shadow_evidence", "route"])
            == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_EVIDENCE_ROUTE)
        && bool_path(bundle, &["search_candidate_shadow_evidence", "ready"]) == Some(true)
        && str_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_primary_engine",
            ],
        ) == Some("skein")
}

fn replacement_summary_bounded_read_ready(bundle: &serde_json::Value) -> bool {
    bool_path(
        bundle,
        &["replacement_summary", "bounded_read_evidence", "present"],
    ) == Some(true)
        && str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "protocol"],
        ) == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL)
        && bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "ready"],
        ) == Some(true)
        && u64_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "max_rows"],
        )
        .is_some_and(|value| value > 0)
        && str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "mode"],
        ) == Some("shadow_read_only")
        && bounded_read_execution_cap_matches(bundle)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "row_limit_enforced_before_output",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "operator_row_cap_enabled",
            ],
        ) == Some(true)
        && u64_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "blocking_operator_count",
            ],
        ) == Some(0)
        && bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "streaming"],
        ) == Some(false)
        && bounded_read_route_coverage_ready(bundle)
}

fn bounded_read_alignment_ready(bundle: &serde_json::Value) -> bool {
    bool_path(bundle, &["bounded_read_evidence", "ready"]) == Some(true)
        && [
            &["replacement_summary_bounded_read_alignment", "ready"][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "evidence_ready",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "summary_ready",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "protocol_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "readiness_matches",
            ][..],
            &["replacement_summary_bounded_read_alignment", "mode_matches"][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "max_rows_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "streaming_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "covered_routes_matches",
            ][..],
        ]
        .iter()
        .all(|path| bool_path(bundle, path) == Some(true))
}

fn bounded_read_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    let covered_routes = string_array_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "covered_routes",
        ],
    );
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| covered_routes.iter().any(|covered| covered == route))
        && string_array_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "missing_covered_routes",
            ],
        )
        .is_empty()
}

fn graph_route_readiness_ready(bundle: &serde_json::Value) -> bool {
    str_path(bundle, &["graph_route_readiness", "protocol"])
        == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL)
        && str_path(bundle, &["graph_route_readiness", "evidence_protocol"])
            == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL)
        && bool_path(bundle, &["graph_route_readiness", "evidence_ready"]) == Some(true)
        && u64_path(bundle, &["graph_route_readiness", "route_count"])
            .is_some_and(|value| value > 0)
        && u64_path(bundle, &["graph_route_readiness", "required_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && graph_route_readiness_route_coverage_ready(bundle)
        && string_array_path_is_empty(
            bundle,
            &["graph_route_readiness", "missing_required_routes"],
        )
        && u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_route_count"],
        ) == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_report_count"],
        )
        .is_some_and(|value| value >= REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && string_array_path_is_empty(
            bundle,
            &["graph_route_readiness", "missing_query_runtime_routes"],
        )
        && bool_path(
            bundle,
            &["graph_route_readiness", "route_query_runtime_ready"],
        ) == Some(true)
        && bool_path(bundle, &["graph_route_readiness", "route_primary_ready"]) == Some(true)
        && graph_route_primary_ready_count_matches(bundle)
        && string_array_path(
            bundle,
            &["graph_route_readiness", "route_primary_blocker_codes"],
        )
        .is_empty()
        && bool_path(
            bundle,
            &["graph_route_readiness", "evidence_route_coverage_present"],
        ) == Some(true)
        && bool_path(
            bundle,
            &["graph_route_readiness", "evidence_route_coverage_matches"],
        ) == Some(true)
        && string_array_path(
            bundle,
            &[
                "graph_route_readiness",
                "evidence_route_coverage_blocker_codes",
            ],
        )
        .is_empty()
        && graph_route_query_profiles_ready(bundle)
}

fn graph_route_readiness_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    let Some(value) = json_get_path(bundle, &["graph_route_readiness"]) else {
        return false;
    };
    let covered_routes = string_array_path(value, &["covered_routes"]);
    covered_routes.len() == REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        && REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .all(|route| covered_routes.iter().any(|covered| covered == route))
        && u64_path(value, &["covered_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && bool_path(value, &["required_routes_covered"]) == Some(true)
        && string_array_path(value, &["missing_required_routes"]).is_empty()
        && string_array_path(value, &["unknown_routes"]).is_empty()
        && string_array_path(value, &["duplicate_routes"]).is_empty()
        && bool_path(value, &["route_coverage_ready"]) == Some(true)
        && string_array_path(value, &["route_coverage_blocker_codes"]).is_empty()
}

fn graph_route_primary_ready_count_matches(bundle: &serde_json::Value) -> bool {
    let route_count = u64_path(bundle, &["graph_route_readiness", "route_count"]);
    let primary_ready_route_count = u64_path(
        bundle,
        &["graph_route_readiness", "primary_ready_route_count"],
    );
    route_count.is_some_and(|value| value > 0) && route_count == primary_ready_route_count
}

fn graph_route_query_profiles_ready(bundle: &serde_json::Value) -> bool {
    let Some(routes) = json_get_path(bundle, &["graph_route_readiness", "routes"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    if routes.len() != REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() {
        return false;
    }

    let mut observed_routes = BTreeSet::new();
    for route in routes {
        let Some(route_name) = str_path(route, &["route"]) else {
            return false;
        };
        if !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route_name) {
            return false;
        }
        if !observed_routes.insert(route_name) {
            return false;
        }
        if bool_path(route, &["primary_ready"]) != Some(true)
            || bool_path(route, &["query_runtime_ready"]) != Some(true)
            || bool_path(route, &["shadow_compare_ready"]) != Some(true)
            || str_path(route, &["shadow_compare_evidence_source"]) != Some("route_parity_evidence")
            || u64_path(route, &["query_report_count"]).is_none_or(|value| value == 0)
        {
            return false;
        }
        let Some(query_reports) =
            json_get_path(route, &["query_reports"]).and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        if query_reports.is_empty()
            || u64_path(route, &["query_report_count"]) != Some(query_reports.len() as u64)
            || !query_reports.iter().all(graph_route_query_report_ready)
        {
            return false;
        }
    }

    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| observed_routes.contains(route))
}

fn graph_route_query_report_ready(report: &serde_json::Value) -> bool {
    non_empty_str_path(report, &["query_name"])
        && u64_path(report, &["query_index"]).is_some()
        && str_path(report, &["protocol"]) == Some(SKEIN_NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL)
        && bool_path(report, &["ready"]) == Some(true)
        && string_array_path(report, &["blocker_codes"]).is_empty()
        && non_empty_str_path(report, &["statement_kind"])
        && matches!(
            str_path(report, &["execution_path"]),
            Some("fast_path" | "optimized_path")
        )
        && bool_path(report, &["fast_path_selected"]).is_some()
        && bool_path(report, &["slow_log_candidate"]).is_some()
        && bool_path(report, &["physical_plan_captured"]).is_some()
        && u64_path(report, &["elapsed_micros"]).is_some()
        && graph_route_query_report_physical_operators_present(report)
        && u64_path(report, &["optimizer_decision_count"]).is_some()
        && u64_path(report, &["scan_pruning_report_count"]).is_some()
        && graph_route_query_report_scan_pruning_present(report)
        && non_empty_str_path(report, &["plan_cache", "lookup"])
        && bool_path(report, &["plan_cache", "cacheable"]).is_some()
        && bool_path(report, &["plan_cache", "hit"]).is_some()
        && bool_path(report, &["plan_cache", "miss"]).is_some()
        && bool_path(report, &["plan_cache", "bypassed"]) == Some(false)
}

fn graph_route_query_report_physical_operators_present(report: &serde_json::Value) -> bool {
    bool_path(report, &["physical_operator_counts_present"]) == Some(true)
        || json_get_path(report, &["physical_operator_counts"])
            .is_some_and(serde_json::Value::is_object)
}

fn graph_route_query_report_scan_pruning_present(report: &serde_json::Value) -> bool {
    let Some(report_count) = u64_path(report, &["scan_pruning_report_count"]) else {
        return false;
    };
    let Some(reports) =
        json_get_path(report, &["scan_pruning_reports"]).and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    report_count == reports.len() as u64
        && !reports.is_empty()
        && reports.iter().all(query_runtime_scan_pruning_report_ready)
}

fn graph_route_readiness_alignment_ready(bundle: &serde_json::Value) -> bool {
    [
        &["replacement_summary_graph_route_alignment", "ready"][..],
        &[
            "replacement_summary_graph_route_alignment",
            "evidence_protocol_matches",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "evidence_ready",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "evidence_route_primary_ready",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "summary_route_primary_ready",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "route_primary_ready_matches",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "primary_ready_routes_match",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "evidence_required_routes_covered",
        ][..],
        &[
            "replacement_summary_graph_route_alignment",
            "summary_required_routes_covered",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
}

fn graph_route_parity_alignment_ready(bundle: &serde_json::Value) -> bool {
    bool_path(bundle, &["graph_route_parity_alignment", "ready"]) == Some(true)
        && u64_path(
            bundle,
            &["graph_route_parity_alignment", "required_route_count"],
        )
        .is_some_and(|value| value > 0)
        && u64_path(
            bundle,
            &["graph_route_parity_alignment", "ready_route_count"],
        ) == u64_path(
            bundle,
            &["graph_route_parity_alignment", "required_route_count"],
        )
        && string_array_path(bundle, &["graph_route_parity_alignment", "missing_routes"]).is_empty()
        && string_array_path(
            bundle,
            &["graph_route_parity_alignment", "not_ready_routes"],
        )
        .is_empty()
        && string_array_path(
            bundle,
            &["graph_route_parity_alignment", "route_mismatch_routes"],
        )
        .is_empty()
        && string_array_path(
            bundle,
            &["graph_route_parity_alignment", "protocol_mismatch_routes"],
        )
        .is_empty()
        && string_array_path(bundle, &["graph_route_parity_alignment", "blocker_routes"]).is_empty()
}

fn query_runtime_preflight_ready(bundle: &serde_json::Value) -> bool {
    str_path(bundle, &["query_runtime_preflight", "protocol"])
        == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL)
        && bool_path(bundle, &["query_runtime_preflight", "ready"]) == Some(true)
        && bool_path(bundle, &["query_runtime_preflight", "database_opened"]) == Some(true)
        && u64_path(bundle, &["query_runtime_preflight", "probe_count"])
            .is_some_and(|value| value > 0)
        && query_runtime_preflight_counts_match(bundle)
        && u64_path(bundle, &["query_runtime_preflight", "failed_probe_count"]) == Some(0)
        && query_runtime_preflight_route_coverage_ready(bundle)
        && query_runtime_preflight_probe_details_ready(bundle)
}

fn query_runtime_preflight_counts_match(bundle: &serde_json::Value) -> bool {
    let probe_count = u64_path(bundle, &["query_runtime_preflight", "probe_count"]);
    let passed_probe_count = u64_path(bundle, &["query_runtime_preflight", "passed_probe_count"]);
    probe_count.is_some_and(|value| value > 0) && probe_count == passed_probe_count
}

fn query_runtime_preflight_probe_details_ready(bundle: &serde_json::Value) -> bool {
    let Some(probes) = json_get_path(bundle, &["query_runtime_preflight", "probes"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    !probes.is_empty() && probes.iter().all(query_runtime_preflight_probe_ready)
}

fn query_runtime_preflight_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    let value = match json_get_path(bundle, &["query_runtime_preflight"]) {
        Some(value) => value,
        None => return false,
    };
    let Some(probes) = value
        .get("probes")
        .and_then(serde_json::Value::as_array)
        .filter(|probes| !probes.is_empty())
    else {
        return false;
    };
    let observed_routes = probes
        .iter()
        .filter_map(|probe| str_path(probe, &["route"]))
        .collect::<Vec<_>>();
    let observed_route_set = observed_routes.iter().copied().collect::<BTreeSet<_>>();
    let duplicate_routes = duplicate_routes(&observed_routes);
    let unknown_routes = observed_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(route))
        .count();
    u64_path(value, &["required_route_count"])
        == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && u64_path(value, &["covered_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && bool_path(value, &["required_routes_covered"]) == Some(true)
        && string_array_path(value, &["missing_required_routes"]).is_empty()
        && string_array_path(value, &["unknown_routes"]).is_empty()
        && string_array_path(value, &["duplicate_routes"]).is_empty()
        && bool_path(value, &["route_coverage_ready"]) == Some(true)
        && string_array_path(value, &["route_coverage_blocker_codes"]).is_empty()
        && unknown_routes == 0
        && duplicate_routes.is_empty()
        && REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .all(|route| observed_route_set.contains(route))
}

fn duplicate_routes(routes: &[&str]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(*route).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

fn query_runtime_preflight_probe_ready(probe: &serde_json::Value) -> bool {
    bool_path(probe, &["ready"]) == Some(true)
        && bool_path(probe, &["success"]) == Some(true)
        && non_empty_str_path(probe, &["selected_plan_fingerprint"])
        && u64_path(probe, &["output_row_count"]).is_some()
        && json_object_path_is_non_empty(probe, &["selected_plan_operator_counts"])
        && json_object_path_is_non_empty(probe, &["selected_plan_class_counts"])
        && u64_path(probe, &["optimizer_decision_count"]).is_some()
        && query_runtime_preflight_probe_plan_cache_ready(probe)
        && query_runtime_preflight_probe_scan_pruning_ready(probe)
        && string_array_path(probe, &["blocker_codes"]).is_empty()
}

fn query_runtime_preflight_probe_plan_cache_ready(probe: &serde_json::Value) -> bool {
    non_empty_str_path(probe, &["plan_cache_lookup"])
        && non_empty_str_path(probe, &["plan_cache", "lookup"])
        && bool_path(probe, &["plan_cache", "cacheable"]).is_some()
        && bool_path(probe, &["plan_cache", "hit"]).is_some()
        && bool_path(probe, &["plan_cache", "miss"]).is_some()
        && bool_path(probe, &["plan_cache", "bypassed"]) == Some(false)
}

fn query_runtime_preflight_probe_scan_pruning_ready(probe: &serde_json::Value) -> bool {
    let Some(report_count) = u64_path(probe, &["execution_profile", "scan_pruning_report_count"])
    else {
        return false;
    };
    let Some(reports) = json_get_path(probe, &["execution_profile", "scan_pruning_reports"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    report_count == reports.len() as u64
        && u64_path(probe, &["execution_profile", "pruned_scan_count"]).is_some()
        && reports.iter().all(query_runtime_scan_pruning_report_ready)
}

fn query_runtime_preflight_alignment_ready(bundle: &serde_json::Value) -> bool {
    [
        &["replacement_summary_query_runtime_alignment", "ready"][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "evidence_ready",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "summary_ready",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "protocol_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "readiness_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "database_opened_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "probe_count_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "passed_probe_count_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "failed_probe_count_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "required_route_count_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "covered_route_count_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "covered_routes_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "required_routes_covered_matches",
        ][..],
        &[
            "replacement_summary_query_runtime_alignment",
            "route_coverage_ready_matches",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
}

fn query_runtime_scan_pruning_report_ready(report: &serde_json::Value) -> bool {
    json_get_path(report, &["strategy"]).is_some_and(serde_json::Value::is_object)
        && bool_path(report, &["pruned"]).is_some()
        && bool_path(report, &["exact_empty"]).is_some()
        && u64_path(report, &["candidate_count_before_pruning"]).is_some()
        && u64_path(report, &["pruned_candidate_count"]).is_some()
        && u64_path(report, &["candidate_count_before_filter"]).is_some()
        && u64_path(report, &["output_count"]).is_some()
        && u64_path(report, &["filtered_out_count"]).is_some()
}

fn library_readiness_ready(bundle: &serde_json::Value) -> bool {
    str_path(bundle, &["library_readiness", "protocol"])
        == Some(NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL)
        && bool_path(bundle, &["library_readiness", "present"]) == Some(true)
        && bool_path(bundle, &["library_readiness", "ready"]) == Some(true)
        && u64_path(bundle, &["library_readiness", "ready_area_count"])
            .is_some_and(|value| value > 0)
        && u64_path(bundle, &["library_readiness", "blocked_area_count"]) == Some(0)
        && bool_path(
            bundle,
            &["library_readiness", "open_report", "graph_opened"],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "library_readiness",
                "open_report",
                "search_projection_opened",
            ],
        ) == Some(true)
        && [
            "graph",
            "query",
            "storage",
            "background",
            "query_family",
            "search_projection",
            "search_projection_shadow",
        ]
        .into_iter()
        .all(|area| library_readiness_area_ready(bundle, area))
}

fn library_readiness_area_ready(bundle: &serde_json::Value, area: &str) -> bool {
    json_get_path(
        bundle,
        &["library_readiness", "readiness_by_area", area, "ready"],
    )
    .and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn replacement_summary_storage_recovery_ready(bundle: &serde_json::Value) -> bool {
    [
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_required",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_ready",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_protocol_matches",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_durable",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_checkpoint_boundary_present",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_wal_replay_bounded",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_torn_tail_clean",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
}

fn bounded_read_execution_cap_matches(bundle: &serde_json::Value) -> bool {
    let max_rows = u64_path(
        bundle,
        &["replacement_summary", "bounded_read_evidence", "max_rows"],
    );
    let execution_row_cap = u64_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "execution_row_cap",
        ],
    );
    max_rows.and_then(|value| value.checked_add(1)) == execution_row_cap
}

fn replacement_summary_background_maintenance_ready(bundle: &serde_json::Value) -> bool {
    bool_path(
        bundle,
        &[
            "replacement_summary",
            "cutover_evidence",
            "background_maintenance_required",
        ],
    ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_ready",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_protocol_matches",
            ],
        ) == Some(true)
        && [
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_deferred_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_rejected_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_operations",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_operations",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ][..],
        ]
        .iter()
        .all(|path| u64_path(bundle, path).is_some())
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn string_array_path_is_empty(value: &serde_json::Value, path: &[&str]) -> bool {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn search_projection_scan_filter_fields_cover_required(
    value: &serde_json::Value,
    path: &[&str],
) -> bool {
    let fields = string_array_path(value, path);
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.iter().any(|field| field == required))
}

fn search_projection_segment_descriptor_summaries_cover_required(
    value: &serde_json::Value,
    path: &[&str],
) -> bool {
    let Some(summaries) = json_get_path(value, path).and_then(serde_json::Value::as_array) else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let fields = summaries
        .iter()
        .filter_map(|summary| str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
}

fn non_empty_str_path(value: &serde_json::Value, path: &[&str]) -> bool {
    str_path(value, path).is_some_and(|s| !s.trim().is_empty())
}

fn json_object_path_is_non_empty(value: &serde_json::Value, path: &[&str]) -> bool {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_object)
        .is_some_and(|object| !object.is_empty())
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::nowledge_mem_integration_readiness_json;
    use skein::{
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    };

    #[test]
    fn reports_ready_when_mem_integration_evidence_is_complete() {
        let report = nowledge_mem_integration_readiness_json(&ready_bundle());

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        assert_eq!(report["next_actions"], serde_json::json!([]));
    }

    #[test]
    fn requires_versioned_integration_bundle_protocol() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("protocol");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["integration_bundle_protocol"])
        );
        let protocol_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "integration_bundle_protocol")
            .unwrap();
        assert_eq!(
            protocol_check["failed_evidence_fields"],
            serde_json::json!(["protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_skein_integration_bundle"));
    }

    #[test]
    fn requires_replacement_summary_protocol() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("protocol");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_protocol"])
        );
        let summary_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "replacement_summary_protocol")
            .unwrap();
        assert_eq!(
            summary_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "produce_replacement_summary"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "replacement_summary.protocol")));
    }

    #[test]
    fn fails_closed_without_submodule_and_coexistence() {
        let mut bundle = ready_bundle();
        bundle["submodule"]["present"] = serde_json::json!(false);
        bundle["submodule"]["commit"] = serde_json::json!("");
        bundle["coexistence"]["old_database_retained"] = serde_json::json!(false);
        bundle["coexistence"]["old_database_deleted"] = serde_json::json!(true);
        bundle["coexistence"]["mode"] = serde_json::json!("replace_in_place");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["skein_submodule", "legacy_coexistence"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "add_skein_submodule"));
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "enable_side_by_side_coexistence"));
    }

    #[test]
    fn requires_search_projection_shadow_parity() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["document_count_parity"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["document_count_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["document_count_mismatch"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_search_projection_replacement_evidence"));
    }

    #[test]
    fn requires_skein_search_candidate_primary_engine() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["candidate_primary_engine"] =
            serde_json::json!("lancedb");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.candidate_primary_engine"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "enable_skein_search_candidate_primary_reads"));
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_presence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_candidate_shadow_evidence");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_source() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["evidence_source"] =
            serde_json::json!("manual-json");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.evidence_source"])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_route() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["route"] = serde_json::json!("/manual");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.route"])
        );
    }

    #[test]
    fn requires_search_projection_shadow_descriptor_field_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready"
            ])
        );
    }

    #[test]
    fn recomputes_search_projection_shadow_descriptor_field_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["primary_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_field_summaries"] =
            serde_json::json!([{ "field": "unit_type" }, { "field": "importance" }]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries"
            ])
        );
    }

    #[test]
    fn requires_search_projection_shadow_evidence_source() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["evidence_source"] =
            serde_json::json!("manual-json");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.evidence_source"
            ])
        );
    }

    #[test]
    fn requires_compressed_vector_projection_readiness() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]
            ["compressed_vector_projection_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["compressed_vector_projection_not_ready"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["compressed_vector_projection_not_ready"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready"
            ])
        );
    }

    #[test]
    fn requires_search_projection_evidence_protocols() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]["protocol"] =
            serde_json::json!("handwritten");
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["protocol"] =
            serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.protocol"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_search_projection_replacement_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "replacement_summary.search_projection_evidence.protocol"
                        })
            }));
    }

    #[test]
    fn requires_explicit_required_query_family_readiness() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("replacement_readiness_family_summary");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_family_replacement_evidence"])
        );
        let family_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_family_replacement_evidence")
            .unwrap();
        assert_eq!(
            family_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "close_required_query_families"));
    }

    #[test]
    fn rejects_missing_required_query_family_even_if_cutover_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["replacement_readiness_family_summary"]
            ["missing_required_query_families"] = serde_json::json!(["projected_graph"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_family_replacement_evidence"])
        );
        let family_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_family_replacement_evidence")
            .unwrap();
        assert_eq!(
            family_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families"
            ])
        );
    }

    #[test]
    fn requires_bounded_read_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]
            ["row_limit_enforced_before_output"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["row_cap_not_enforced"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["row_cap_not_enforced"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn requires_bounded_read_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["covered_routes"] =
            serde_json::json!(["/graph/overview"]);
        bundle["replacement_summary"]["bounded_read_evidence"]["missing_covered_routes"] =
            serde_json::json!(["/graph/explore"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.bounded_read_evidence.covered_routes"])
        );
    }

    #[test]
    fn rejects_stale_bounded_read_summary_when_live_evidence_is_not_ready() {
        let mut bundle = ready_bundle();
        bundle["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["skein_shadow_runtime_not_open"]);
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["evidence_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["readiness_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_bounded_read_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "replacement_summary_bounded_read_evidence_mismatch",
                "skein_shadow_runtime_not_open"
            ])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.readiness_matches"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_bounded_read_alignment"));
    }

    #[test]
    fn rejects_stale_bounded_read_summary_when_route_coverage_differs() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_bounded_read_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.covered_routes_matches"
            ])
        );
    }

    #[test]
    fn rejects_inconsistent_bounded_read_summary_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["mode"] =
            serde_json::json!("writable_cutover");
        bundle["replacement_summary"]["bounded_read_evidence"]["execution_row_cap"] =
            serde_json::json!(512);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocking_operator_count"] =
            serde_json::json!(1);
        bundle["replacement_summary"]["bounded_read_evidence"]["streaming"] =
            serde_json::json!(true);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn requires_bounded_read_evidence_protocol() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["protocol"] =
            serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.bounded_read_evidence.protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_bounded_read_profile"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "replacement_summary.bounded_read_evidence.protocol")
            }));
    }

    #[test]
    fn requires_graph_route_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("graph_route_readiness");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.protocol",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_count",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.route_coverage",
                "graph_route_readiness.missing_required_routes",
                "graph_route_readiness.query_runtime_route_count",
                "graph_route_readiness.query_runtime_report_count",
                "graph_route_readiness.missing_query_runtime_routes",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.routes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
    }

    #[test]
    fn rejects_graph_route_readiness_without_route_coverage_envelope() {
        let mut bundle = ready_bundle();
        for field in [
            "covered_route_count",
            "covered_routes",
            "required_routes_covered",
            "unknown_routes",
            "duplicate_routes",
            "route_coverage_ready",
            "route_coverage_blocker_codes",
            "evidence_route_coverage_present",
            "evidence_route_coverage_matches",
            "evidence_route_coverage_blocker_codes",
        ] {
            bundle["graph_route_readiness"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.route_coverage"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.evidence_route_coverage_present"));
    }

    #[test]
    fn rejects_graph_route_readiness_with_stale_route_coverage_envelope() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["covered_routes"] = serde_json::json!(["/graph/overview"]);
        bundle["graph_route_readiness"]["covered_route_count"] = serde_json::json!(1);
        bundle["graph_route_readiness"]["evidence_route_coverage_matches"] =
            serde_json::json!(false);
        bundle["graph_route_readiness"]["evidence_route_coverage_blocker_codes"] =
            serde_json::json!(["route_coverage_evidence_mismatch"]);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["route_coverage_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert!(route_check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_coverage_evidence_mismatch"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.route_coverage"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.evidence_route_coverage_matches"));
    }

    #[test]
    fn requires_query_runtime_preflight_evidence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("query_runtime_preflight");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.probes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_query_runtime_preflight_evidence"));
    }

    #[test]
    fn rejects_weak_query_runtime_preflight_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]
            .as_object_mut()
            .unwrap()
            .remove("selected_plan_fingerprint");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_without_scan_pruning_reports() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]["execution_profile"]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_without_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .pop();

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_with_unknown_route() {
        let mut bundle = ready_bundle();
        let mut probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        probe["route"] = serde_json::json!("/graph/stale-route");
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);
        bundle["query_runtime_preflight"]["unknown_routes"] =
            serde_json::json!(["/graph/stale-route"]);
        bundle["query_runtime_preflight"]["route_coverage_ready"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_unknown_routes"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
        assert!(check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_unknown_routes"));
    }

    #[test]
    fn rejects_query_runtime_preflight_with_duplicate_route() {
        let mut bundle = ready_bundle();
        let probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);
        bundle["query_runtime_preflight"]["duplicate_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]);
        bundle["query_runtime_preflight"]["route_coverage_ready"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_duplicate_routes"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
        assert!(check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_duplicate_routes"));
    }

    #[test]
    fn requires_query_runtime_preflight_alignment() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("replacement_summary_query_runtime_alignment");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight_alignment"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.evidence_ready",
                "replacement_summary_query_runtime_alignment.summary_ready",
                "replacement_summary_query_runtime_alignment.protocol_matches",
                "replacement_summary_query_runtime_alignment.readiness_matches",
                "replacement_summary_query_runtime_alignment.database_opened_matches",
                "replacement_summary_query_runtime_alignment.probe_count_matches",
                "replacement_summary_query_runtime_alignment.passed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.failed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.required_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_routes_matches",
                "replacement_summary_query_runtime_alignment.required_routes_covered_matches",
                "replacement_summary_query_runtime_alignment.route_coverage_ready_matches"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_query_runtime_preflight_alignment"));
    }

    #[test]
    fn rejects_stale_query_runtime_summary_when_route_coverage_differs() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_query_runtime_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["blocker_codes"] =
            serde_json::json!(["query_runtime_preflight_covered_routes_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["query_runtime_preflight_covered_routes_mismatch"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.covered_routes_matches"
            ])
        );
    }

    #[test]
    fn rejects_weak_graph_route_query_profiles_even_if_summary_is_ready() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
    }

    #[test]
    fn rejects_graph_route_query_profiles_with_only_scan_pruning_presence_flag() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            ["scan_pruning_reports_present"] = serde_json::json!(true);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_query_profiles_without_query_identity() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("query_name");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_profiles_without_route_parity_source() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]
            .as_object_mut()
            .unwrap()
            .remove("shadow_compare_evidence_source");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_readiness_without_primary_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["route_primary_ready"] = serde_json::json!(false);
        bundle["graph_route_readiness"]["primary_ready_route_count"] = serde_json::json!(13);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["graph_route_primary_not_enabled"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_primary_not_enabled"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes"
            ])
        );
    }

    #[test]
    fn requires_graph_route_readiness_protocol() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["protocol"] = serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.protocol"])
        );
    }

    #[test]
    fn requires_graph_route_readiness_evidence_protocol() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["evidence_protocol"] = serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.evidence_protocol"])
        );
    }

    #[test]
    fn rejects_graph_route_readiness_when_route_evidence_is_not_ready() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["evidence_ready"] = serde_json::json!(false);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["graph_route_evidence_not_ready"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_evidence_not_ready"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_primary_blocker_codes"
            ])
        );
    }

    #[test]
    fn rejects_stale_graph_route_readiness_summary() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["summary_route_primary_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["route_primary_ready_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["primary_ready_routes_match"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_graph_route_readiness_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["replacement_summary_graph_route_readiness_mismatch"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.summary_route_primary_ready",
                "replacement_summary_graph_route_alignment.route_primary_ready_matches",
                "replacement_summary_graph_route_alignment.primary_ready_routes_match"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_graph_route_readiness_alignment"));
    }

    #[test]
    fn rejects_graph_route_alignment_without_route_evidence_envelope() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["evidence_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["evidence_protocol_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] = serde_json::json!([
            "graph_route_evidence_not_ready",
            "graph_route_evidence_protocol_mismatch"
        ]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "graph_route_evidence_not_ready",
                "graph_route_evidence_protocol_mismatch"
            ])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.evidence_protocol_matches",
                "replacement_summary_graph_route_alignment.evidence_ready"
            ])
        );
    }

    #[test]
    fn requires_graph_route_parity_alignment() {
        let mut bundle = ready_bundle();
        bundle["graph_route_parity_alignment"]["ready"] = serde_json::json!(false);
        bundle["graph_route_parity_alignment"]["ready_route_count"] = serde_json::json!(17);
        bundle["graph_route_parity_alignment"]["missing_routes"] =
            serde_json::json!(["agent_evolves"]);
        bundle["graph_route_parity_alignment"]["observed_blocker_codes"] =
            serde_json::json!(["agent_evolves_parity_evidence_missing"]);
        bundle["graph_route_parity_alignment"]["blocker_codes"] =
            serde_json::json!(["graph_route_parity_evidence_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_parity_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_parity_evidence_missing"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_parity_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_parity_alignment.ready",
                "graph_route_parity_alignment.ready_route_count",
                "graph_route_parity_alignment.missing_routes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_parity_evidence"));
    }

    #[test]
    fn requires_library_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("library_readiness");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.protocol",
                "library_readiness.present",
                "library_readiness.ready",
                "library_readiness.ready_area_count",
                "library_readiness.blocked_area_count",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area.graph.ready",
                "library_readiness.readiness_by_area.query.ready",
                "library_readiness.readiness_by_area.storage.ready",
                "library_readiness.readiness_by_area.background.ready",
                "library_readiness.readiness_by_area.query_family.ready",
                "library_readiness.readiness_by_area.search_projection.ready",
                "library_readiness.readiness_by_area.search_projection_shadow.ready"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_library_readiness_evidence"));
    }

    #[test]
    fn rejects_blocked_library_readiness_area() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["blocked_area_count"] = serde_json::json!(1);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["search_projection_not_ready"]);
        bundle["library_readiness"]["readiness_by_area"]["search_projection"]["ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["readiness_by_area"]["search_projection"]["blocker_codes"] =
            serde_json::json!(["search_projection_probe_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "search_projection_not_ready",
                "search_projection_probe_missing"
            ])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.search_projection.ready"
            ])
        );
    }

    #[test]
    fn requires_background_maintenance_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_protocol_matches"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_blocker_codes"] =
            serde_json::json!(["background_disabled"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["background_disabled"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn rejects_incomplete_background_maintenance_graph_delta_summary() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_deferred_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_rejected_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"] =
            serde_json::Value::Null;

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn requires_storage_recovery_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_wal_replay_bounded"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        let storage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "storage_recovery_evidence")
            .unwrap();
        assert_eq!(
            storage_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_storage_recovery_report"));
    }

    #[test]
    fn rejects_inconsistent_storage_recovery_summary_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_durable"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["storage_recovery_checkpoint_boundary_present"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_torn_tail_clean"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery_evidence"])
        );
        let storage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "storage_recovery_evidence")
            .unwrap();
        assert_eq!(
            storage_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_storage_recovery_report"));
    }

    fn ready_bundle() -> serde_json::Value {
        let mut bundle = serde_json::json!({
            "protocol": "nowledge-mem-skein-integration-bundle",
            "submodule": {
                "present": true,
                "path": "vendor/skein",
                "commit": "46f8bfb",
                "blocker_codes": []
            },
            "coexistence": {
                "old_database_retained": true,
                "old_database_deleted": false,
                "mode": "shadow",
                "blocker_codes": []
            },
            "content_store": {
                "present": true,
                "engine": "sqlite",
                "messages_available": true,
                "source_chunks_available": true,
                "blocker_codes": []
            },
            "previous_wrapper_preflight": {
                "ready": true,
                "blocker_codes": [],
                "failed_checks": []
            },
            "bounded_read_evidence": {
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "blocking_operator_count": 0,
                "streaming": false,
                "covered_routes": [
                    "/graph/overview",
                    "/graph/explore",
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
                ],
                "blocker_codes": []
            },
            "replacement_summary_bounded_read_alignment": {
                "ready": true,
                "evidence_present": true,
                "summary_present": true,
                "evidence_ready": true,
                "summary_ready": true,
                "protocol_matches": true,
                "readiness_matches": true,
                "mode_matches": true,
                "max_rows_matches": true,
                "streaming_matches": true,
                "covered_routes_matches": true,
                "blocker_codes": []
            },
            "replacement_summary": {
                "protocol": "skein-nowledge-replacement-summary",
                "production_cutover_ready": true,
                "blocking_categories": [],
                "missing_evidence": [],
                "shadow_evidence": {
                    "ready": true
                },
                "dual_engine_evidence": {
                    "present": true,
                    "ready": true,
                    "consistent": true
                },
                "replacement_readiness_family_summary": {
                    "total_count": 5,
                    "ready_count": 5,
                    "blocked_count": 0,
                    "omitted_count": 0,
                    "min_replacement_readiness_per_million": 1_000_000,
                    "blocked_query_families": [],
                    "required_query_families": [
                        "memory_lookup",
                        "graph_traversal",
                        "projected_graph",
                        "label_stats_read",
                        "search_projection"
                    ],
                    "missing_required_query_families": []
                },
                "search_projection_evidence": {
                    "protocol": "skein-nowledge-search-projection-evidence",
                    "ready": true,
                    "fts_ready": true,
                    "vector_ready": true,
                    "incremental_update_ready": true,
                    "predicate_pushdown_ready": true,
                    "compressed_vector_projection_required": true,
                    "compressed_vector_projection_ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow_evidence": {
                    "protocol": "skein-nowledge-search-projection-shadow-evidence",
                    "evidence_source": "skein-rust-cli",
                    "present": true,
                    "ready": true,
                    "document_count_parity": true,
                    "table_parity_ready": true,
                    "embedding_identity_parity": true,
                    "incremental_watermark_parity": true,
                    "pushdown_evidence": {
                        "ready": true,
                        "shadow_segment_descriptor_scan_filter_fields_ready": true,
                        "primary_scan_filter_fields": scan_filter_fields_json(),
                        "shadow_scan_filter_fields": scan_filter_fields_json(),
                        "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
                    },
                    "blocker_codes": []
                },
                "bounded_read_evidence": {
                    "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                    "present": true,
                    "ready": true,
                    "mode": "shadow_read_only",
                    "max_rows": 512,
                    "execution_row_cap": 513,
                    "row_limit_enforced_before_output": true,
                    "operator_row_cap_enabled": true,
                    "blocking_operator_count": 0,
                    "streaming": false,
                    "covered_routes": [
                        "/graph/overview",
                        "/graph/explore",
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
                    ],
                    "missing_covered_routes": [],
                    "blocker_codes": []
                },
                "cutover_evidence": {
                    "storage_recovery_required": true,
                    "storage_recovery_ready": true,
                    "storage_recovery_protocol_matches": true,
                    "storage_recovery_durable": true,
                    "storage_recovery_checkpoint_boundary_present": true,
                    "storage_recovery_wal_replay_bounded": true,
                    "storage_recovery_torn_tail_clean": true,
                    "storage_recovery_blocker_codes": [],
                    "storage_recovery_blockers": [],
                    "background_maintenance_required": true,
                    "background_maintenance_ready": true,
                    "background_maintenance_protocol_matches": true,
                    "background_maintenance_executable_search_projection_graph_delta_count": 1,
                    "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                    "background_maintenance_deferred_search_projection_graph_delta_count": 0,
                    "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                    "background_maintenance_executable_search_projection_graph_delta_operations": 2,
                    "background_maintenance_admitted_search_projection_graph_delta_operations": 2,
                    "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                    "background_maintenance_blocker_codes": [],
                    "background_maintenance_blockers": []
                }
            }
        });
        bundle["replacement_summary"]["query_runtime_preflight"] = ready_query_runtime_summary();
        bundle["replacement_summary_graph_route_alignment"] = serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "protocol_matches": true,
            "evidence_protocol_matches": true,
            "evidence_ready": true,
            "evidence_route_primary_ready": true,
            "summary_route_primary_ready": true,
            "route_primary_ready_matches": true,
            "primary_ready_routes_match": true,
            "evidence_required_routes_covered": true,
            "summary_required_routes_covered": true,
            "blocker_codes": []
        });
        bundle["replacement_summary_query_runtime_alignment"] = serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "evidence_ready": true,
            "summary_ready": true,
            "protocol_matches": true,
            "readiness_matches": true,
            "database_opened_matches": true,
            "probe_count_matches": true,
            "passed_probe_count_matches": true,
            "failed_probe_count_matches": true,
            "required_route_count_matches": true,
            "covered_route_count_matches": true,
            "covered_routes_matches": true,
            "required_routes_covered_matches": true,
            "route_coverage_ready_matches": true,
            "evidence_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "summary_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "blocker_codes": []
        });
        bundle["graph_route_readiness"] = serde_json::json!({
            "protocol": "nmem-graph-route-readiness-v1",
            "evidence_protocol": "nmem-graph-route-evidence-v1",
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
            "missing_query_runtime_routes": [],
            "route_query_runtime_ready": true,
            "route_primary_ready": true,
            "route_primary_blocker_codes": [],
            "routes": ready_graph_route_profile_routes()
        });
        bundle["graph_route_parity_alignment"] = serde_json::json!({
            "ready": true,
            "required_route_count": 18,
            "ready_route_count": 18,
            "ready_routes": [
                "augmentation_state",
                "pagerank_plan",
                "overview",
                "graph_search",
                "explore",
                "expand",
                "live_preview",
                "live_preview_node",
                "node_details",
                "source_detail",
                "orphans",
                "shortest_path",
                "community_members",
                "community_subgraph",
                "community_recent_memories",
                "related_communities",
                "graph_analysis",
                "agent_evolves"
            ],
            "missing_routes": [],
            "not_ready_routes": [],
            "route_mismatch_routes": [],
            "protocol_mismatch_routes": [],
            "blocker_routes": [],
            "observed_blocker_codes": [],
            "blocker_codes": []
        });
        bundle["query_runtime_preflight"] = serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "passed_probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
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
            "probes": ready_query_runtime_preflight_probes()
        });
        bundle["search_candidate_shadow_evidence"] = serde_json::json!({
            "protocol": "skein-nowledge-search-candidate-shadow-evidence",
            "route": "/search-index/skein-shadow/candidate-evidence",
            "evidence_source": "nmem-rust-bridge",
            "engine": "skein-shadow",
            "ready": true,
            "candidate_primary_engine": "skein",
            "blocker_codes": []
        });
        bundle["library_readiness"] = ready_library_readiness();
        bundle
    }

    fn scan_filter_fields_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
    }

    fn scan_filter_field_summaries_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| serde_json::json!({ "field": field }))
            .collect::<Vec<_>>())
    }

    fn ready_graph_route_profile_routes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "shadow_compare_ready": true,
                    "shadow_compare_evidence_source": "route_parity_evidence",
                    "primary_ready": true,
                    "query_runtime_ready": true,
                    "query_report_count": 1,
                    "query_reports": [ready_graph_route_query_report()],
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_runtime_preflight_probes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "name": format!("probe:{route}"),
                    "route": route,
                    "query_family": "memory_lookup",
                    "ready": true,
                    "success": true,
                    "output_row_count": 1,
                    "selected_plan_fingerprint": "IndexNodeSeek(1:m:6:Memory)",
                    "selected_plan_operator_counts": {
                        "IndexNodeSeek": 1,
                        "ProjectExec": 1
                    },
                    "selected_plan_class_counts": {
                        "access": 1,
                        "relational": 1
                    },
                    "optimizer_decision_count": 2,
                    "plan_cache_lookup": "miss",
                    "plan_cache": {
                        "lookup": "miss",
                        "bypass_reason": null,
                        "cacheable": true,
                        "hit": false,
                        "miss": true,
                        "bypassed": false
                    },
                    "execution_profile": {
                        "scan_pruning_report_count": 1,
                        "pruned_scan_count": 1,
                        "scan_pruning_reports": [
                            {
                                "label_id": 1,
                                "strategy": {
                                    "kind": "property_eq",
                                    "property": "id"
                                },
                                "pruned": true,
                                "exact_empty": false,
                                "candidate_count_before_pruning": 2,
                                "pruned_candidate_count": 1,
                                "candidate_count_before_filter": 1,
                                "output_count": 1,
                                "filtered_out_count": 0
                            }
                        ]
                    },
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_runtime_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "present": true,
            "ready": true,
            "database_opened": true,
            "probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "passed_probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "probe_details_ready": true,
            "blocker_codes": []
        })
    }

    fn ready_graph_route_query_report() -> serde_json::Value {
        serde_json::json!({
            "query_name": "overview-memory-lookup",
            "query_index": 0,
            "protocol": "skein-nowledge-mem-query-report-v1",
            "statement_kind": "match_return",
            "execution_path": "fast_path",
            "fast_path_selected": true,
            "slow_log_candidate": false,
            "physical_plan_captured": false,
            "elapsed_micros": 12,
            "physical_operator_counts_present": true,
            "optimizer_decision_count": 2,
            "scan_pruning_report_count": 1,
            "scan_pruning_reports_present": true,
            "scan_pruning_reports": [
                {
                    "label_id": 1,
                    "strategy": {
                        "kind": "property_eq",
                        "property": "id"
                    },
                    "pruned": true,
                    "exact_empty": false,
                    "candidate_count_before_pruning": 2,
                    "pruned_candidate_count": 1,
                    "candidate_count_before_filter": 1,
                    "output_count": 1,
                    "filtered_out_count": 0
                }
            ],
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "ready": true,
            "blocker_codes": []
        })
    }

    fn ready_library_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-library-readiness-v1",
            "present": true,
            "ready": true,
            "mode": "shadow_read_only",
            "ready_area_count": 7,
            "blocked_area_count": 0,
            "blocker_codes": [],
            "open_report": {
                "protocol": "skein-nowledge-mem-open-report",
                "mode": "shadow_read_only",
                "graph_configured": true,
                "search_projection_configured": true,
                "compressed_vector_search_mode": "disabled",
                "graph_opened": true,
                "search_projection_opened": true
            },
            "graph": {
                "open": true,
                "mode": "shadow_read_only",
                "read_only": true
            },
            "readiness_by_area": {
                "graph": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query": {
                    "ready": true,
                    "blocker_codes": []
                },
                "storage": {
                    "ready": true,
                    "blocker_codes": []
                },
                "background": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query_family": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow": {
                    "ready": true,
                    "blocker_codes": []
                }
            },
            "bounded_read_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "query_family_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "storage_recovery": {
                "ready": true,
                "blocker_codes": []
            },
            "background_maintenance": {
                "blocker_codes": []
            },
            "search_projection_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "ready": true,
                "blocker_codes": []
            }
        })
    }
}
