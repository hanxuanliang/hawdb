use super::*;
use skein::PRODUCTION_QUALIFICATION_POLICY_VERSION;

#[test]
fn complete_raw_artifact_bundle_is_ready() {
    let expected = identity("linux", "x86_64");
    let artifacts = ProductionReleaseQualificationArtifacts {
        graph_storage: Some(graph(&expected)),
        search: Some(search(&expected)),
        vector_targets: [
            ("linux", "x86_64"),
            ("linux", "aarch64"),
            ("macos", "aarch64"),
            ("windows", "x86_64"),
        ]
        .into_iter()
        .map(|(target_os, target_arch)| vector(&identity(target_os, target_arch)))
        .collect(),
        morsel_profiles: [4, 8, 16]
            .into_iter()
            .enumerate()
            .map(|(index, workers)| morsel(&expected, workers, (index + 1) as u64))
            .collect(),
        blocking_operators: Some(blocking(&expected)),
    };

    let report = evaluate_production_release_qualification_bundle(
        artifacts,
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(report.ready, "{:?}", report.blocker_codes);
    assert!(report.graph_storage.ready);
    assert!(report.search.ready);
    assert!(report.vector_matrix.ready);
    assert!(report.morsel_matrix.ready);
    assert!(report.blocking_operators.ready);
    assert_eq!(
        report.json()["protocol"],
        PRODUCTION_RELEASE_QUALIFICATION_BUNDLE_PROTOCOL
    );
}

#[test]
fn top_level_ready_cannot_hide_invalid_raw_storage_evidence() {
    let expected = identity("linux", "x86_64");
    let mut artifact = graph(&expected);
    artifact["storage_resource_profile"]["storage"]["canonical_exceeds_cache"] =
        serde_json::json!(false);
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            graph_storage: Some(artifact),
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.ready);
    assert!(!report.graph_storage.ready);
    assert!(report
        .graph_storage
        .blocker_codes
        .contains(&"canonical_does_not_exceed_cache".to_string()));
}

#[test]
fn vector_matrix_rejects_stale_shared_release_identity() {
    let expected = identity("linux", "x86_64");
    let mut stale = identity("linux", "aarch64");
    stale.source_revision = "stale".to_string();
    let report = evaluate_production_release_qualification_bundle(
        ProductionReleaseQualificationArtifacts {
            vector_targets: vec![vector(&stale)],
            ..ProductionReleaseQualificationArtifacts::default()
        },
        expected,
        ProductionReleaseQualificationPolicy::default(),
    );

    assert!(!report.vector_matrix.ready);
    assert!(report.vector_matrix.target_reports[0]
        .blocker_codes
        .contains(&"release_identity_mismatch".to_string()));
}

fn identity(target_os: &str, target_arch: &str) -> ProductionQualificationIdentity {
    ProductionQualificationIdentity {
        source_revision: "revision".to_string(),
        rust_toolchain: "1.97.1".to_string(),
        target_os: target_os.to_string(),
        target_arch: target_arch.to_string(),
        enabled_features: vec![
            "full-text-search".to_string(),
            "tokio-runtime".to_string(),
            "vector-search".to_string(),
        ],
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "sha256:config".to_string(),
        deployment_profile: "production".to_string(),
        dataset_fingerprint: "sha256:dataset".to_string(),
        canonical_graph_commit_epoch: 42,
        policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
    }
}

fn binding(identity: &ProductionQualificationIdentity) -> Value {
    serde_json::to_value(ProductionEvidenceBinding {
        identity: identity.clone(),
        generated_at_unix_seconds: 1,
    })
    .unwrap()
}

fn runtime() -> Value {
    serde_json::json!({
        "admissions_delta": 100,
        "admission_waits_delta": 0,
        "admission_rejections_delta": 0,
        "completions_delta": 100,
        "cancellations_delta": 1,
        "deadline_exceeded_delta": 0,
        "final_active_foreground_tasks": 0,
        "final_active_background_tasks": 0,
        "final_active_blocking_tasks": 0,
        "final_admitted_memory_bytes": 0,
        "final_overcommitted": false,
    })
}

fn latency() -> Value {
    serde_json::json!({
        "sample_count": 100,
        "min_micros": 1,
        "p50_micros": 10,
        "p95_micros": 20,
        "p99_micros": 30,
        "max_micros": 40,
    })
}

fn graph(identity: &ProductionQualificationIdentity) -> Value {
    serde_json::json!({
        "protocol": "skein-production-graph-storage-qualification-v1",
        "evidence_kind": "representative_production_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "execution": {
            "measurement_runs": 100,
            "intermediate_rows": 100_000,
        },
        "runtime": runtime(),
        "storage_resource_profile": {
            "protocol": "skein-storage-resource-profile-v2",
            "resource_ready": true,
            "ready": true,
            "blocker_codes": [],
            "identity_matches_expected": true,
            "limits": {
                "min_canonical_artifact_bytes": 1_000,
                "max_steady_resident_bytes": 10_000,
                "max_peak_resident_bytes": 20_000,
                "max_total_page_faults": 1_000,
                "max_minor_page_faults": null,
                "max_major_page_faults": null,
                "max_intermediate_rows": 200_000,
                "max_intermediate_payload_bytes": 2_000_000,
                "max_output_rows": 100,
                "max_output_payload_bytes": 10_000,
            },
            "storage": {
                "durable": true,
                "out_of_core": true,
                "canonical_artifact_bytes": 2_000,
                "canonical_exceeds_cache": true,
                "delta_within_budget": true,
            },
            "execution": {
                "fully_streamed": true,
                "steady_resident_bytes": 5_000,
                "peak_resident_bytes": 7_000,
                "total_page_faults": 10,
                "minor_page_faults": 10,
                "major_page_faults": 0,
                "intermediate_rows": 100_000,
                "intermediate_payload_bytes": 1_000_000,
                "output_rows": 10,
                "output_payload_bytes": 1_000,
                "metric_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": true,
                },
            },
        },
    })
}

fn search(identity: &ProductionQualificationIdentity) -> Value {
    let coverage = serde_json::json!({
        "selective_identifier": true,
        "cjk_text": true,
        "common_term": true,
        "no_hit": true,
        "metadata_filter": true,
        "acl_filter": false,
        "hybrid_rrf": true,
        "incremental_upsert_delete": true,
        "checkpoint_reopen": true,
        "corrupt_artifact": true,
        "stale_manifest": true,
        "mixed_foreground_background": true,
        "larger_than_memory": true,
    });
    let query = |kind: &str| {
        serde_json::json!({
            "kind": kind,
            "exact_topk_score_parity": true,
            "out_of_core_latency": latency(),
        })
    };
    serde_json::json!({
        "protocol": "skein-production-search-out-of-core-qualification-v1",
        "evidence_kind": "representative_production_search_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "qualification": {
            "protocol": "skein-search-lexical-production-qualification",
            "protocol_version": 2,
            "ready": true,
            "blocker_codes": [],
            "evidence_binding": binding(identity),
            "projection_identity": {
                "projection_generation": 7,
                "source_graph_commit_epoch": 42,
                "document_count": 100_000,
                "documents_digest": 1,
                "analyzer_digest": 2,
                "embedding_model": "model",
                "embedding_version": "v1",
                "embedding_dimension": 3,
            },
            "topk_score_parity": {"text": true, "vector": true, "hybrid": true},
            "exact_topk_score_parity": true,
            "coverage": coverage,
            "metrics": {
                "canonical_dataset_bytes": 2_000,
                "storage_memory_budget_bytes": 1_000,
                "process_memory_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": true,
                },
            },
        },
        "query_evidence": [
            query("selective_identifier"), query("cjk_text"), query("common_term"),
            query("no_hit"), query("metadata_filter"), query("vector"), query("hybrid")
        ],
        "lifecycle": {
            "incremental_upsert_delete": true,
            "checkpoint_reopen": true,
            "stale_generation": true,
            "corrupt_artifact_rejected": true,
            "mixed_foreground_background": true,
        },
        "process_memory": {
            "capabilities": {"resident_memory": true, "total_page_faults": true},
        },
        "out_of_core_metrics": {
            "segment_range_reads": 1,
            "segment_bytes_read": 1,
            "hydrated_bytes": 1,
        },
    })
}

fn vector(identity: &ProductionQualificationIdentity) -> Value {
    let metrics = |kernel: &str| {
        serde_json::json!({
            "kernel": kernel,
            "max_admitted_workers": 1,
            "segment_count": 1,
            "peak_admitted_working_bytes": 1,
            "fallback_count": 0,
        })
    };
    serde_json::json!({
        "protocol": "skein-production-vector-qualification-v1",
        "evidence_kind": "representative_production_vector_replica",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "expected_identity": serde_json::to_value(identity).unwrap(),
        "projection_identity": {
            "projection_generation": 9,
            "source_graph_commit_epoch": 42,
            "document_count": 100_000,
            "source_digest": 1,
            "payload_bytes": 1,
            "payload_checksum": 1,
            "format_version": 1,
            "algorithm": "turboquant",
            "bit_width": 4,
            "dimension": 3,
            "transform_seed": 1,
            "embedding_model": "model",
            "embedding_version": "v1",
            "file_backed": true,
        },
        "projection_resources": {
            "segment_count": 1,
            "configured_build_working_bytes": 1,
            "peak_build_working_bytes": 1,
            "raw_vector_bytes": 1,
            "projection_payload_bytes": 1,
        },
        "recall_evidence": [{
            "report": {
                "protocol": "skein-vector-recall-validation-v1",
                "ready": true,
                "approximate_backend": "skein_turboquant_candidate_projection",
                "requested_sample_count": 1,
                "executed_sample_count": 1,
                "minimum_recall_per_million": 950_000,
                "candidate_recall_at_k_per_million": 950_000,
                "recall_at_k_per_million": 950_000,
                "exact_hit_count": 1,
                "candidate_hit_count": 1,
                "fallback_count": 0,
                "index_coverage_incomplete_count": 0,
                "blocker_codes": [],
            },
        }],
        "query_evidence": [{
            "auto_scalar_candidate_parity": true,
            "auto_scalar_final_parity": true,
            "auto_final_matches_exact": true,
            "scalar_candidate_final_matches_exact": true,
            "exact_latency": latency(),
            "auto_latency": latency(),
            "scalar_candidate_latency": latency(),
            "auto_metrics": metrics("portable"),
            "scalar_candidate_metrics": metrics("scalar"),
        }],
        "differential_oracle": {
            "required": true,
            "compiled": true,
            "available": true,
            "ready": true,
            "implementation": "upstream_turbovec",
            "role": "development_differential_oracle_not_truth",
            "bit_width": 4,
            "cases": [{}],
        },
        "lifecycle": {
            "incremental_fallback_safe": true,
            "checkpoint_reopen_restores_projection": true,
            "stale_generation_isolated": true,
            "corrupt_projection_rejected": true,
            "cancellation_propagated": true,
            "mixed_foreground_background": true,
            "update_latency": latency(),
            "checkpoint_latency": latency(),
            "reopen_latency": latency(),
            "cancellation_latency": latency(),
        },
        "process_memory": {
            "capabilities": {"resident_memory": true, "total_page_faults": true},
        },
    })
}

fn morsel(identity: &ProductionQualificationIdentity, workers: usize, process_id: u64) -> Value {
    let throughput = u64::try_from(workers).unwrap() * 100;
    serde_json::json!({
        "protocol": "skein-production-morsel-profile-v1",
        "evidence_kind": "representative_production_morsel_profile",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "process_id": process_id,
        "expected_workers": workers,
        "query_identity": {"query_digest": "query", "parameter_digest": "params"},
        "evidence_binding": binding(identity),
        "runtime_shape": {"effective_cpu_slots": workers},
        "execution": {
            "warmup_runs": 3,
            "measurement_runs": 100,
            "fully_streamed_runs": 100,
            "morsel_max_admitted_workers": workers,
            "morsel_peak_active_workers": workers,
            "rows_per_second": throughput,
            "peak_resident_bytes": 1_000,
            "latency": latency(),
        },
        "cancellation": {"cancellation_observed": true, "latency_micros": 1},
        "runtime": runtime(),
    })
}

fn blocking(identity: &ProductionQualificationIdentity) -> Value {
    let case = |kind: &str, route: &str| {
        serde_json::json!({
            "route_name": route,
            "operator_kind": kind,
            "ready": true,
            "blocker_codes": [],
            "disposition": "external_spill_observed",
            "input_rows": 100_000,
            "minimum_input_rows": 100_000,
            "budget_bytes": 1_000,
            "peak_tracked_bytes": 900,
            "max_spill_bytes": 10_000,
            "max_spill_runs": 10,
            "spilled_bytes": 5_000,
            "spill_run_count": 5,
            "fully_streamed": true,
        })
    };
    serde_json::json!({
        "protocol": "skein-production-blocking-qualification-v1",
        "evidence_kind": "active_route_blocking_operators",
        "production_eligible": true,
        "ready": true,
        "blocker_codes": [],
        "evidence_binding": binding(identity),
        "cases": [case("distinct", "distinct_route"), case("cartesian_build", "cartesian_route")],
        "spill_pool": {
            "active_bytes": 0,
            "pending_write_bytes": 0,
            "active_runs": 0,
            "orphan_cleanup_failures": 0,
            "run_delete_failures": 0,
        },
        "runtime": runtime(),
    })
}
