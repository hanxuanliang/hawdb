# Production Content Store Qualification

This runbook collects read-only storage evidence from an already imported,
representative Skein database. It does not import SQLite, create replicas, or
derive an oracle from Skein itself.

## Preconditions

- The database is a caller-owned representative copy, not the live Mem store.
- Canonical row and index artifacts both exceed the configured segment cache.
- The database has a clean authoritative checkpoint and the expected canonical
  commit epoch.
- Expected row counts and SHA-256 result digests come from an offline reference
  snapshot owned by the Mem adapter.
- The identity records the exact revision, target, features, configuration,
  dataset, and schema being qualified.

## Plan

Write a bounded JSON plan with protocol
`skein-production-content-store-read-plan-v1`:

The checked-in
[`production_read_plan_example_v1.json`](../crates/qualification/fixtures/nowledge_content_store/production_read_plan_example_v1.json)
is parser-tested and can be copied as the starting point.

```json
{
  "protocol": "skein-production-content-store-read-plan-v1",
  "evidence_binding": {
    "identity": {
      "source_revision": "replace-with-skein-revision",
      "rust_toolchain": "rustc 1.xx.x",
      "target_os": "macos",
      "target_arch": "aarch64",
      "enabled_features": [],
      "durable_format_version": 1,
      "schema_version": 1,
      "configuration_digest": "sha256:replace-with-qualified-config",
      "deployment_profile": "desktop-bound-8-gib",
      "dataset_fingerprint": "sha256:replace-with-offline-dataset-digest",
      "canonical_graph_commit_epoch": 1,
      "policy_version": 1
    },
    "generated_at_unix_seconds": 1
  },
  "expected_identity": {
    "source_revision": "replace-with-skein-revision",
    "rust_toolchain": "rustc 1.xx.x",
    "target_os": "macos",
    "target_arch": "aarch64",
    "enabled_features": [],
    "durable_format_version": 1,
    "schema_version": 1,
    "configuration_digest": "sha256:replace-with-qualified-config",
    "deployment_profile": "desktop-bound-8-gib",
    "dataset_fingerprint": "sha256:replace-with-offline-dataset-digest",
    "canonical_graph_commit_epoch": 1,
    "policy_version": 1
  },
  "resource_profile": "desktop_bound_8_gib",
  "database": {
    "max_read_result_rows": 100000,
    "max_read_result_payload_bytes": 67108864,
    "execution_batch_rows": 256,
    "execution_batch_payload_bytes": 16777216,
    "blocking_operator_bytes": 67108864,
    "segment_cache_capacity_bytes": 268435456,
    "max_relational_index_read_bytes": 67108864,
    "max_relational_hydration_bytes": 67108864
  },
  "measurement_runs": 5,
  "resource_limits": {
    "max_steady_resident_bytes": 2147483648,
    "max_peak_resident_bytes": 2147483648,
    "max_total_page_faults_per_run": null,
    "max_minor_page_faults_per_run": null,
    "max_major_page_faults_per_run": null
  },
  "read_cases": [
    {
      "case_name": "thread-message-page",
      "statement_name": "thread_messages_page",
      "parameters": ["replace-with-thread-storage-id", 100],
      "expected_output_rows": 100,
      "expected_output_sha256": "replace-with-offline-result-digest",
      "max_intermediate_rows": 1000,
      "max_physical_pages_per_run": 128,
      "max_physical_bytes_per_run": 16777216
    }
  ]
}
```

The example values are placeholders, not accepted release evidence. The
`evidence_binding.identity` and `expected_identity` objects must be identical.
Plan parsing rejects unknown fields and files larger than 32 MiB.

Use `capability_512_mib` for the separately configured low-memory capability
run. It sets an explicit 512 MiB Skein runtime ceiling. Use
`desktop_bound_8_gib` for the dynamic desktop policy: Skein derives its budget
from current headroom and caps automatic capacity at 2 GiB. A custom profile is
represented as:

```json
{
  "configured_workload": {
    "available_memory_bytes": 4294967296,
    "runtime_memory_ceiling_bytes": 1073741824
  }
}
```

## Execute

```bash
cargo run -p skein-qualification \
  --bin skein-content-store-read-qualification -- \
  --database-path /path/to/representative.skein \
  --plan-json /path/to/content-store-read-plan.json \
  > content-store-read-evidence.json
```

The wrapper always constructs `read_only + OutOfCore + Authoritative` and
passes the plan to `run_production_content_store_storage_qualification`. It
does not retain the local database path. Exit code `0` means the complete report
is ready, `1` means the report is complete but blocked, and `2` means input or
execution failed.

Retain the plan, evidence JSON, exact binary revision, and offline oracle
artifact together. A successful local run is not a release gate until those
artifacts are reviewed and included in the production qualification bundle.
