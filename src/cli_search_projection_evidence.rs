use crate::{Result, SkeinError};
use skein::{SearchIndex, SearchProjectionProbeOptions};
use std::collections::BTreeSet;
use std::path::Path;

const REQUIRED_TABLES: &[&str] = &[
    "memories_index",
    "messages_index",
    "communities_index",
    "entities_index",
    "sources_index",
    "source_chunks_index",
];

const VECTOR_TABLES: &[&str] = &[
    "memories_index",
    "communities_index",
    "entities_index",
    "sources_index",
    "source_chunks_index",
];

pub fn nowledge_search_projection_evidence_usage() -> String {
    "nowledge-search-projection-evidence requires [--require-ready] <search-projection-probe-json>"
        .to_string()
}

pub fn skein_search_projection_probe_usage() -> String {
    "skein-search-projection-probe requires [--active-model <model>] [--active-dimension <dimension>] <search-index-dir>"
        .to_string()
}

pub fn run_nowledge_search_projection_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_search_projection_evidence_usage(),
                    ));
                }
                let probe = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_search_projection_evidence_json(&probe),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_search_projection_evidence_usage(),
    ))
}

pub fn run_skein_search_projection_probe(
    mut args: impl Iterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut options = SearchProjectionProbeOptions::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--active-model" => {
                options.active_embedding_model =
                    Some(args.next().ok_or_else(|| {
                        SkeinError::Semantic(skein_search_projection_probe_usage())
                    })?);
            }
            "--active-dimension" => {
                let raw_dimension = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(skein_search_projection_probe_usage()))?;
                options.active_embedding_dimension =
                    Some(parse_positive_usize("--active-dimension", &raw_dimension)?);
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(skein_search_projection_probe_usage()));
                }
                let index = SearchIndex::open(path)?;
                return Ok(index.nowledge_search_projection_probe_json(options));
            }
        }
    }
    Err(SkeinError::Semantic(skein_search_projection_probe_usage()))
}

pub fn nowledge_search_projection_evidence_json(probe: &serde_json::Value) -> serde_json::Value {
    let table_reports = required_table_reports(probe);
    let covered_table_count = table_reports
        .iter()
        .filter(|table| bool_path(table, &["present"]) == Some(true))
        .count() as u64;
    let required_table_count = REQUIRED_TABLES.len() as u64;
    let all_tables_covered = covered_table_count == required_table_count;
    let fts_ready = table_reports
        .iter()
        .all(|table| bool_path(table, &["fts_ready"]) == Some(true));
    let vector_ready = table_reports.iter().all(|table| {
        str_path(table, &["name"]).is_some_and(|name| {
            !VECTOR_TABLES.contains(&name) || bool_path(table, &["vector_ready"]) == Some(true)
        })
    });
    let source_chunk_ready = table_reports.iter().any(|table| {
        str_path(table, &["name"]) == Some("source_chunks_index")
            && bool_path(table, &["present"]) == Some(true)
            && bool_path(table, &["fts_ready"]) == Some(true)
            && bool_path(table, &["vector_ready"]) == Some(true)
    });
    let embedding_identity = embedding_identity_report(probe);
    let embedding_identity_ready = bool_path(&embedding_identity, &["ready"]) == Some(true);
    let fail_soft = fail_soft_report(probe);
    let fail_soft_ready = bool_path(&fail_soft, &["ready"]) == Some(true);
    let lifecycle = lifecycle_report(probe);
    let rebuild_marker_ready = bool_path(&lifecycle, &["rebuild_marker_ready"]) == Some(true);
    let metadata_repair_marker_ready =
        bool_path(&lifecycle, &["metadata_repair_marker_ready"]) == Some(true);
    let incremental_update = incremental_update_report(probe);
    let incremental_update_ready = bool_path(&incremental_update, &["ready"]) == Some(true);
    let derived_projection = bool_path(probe, &["derived_projection"])
        .or_else(|| bool_path(probe, &["projection", "derived"]))
        == Some(true);

    let mut blocker_codes = BTreeSet::new();
    collect_probe_blockers(probe, &mut blocker_codes);
    if !derived_projection {
        blocker_codes.insert("not_derived_projection".to_string());
    }
    if !all_tables_covered {
        blocker_codes.insert("missing_required_search_tables".to_string());
    }
    if !fts_ready {
        blocker_codes.insert("fts_not_ready".to_string());
    }
    if !vector_ready {
        blocker_codes.insert("vector_not_ready".to_string());
    }
    if !embedding_identity_ready {
        blocker_codes.insert("embedding_identity_not_ready".to_string());
    }
    if !fail_soft_ready {
        blocker_codes.insert("fail_soft_not_ready".to_string());
    }
    if !rebuild_marker_ready {
        blocker_codes.insert("rebuild_marker_not_ready".to_string());
    }
    if !metadata_repair_marker_ready {
        blocker_codes.insert("metadata_repair_marker_not_ready".to_string());
    }
    if !incremental_update_ready {
        blocker_codes.insert("incremental_update_not_ready".to_string());
    }
    if !source_chunk_ready {
        blocker_codes.insert("source_chunks_index_not_ready".to_string());
    }

    let ready = blocker_codes.is_empty();
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-evidence",
        "ready": ready,
        "derived_projection": derived_projection,
        "all_tables_covered": all_tables_covered,
        "covered_table_count": covered_table_count,
        "required_table_count": required_table_count,
        "required_tables": REQUIRED_TABLES,
        "fts_ready": fts_ready,
        "vector_ready": vector_ready,
        "embedding_identity_ready": embedding_identity_ready,
        "fail_soft_ready": fail_soft_ready,
        "rebuild_marker_ready": rebuild_marker_ready,
        "metadata_repair_marker_ready": metadata_repair_marker_ready,
        "incremental_update_ready": incremental_update_ready,
        "source_chunk_ready": source_chunk_ready,
        "tables": table_reports,
        "embedding_identity": embedding_identity,
        "fail_soft": fail_soft,
        "lifecycle": lifecycle,
        "incremental_update": incremental_update,
        "blocker_codes": blocker_codes.into_iter().collect::<Vec<_>>(),
    })
}

fn required_table_reports(probe: &serde_json::Value) -> Vec<serde_json::Value> {
    REQUIRED_TABLES
        .iter()
        .map(|name| {
            let table = find_table(probe, name);
            let present = table.is_some();
            let fts_ready = table
                .and_then(|table| bool_path(table, &["fts_ready"]))
                .unwrap_or(false);
            let vector_ready = if VECTOR_TABLES.contains(name) {
                table
                    .and_then(|table| bool_path(table, &["vector_ready"]))
                    .unwrap_or(false)
            } else {
                table
                    .and_then(|table| bool_path(table, &["vector_ready"]))
                    .unwrap_or(true)
            };
            let row_count = table.and_then(|table| u64_path(table, &["row_count"]));
            let blocker_codes = table
                .and_then(|table| array_path(table, &["blocker_codes"]))
                .unwrap_or_default();
            serde_json::json!({
                "name": name,
                "present": present,
                "fts_ready": fts_ready,
                "vector_ready": vector_ready,
                "row_count": row_count,
                "blocker_codes": blocker_codes,
            })
        })
        .collect()
}

fn embedding_identity_report(probe: &serde_json::Value) -> serde_json::Value {
    let manifest = value_path(probe, &["embedding_manifest"])
        .or_else(|| value_path(probe, &["embedding_identity"]))
        .unwrap_or(&serde_json::Value::Null);
    let model = str_path(manifest, &["model"]).or_else(|| str_path(manifest, &["model_id"]));
    let dimension =
        u64_path(manifest, &["dimension"]).or_else(|| u64_path(manifest, &["embedding_dimension"]));
    let active_model =
        str_path(manifest, &["active_model"]).or_else(|| str_path(manifest, &["active_model_id"]));
    let active_dimension = u64_path(manifest, &["active_dimension"])
        .or_else(|| u64_path(manifest, &["active_embedding_dimension"]));
    let model_matches = active_model
        .map(|active| model == Some(active))
        .or_else(|| bool_path(manifest, &["model_matches"]))
        .unwrap_or(false);
    let dimension_matches = active_dimension
        .map(|active| dimension == Some(active))
        .or_else(|| bool_path(manifest, &["dimension_matches"]))
        .unwrap_or(false);
    let ready = model.is_some()
        && dimension.is_some_and(|dimension| dimension > 0)
        && model_matches
        && dimension_matches;
    serde_json::json!({
        "ready": ready,
        "model": model,
        "dimension": dimension,
        "active_model": active_model,
        "active_dimension": active_dimension,
        "model_matches": model_matches,
        "dimension_matches": dimension_matches,
    })
}

fn fail_soft_report(probe: &serde_json::Value) -> serde_json::Value {
    let fail_soft = value_path(probe, &["fail_soft"])
        .or_else(|| value_path(probe, &["degradation"]))
        .unwrap_or(&serde_json::Value::Null);
    let fts_to_vector_ready = bool_path(fail_soft, &["fts_to_vector_ready"]).unwrap_or(false);
    let vector_to_fts_ready = bool_path(fail_soft, &["vector_to_fts_ready"]).unwrap_or(false);
    let no_500_on_leg_failure = bool_path(fail_soft, &["no_500_on_leg_failure"]).unwrap_or(false);
    serde_json::json!({
        "ready": fts_to_vector_ready && vector_to_fts_ready && no_500_on_leg_failure,
        "fts_to_vector_ready": fts_to_vector_ready,
        "vector_to_fts_ready": vector_to_fts_ready,
        "no_500_on_leg_failure": no_500_on_leg_failure,
    })
}

fn lifecycle_report(probe: &serde_json::Value) -> serde_json::Value {
    let lifecycle = value_path(probe, &["lifecycle"])
        .or_else(|| value_path(probe, &["projection_lifecycle"]))
        .unwrap_or(&serde_json::Value::Null);
    serde_json::json!({
        "rebuild_marker_ready": bool_path(lifecycle, &["rebuild_marker_ready"]).unwrap_or(false),
        "metadata_repair_marker_ready": bool_path(lifecycle, &["metadata_repair_marker_ready"]).unwrap_or(false),
    })
}

fn incremental_update_report(probe: &serde_json::Value) -> serde_json::Value {
    let incremental = value_path(probe, &["incremental_update"])
        .or_else(|| value_path(probe, &["incremental"]))
        .unwrap_or(&serde_json::Value::Null);
    serde_json::json!({
        "ready": bool_path(incremental, &["ready"]).unwrap_or(false),
        "upsert_ready": bool_path(incremental, &["upsert_ready"]).unwrap_or(false),
        "delete_ready": bool_path(incremental, &["delete_ready"]).unwrap_or(false),
        "watermark_ready": bool_path(incremental, &["watermark_ready"]).unwrap_or(false),
    })
}

fn collect_probe_blockers(probe: &serde_json::Value, blockers: &mut BTreeSet<String>) {
    for code in array_path(probe, &["blocker_codes"]).unwrap_or_default() {
        blockers.insert(code);
    }
    if let Some(tables) = value_path(probe, &["tables"]).and_then(serde_json::Value::as_array) {
        for table in tables {
            for code in array_path(table, &["blocker_codes"]).unwrap_or_default() {
                blockers.insert(code);
            }
        }
    }
}

fn find_table<'a>(probe: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    value_path(probe, &["tables"])?
        .as_array()?
        .iter()
        .find(|table| str_path(table, &["name"]) == Some(name))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read search projection evidence JSON '{}': {error}",
            path.display()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse search projection evidence JSON '{}': {error}",
            path.display()
        ))
    })
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value.parse::<usize>().map_err(|error| {
        SkeinError::Semantic(format!("invalid {flag} value '{value}': {error}"))
    })?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "invalid {flag} value '{value}': expected a positive integer"
        )));
    }
    Ok(parsed)
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
}

fn array_path(value: &serde_json::Value, path: &[&str]) -> Option<Vec<String>> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::{nowledge_search_projection_evidence_json, run_skein_search_projection_probe};
    use skein::{
        SearchEmbeddingManifest, SearchIndex, SearchProjectionDelta, SearchProjectionKind,
        SearchProjectionRow,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn search_projection_evidence_reports_ready_for_complete_probe() {
        let report = nowledge_search_projection_evidence_json(&ready_probe());

        assert_eq!(report["ready"], true);
        assert_eq!(report["derived_projection"], true);
        assert_eq!(report["all_tables_covered"], true);
        assert_eq!(report["covered_table_count"], 6);
        assert_eq!(report["required_table_count"], 6);
        assert_eq!(report["fts_ready"], true);
        assert_eq!(report["vector_ready"], true);
        assert_eq!(report["embedding_identity_ready"], true);
        assert_eq!(report["fail_soft_ready"], true);
        assert_eq!(report["rebuild_marker_ready"], true);
        assert_eq!(report["metadata_repair_marker_ready"], true);
        assert_eq!(report["incremental_update_ready"], true);
        assert_eq!(report["source_chunk_ready"], true);
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_projection_evidence_fails_closed_for_missing_source_chunks() {
        let mut probe = ready_probe();
        probe["tables"]
            .as_array_mut()
            .unwrap()
            .retain(|table| table["name"].as_str() != Some("source_chunks_index"));

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["all_tables_covered"], false);
        assert_eq!(report["covered_table_count"], 5);
        assert_eq!(report["source_chunk_ready"], false);
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "fts_not_ready",
                "missing_required_search_tables",
                "source_chunks_index_not_ready",
                "vector_not_ready"
            ])
        );
    }

    #[test]
    fn search_projection_evidence_recomputes_embedding_identity() {
        let mut probe = ready_probe();
        probe["embedding_manifest"]["active_dimension"] = serde_json::json!(1536);

        let report = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(report["ready"], false);
        assert_eq!(report["embedding_identity_ready"], false);
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["embedding_identity_not_ready"])
        );
    }

    #[test]
    fn skein_probe_output_feeds_search_projection_evidence() {
        let path = unique_test_dir("search_projection_probe_command");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: nowledge_probe_rows(),
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(11),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }

        let probe = run_skein_search_projection_probe(
            [
                "--active-model",
                "bge-m3",
                "--active-dimension",
                "2",
                path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        let evidence = nowledge_search_projection_evidence_json(&probe);

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn ready_probe() -> serde_json::Value {
        serde_json::json!({
            "derived_projection": true,
            "tables": [
                table("memories_index", true),
                table("messages_index", false),
                table("communities_index", true),
                table("entities_index", true),
                table("sources_index", true),
                table("source_chunks_index", true)
            ],
            "embedding_manifest": {
                "model": "bge-m3",
                "dimension": 1024,
                "active_model": "bge-m3",
                "active_dimension": 1024
            },
            "fail_soft": {
                "fts_to_vector_ready": true,
                "vector_to_fts_ready": true,
                "no_500_on_leg_failure": true
            },
            "lifecycle": {
                "rebuild_marker_ready": true,
                "metadata_repair_marker_ready": true
            },
            "incremental_update": {
                "ready": true,
                "upsert_ready": true,
                "delete_ready": true,
                "watermark_ready": true
            }
        })
    }

    fn table(name: &str, vector_ready: bool) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "fts_ready": true,
            "vector_ready": vector_ready,
            "row_count": 1,
            "blocker_codes": []
        })
    }

    fn nowledge_probe_rows() -> Vec<SearchProjectionRow> {
        vec![
            nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
            nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
            nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
            nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
            nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
            nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
        ]
    }

    fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: Some(vec![1.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        }
    }

    fn nowledge_probe_row_without_embedding(
        kind: SearchProjectionKind,
        external_id: &str,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            embedding: None,
            ..nowledge_probe_row(kind, external_id)
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
