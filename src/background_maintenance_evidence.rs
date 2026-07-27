use crate::{background_maintenance_evidence_health, Result, SkeinError};
use std::path::Path;

pub fn nowledge_background_maintenance_evidence_usage() -> String {
    "nowledge-background-maintenance-evidence requires [--require-ready] [--optional] <background-maintenance-json>".to_string()
}

pub fn run_nowledge_background_maintenance_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut required = true;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--optional" => {
                required = false;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_background_maintenance_evidence_usage(),
                    ));
                }
                let summary = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_background_maintenance_evidence_json(&summary, required),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_background_maintenance_evidence_usage(),
    ))
}

pub fn nowledge_background_maintenance_evidence_json(
    summary: &serde_json::Value,
    required: bool,
) -> serde_json::Value {
    let health = background_maintenance_evidence_health(Some(summary), required);
    serde_json::json!({
        "protocol": "skein-nowledge-background-maintenance-evidence-v1",
        "required": health.required,
        "present": health.present,
        "ready": health.ready,
        "protocol_matches": health.protocol_matches,
        "total_candidates": health.total_candidates,
        "ranked_count": health.ranked_count,
        "executable_search_projection_graph_delta_count": health.executable_search_projection_graph_delta_count,
        "admitted_search_projection_graph_delta_count": health.admitted_search_projection_graph_delta_count,
        "deferred_search_projection_graph_delta_count": health.deferred_search_projection_graph_delta_count,
        "rejected_search_projection_graph_delta_count": health.rejected_search_projection_graph_delta_count,
        "executable_search_projection_graph_delta_operations": health.executable_search_projection_graph_delta_operations,
        "admitted_search_projection_graph_delta_operations": health.admitted_search_projection_graph_delta_operations,
        "max_search_projection_graph_delta_complete_through_graph_commit_epoch": health.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        "memory_pressure_ready": health.memory_pressure_ready,
        "memory_budget_bytes": health.memory_budget_bytes,
        "estimated_memory_bytes": health.estimated_memory_bytes,
        "foreground_ranked_count": health.foreground_ranked_count,
        "unknown_admission_count": health.unknown_admission_count,
        "blocker_codes": health.blocker_codes,
        "blockers": health.blockers,
        "background_maintenance_required": health.required,
        "background_maintenance_present": health.present,
        "background_maintenance_ready": health.ready,
        "background_maintenance_protocol_matches": health.protocol_matches,
        "background_maintenance_executable_search_projection_graph_delta_count": health.executable_search_projection_graph_delta_count,
        "background_maintenance_admitted_search_projection_graph_delta_count": health.admitted_search_projection_graph_delta_count,
        "background_maintenance_deferred_search_projection_graph_delta_count": health.deferred_search_projection_graph_delta_count,
        "background_maintenance_rejected_search_projection_graph_delta_count": health.rejected_search_projection_graph_delta_count,
        "background_maintenance_executable_search_projection_graph_delta_operations": health.executable_search_projection_graph_delta_operations,
        "background_maintenance_admitted_search_projection_graph_delta_operations": health.admitted_search_projection_graph_delta_operations,
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": health.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        "background_maintenance_memory_pressure_ready": health.memory_pressure_ready,
        "background_maintenance_memory_budget_bytes": health.memory_budget_bytes,
        "background_maintenance_estimated_memory_bytes": health.estimated_memory_bytes,
        "background_maintenance_blocker_codes": health.blocker_codes,
        "background_maintenance_blockers": health.blockers,
    })
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read background maintenance JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse background maintenance JSON: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_background_maintenance_evidence;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn background_maintenance_evidence_command_accepts_ready_summary() {
        let path = unique_test_file("background_maintenance_ready");
        std::fs::write(&path, ready_summary().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_background_maintenance_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-background-maintenance-evidence-v1"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["background_maintenance_required"], true);
        assert_eq!(evidence["background_maintenance_ready"], true);
        assert_eq!(
            evidence["background_maintenance_executable_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            evidence["background_maintenance_admitted_search_projection_graph_delta_operations"],
            3
        );
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!([])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_evidence_command_fails_closed_for_protocol_mismatch() {
        let path = unique_test_file("background_maintenance_protocol_mismatch");
        let mut summary = ready_summary();
        summary["protocol"] = serde_json::json!("unexpected-background-maintenance-report");
        std::fs::write(&path, summary.to_string()).unwrap();

        let (evidence, _) = run_nowledge_background_maintenance_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!(["protocol_mismatch"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_evidence_command_fails_closed_for_memory_pressure() {
        let path = unique_test_file("background_maintenance_memory_pressure");
        let mut summary = ready_summary();
        summary["memory_pressure"] = serde_json::json!({
            "ready": false,
            "budget_bytes": 4096,
            "estimated_bytes": 8192
        });
        std::fs::write(&path, summary.to_string()).unwrap();

        let (evidence, _) = run_nowledge_background_maintenance_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["memory_pressure_ready"], false);
        assert_eq!(evidence["memory_budget_bytes"], 4096);
        assert_eq!(evidence["estimated_memory_bytes"], 8192);
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!(["memory_budget_exceeded"])
        );
        std::fs::remove_file(path).unwrap();
    }

    fn ready_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-background-maintenance-report",
            "total_candidates": 1,
            "ranked": [
                {
                    "kind": "search_projection_graph_delta",
                    "name": "search_projection_graph_delta",
                    "work_class": "projection",
                    "priority": "background",
                    "admission": "admit",
                    "has_executable_search_projection_graph_delta": true,
                    "search_projection_graph_delta_operation_count": 3,
                    "search_projection_graph_delta_complete_through_graph_commit_epoch": 42
                }
            ]
        })
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}.json", std::process::id()))
    }
}
