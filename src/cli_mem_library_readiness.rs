use skein::{
    NowledgeGraphStatement, NowledgeMemEmbeddedStore, NowledgeMemGraphMode, NowledgeMemOpenOptions,
    NowledgeMemReadinessOptions, Result, SearchProjectionProbeOptions, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

pub fn nowledge_mem_library_readiness_usage() -> String {
    "nowledge-mem-library-readiness requires [--require-ready] [--mode shadow_read_only|writable_cutover] [--search-projection <path>] [--bounded-probe-json <path>] [--covered-routes-json <path>] [--query-family-evidence-json <path>] [--primary-search-projection-probe-json <path>] [--active-model <model>] [--active-dimension <n>] <graph-db>"
        .to_string()
}

pub fn run_nowledge_mem_library_readiness(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut mode = NowledgeMemGraphMode::ShadowReadOnly;
    let mut search_projection_path = None;
    let mut bounded_read_probe = None;
    let mut covered_routes = Vec::new();
    let mut replacement_readiness_by_query_family = None;
    let mut primary_search_projection_probe = None;
    let mut search_projection_probe_options = SearchProjectionProbeOptions::default();
    let mut graph_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--mode" => {
                mode = parse_mode(&args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_mem_library_readiness_usage())
                })?)?;
            }
            "--search-projection" => {
                search_projection_path =
                    Some(args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_mem_library_readiness_usage())
                    })?);
            }
            "--bounded-probe-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
                bounded_read_probe = Some(parse_bounded_probe_json(&read_json_file(Path::new(
                    &path,
                ))?)?);
            }
            "--covered-routes-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
                covered_routes.extend(parse_covered_routes_json(&read_json_file(Path::new(
                    &path,
                ))?)?);
            }
            "--query-family-evidence-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
                replacement_readiness_by_query_family = Some(read_json_file(Path::new(&path))?);
            }
            "--primary-search-projection-probe-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
                primary_search_projection_probe = Some(read_json_file(Path::new(&path))?);
            }
            "--active-model" => {
                search_projection_probe_options.active_embedding_model =
                    Some(args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_mem_library_readiness_usage())
                    })?);
            }
            "--active-dimension" => {
                let raw_dimension = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
                search_projection_probe_options.active_embedding_dimension =
                    Some(parse_positive_usize("--active-dimension", &raw_dimension)?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_mem_library_readiness_usage()));
            }
            path => {
                if graph_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_mem_library_readiness_usage()));
                }
            }
        }
    }

    let Some(graph_path) = graph_path else {
        return Err(SkeinError::Semantic(nowledge_mem_library_readiness_usage()));
    };
    let open_options = match search_projection_path {
        Some(path) => NowledgeMemOpenOptions::with_search_projection(graph_path, path, mode),
        None => NowledgeMemOpenOptions::graph_only(graph_path, mode),
    };
    let (mut store, open_report) = NowledgeMemEmbeddedStore::open_with_options(open_options)?;
    let options = NowledgeMemReadinessOptions {
        bounded_read_probe,
        covered_routes,
        replacement_readiness_by_query_family,
        search_projection_probe_options,
        primary_search_projection_probe,
        ..NowledgeMemReadinessOptions::default()
    };
    let mut readiness = store.library_readiness_json(&options);
    if let Some(object) = readiness.as_object_mut() {
        object.insert("open_report".to_string(), open_report.json());
    }
    Ok((readiness, require_ready))
}

fn parse_mode(raw: &str) -> Result<NowledgeMemGraphMode> {
    match raw {
        "shadow_read_only" => Ok(NowledgeMemGraphMode::ShadowReadOnly),
        "writable_cutover" => Ok(NowledgeMemGraphMode::WritableCutover),
        _ => Err(SkeinError::Semantic(format!(
            "invalid nowledge mem library readiness mode: {raw}"
        ))),
    }
}

fn parse_bounded_probe_json(value: &serde_json::Value) -> Result<NowledgeGraphStatement> {
    let object = value
        .as_object()
        .ok_or_else(|| SkeinError::Semantic("bounded probe JSON must be an object".to_string()))?;
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic("bounded probe JSON field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    Ok(NowledgeGraphStatement { cypher, parameters })
}

fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("bounded probe JSON field 'parameters' must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(format!(
                    "unsupported JSON number in bounded probe parameters: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn parse_covered_routes_json(value: &serde_json::Value) -> Result<Vec<String>> {
    if value.is_array() {
        return required_string_array_value(value, "covered routes JSON");
    }
    required_string_array(value, "covered_routes")
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "readiness JSON field '{field}' must be a string array"
            ))
        })?;
    required_string_array_items(items, field)
}

fn required_string_array_value(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "readiness JSON field '{field}' must be a string array"
        ))
    })?;
    required_string_array_items(items, field)
}

fn required_string_array_items(items: &[serde_json::Value], field: &str) -> Result<Vec<String>> {
    items
        .iter()
        .map(|item| {
            item.as_str().map(str::to_string).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "readiness JSON field '{field}' must be a string array"
                ))
            })
        })
        .collect()
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| SkeinError::Semantic(format!("{flag} must be a positive integer")))?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be a positive integer"
        )));
    }
    Ok(parsed)
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read nowledge mem library readiness JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse nowledge mem library readiness JSON: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_mem_library_readiness;
    use skein::{Database, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn library_readiness_command_emits_fail_closed_report() {
        let root = unique_test_dir("library-readiness");
        let graph_path = root.join("graph");
        let bounded_probe_path = root.join("bounded-probe.json");
        let covered_routes_path = root.join("covered-routes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-cli', title: 'CLI readiness'})")
            .unwrap();
        drop(db);
        std::fs::write(
            &bounded_probe_path,
            serde_json::json!({
                "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                "parameters": {
                    "id": "mem-cli"
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &covered_routes_path,
            serde_json::json!({
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            })
            .to_string(),
        )
        .unwrap();

        let (readiness, require_ready) = run_nowledge_mem_library_readiness(
            [
                "--bounded-probe-json",
                bounded_probe_path.to_str().unwrap(),
                "--covered-routes-json",
                covered_routes_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert!(!require_ready);
        assert_eq!(
            readiness["protocol"],
            "skein-nowledge-mem-library-readiness-v1"
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(
            readiness["readiness_by_area"]["query"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["query_family_evidence"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert_eq!(readiness["open_report"]["graph_opened"], true);
        assert_eq!(readiness["open_report"]["search_projection_opened"], false);
        assert!(readiness.get("graph_path").is_none());
        assert!(!readiness.to_string().contains(graph_path.to_str().unwrap()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn library_readiness_command_requires_graph_path() {
        let error = run_nowledge_mem_library_readiness([].into_iter().map(str::to_string))
            .expect_err("missing graph path should fail");

        assert!(error
            .to_string()
            .contains("nowledge-mem-library-readiness requires"));
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
