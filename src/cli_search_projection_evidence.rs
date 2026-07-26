pub use skein::search_projection_evidence::nowledge_search_projection_probe_contract_json;
use skein::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
};
use skein::{Result, SearchIndex, SearchProjectionProbeOptions, SkeinError};
use std::path::Path;

pub fn nowledge_search_projection_evidence_usage() -> String {
    "nowledge-search-projection-evidence requires [--require-ready] <search-projection-probe-json>"
        .to_string()
}

pub fn skein_search_projection_probe_usage() -> String {
    "skein-search-projection-probe requires [--active-model <model>] [--active-dimension <dimension>] <search-index-dir>"
        .to_string()
}

pub fn nowledge_search_projection_shadow_evidence_usage() -> String {
    "nowledge-search-projection-shadow-evidence requires [--require-ready] --primary-probe-json <path> --shadow-probe-json <path>"
        .to_string()
}

pub fn nowledge_search_projection_probe_contract_usage() -> String {
    "nowledge-search-projection-probe-contract requires no arguments".to_string()
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

pub fn run_nowledge_search_projection_shadow_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut primary_probe = None;
    let mut shadow_probe = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--primary-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                primary_probe = Some(read_json_file(Path::new(&path))?);
            }
            "--shadow-probe-json" => {
                let path = args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage())
                })?;
                shadow_probe = Some(read_json_file(Path::new(&path))?);
            }
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_search_projection_shadow_evidence_usage(),
                ));
            }
        }
    }
    let primary_probe = primary_probe
        .ok_or_else(|| SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    let shadow_probe = shadow_probe
        .ok_or_else(|| SkeinError::Semantic(nowledge_search_projection_shadow_evidence_usage()))?;
    Ok((
        nowledge_search_projection_shadow_evidence_json(&primary_probe, &shadow_probe),
        require_ready,
    ))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read search projection evidence JSON: {error}"
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse search projection evidence JSON: {error}"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_contract_delegates_to_library_contract() {
        let contract = nowledge_search_projection_probe_contract_json();

        assert_eq!(
            contract["protocol"],
            "skein-nowledge-search-projection-probe-contract-v1"
        );
    }
}
