use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use crate::error::{Result, SkeinError};

static REGEX_CACHE: OnceLock<Mutex<BTreeMap<String, regex::Regex>>> = OnceLock::new();

pub(crate) fn validate_regex_pattern(pattern: &str) -> Result<()> {
    regex::Regex::new(pattern)
        .map(|_| ())
        .map_err(|error| SkeinError::Semantic(format!("invalid regex pattern: {error}")))
}

pub(crate) fn regex_is_match(pattern: &str, actual: &str) -> bool {
    let cache = REGEX_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut cache = cache.lock().expect("regex cache lock poisoned");
    let regex = match cache.get(pattern) {
        Some(regex) => regex.clone(),
        None => match regex::Regex::new(pattern) {
            Ok(regex) => {
                cache.insert(pattern.to_string(), regex.clone());
                regex
            }
            Err(_) => return false,
        },
    };
    drop(cache);
    regex.is_match(actual)
}
