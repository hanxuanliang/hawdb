use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::error::{Result, SkeinError};

const MAX_REGEX_CACHE_ENTRIES: usize = 64;
const MAX_REGEX_CACHE_PATTERN_BYTES: usize = 64 * 1024;
const MAX_REGEX_COMPILED_BYTES: usize = 256 * 1024;
const MAX_REGEX_DFA_BYTES: usize = 256 * 1024;

static REGEX_CACHE: OnceLock<Mutex<RegexCache>> = OnceLock::new();

#[derive(Debug)]
struct CachedRegex {
    regex: Arc<regex::Regex>,
    last_used: u64,
    pattern_bytes: usize,
}

#[derive(Debug, Default)]
struct RegexCache {
    entries: BTreeMap<String, CachedRegex>,
    retained_pattern_bytes: usize,
    clock: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl RegexCache {
    fn get(&mut self, pattern: &str) -> Option<Arc<regex::Regex>> {
        self.clock = self.clock.saturating_add(1);
        match self.entries.get_mut(pattern) {
            Some(entry) => {
                entry.last_used = self.clock;
                self.hits = self.hits.saturating_add(1);
                Some(Arc::clone(&entry.regex))
            }
            None => {
                self.misses = self.misses.saturating_add(1);
                None
            }
        }
    }

    fn insert(&mut self, pattern: &str, regex: Arc<regex::Regex>) -> Arc<regex::Regex> {
        self.clock = self.clock.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(pattern) {
            entry.last_used = self.clock;
            return Arc::clone(&entry.regex);
        }
        let pattern_bytes = pattern.len();
        if MAX_REGEX_CACHE_ENTRIES == 0 || pattern_bytes > MAX_REGEX_CACHE_PATTERN_BYTES {
            return regex;
        }
        while self.entries.len() >= MAX_REGEX_CACHE_ENTRIES
            || self.retained_pattern_bytes.saturating_add(pattern_bytes)
                > MAX_REGEX_CACHE_PATTERN_BYTES
        {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(pattern, _)| pattern.clone())
            else {
                break;
            };
            let removed = self
                .entries
                .remove(&victim)
                .expect("selected regex cache victim remains present");
            self.retained_pattern_bytes = self
                .retained_pattern_bytes
                .saturating_sub(removed.pattern_bytes);
            self.evictions = self.evictions.saturating_add(1);
        }
        self.retained_pattern_bytes = self.retained_pattern_bytes.saturating_add(pattern_bytes);
        self.entries.insert(
            pattern.to_string(),
            CachedRegex {
                regex: Arc::clone(&regex),
                last_used: self.clock,
                pattern_bytes,
            },
        );
        regex
    }
}

pub(crate) fn validate_regex_pattern(pattern: &str) -> Result<()> {
    compiled_regex(pattern)
        .map(|_| ())
        .map_err(|error| SkeinError::Semantic(format!("invalid regex pattern: {error}")))
}

pub(crate) fn regex_is_match(pattern: &str, actual: &str) -> bool {
    compiled_regex(pattern).is_ok_and(|regex| regex.is_match(actual))
}

fn compiled_regex(pattern: &str) -> std::result::Result<Arc<regex::Regex>, regex::Error> {
    let cache = REGEX_CACHE.get_or_init(|| Mutex::new(RegexCache::default()));
    if let Some(regex) = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(pattern)
    {
        return Ok(regex);
    }
    let compiled = Arc::new(compile_regex(pattern)?);
    Ok(cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(pattern, compiled))
}

fn compile_regex(pattern: &str) -> std::result::Result<regex::Regex, regex::Error> {
    regex::RegexBuilder::new(pattern)
        .size_limit(MAX_REGEX_COMPILED_BYTES)
        .dfa_size_limit(MAX_REGEX_DFA_BYTES)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_is_bounded_and_evicts_the_least_recently_used_pattern() {
        let mut cache = RegexCache::default();
        for index in 0..MAX_REGEX_CACHE_ENTRIES {
            let pattern = format!("^pattern-{index}$");
            cache.insert(&pattern, Arc::new(regex::Regex::new(&pattern).unwrap()));
        }
        assert!(cache.get("^pattern-0$").is_some());
        let next = "^pattern-next$";
        cache.insert(next, Arc::new(regex::Regex::new(next).unwrap()));

        assert_eq!(cache.entries.len(), MAX_REGEX_CACHE_ENTRIES);
        assert!(cache.entries.contains_key("^pattern-0$"));
        assert!(!cache.entries.contains_key("^pattern-1$"));
        assert_eq!(cache.evictions, 1);
    }

    #[test]
    fn oversized_pattern_is_compiled_but_not_retained() {
        let mut cache = RegexCache::default();
        let pattern = "a".repeat(MAX_REGEX_CACHE_PATTERN_BYTES + 1);
        let regex = Arc::new(regex::Regex::new("^a$").unwrap());

        assert!(Arc::ptr_eq(
            &cache.insert(&pattern, Arc::clone(&regex)),
            &regex
        ));
        assert!(cache.entries.is_empty());
        assert_eq!(cache.retained_pattern_bytes, 0);
    }

    #[test]
    fn validation_populates_the_shared_cache_for_matching() {
        let pattern = "^bounded-(alpha|beta)$";
        validate_regex_pattern(pattern).unwrap();
        assert!(regex_is_match(pattern, "bounded-alpha"));
        assert!(!regex_is_match(pattern, "unbounded-alpha"));
    }
}
