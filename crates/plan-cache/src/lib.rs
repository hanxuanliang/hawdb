use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCacheStats {
    pub max_entries: Option<usize>,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct LfuCacheEntry<V> {
    value: V,
    frequency: u64,
    last_access_tick: u64,
}

#[derive(Debug, Clone)]
pub struct LfuCache<K, V> {
    max_entries: Option<usize>,
    entries: BTreeMap<K, LfuCacheEntry<V>>,
    access_tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl<K, V> LfuCache<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    pub fn new(max_entries: Option<usize>) -> Self {
        Self {
            max_entries,
            entries: BTreeMap::new(),
            access_tick: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        if self.max_entries == Some(0) {
            self.misses += 1;
            return None;
        }
        self.access_tick = self.access_tick.saturating_add(1);
        let Some(entry) = self.entries.get_mut(key) else {
            self.misses += 1;
            return None;
        };
        self.hits += 1;
        entry.frequency = entry.frequency.saturating_add(1);
        entry.last_access_tick = self.access_tick;
        Some(entry.value.clone())
    }

    pub fn insert(&mut self, key: K, value: V) {
        let Some(max_entries) = self.max_entries else {
            self.insert_entry(key, value);
            return;
        };
        if max_entries == 0 {
            return;
        }
        self.insert_entry(key, value);
        while self.entries.len() > max_entries {
            let Some(evicted) = self.lfu_victim_key() else {
                break;
            };
            if self.entries.remove(&evicted).is_some() {
                self.evictions += 1;
            }
        }
    }

    fn insert_entry(&mut self, key: K, value: V) {
        self.access_tick = self.access_tick.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.value = value;
            entry.frequency = entry.frequency.saturating_add(1);
            entry.last_access_tick = self.access_tick;
            return;
        }
        self.entries.insert(
            key,
            LfuCacheEntry {
                value,
                frequency: 1,
                last_access_tick: self.access_tick,
            },
        );
    }

    fn lfu_victim_key(&self) -> Option<K> {
        self.entries
            .iter()
            .min_by(|(left_key, left), (right_key, right)| {
                (left.frequency, left.last_access_tick, *left_key).cmp(&(
                    right.frequency,
                    right.last_access_tick,
                    *right_key,
                ))
            })
            .map(|(key, _)| key.clone())
    }

    pub fn stats(&self) -> PlanCacheStats {
        PlanCacheStats {
            max_entries: self.max_entries,
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LfuCache;

    #[test]
    fn lfu_cache_evicts_least_frequently_used_entry() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("hot", 1);
        cache.insert("cold", 2);

        assert_eq!(cache.get(&"hot"), Some(1));
        cache.insert("new", 3);

        assert_eq!(cache.get(&"hot"), Some(1));
        assert_eq!(cache.get(&"new"), Some(3));
        assert_eq!(cache.get(&"cold"), None);
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn lfu_cache_uses_oldest_access_as_tie_breaker() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("older", 1);
        cache.insert("newer", 2);
        cache.insert("third", 3);

        assert_eq!(cache.get(&"older"), None);
        assert_eq!(cache.get(&"newer"), Some(2));
        assert_eq!(cache.get(&"third"), Some(3));
    }

    #[test]
    fn zero_capacity_cache_records_misses_without_entries() {
        let mut cache = LfuCache::new(Some(0));
        cache.insert("ignored", 1);

        assert_eq!(cache.get(&"ignored"), None);
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().misses, 1);
    }
}
