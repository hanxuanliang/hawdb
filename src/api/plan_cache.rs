use crate::optimizer::{OptimizerTrace, PhysicalPlan};
use crate::value::Value;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_PLAN_CACHE_MAX_ENTRIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCacheStats {
    pub max_entries: Option<usize>,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PlanCacheKey {
    pub(crate) cypher: String,
    pub(crate) parameters: BTreeMap<String, Value>,
    pub(crate) graph_commit_epoch: u64,
    pub(crate) max_optimizer_groups: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CachedPlan {
    pub(crate) physical_plan: PhysicalPlan,
    pub(crate) trace: OptimizerTrace,
}

#[derive(Debug, Clone, PartialEq)]
struct PlanCacheEntry {
    plan: CachedPlan,
    frequency: u64,
    last_access_tick: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct PlanCache {
    max_entries: Option<usize>,
    entries: BTreeMap<PlanCacheKey, PlanCacheEntry>,
    access_tick: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl PlanCache {
    pub(crate) fn new(max_entries: Option<usize>) -> Self {
        Self {
            max_entries,
            entries: BTreeMap::new(),
            access_tick: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    pub(crate) fn get(&mut self, key: &PlanCacheKey) -> Option<CachedPlan> {
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
        Some(entry.plan.clone())
    }

    pub(crate) fn insert(&mut self, key: PlanCacheKey, plan: CachedPlan) {
        let Some(max_entries) = self.max_entries else {
            self.insert_entry(key, plan);
            return;
        };
        if max_entries == 0 {
            return;
        }
        self.insert_entry(key, plan);
        while self.entries.len() > max_entries {
            let Some(evicted) = self.lfu_victim_key() else {
                break;
            };
            if self.entries.remove(&evicted).is_some() {
                self.evictions += 1;
            }
        }
    }

    fn insert_entry(&mut self, key: PlanCacheKey, plan: CachedPlan) {
        self.access_tick = self.access_tick.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.plan = plan;
            entry.frequency = entry.frequency.saturating_add(1);
            entry.last_access_tick = self.access_tick;
            return;
        }
        self.entries.insert(
            key,
            PlanCacheEntry {
                plan,
                frequency: 1,
                last_access_tick: self.access_tick,
            },
        );
    }

    fn lfu_victim_key(&self) -> Option<PlanCacheKey> {
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

    pub(crate) fn stats(&self) -> PlanCacheStats {
        PlanCacheStats {
            max_entries: self.max_entries,
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }
}
