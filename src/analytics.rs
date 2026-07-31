use crate::schema::RelTypeId;
use crate::store::{GraphStore, NodeId, NodeRecord};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraph {
    nodes: Vec<NodeId>,
    offsets: Vec<usize>,
    targets: Vec<usize>,
    incoming_offsets: Vec<usize>,
    incoming_sources: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankOptions {
    pub iterations: usize,
    pub damping: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankScore {
    pub node: NodeId,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LouvainOptions {
    pub max_iterations: usize,
    pub max_levels: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommunityAssignment {
    pub node: NodeId,
    pub community: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HierarchicalCommunityAssignment {
    pub level: usize,
    pub node: NodeId,
    pub community: NodeId,
}

impl Default for PageRankOptions {
    fn default() -> Self {
        Self {
            iterations: 20,
            damping: 0.85,
        }
    }
}

impl Default for LouvainOptions {
    fn default() -> Self {
        Self {
            max_iterations: 20,
            max_levels: 1,
        }
    }
}

impl ProjectedGraph {
    pub fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            offsets: vec![0],
            targets: Vec::new(),
            incoming_offsets: vec![0],
            incoming_sources: Vec::new(),
        }
    }

    pub fn from_parts(
        nodes: Vec<NodeId>,
        offsets: Vec<usize>,
        targets: Vec<usize>,
        incoming_offsets: Vec<usize>,
        incoming_sources: Vec<usize>,
    ) -> std::result::Result<Self, String> {
        validate_offsets("csr_offsets", nodes.len(), &offsets, targets.len())?;
        validate_offsets(
            "csc_offsets",
            nodes.len(),
            &incoming_offsets,
            incoming_sources.len(),
        )?;
        validate_indexes("csr_targets", nodes.len(), &targets)?;
        validate_indexes("csc_sources", nodes.len(), &incoming_sources)?;
        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
        })
    }

    pub fn from_store(store: &GraphStore, rel_type: Option<RelTypeId>) -> Self {
        Self::from_store_with_node_filter(store, rel_type, |_| true)
    }

    pub fn from_store_with_node_filter(
        store: &GraphStore,
        rel_type: Option<RelTypeId>,
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self {
        let nodes = store
            .scan_nodes(None)
            .filter(|node| include_node(node))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::from_nodes_and_relationships(store, nodes, move |relationship| {
            rel_type
                .map(|rel_type| relationship.rel_type == rel_type)
                .unwrap_or(true)
        })
    }

    pub fn from_store_labels_and_rel_types(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
        rel_types: &[RelTypeId],
    ) -> Self {
        Self::from_store_labels_and_rel_types_with_node_filter(store, labels, rel_types, |_| true)
    }

    pub fn from_store_labels_and_rel_types_with_node_filter(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
        rel_types: &[RelTypeId],
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self {
        let labels = labels.iter().copied().collect::<BTreeSet<_>>();
        let nodes = store
            .scan_nodes(None)
            .filter(|node| {
                (labels.is_empty() || node.labels.iter().any(|label| labels.contains(label)))
                    && include_node(node)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let rel_types = rel_types.iter().copied().collect::<BTreeSet<_>>();
        Self::from_nodes_and_relationships(store, nodes, move |relationship| {
            rel_types.is_empty() || rel_types.contains(&relationship.rel_type)
        })
    }

    pub fn from_store_without_edges(store: &GraphStore) -> Self {
        Self::from_store_without_edges_with_node_filter(store, |_| true)
    }

    pub fn from_store_without_edges_with_node_filter(
        store: &GraphStore,
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self {
        let nodes = store
            .scan_nodes(None)
            .filter(|node| include_node(node))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::from_nodes_without_edges(nodes)
    }

    pub fn from_store_labels_without_edges(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
    ) -> Self {
        Self::from_store_labels_without_edges_with_node_filter(store, labels, |_| true)
    }

    pub fn from_store_labels_without_edges_with_node_filter(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self {
        let labels = labels.iter().copied().collect::<BTreeSet<_>>();
        let nodes = store
            .scan_nodes(None)
            .filter(|node| {
                node.labels.iter().any(|label| labels.contains(label)) && include_node(node)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::from_nodes_without_edges(nodes)
    }

    fn from_nodes_without_edges(nodes: Vec<NodeId>) -> Self {
        let offsets = vec![0; nodes.len() + 1];
        Self {
            nodes,
            offsets: offsets.clone(),
            targets: Vec::new(),
            incoming_offsets: offsets,
            incoming_sources: Vec::new(),
        }
    }

    fn from_nodes_and_relationships(
        store: &GraphStore,
        nodes: Vec<NodeId>,
        include_relationship: impl Fn(&crate::store::RelRecord) -> bool,
    ) -> Self {
        let node_positions = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (*node, index))
            .collect::<BTreeMap<_, _>>();
        let mut adjacency = vec![Vec::new(); nodes.len()];
        let mut incoming = vec![Vec::new(); nodes.len()];

        for relationship in store.scan_relationships(None) {
            if !include_relationship(relationship) {
                continue;
            }
            let Some(source) = node_positions.get(&relationship.source).copied() else {
                continue;
            };
            let Some(target) = node_positions.get(&relationship.target).copied() else {
                continue;
            };
            adjacency[source].push(target);
            incoming[target].push(source);
        }

        let (offsets, targets) = build_compressed_adjacency(adjacency);
        let (incoming_offsets, incoming_sources) = build_compressed_adjacency(incoming);

        Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.targets.len()
    }

    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    pub fn csr_offsets(&self) -> &[usize] {
        &self.offsets
    }

    pub fn csr_targets(&self) -> &[usize] {
        &self.targets
    }

    pub fn csc_offsets(&self) -> &[usize] {
        &self.incoming_offsets
    }

    pub fn csc_sources(&self) -> &[usize] {
        &self.incoming_sources
    }

    pub fn outgoing_targets(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.outgoing_target_indexes(index)
                .map(|target| self.nodes[target]),
        )
    }

    pub fn incoming_sources(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.incoming_source_indexes(index)
                .map(|source| self.nodes[source]),
        )
    }

    pub fn page_rank(&self, options: PageRankOptions) -> Vec<PageRankScore> {
        let node_count = self.nodes.len();
        if node_count == 0 {
            return Vec::new();
        }

        let damping = options.damping.clamp(0.0, 1.0);
        let mut ranks = vec![1.0 / node_count as f64; node_count];
        for _ in 0..options.iterations {
            let dangling = ranks
                .iter()
                .enumerate()
                .filter(|(index, _)| self.out_degree(*index) == 0)
                .map(|(_, rank)| rank)
                .sum::<f64>();
            let mut next = vec![(1.0 - damping) / node_count as f64; node_count];
            let dangling_share = damping * dangling / node_count as f64;
            for score in &mut next {
                *score += dangling_share;
            }

            for (source, rank) in ranks.iter().enumerate() {
                let out_degree = self.out_degree(source);
                if out_degree == 0 {
                    continue;
                }
                let contribution = damping * rank / out_degree as f64;
                for target in self.outgoing_target_indexes(source) {
                    next[target] += contribution;
                }
            }
            ranks = next;
        }

        let mut scores = self
            .nodes
            .iter()
            .copied()
            .zip(ranks)
            .map(|(node, score)| PageRankScore { node, score })
            .collect::<Vec<_>>();
        scores.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node.cmp(&right.node))
        });
        scores
    }

    pub fn louvain_communities(&self, options: LouvainOptions) -> Vec<CommunityAssignment> {
        self.single_level_louvain(options)
    }

    pub fn hierarchical_louvain_communities(
        &self,
        options: LouvainOptions,
    ) -> Vec<HierarchicalCommunityAssignment> {
        let max_levels = options.max_levels.max(1);
        let mut graph = self.clone();
        let mut original_to_current = (0..self.nodes.len()).collect::<Vec<_>>();
        let mut output = Vec::new();

        for level in 0..max_levels {
            let assignments = graph.single_level_louvain(LouvainOptions {
                max_levels: 1,
                ..options
            });
            if assignments.is_empty() {
                break;
            }
            for (original_index, current_index) in original_to_current.iter().copied().enumerate() {
                let community = assignments[current_index].community;
                output.push(HierarchicalCommunityAssignment {
                    level,
                    node: self.nodes[original_index],
                    community,
                });
            }
            if level + 1 == max_levels {
                break;
            }
            let contracted = graph.contract_by_communities(&assignments);
            if contracted.nodes.len() == graph.nodes.len() {
                break;
            }
            let contracted_positions = contracted
                .nodes
                .iter()
                .enumerate()
                .map(|(index, node)| (*node, index))
                .collect::<BTreeMap<_, _>>();
            original_to_current = original_to_current
                .into_iter()
                .map(|current_index| {
                    let community = assignments[current_index].community;
                    contracted_positions[&community]
                })
                .collect();
            graph = contracted;
        }

        output
    }

    fn single_level_louvain(&self, options: LouvainOptions) -> Vec<CommunityAssignment> {
        let node_count = self.nodes.len();
        if node_count == 0 {
            return Vec::new();
        }

        let adjacency = self.undirected_adjacency();
        let degrees = adjacency
            .iter()
            .map(|neighbors| neighbors.len() as f64)
            .collect::<Vec<_>>();
        let total_degree = degrees.iter().sum::<f64>();
        if total_degree == 0.0 {
            return self
                .nodes
                .iter()
                .copied()
                .map(|node| CommunityAssignment {
                    node,
                    community: node,
                })
                .collect();
        }

        let mut communities = (0..node_count).collect::<Vec<_>>();
        let mut community_degrees = degrees.clone();
        for _ in 0..options.max_iterations {
            let mut changed = false;
            for node in 0..node_count {
                let current = communities[node];
                let node_degree = degrees[node];
                community_degrees[current] -= node_degree;

                let mut candidates = BTreeSet::from([current]);
                for neighbor in &adjacency[node] {
                    candidates.insert(communities[*neighbor]);
                }

                let mut best = current;
                let mut best_gain = 0.0;
                for candidate in candidates {
                    let links_to_candidate = adjacency[node]
                        .iter()
                        .filter(|neighbor| communities[**neighbor] == candidate)
                        .count() as f64;
                    let gain = links_to_candidate
                        - (node_degree * community_degrees[candidate] / total_degree);
                    match gain.total_cmp(&best_gain) {
                        Ordering::Greater => {
                            best = candidate;
                            best_gain = gain;
                        }
                        Ordering::Equal
                            if community_representative(candidate, &communities, &self.nodes)
                                < community_representative(best, &communities, &self.nodes) =>
                        {
                            best = candidate;
                            best_gain = gain;
                        }
                        _ => {}
                    }
                }

                community_degrees[best] += node_degree;
                if best != current {
                    communities[node] = best;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let mut representatives = BTreeMap::<usize, NodeId>::new();
        for (index, community) in communities.iter().copied().enumerate() {
            representatives
                .entry(community)
                .and_modify(|node| *node = (*node).min(self.nodes[index]))
                .or_insert(self.nodes[index]);
        }

        self.nodes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, node)| CommunityAssignment {
                node,
                community: representatives[&communities[index]],
            })
            .collect()
    }

    fn contract_by_communities(&self, assignments: &[CommunityAssignment]) -> Self {
        let nodes = assignments
            .iter()
            .map(|assignment| assignment.community)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let node_positions = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (*node, index))
            .collect::<BTreeMap<_, _>>();
        let original_positions = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (*node, index))
            .collect::<BTreeMap<_, _>>();
        let mut adjacency = vec![Vec::new(); nodes.len()];
        let mut incoming = vec![Vec::new(); nodes.len()];

        for source in 0..self.nodes.len() {
            for target in self.outgoing_target_indexes(source) {
                let source_community = assignments[source].community;
                let target_community = assignments[target].community;
                if source_community == target_community {
                    continue;
                }
                let source_position = node_positions[&source_community];
                let target_position = node_positions[&target_community];
                adjacency[source_position].push(target_position);
                incoming[target_position].push(source_position);
            }
        }
        for assignment in assignments {
            debug_assert!(original_positions.contains_key(&assignment.node));
        }
        let (offsets, targets) = build_compressed_adjacency(adjacency);
        let (incoming_offsets, incoming_sources) = build_compressed_adjacency(incoming);
        Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
        }
    }

    fn out_degree(&self, index: usize) -> usize {
        self.offsets[index + 1] - self.offsets[index]
    }

    fn outgoing_target_indexes(&self, index: usize) -> impl Iterator<Item = usize> + '_ {
        self.targets[self.offsets[index]..self.offsets[index + 1]]
            .iter()
            .copied()
    }

    fn incoming_source_indexes(&self, index: usize) -> impl Iterator<Item = usize> + '_ {
        self.incoming_sources[self.incoming_offsets[index]..self.incoming_offsets[index + 1]]
            .iter()
            .copied()
    }

    fn undirected_adjacency(&self) -> Vec<BTreeSet<usize>> {
        let mut adjacency = vec![BTreeSet::new(); self.nodes.len()];
        for source in 0..self.nodes.len() {
            for target in self.outgoing_target_indexes(source) {
                if source == target {
                    continue;
                }
                adjacency[source].insert(target);
                adjacency[target].insert(source);
            }
        }
        adjacency
    }
}

fn community_representative(community: usize, assignments: &[usize], nodes: &[NodeId]) -> NodeId {
    assignments
        .iter()
        .enumerate()
        .filter_map(|(index, assigned)| (*assigned == community).then_some(nodes[index]))
        .min()
        .unwrap_or(nodes[community])
}

fn build_compressed_adjacency(mut adjacency: Vec<Vec<usize>>) -> (Vec<usize>, Vec<usize>) {
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut neighbors = Vec::new();
    offsets.push(0);
    for neighbors_for_node in &mut adjacency {
        neighbors_for_node.sort_unstable();
        neighbors_for_node.dedup();
        neighbors.extend(neighbors_for_node.iter().copied());
        offsets.push(neighbors.len());
    }
    (offsets, neighbors)
}

fn validate_offsets(
    name: &str,
    node_count: usize,
    offsets: &[usize],
    edge_count: usize,
) -> std::result::Result<(), String> {
    if offsets.len() != node_count + 1 {
        return Err(format!(
            "{name} length {} does not match node count {node_count}",
            offsets.len()
        ));
    }
    if offsets.first() != Some(&0) {
        return Err(format!("{name} must start at 0"));
    }
    if offsets.last() != Some(&edge_count) {
        return Err(format!("{name} must end at edge count {edge_count}"));
    }
    if offsets.windows(2).any(|window| window[0] > window[1]) {
        return Err(format!("{name} must be monotonic"));
    }
    Ok(())
}

fn validate_indexes(
    name: &str,
    node_count: usize,
    indexes: &[usize],
) -> std::result::Result<(), String> {
    if let Some(index) = indexes.iter().find(|index| **index >= node_count) {
        return Err(format!("{name} contains out-of-range index {index}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LouvainOptions, PageRankOptions, ProjectedGraph};
    use crate::schema::Catalog;
    use crate::store::{GraphStore, NodeId};
    use crate::Value;
    use std::collections::BTreeMap;

    #[test]
    fn projects_relationships_into_csr() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, target, source, "RELATED", BTreeMap::new())
            .unwrap();

        let mentions = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));

        assert_eq!(mentions.node_count(), 2);
        assert_eq!(mentions.edge_count(), 1);
        assert_eq!(
            mentions
                .outgoing_targets(source)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![target]
        );
        assert_eq!(
            mentions
                .incoming_sources(target)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![source]
        );
        assert!(mentions
            .incoming_sources(source)
            .unwrap()
            .collect::<Vec<_>>()
            .is_empty());
    }

    #[test]
    fn page_rank_orders_sink_higher_than_source() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, a, c, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, b, c, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let scores = graph.page_rank(PageRankOptions::default());
        let score_for = |node: NodeId| {
            scores
                .iter()
                .find(|score| score.node == node)
                .map(|score| score.score)
                .unwrap()
        };

        assert!(score_for(c) > score_for(b));
        assert!(score_for(b) > score_for(a));
    }

    #[test]
    fn louvain_groups_disconnected_pairs_deterministically() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        let d = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 4)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, c, d, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let communities = graph
            .louvain_communities(LouvainOptions::default())
            .into_iter()
            .map(|assignment| (assignment.node, assignment.community))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(communities[&a], a);
        assert_eq!(communities[&b], a);
        assert_eq!(communities[&c], c);
        assert_eq!(communities[&d], c);
        assert_ne!(communities[&a], communities[&c]);
    }

    #[test]
    fn louvain_keeps_edgeless_nodes_in_singleton_communities() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();

        let graph = ProjectedGraph::from_store_without_edges(&store);
        let communities = graph.louvain_communities(LouvainOptions::default());

        assert_eq!(
            communities,
            vec![
                super::CommunityAssignment {
                    node: a,
                    community: a,
                },
                super::CommunityAssignment {
                    node: b,
                    community: b,
                },
            ]
        );
    }

    #[test]
    fn hierarchical_louvain_emits_stable_levels() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        let d = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 4)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, c, d, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let assignments = graph.hierarchical_louvain_communities(LouvainOptions {
            max_iterations: 20,
            max_levels: 2,
        });

        assert_eq!(assignments.len(), 8);
        assert!(assignments.iter().any(|assignment| assignment.level == 0
            && assignment.node == b
            && assignment.community == a));
        assert!(assignments.iter().any(|assignment| assignment.level == 1
            && assignment.node == d
            && assignment.community == c));
    }

    fn properties(values: &[(&str, i64)]) -> BTreeMap<String, Value> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), Value::Int(*value)))
            .collect()
    }
}
