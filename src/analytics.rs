use crate::schema::RelTypeId;
use crate::store::{GraphStore, NodeId, NodeRecord};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionLayout {
    Outgoing,
    Incoming,
    Bidirectional,
    Undirected,
}

impl ProjectionLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
            Self::Bidirectional => "bidirectional",
            Self::Undirected => "undirected",
        }
    }

    fn stores_outgoing(self) -> bool {
        matches!(
            self,
            Self::Outgoing | Self::Bidirectional | Self::Undirected
        )
    }

    fn stores_incoming(self) -> bool {
        matches!(self, Self::Incoming | Self::Bidirectional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryBudget {
    max_bytes: Option<NonZeroUsize>,
}

impl ProjectionMemoryBudget {
    pub const fn unlimited() -> Self {
        Self { max_bytes: None }
    }

    pub const fn new(max_bytes: NonZeroUsize) -> Self {
        Self {
            max_bytes: Some(max_bytes),
        }
    }

    pub fn max_bytes(self) -> Option<usize> {
        self.max_bytes.map(NonZeroUsize::get)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryEstimate {
    pub layout: ProjectionLayout,
    pub node_count: usize,
    pub relationship_count: usize,
    pub projected_edge_count: usize,
    pub estimated_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionMemoryAdmissionError {
    pub estimate: ProjectionMemoryEstimate,
    pub budget_bytes: usize,
}

impl Display for ProjectionMemoryAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "analytics projection layout '{}' requires an estimated {} bytes for {} nodes and {} relationships, exceeding the {} byte budget",
            self.estimate.layout.as_str(),
            self.estimate.estimated_bytes,
            self.estimate.node_count,
            self.estimate.relationship_count,
            self.budget_bytes,
        )
    }
}

impl std::error::Error for ProjectionMemoryAdmissionError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraph {
    nodes: Vec<NodeId>,
    offsets: Vec<usize>,
    targets: Vec<usize>,
    incoming_offsets: Option<Vec<usize>>,
    incoming_sources: Option<Vec<usize>>,
    layout: ProjectionLayout,
    edge_count: usize,
    memory_estimate: ProjectionMemoryEstimate,
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

enum UndirectedNeighborIndexes<'a> {
    Projected(std::iter::Copied<std::slice::Iter<'a, usize>>),
    Materialized(std::iter::Copied<std::collections::btree_set::Iter<'a, usize>>),
}

impl Iterator for UndirectedNeighborIndexes<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Projected(iter) => iter.next(),
            Self::Materialized(iter) => iter.next(),
        }
    }
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
        Self::from_nodes_without_edges(Vec::new(), ProjectionLayout::Bidirectional)
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
        let relationship_count = targets.len();
        let memory_estimate = projection_memory_estimate(
            ProjectionLayout::Bidirectional,
            nodes.len(),
            relationship_count,
        );
        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets: Some(incoming_offsets),
            incoming_sources: Some(incoming_sources),
            layout: ProjectionLayout::Bidirectional,
            edge_count: relationship_count,
            memory_estimate,
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
        Self::try_from_store_with_node_filter_and_layout(
            store,
            rel_type,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_with_node_filter_and_layout(
        store: &GraphStore,
        rel_type: Option<RelTypeId>,
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let nodes = store
            .scan_nodes(None)
            .filter(|node| include_node(node))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::try_from_nodes_and_relationships(
            store,
            nodes,
            move |relationship| {
                rel_type
                    .map(|rel_type| relationship.rel_type == rel_type)
                    .unwrap_or(true)
            },
            layout,
            budget,
        )
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
        Self::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
            store,
            labels,
            rel_types,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_labels_and_rel_types_with_node_filter_and_layout(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
        rel_types: &[RelTypeId],
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
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
        Self::try_from_nodes_and_relationships(
            store,
            nodes,
            move |relationship| rel_types.is_empty() || rel_types.contains(&relationship.rel_type),
            layout,
            budget,
        )
    }

    pub fn from_store_without_edges(store: &GraphStore) -> Self {
        Self::from_store_without_edges_with_node_filter(store, |_| true)
    }

    pub fn from_store_without_edges_with_node_filter(
        store: &GraphStore,
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self {
        Self::try_from_store_without_edges_with_node_filter_and_layout(
            store,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_without_edges_with_node_filter_and_layout(
        store: &GraphStore,
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let nodes = store
            .scan_nodes(None)
            .filter(|node| include_node(node))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::try_from_nodes_without_edges(nodes, layout, budget)
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
        Self::try_from_store_labels_without_edges_with_node_filter_and_layout(
            store,
            labels,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_labels_without_edges_with_node_filter_and_layout(
        store: &GraphStore,
        labels: &[crate::schema::LabelId],
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let labels = labels.iter().copied().collect::<BTreeSet<_>>();
        let nodes = store
            .scan_nodes(None)
            .filter(|node| {
                node.labels.iter().any(|label| labels.contains(label)) && include_node(node)
            })
            .map(|node| node.id)
            .collect::<Vec<_>>();
        Self::try_from_nodes_without_edges(nodes, layout, budget)
    }

    fn from_nodes_without_edges(nodes: Vec<NodeId>, layout: ProjectionLayout) -> Self {
        Self::try_from_nodes_without_edges(nodes, layout, ProjectionMemoryBudget::unlimited())
            .expect("unlimited analytics projection is admitted")
    }

    fn try_from_nodes_without_edges(
        nodes: Vec<NodeId>,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let memory_estimate = projection_memory_estimate(layout, nodes.len(), 0);
        admit_projection(memory_estimate, budget)?;
        let offsets = vec![0; nodes.len() + 1];
        let incoming_offsets = layout.stores_incoming().then(|| offsets.clone());
        Ok(Self {
            nodes,
            offsets,
            targets: Vec::new(),
            incoming_offsets,
            incoming_sources: layout.stores_incoming().then(Vec::new),
            layout,
            edge_count: 0,
            memory_estimate,
        })
    }

    fn try_from_nodes_and_relationships(
        store: &GraphStore,
        nodes: Vec<NodeId>,
        include_relationship: impl Fn(&crate::store::RelRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let relationship_count = store
            .scan_relationships(None)
            .filter(|relationship| include_relationship(relationship))
            .filter(|relationship| {
                nodes.binary_search(&relationship.source).is_ok()
                    && nodes.binary_search(&relationship.target).is_ok()
            })
            .count();
        let memory_estimate = projection_memory_estimate(layout, nodes.len(), relationship_count);
        admit_projection(memory_estimate, budget)?;

        let mut adjacency = layout
            .stores_outgoing()
            .then(|| vec![Vec::new(); nodes.len()]);
        let mut incoming = layout
            .stores_incoming()
            .then(|| vec![Vec::new(); nodes.len()]);

        for relationship in store.scan_relationships(None) {
            if !include_relationship(relationship) {
                continue;
            }
            let Ok(source) = nodes.binary_search(&relationship.source) else {
                continue;
            };
            let Ok(target) = nodes.binary_search(&relationship.target) else {
                continue;
            };
            if let Some(adjacency) = adjacency.as_mut() {
                adjacency[source].push(target);
                if layout == ProjectionLayout::Undirected && source != target {
                    adjacency[target].push(source);
                }
            }
            if let Some(incoming) = incoming.as_mut() {
                incoming[target].push(source);
            }
        }

        let (offsets, targets) = adjacency
            .map(build_compressed_adjacency)
            .unwrap_or_else(|| (vec![0; nodes.len() + 1], Vec::new()));
        let (incoming_offsets, incoming_sources) = incoming
            .map(build_compressed_adjacency)
            .map(|(offsets, sources)| (Some(offsets), Some(sources)))
            .unwrap_or((None, None));
        let edge_count = match layout {
            ProjectionLayout::Incoming => incoming_sources
                .as_deref()
                .map_or(0, |sources| sources.len()),
            ProjectionLayout::Undirected => (0..nodes.len())
                .map(|source| {
                    targets[offsets[source]..offsets[source + 1]]
                        .iter()
                        .filter(|target| source <= **target)
                        .count()
                })
                .sum(),
            ProjectionLayout::Outgoing | ProjectionLayout::Bidirectional => targets.len(),
        };

        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
            layout,
            edge_count,
            memory_estimate,
        })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edge_count
    }

    pub fn layout(&self) -> ProjectionLayout {
        self.layout
    }

    pub fn memory_estimate(&self) -> ProjectionMemoryEstimate {
        self.memory_estimate
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
        self.incoming_offsets.as_deref().unwrap_or(&[])
    }

    pub fn csc_sources(&self) -> &[usize] {
        self.incoming_sources.as_deref().unwrap_or(&[])
    }

    pub fn outgoing_targets(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        if !self.layout.stores_outgoing() {
            return None;
        }
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.outgoing_target_indexes(index)
                .map(|target| self.nodes[target]),
        )
    }

    pub fn incoming_sources(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        if !self.layout.stores_incoming() {
            return None;
        }
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.incoming_source_indexes(index)
                .map(|source| self.nodes[source]),
        )
    }

    pub fn page_rank(&self, options: PageRankOptions) -> Vec<PageRankScore> {
        if !self.layout.stores_outgoing() {
            return Vec::new();
        }
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

        let materialized_adjacency =
            (self.layout != ProjectionLayout::Undirected).then(|| self.undirected_adjacency());
        let degrees = (0..node_count)
            .map(|node| {
                self.undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                    .count() as f64
            })
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
                for neighbor in
                    self.undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                {
                    candidates.insert(communities[neighbor]);
                }

                let mut best = current;
                let mut best_gain = 0.0;
                for candidate in candidates {
                    let links_to_candidate = self
                        .undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                        .filter(|neighbor| communities[*neighbor] == candidate)
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
        let mut incoming = self
            .layout
            .stores_incoming()
            .then(|| vec![Vec::new(); nodes.len()]);

        let mut add_edge = |source: usize, target: usize| {
            let source_community = assignments[source].community;
            let target_community = assignments[target].community;
            if source_community == target_community {
                return;
            }
            let source_position = node_positions[&source_community];
            let target_position = node_positions[&target_community];
            adjacency[source_position].push(target_position);
            if let Some(incoming) = incoming.as_mut() {
                incoming[target_position].push(source_position);
            }
        };
        if self.layout == ProjectionLayout::Incoming {
            for target in 0..self.nodes.len() {
                for source in self.incoming_source_indexes(target) {
                    add_edge(source, target);
                }
            }
        } else {
            for source in 0..self.nodes.len() {
                for target in self.outgoing_target_indexes(source) {
                    add_edge(source, target);
                }
            }
        }
        for assignment in assignments {
            debug_assert!(original_positions.contains_key(&assignment.node));
        }
        let (offsets, targets) = build_compressed_adjacency(adjacency);
        let (incoming_offsets, incoming_sources) = incoming
            .map(build_compressed_adjacency)
            .map(|(offsets, sources)| (Some(offsets), Some(sources)))
            .unwrap_or((None, None));
        let edge_count = if self.layout == ProjectionLayout::Undirected {
            (0..nodes.len())
                .map(|source| {
                    targets[offsets[source]..offsets[source + 1]]
                        .iter()
                        .filter(|target| source <= **target)
                        .count()
                })
                .sum()
        } else {
            targets.len()
        };
        let memory_estimate = projection_memory_estimate(self.layout, nodes.len(), edge_count);
        Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
            layout: self.layout,
            edge_count,
            memory_estimate,
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
        let offsets = self
            .incoming_offsets
            .as_deref()
            .expect("incoming indexes require an incoming projection");
        self.incoming_sources
            .as_deref()
            .expect("incoming indexes require an incoming projection")
            [offsets[index]..offsets[index + 1]]
            .iter()
            .copied()
    }

    fn undirected_adjacency(&self) -> Vec<BTreeSet<usize>> {
        let mut adjacency = vec![BTreeSet::new(); self.nodes.len()];
        if self.layout == ProjectionLayout::Incoming {
            for target in 0..self.nodes.len() {
                for source in self.incoming_source_indexes(target) {
                    if source == target {
                        continue;
                    }
                    adjacency[source].insert(target);
                    adjacency[target].insert(source);
                }
            }
            return adjacency;
        }
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

    fn undirected_neighbor_indexes<'a>(
        &'a self,
        index: usize,
        materialized: Option<&'a [BTreeSet<usize>]>,
    ) -> UndirectedNeighborIndexes<'a> {
        if self.layout == ProjectionLayout::Undirected {
            return UndirectedNeighborIndexes::Projected(
                self.targets[self.offsets[index]..self.offsets[index + 1]]
                    .iter()
                    .copied(),
            );
        }
        UndirectedNeighborIndexes::Materialized(
            materialized.expect("directed projection requires an undirected view")[index]
                .iter()
                .copied(),
        )
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

fn projection_memory_estimate(
    layout: ProjectionLayout,
    node_count: usize,
    relationship_count: usize,
) -> ProjectionMemoryEstimate {
    let outgoing_edge_count = match layout {
        ProjectionLayout::Incoming => 0,
        ProjectionLayout::Undirected => relationship_count.saturating_mul(2),
        ProjectionLayout::Outgoing | ProjectionLayout::Bidirectional => relationship_count,
    };
    let incoming_edge_count = if layout.stores_incoming() {
        relationship_count
    } else {
        0
    };
    let projected_edge_count = outgoing_edge_count.saturating_add(incoming_edge_count);
    let direction_count =
        usize::from(layout.stores_outgoing()).saturating_add(usize::from(layout.stores_incoming()));
    let offset_direction_count = 1usize.saturating_add(usize::from(layout.stores_incoming()));
    let node_bytes = node_count.saturating_mul(std::mem::size_of::<NodeId>());
    let outer_adjacency_bytes = node_count
        .saturating_mul(std::mem::size_of::<Vec<usize>>())
        .saturating_mul(direction_count);
    let offset_bytes = node_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_mul(offset_direction_count);
    // Building compressed adjacency temporarily overlaps the per-node vectors
    // and their final compressed neighbor array, so account for both copies.
    let edge_bytes = projected_edge_count
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_mul(2);
    ProjectionMemoryEstimate {
        layout,
        node_count,
        relationship_count,
        projected_edge_count,
        estimated_bytes: node_bytes
            .saturating_add(outer_adjacency_bytes)
            .saturating_add(offset_bytes)
            .saturating_add(edge_bytes),
    }
}

fn admit_projection(
    estimate: ProjectionMemoryEstimate,
    budget: ProjectionMemoryBudget,
) -> std::result::Result<(), ProjectionMemoryAdmissionError> {
    if let Some(budget_bytes) = budget.max_bytes()
        && estimate.estimated_bytes > budget_bytes
    {
        return Err(ProjectionMemoryAdmissionError {
            estimate,
            budget_bytes,
        });
    }
    Ok(())
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
    use super::{
        LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
    };
    use crate::schema::Catalog;
    use crate::store::{GraphStore, NodeId};
    use crate::Value;
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;

    #[test]
    fn algorithm_layouts_only_materialize_required_directions() {
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
        let rel_type = catalog.rel_type_id("MENTIONS");

        let page_rank = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            rel_type,
            |_| true,
            ProjectionLayout::Outgoing,
            ProjectionMemoryBudget::unlimited(),
        )
        .unwrap();
        assert_eq!(page_rank.layout(), ProjectionLayout::Outgoing);
        assert_eq!(page_rank.edge_count(), 1);
        assert_eq!(page_rank.csr_targets().len(), 1);
        assert!(page_rank.csc_offsets().is_empty());
        assert!(page_rank.incoming_sources(target).is_none());

        let louvain = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            rel_type,
            |_| true,
            ProjectionLayout::Undirected,
            ProjectionMemoryBudget::unlimited(),
        )
        .unwrap();
        assert_eq!(louvain.layout(), ProjectionLayout::Undirected);
        assert_eq!(louvain.edge_count(), 1);
        assert_eq!(louvain.csr_targets().len(), 2);
        assert!(louvain.csc_offsets().is_empty());
        assert_eq!(
            louvain.louvain_communities(LouvainOptions::default()).len(),
            2
        );

        let bidirectional = ProjectedGraph::from_store(&store, rel_type);
        assert!(
            page_rank.memory_estimate().estimated_bytes
                < bidirectional.memory_estimate().estimated_bytes
        );
        assert!(
            louvain.memory_estimate().estimated_bytes
                < bidirectional.memory_estimate().estimated_bytes
        );
    }

    #[test]
    fn projection_memory_admission_fails_before_adjacency_allocation() {
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

        let error = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            catalog.rel_type_id("MENTIONS"),
            |_| true,
            ProjectionLayout::Outgoing,
            ProjectionMemoryBudget::new(NonZeroUsize::new(1).unwrap()),
        )
        .unwrap_err();

        assert_eq!(error.budget_bytes, 1);
        assert_eq!(error.estimate.layout, ProjectionLayout::Outgoing);
        assert_eq!(error.estimate.node_count, 2);
        assert_eq!(error.estimate.relationship_count, 1);
        assert!(error.estimate.estimated_bytes > error.budget_bytes);
    }

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
