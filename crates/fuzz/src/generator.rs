use crate::{FuzzCase, GraphTlpCase, Mutation, Parameters, QueryInvocation, ResultSemantics};
use skein::Value;

const ENTITY_COUNT: usize = 6;

#[derive(Debug, Clone, Copy)]
pub(crate) struct StateAwareCaseGenerator {
    rng: DeterministicRng,
}

impl StateAwareCaseGenerator {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: DeterministicRng::new(seed),
        }
    }

    pub(crate) fn case(&mut self, index: usize) -> FuzzCase {
        let seed = self.rng.next_u64();
        let index_enabled = seed & 1 == 0;
        let graph = GeneratedGraphState::from_seed(seed);
        let query = graph.plan_differential_query(seed, index);

        FuzzCase {
            seed,
            shape: query.name.clone(),
            mutations: graph.mutations(index_enabled),
            query: query.invocation,
            graph_tlp: graph.graph_tlp_case(seed),
            index_enabled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GeneratedGraphState {
    memory_count: usize,
}

impl GeneratedGraphState {
    fn from_seed(seed: u64) -> Self {
        Self {
            memory_count: 12 + ((seed >> 8) as usize % 5),
        }
    }

    fn mutations(self, index_enabled: bool) -> Vec<Mutation> {
        let mut mutations = Vec::new();
        for index in 0..self.memory_count {
            let kind = memory_kind(index);
            let optional_note = match index % 3 {
                0 => ", optional_note: null",
                1 => ", optional_note: 'present'",
                _ => "",
            };
            let optional_score = match index % 3 {
                0 => ", optional_score: null".to_string(),
                1 => format!(", optional_score: {index}"),
                _ => String::new(),
            };
            mutations.push(Mutation::new(format!(
                "CREATE (:Memory {{id: 'mem-{index}', kind: '{kind}', title: 'Memory {index}', importance: {index}{optional_note}{optional_score}}})"
            )));
        }
        for index in 0..ENTITY_COUNT {
            mutations.push(Mutation::new(format!(
                "CREATE (:Entity {{id: 'entity-{index}', name: 'Entity {index}'}})"
            )));
        }
        for index in 0..self.memory_count {
            mutations.push(Mutation::new(format!(
                "MATCH (m:Memory {{id: 'mem-{index}'}}), (e:Entity {{id: 'entity-{}'}}) CREATE (m)-[:MENTIONS {{weight: {}}}]->(e)",
                index % ENTITY_COUNT,
                index % 4,
            )));
        }
        mutations.push(Mutation::new(
            "MATCH (source:Entity {id: 'entity-0'}), (target:Entity {id: 'entity-0'}) CREATE (source)-[:RELATES_TO {weight: 0}]->(target)",
        ));
        for weight in [1, 2] {
            mutations.push(Mutation::new(format!(
                "MATCH (a:Entity {{id: 'entity-1'}}), (b:Entity {{id: 'entity-2'}}) CREATE (a)-[:RELATES_TO {{weight: {weight}}}]->(b)"
            )));
        }
        mutations.push(Mutation::new(
            "MATCH (a:Entity {id: 'entity-2'}), (b:Entity {id: 'entity-3'}) CREATE (a)-[:RELATES_TO {weight: 3}]->(b)",
        ));
        mutations.push(Mutation::new(
            "MATCH (a:Entity {id: 'entity-4'}), (b:Entity {id: 'entity-5'}) CREATE (a)-[:RELATES_TO {weight: null}]->(b)",
        ));
        mutations.push(Mutation::new(
            "MATCH (a:Entity {id: 'entity-5'}), (b:Entity {id: 'entity-4'}) CREATE (a)-[:RELATES_TO]->(b)",
        ));
        if index_enabled {
            mutations.push(Mutation::new("CREATE INDEX ON :Memory(id)"));
            mutations.push(Mutation::new("CREATE RANGE INDEX ON :Memory(importance)"));
        }
        mutations
    }

    fn plan_differential_query(self, seed: u64, index: usize) -> GeneratedQuery {
        let selected_memory = (seed as usize) % self.memory_count;
        let alternate_memory = ((seed >> 16) as usize) % self.memory_count;
        let kind = if seed & 2 == 0 { "note" } else { "thread" };
        let mut parameters = Parameters::new();

        let (name, cypher, result_semantics) = match index % crate::QUERY_SHAPE_COUNT {
            0 => (
                "node_scan",
                "MATCH (m:Memory) RETURN m.id AS id, m.kind AS kind ORDER BY id ASC",
                ResultSemantics::Ordered,
            ),
            1 => {
                parameters.insert("kind".to_string(), Value::String(kind.to_string()));
                (
                    "equality_filter",
                    "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.id AS id",
                    ResultSemantics::Bag,
                )
            }
            2 => {
                parameters.insert(
                    "ids".to_string(),
                    Value::List(vec![
                        memory_id(selected_memory),
                        memory_id(alternate_memory),
                        memory_id(selected_memory),
                    ]),
                );
                (
                    "in_filter",
                    "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id AS id ORDER BY id ASC",
                    ResultSemantics::Ordered,
                )
            }
            3 => {
                parameters.insert(
                    "minimum".to_string(),
                    Value::Int((seed % self.memory_count as u64) as i64),
                );
                (
                    "range_filter",
                    "MATCH (m:Memory) WHERE m.importance >= $minimum RETURN m.id AS id, m.importance AS importance ORDER BY importance ASC, id ASC",
                    ResultSemantics::Ordered,
                )
            }
            4 => {
                parameters.insert("id".to_string(), memory_id(selected_memory));
                (
                    "one_hop_expand",
                    "MATCH (m:Memory {id: $id})-[r:MENTIONS]->(e:Entity) RETURN m.id AS memory_id, e.id AS entity_id ORDER BY entity_id ASC",
                    ResultSemantics::Ordered,
                )
            }
            5 => (
                "self_loop",
                "MATCH (e:Entity)-[r:RELATES_TO]->(e) RETURN e.id AS id",
                ResultSemantics::Bag,
            ),
            6 => {
                parameters.insert("source".to_string(), entity_id(1));
                parameters.insert("target".to_string(), entity_id(2));
                (
                    "parallel_edges",
                    "MATCH (a:Entity {id: $source})-[r:RELATES_TO]->(b:Entity {id: $target}) RETURN a.id AS source, b.id AS target",
                    ResultSemantics::Bag,
                )
            }
            7 => (
                "cartesian_product",
                "MATCH (m:Memory), (e:Entity) RETURN m.id AS memory_id, e.id AS entity_id",
                ResultSemantics::Bag,
            ),
            8 => (
                "distinct_projection",
                "MATCH (m:Memory) RETURN DISTINCT m.kind AS kind ORDER BY kind ASC",
                ResultSemantics::Ordered,
            ),
            9 => (
                "aggregate",
                "MATCH (m:Memory) RETURN m.kind AS kind, count(m) AS count ORDER BY kind ASC",
                ResultSemantics::Ordered,
            ),
            10 => (
                "top_n",
                "MATCH (m:Memory) RETURN m.id AS id, m.importance AS importance ORDER BY importance DESC, id ASC LIMIT 5",
                ResultSemantics::Ordered,
            ),
            _ => (
                "missing_or_null",
                "MATCH (m:Memory) WHERE m.optional_note IS NULL RETURN m.id AS id ORDER BY id ASC",
                ResultSemantics::Ordered,
            ),
        };

        GeneratedQuery {
            name: name.to_string(),
            invocation: QueryInvocation {
                cypher: cypher.to_string(),
                parameters,
                result_semantics,
            },
        }
    }

    fn graph_tlp_case(self, seed: u64) -> GraphTlpCase {
        match (seed >> 3) % 3 {
            0 => GraphTlpBuilder::new(
                "nullable_node_property",
                "MATCH (m:Memory)",
                GeneratedPredicate::compare(
                    PropertyRef::new("m", "optional_note"),
                    ComparisonOperator::Equal,
                    "tlp_value",
                ),
                "m.id AS id, m.optional_note AS optional_note",
            )
            .with_parameter("tlp_value", Value::String("present".to_string()))
            .build(),
            1 => GraphTlpBuilder::new(
                "node_range",
                "MATCH (m:Memory)",
                GeneratedPredicate::compare(
                    PropertyRef::new("m", "optional_score"),
                    ComparisonOperator::GreaterThanOrEqual,
                    "tlp_value",
                ),
                "m.id AS id, m.optional_score AS optional_score",
            )
            .with_parameter(
                "tlp_value",
                Value::Int((seed % self.memory_count as u64) as i64),
            )
            .build(),
            _ => GraphTlpBuilder::new(
                "relationship_range",
                "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity)",
                GeneratedPredicate::compare(
                    PropertyRef::new("r", "weight"),
                    ComparisonOperator::GreaterThanOrEqual,
                    "tlp_value",
                ),
                "a.id AS source, b.id AS target, r.weight AS weight",
            )
            .with_parameter("tlp_value", Value::Int((seed % 4) as i64))
            .build(),
        }
    }
}

#[derive(Debug)]
struct GeneratedQuery {
    name: String,
    invocation: QueryInvocation,
}

#[derive(Debug)]
struct GraphTlpBuilder {
    name: &'static str,
    match_clause: &'static str,
    predicate: GeneratedPredicate,
    projection: &'static str,
    parameters: Parameters,
}

impl GraphTlpBuilder {
    fn new(
        name: &'static str,
        match_clause: &'static str,
        predicate: GeneratedPredicate,
        projection: &'static str,
    ) -> Self {
        Self {
            name,
            match_clause,
            predicate,
            projection,
            parameters: Parameters::new(),
        }
    }

    fn with_parameter(mut self, name: &str, value: Value) -> Self {
        self.parameters.insert(name.to_string(), value);
        self
    }

    fn build(self) -> GraphTlpCase {
        let query = |predicate: Option<&GeneratedPredicate>| QueryInvocation {
            cypher: match predicate {
                Some(predicate) => format!(
                    "{} WHERE {} RETURN {}",
                    self.match_clause,
                    predicate.render(),
                    self.projection,
                ),
                None => format!("{} RETURN {}", self.match_clause, self.projection),
            },
            parameters: self.parameters.clone(),
            result_semantics: ResultSemantics::Bag,
        };

        GraphTlpCase {
            name: self.name.to_string(),
            original: query(None),
            predicate_true: query(Some(&self.predicate)),
            predicate_false: query(Some(&self.predicate.clone().negated())),
            predicate_null: query(Some(&GeneratedPredicate::IsNull(self.predicate.property()))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PropertyRef {
    variable: &'static str,
    property: &'static str,
}

impl PropertyRef {
    const fn new(variable: &'static str, property: &'static str) -> Self {
        Self { variable, property }
    }

    fn render(self) -> String {
        format!("{}.{}", self.variable, self.property)
    }
}

#[derive(Debug, Clone, Copy)]
enum ComparisonOperator {
    Equal,
    GreaterThanOrEqual,
}

impl ComparisonOperator {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::GreaterThanOrEqual => ">=",
        }
    }
}

#[derive(Debug, Clone)]
enum GeneratedPredicate {
    Compare {
        property: PropertyRef,
        operator: ComparisonOperator,
        parameter: &'static str,
    },
    IsNull(PropertyRef),
    Not(Box<Self>),
}

impl GeneratedPredicate {
    const fn compare(
        property: PropertyRef,
        operator: ComparisonOperator,
        parameter: &'static str,
    ) -> Self {
        Self::Compare {
            property,
            operator,
            parameter,
        }
    }

    fn property(&self) -> PropertyRef {
        match self {
            Self::Compare { property, .. } | Self::IsNull(property) => *property,
            Self::Not(predicate) => predicate.property(),
        }
    }

    fn negated(self) -> Self {
        Self::Not(Box::new(self))
    }

    fn render(&self) -> String {
        match self {
            Self::Compare {
                property,
                operator,
                parameter,
            } => format!("{} {} ${parameter}", property.render(), operator.as_str()),
            Self::IsNull(property) => format!("{} IS NULL", property.render()),
            Self::Not(predicate) => format!("NOT ({})", predicate.render()),
        }
    }
}

fn memory_kind(index: usize) -> &'static str {
    if index.is_multiple_of(2) {
        "note"
    } else {
        "thread"
    }
}

fn memory_id(index: usize) -> Value {
    Value::String(format!("mem-{index}"))
}

fn entity_id(index: usize) -> Value {
    Value::String(format!("entity-{index}"))
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}
