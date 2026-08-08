use super::{SqlFuzzCase, SqlMutation, SqlQueryInvocation, SqlTlpCase, SQL_QUERY_SHAPE_COUNT};
use crate::ResultSemantics;
use skein::Value;

pub(super) fn generate_sql_case(seed: u64, index: usize, index_enabled: bool) -> SqlFuzzCase {
    let mut setup = vec![
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_groups (id BIGINT PRIMARY KEY, priority BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_rows (id BIGINT PRIMARY KEY, group_id BIGINT REFERENCES sql_fuzz_groups(id), bucket BIGINT NOT NULL, score BIGINT, tag TEXT)",
        ),
    ];
    for group in 0..4_i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_groups (id, priority, label) VALUES ($1, $2, $3)",
            vec![
                Value::Int(group),
                if group == 0 {
                    Value::Null
                } else {
                    Value::Int(group * 10)
                },
                Value::String(format!("group-{group}")),
            ],
        ));
    }
    let row_count = 12 + ((seed >> 8) as usize % 5);
    for row in 0..row_count {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_rows (id, group_id, bucket, score, tag) VALUES ($1, $2, $3, $4, $5)",
            vec![
                Value::Int(row as i64),
                if row.is_multiple_of(5) {
                    Value::Null
                } else {
                    Value::Int((row % 4) as i64)
                },
                Value::Int((row % 3) as i64),
                match row % 3 {
                    0 => Value::Null,
                    _ => Value::Int(row as i64),
                },
                match row % 3 {
                    0 => Value::Null,
                    1 => Value::String("alpha".to_string()),
                    _ => Value::String("beta".to_string()),
                },
            ],
        ));
    }
    if index_enabled {
        setup.extend([
            SqlMutation::index("CREATE INDEX sql_fuzz_rows_score_idx ON sql_fuzz_rows (score, id)"),
            SqlMutation::index("CREATE INDEX sql_fuzz_rows_tag_idx ON sql_fuzz_rows (tag, id)"),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_rows_group_idx ON sql_fuzz_rows (group_id, id)",
            ),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_groups_priority_idx ON sql_fuzz_groups (priority, id)",
            ),
        ]);
    }

    let specification = sql_query_spec(seed, index);
    SqlFuzzCase {
        seed,
        shape: specification.name.to_string(),
        setup,
        row_tlp: specification.build(false),
        aggregate_tlp: specification.build(true),
        index_enabled,
    }
}

#[derive(Debug)]
struct SqlQuerySpec {
    name: &'static str,
    from: &'static str,
    projection: &'static str,
    predicate: String,
    null_predicate: String,
    predicate_parameters: Vec<Value>,
    null_parameters: Vec<Value>,
}

impl SqlQuerySpec {
    fn build(&self, aggregate: bool) -> SqlTlpCase {
        let projection = if aggregate {
            "COUNT(*) AS count"
        } else {
            self.projection
        };
        let query = |predicate: Option<&str>, parameters: Vec<Value>| SqlQueryInvocation {
            sql: match predicate {
                Some(predicate) => {
                    format!("SELECT {projection} FROM {} WHERE {predicate}", self.from)
                }
                None => format!("SELECT {projection} FROM {}", self.from),
            },
            parameters,
            result_semantics: ResultSemantics::Bag,
        };
        SqlTlpCase {
            name: if aggregate {
                format!("{}_count", self.name)
            } else {
                self.name.to_string()
            },
            original: query(None, Vec::new()),
            predicate_true: query(Some(&self.predicate), self.predicate_parameters.clone()),
            predicate_false: query(
                Some(&format!("NOT ({})", self.predicate)),
                self.predicate_parameters.clone(),
            ),
            predicate_null: query(Some(&self.null_predicate), self.null_parameters.clone()),
        }
    }
}

fn sql_query_spec(seed: u64, index: usize) -> SqlQuerySpec {
    match index % SQL_QUERY_SHAPE_COUNT {
        0 => SqlQuerySpec {
            name: "nullable_score_range",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score >= $1".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: vec![Value::Int((seed % 12) as i64)],
            null_parameters: Vec::new(),
        },
        1 => SqlQuerySpec {
            name: "nullable_tag_equality",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.tag AS value",
            predicate: "r.tag = $1".to_string(),
            null_predicate: "r.tag IS NULL".to_string(),
            predicate_parameters: vec![Value::String(if seed & 2 == 0 {
                "alpha".to_string()
            } else {
                "beta".to_string()
            })],
            null_parameters: Vec::new(),
        },
        2 => SqlQuerySpec {
            name: "inner_join_nullable_priority",
            from: "sql_fuzz_rows AS r INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id",
            projection: "r.bucket AS bucket, g.priority AS value",
            predicate: "g.priority >= $1".to_string(),
            null_predicate: "g.priority IS NULL".to_string(),
            predicate_parameters: vec![Value::Int(((seed % 3) as i64 + 1) * 10)],
            null_parameters: Vec::new(),
        },
        3 => SqlQuerySpec {
            name: "left_join_nullable_priority",
            from: "sql_fuzz_rows AS r LEFT JOIN sql_fuzz_groups AS g ON g.id = r.group_id",
            projection: "r.bucket AS bucket, g.priority AS value",
            predicate: "g.priority >= $1".to_string(),
            null_predicate: "g.priority IS NULL".to_string(),
            predicate_parameters: vec![Value::Int(((seed % 3) as i64 + 1) * 10)],
            null_parameters: Vec::new(),
        },
        4 => SqlQuerySpec {
            name: "nullable_score_in_list",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score IN ($1, $2)".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: vec![
                Value::Int((seed % 12) as i64),
                Value::Int(((seed >> 4) % 12) as i64),
            ],
            null_parameters: Vec::new(),
        },
        5 => SqlQuerySpec {
            name: "nullable_column_comparison",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score >= r.bucket".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: Vec::new(),
            null_parameters: Vec::new(),
        },
        6 => {
            let bucket = Value::Int(0);
            SqlQuerySpec {
                name: "nullable_conjunction",
                from: "sql_fuzz_rows AS r",
                projection: "r.bucket AS bucket, r.score AS value",
                predicate: "r.score >= $1 AND r.bucket = $2".to_string(),
                null_predicate: "r.score IS NULL AND r.bucket = $1".to_string(),
                predicate_parameters: vec![Value::Int((seed % 12) as i64), bucket.clone()],
                null_parameters: vec![bucket],
            }
        }
        _ => {
            let bucket = Value::Int(1);
            SqlQuerySpec {
                name: "nullable_disjunction",
                from: "sql_fuzz_rows AS r",
                projection: "r.bucket AS bucket, r.score AS value",
                predicate: "r.score >= $1 OR r.bucket = $2".to_string(),
                null_predicate: "r.score IS NULL AND NOT (r.bucket = $1)".to_string(),
                predicate_parameters: vec![Value::Int((seed % 12) as i64), bucket.clone()],
                null_parameters: vec![bucket],
            }
        }
    }
}
