use super::{
    parse_postgres_sql, SelectProjection, SqlColumnRef, SqlComparisonOp, SqlOrderDirection,
    SqlPredicate, SqlStatement, SqlTableName,
};
use skein_core::Value;

#[test]
fn parses_postgres_select_subset() {
    let statement = parse_postgres_sql(
        "SELECT query, elapsed_micros AS elapsed FROM system.slow_queries \
         WHERE start_time >= '2026-07-23T00:00:00Z' AND work_class IN ('query', 'shadow') \
        ORDER BY elapsed_micros DESC LIMIT 20 OFFSET 5",
    )
    .expect("valid PostgreSQL select");

    let SqlStatement::Select(select) = statement;
    assert_eq!(
        select.from,
        SqlTableName {
            schema: Some("system".to_string()),
            name: "slow_queries".to_string(),
        }
    );
    assert_eq!(
        select.projection,
        vec![
            SelectProjection::Column {
                name: SqlColumnRef {
                    qualifier: None,
                    name: "query".to_string(),
                },
                alias: None,
            },
            SelectProjection::Column {
                name: SqlColumnRef {
                    qualifier: None,
                    name: "elapsed_micros".to_string(),
                },
                alias: Some("elapsed".to_string()),
            },
        ]
    );
    assert_eq!(select.limit, Some(20));
    assert_eq!(select.offset, Some(5));
    assert_eq!(select.order_by[0].direction, SqlOrderDirection::Desc);

    let Some(SqlPredicate::And(left, right)) = select.selection else {
        panic!("expected conjunctive predicate");
    };
    assert_eq!(
        *left,
        SqlPredicate::Compare {
            left: SqlColumnRef {
                qualifier: None,
                name: "start_time".to_string(),
            },
            op: SqlComparisonOp::Gte,
            right: Value::String("2026-07-23T00:00:00Z".to_string()),
        }
    );
    assert_eq!(
        *right,
        SqlPredicate::InList {
            left: SqlColumnRef {
                qualifier: None,
                name: "work_class".to_string(),
            },
            values: vec![
                Value::String("query".to_string()),
                Value::String("shadow".to_string()),
            ],
            negated: false,
        }
    );
}

#[test]
fn normalizes_unquoted_identifiers_with_postgres_rules() {
    let statement = parse_postgres_sql(
        r#"SELECT "QueryText", ELAPSED_MICROS FROM SYSTEM.SLOW_QUERIES WHERE "QueryText" IS NOT NULL"#,
    )
    .expect("valid PostgreSQL select");

    let SqlStatement::Select(select) = statement;
    assert_eq!(
        select.from,
        SqlTableName {
            schema: Some("system".to_string()),
            name: "slow_queries".to_string(),
        }
    );
    assert_eq!(
        select.projection[0],
        SelectProjection::Column {
            name: SqlColumnRef {
                qualifier: None,
                name: "QueryText".to_string(),
            },
            alias: None,
        }
    );
    assert_eq!(
        select.projection[1],
        SelectProjection::Column {
            name: SqlColumnRef {
                qualifier: None,
                name: "elapsed_micros".to_string(),
            },
            alias: None,
        }
    );
}

#[test]
fn rejects_non_select_statements() {
    let error = parse_postgres_sql("DELETE FROM system.slow_queries")
        .expect_err("mutating SQL is intentionally unsupported");
    assert!(error
        .to_string()
        .contains("only PostgreSQL SELECT statements are supported"));
}

#[test]
fn rejects_joins_until_execution_model_exists() {
    let error = parse_postgres_sql(
        "SELECT * FROM system.slow_queries q JOIN system.plan_cache p ON q.digest = p.digest",
    )
    .expect_err("joins are not part of the first SQL subset");
    assert!(error.to_string().contains("joins are not supported yet"));
}
