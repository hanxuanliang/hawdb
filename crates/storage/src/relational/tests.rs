use super::*;

#[test]
fn snapshots_share_untouched_segments_and_keep_old_rows_visible() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let before = store.snapshot().expect("snapshot before insert");

    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_documents".to_string(),
                    rows: vec![document_row("doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert document");
    let after = store.snapshot().expect("snapshot after insert");

    assert_eq!(before.value().row_count("content_documents"), 0);
    assert_eq!(after.value().row_count("content_documents"), 1);
    assert!(Arc::ptr_eq(
        before.value().segments.get("content_anchors").unwrap(),
        after.value().segments.get("content_anchors").unwrap()
    ));
}

#[test]
fn durability_failure_does_not_publish_relational_rows() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");

    let result = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "content_documents".to_string(),
                rows: vec![document_row("doc-1")],
                mode: RelationalInsertMode::Error,
            }],
        },
        |_, _| Err(RelationalError::Durability("fsync failed".to_string())),
    );

    assert!(matches!(result, Err(SnapshotCommitError::Durability(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("published snapshot")
            .value()
            .row_count("content_documents"),
        0
    );
}

#[test]
fn table_without_primary_key_is_rejected_before_catalog_publication() {
    let store = RelationalStore::default();
    let error = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: Vec::new(),
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            |_, _| Ok(()),
        )
        .expect_err("primary-key-free tables must not enter the catalog");

    assert!(matches!(
        error,
        SnapshotCommitError::Stage(RelationalError::Schema(message))
            if message == "table documents must declare a primary key"
    ));
    assert!(store
        .snapshot()
        .expect("empty snapshot")
        .value()
        .table_schema("documents")
        .is_none());
}

#[test]
fn foreign_key_is_checked_against_final_atomic_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");

    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("target in the same batch is visible");

    assert_eq!(
        store
            .snapshot()
            .expect("snapshot")
            .value()
            .row_count("content_anchors"),
        1
    );
}

#[test]
fn referenced_delete_is_restricted_unless_child_is_removed_in_same_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".to_string(),
                        rows: vec![document_row("doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".to_string(),
                        rows: vec![anchor_row("anchor-1", "doc-1")],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("seed referenced rows");
    let document_key = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
    let anchor_key = RelationalKey(vec![RelationalValue::Text("anchor-1".to_string())]);

    let rejected = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "content_documents".to_string(),
                keys: vec![document_key.clone()],
            }],
        },
        |_, _| Ok(()),
    );
    assert!(matches!(rejected, Err(SnapshotCommitError::Stage(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot after rejected delete")
            .value()
            .row_count("content_documents"),
        1
    );

    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_documents".to_string(),
                        keys: vec![document_key],
                    },
                    RelationalWrite::DeleteByPrimaryKey {
                        table: "content_anchors".to_string(),
                        keys: vec![anchor_key],
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("delete parent and child atomically");
    let snapshot = store.snapshot().expect("snapshot after atomic delete");
    assert_eq!(snapshot.value().row_count("content_documents"), 0);
    assert_eq!(snapshot.value().row_count("content_anchors"), 0);
}

#[test]
fn unique_constraint_rejects_complete_batch() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let result = store.commit(
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "content_documents".to_string(),
                rows: vec![document_row("doc-1"), document_row("doc-1")],
                mode: RelationalInsertMode::Error,
            }],
        },
        |_, _| Ok(()),
    );
    assert!(matches!(result, Err(SnapshotCommitError::Stage(_))));
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot")
            .value()
            .row_count("content_documents"),
        0
    );
}

#[test]
fn large_payload_is_externalized_and_hydrated_with_explicit_budgets() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 16,
            compression_level: 3,
            max_value_bytes: 1024 * 1024,
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let payload = "compressible-content-".repeat(128);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text(payload.clone()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");

    let snapshot = store.snapshot().expect("snapshot");
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let stored = snapshot.value().row("messages", &key).expect("stored row");
    assert!(matches!(stored.values()[1], RelationalValue::Overflow(_)));
    assert_eq!(snapshot.value().overflow_segment_count(), 1);

    let mut budget = RelationalHydrationBudget::default();
    let hydrated = snapshot
        .value()
        .hydrate_row("messages", &key, &mut budget)
        .expect("bounded hydration")
        .expect("hydrated row");
    assert_eq!(hydrated.values()[1], RelationalValue::Text(payload.clone()));
    assert_eq!(budget.hydrated_rows, 1);
    assert_eq!(budget.decompressed_bytes, payload.len());

    let mut rejected_budget = RelationalHydrationBudget {
        max_decompressed_bytes: payload.len() - 1,
        ..RelationalHydrationBudget::default()
    };
    let error = snapshot
        .value()
        .hydrate_row("messages", &key, &mut rejected_budget)
        .expect_err("decompressed-byte budget must fail before allocation");
    assert!(matches!(error, RelationalError::Admission(_)));
    assert_eq!(rejected_budget.hydrated_rows, 0);
    assert_eq!(rejected_budget.decompressed_bytes, 0);
}

#[test]
fn add_column_rewrite_is_admitted_by_existing_resident_bytes() {
    let store = RelationalStore::new(RelationalMutationLimits {
        max_rows: NonZeroUsize::new(10).unwrap(),
        max_payload_bytes: NonZeroUsize::new(128).unwrap(),
    });
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("m1".to_string()),
                        RelationalValue::Text("payload".repeat(16)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert admitted payload");

    let error = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::AddColumn {
                    table: "messages".to_string(),
                    column: text_column("kind", true),
                }],
            },
            |_, _| Ok(()),
        )
        .expect_err("resident rewrite must honor the mutation byte budget");
    assert!(matches!(
        error,
        SnapshotCommitError::Stage(RelationalError::Admission(message))
            if message.contains("resident bytes")
    ));
    assert!(store
        .snapshot()
        .unwrap()
        .value()
        .table_schema("messages")
        .unwrap()
        .column_position("kind")
        .is_none());
}

#[test]
fn add_column_externalizes_one_shared_default_for_all_rewritten_rows() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 16,
            compression_level: 3,
            max_value_bytes: 1024 * 1024,
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![
                        RelationalRow::new(vec![
                            RelationalValue::Text("m1".to_string()),
                            RelationalValue::Text("first".to_string()),
                        ]),
                        RelationalRow::new(vec![
                            RelationalValue::Text("m2".to_string()),
                            RelationalValue::Text("second".to_string()),
                        ]),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert rows");

    let default = "shared-default-".repeat(128);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::AddColumn {
                    table: "messages".to_string(),
                    column: RelationalColumnSchema {
                        name: "kind".to_string(),
                        scalar_type: RelationalScalarType::Text,
                        nullable: false,
                        default: Some(RelationalValue::Text(default.clone())),
                    },
                }],
            },
            |_, _| Ok(()),
        )
        .expect("add column");

    let snapshot = store.snapshot().expect("snapshot");
    assert_eq!(snapshot.value().overflow_segment_count(), 1);
    for id in ["m1", "m2"] {
        let key = RelationalKey(vec![RelationalValue::Text(id.to_string())]);
        let row = snapshot
            .value()
            .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
            .expect("hydrate rewritten row")
            .expect("rewritten row");
        assert_eq!(row.values()[2], RelationalValue::Text(default.clone()));
    }
}

#[test]
fn row_mutation_clones_only_the_affected_cow_page() {
    let store = RelationalStore::default();
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let rows = (0..768)
        .map(|index| {
            RelationalRow::new(vec![
                RelationalValue::Text(format!("message-{index:04}")),
                RelationalValue::Text(format!("payload-{index}")),
            ])
        })
        .collect::<Vec<_>>();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed paged rows");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-9999".to_string()),
                        RelationalValue::Text("new payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("append one row");

    let old_rows = &pinned.value().segments["messages"].rows;
    let new_rows = &current.value().segments["messages"].rows;
    assert!(old_rows.page_count() >= 3);
    assert!(old_rows.shared_page_count(new_rows) >= old_rows.page_count() - 1);
    assert_eq!(old_rows.len(), 768);
    assert_eq!(new_rows.len(), 769);
    assert!(
        !old_rows.contains_key(&RelationalKey(vec![RelationalValue::Text(
            "message-9999".to_string()
        )]))
    );
}

#[test]
fn append_updates_only_the_affected_posting_page() {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![text_column("id", false), text_column("thread_id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_thread".to_string(),
                            columns: vec!["thread_id".to_string()],
                            unique: false,
                        },
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("create indexed table");
    let rows = (0..768)
        .map(|index| {
            RelationalRow::new(vec![
                RelationalValue::Text(format!("message-{index:04}")),
                RelationalValue::Text("thread-1".to_string()),
            ])
        })
        .collect::<Vec<_>>();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed posting pages");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-9999".to_string()),
                        RelationalValue::Text("thread-1".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("append indexed row");

    let index_key = RelationalKey(vec![RelationalValue::Text("thread-1".to_string())]);
    let old_postings = pinned.value().segments["messages"].indexes["idx_messages_thread"]
        .get(&index_key)
        .expect("old postings");
    let new_postings = current.value().segments["messages"].indexes["idx_messages_thread"]
        .get(&index_key)
        .expect("new postings");
    assert!(old_postings.page_count() >= 3);
    assert!(old_postings.shared_page_count(new_postings) >= old_postings.page_count() - 1);
    assert_eq!(old_postings.len, 768);
    assert_eq!(new_postings.len, 769);
}

#[test]
fn composite_index_prefix_lookup_is_bounded_and_preserves_leading_column_order() {
    let store = RelationalStore::default();
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "messages".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("space_id", false),
                            text_column("thread_id", false),
                            RelationalColumnSchema {
                                name: "order_index".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::CreateIndex {
                        table: "messages".to_string(),
                        index: RelationalIndexSchema {
                            name: "idx_messages_space_thread_order".to_string(),
                            columns: vec![
                                "space_id".to_string(),
                                "thread_id".to_string(),
                                "order_index".to_string(),
                            ],
                            unique: false,
                        },
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .expect("create composite index");
    let rows = [
        ("message-1", "default", "thread-1", 1),
        ("message-2", "default", "thread-1", 2),
        ("message-3", "default", "thread-2", 1),
        ("message-4", "private", "thread-1", 1),
    ]
    .into_iter()
    .map(|(id, space_id, thread_id, order_index)| {
        RelationalRow::new(vec![
            RelationalValue::Text(id.to_string()),
            RelationalValue::Text(space_id.to_string()),
            RelationalValue::Text(thread_id.to_string()),
            RelationalValue::BigInt(order_index),
        ])
    })
    .collect();
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("seed composite index");

    let snapshot = store.snapshot().expect("composite index snapshot");
    let prefix = RelationalKey(vec![
        RelationalValue::Text("default".to_string()),
        RelationalValue::Text("thread-1".to_string()),
    ]);
    assert_eq!(
        snapshot.value().index_prefix_cardinality(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
        ),
        Some(2)
    );
    assert_eq!(
        snapshot.value().index_prefix_cardinality_at_most(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            1,
        ),
        Some(1)
    );
    let keys = snapshot
        .value()
        .index_prefix_lookup("messages", "idx_messages_space_thread_order", &prefix, 1)
        .expect("prefix lookup");
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0],
        &RelationalKey(vec![RelationalValue::Text("message-1".to_string())])
    );
    assert_eq!(
        snapshot.value().index_prefix_lookup(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            0,
        ),
        Some(Vec::new())
    );

    let mut visited = Vec::new();
    snapshot
        .value()
        .visit_index_prefix_rows(
            "messages",
            "idx_messages_space_thread_order",
            &prefix,
            |primary_key, row| {
                visited.push((primary_key.clone(), row.values()[3].clone()));
                false
            },
        )
        .expect("streaming prefix lookup");
    assert_eq!(
        visited,
        vec![(
            RelationalKey(vec![RelationalValue::Text("message-1".to_string())]),
            RelationalValue::BigInt(1),
        )]
    );
}

#[test]
fn overflow_gc_prunes_new_snapshot_without_invalidating_old_snapshot() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");
    let pinned = store.snapshot().expect("pinned snapshot");

    let current = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "messages".to_string(),
                    keys: vec![key],
                }],
            },
            |_, _| Ok(()),
        )
        .expect("delete payload");

    assert_eq!(pinned.value().overflow_segment_count(), 1);
    assert_eq!(current.value().overflow_segment_count(), 0);
}

#[test]
fn overflow_digest_mismatch_fails_closed() {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    store
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".to_string()),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload");
    let snapshot = store.snapshot().expect("snapshot");
    let mut corrupt = snapshot.value().clone();
    let segment = corrupt
        .overflow_segments
        .values_mut()
        .next()
        .expect("overflow envelope");
    let RelationalOverflowSegment::Inline(envelope) = segment else {
        panic!("newly staged overflow must be inline");
    };
    let bytes = Arc::make_mut(envelope);
    bytes[bytes.len() - 1] ^= 0xff;
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);

    let error = corrupt
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect_err("corrupt overflow must fail closed");
    assert!(matches!(error, RelationalError::Corruption(_)));

    let mut checkpoint = std::io::Cursor::new(Vec::new());
    let error = encode_relational_checkpoint_to_writer(
        &mut checkpoint,
        snapshot.epoch(),
        &corrupt,
        RelationalDecodeLimits::checkpoint().max_record_bytes,
    )
    .expect_err("checkpoint publication must reject a corrupt overflow segment");
    assert!(matches!(error, RelationalError::Corruption(_)));
}

#[test]
fn wal_round_trip_replays_only_complete_epoch_sequence() {
    let source = RelationalStore::default();
    let mut schema_record = Vec::new();
    source
        .commit_with_wal(create_content_tables(), |_, bytes| {
            schema_record = bytes.to_vec();
            Ok(())
        })
        .expect("durable schema commit");
    let mut insert_record = Vec::new();
    source
        .commit_with_wal(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "content_documents".to_string(),
                    rows: vec![document_row("doc-1")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, bytes| {
                insert_record = bytes.to_vec();
                Ok(())
            },
        )
        .expect("durable insert commit");

    let replay = RelationalStore::default();
    replay
        .replay_wal_record(&schema_record, RelationalDecodeLimits::wal())
        .expect("replay schema epoch");
    let snapshot = replay
        .replay_wal_record(&insert_record, RelationalDecodeLimits::wal())
        .expect("replay insert epoch");
    assert_eq!(snapshot.epoch(), 2);
    assert_eq!(snapshot.value().row_count("content_documents"), 1);

    let gap = RelationalStore::default();
    let error = gap
        .replay_wal_record(&insert_record, RelationalDecodeLimits::wal())
        .expect_err("epoch gaps must fail closed");
    assert!(matches!(error, SnapshotCommitError::Stage(_)));
    assert_eq!(gap.snapshot().expect("empty snapshot").epoch(), 0);
}

#[test]
fn torn_or_modified_wal_record_is_rejected_before_replay() {
    let record = encode_relational_wal_batch(1, &create_content_tables()).expect("encoded WAL");
    let torn = &record[..record.len() - 1];
    assert!(matches!(
        decode_relational_wal_batch(torn, RelationalDecodeLimits::wal()),
        Err(RelationalError::Corruption(_))
    ));

    let mut modified = record;
    let last = modified.last_mut().expect("payload byte");
    *last ^= 0xff;
    assert!(matches!(
        decode_relational_wal_batch(&modified, RelationalDecodeLimits::wal()),
        Err(RelationalError::Corruption(_))
    ));
}

#[test]
fn checkpoint_restores_catalog_rows_overflow_and_epoch() {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: 1,
        ..RelationalOverflowConfig::default()
    };
    let source =
        RelationalStore::with_overflow_config(RelationalMutationLimits::default(), overflow_config);
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create table");
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert row");

    let checkpoint = source.encode_checkpoint().expect("encode checkpoint");
    let restored = RelationalStore::from_checkpoint(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalMutationLimits::default(),
        overflow_config,
    )
    .expect("restore checkpoint");
    let snapshot = restored.snapshot().expect("restored snapshot");
    assert_eq!(snapshot.epoch(), 2);
    assert_eq!(snapshot.value().row_count("messages"), 1);
    assert_eq!(snapshot.value().overflow_segment_count(), 1);

    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let hydrated = snapshot
        .value()
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect("hydrate restored row")
        .expect("restored row");
    assert_eq!(
        hydrated.values()[1],
        RelationalValue::Text("payload".repeat(64))
    );
}

#[test]
fn checkpoint_file_keeps_overflow_out_of_resident_state_and_checks_size_before_read() {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: 1,
        ..RelationalOverflowConfig::default()
    };
    let source =
        RelationalStore::with_overflow_config(RelationalMutationLimits::default(), overflow_config);
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .expect("create payload table");
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".to_string(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".to_string()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert payload row");
    let checkpoint = source.encode_checkpoint().expect("encode checkpoint");
    let snapshot = source.snapshot().expect("checkpoint source snapshot");
    let mut undersized_writer = std::io::Cursor::new(Vec::new());
    let error = encode_relational_checkpoint_to_writer(
        &mut undersized_writer,
        snapshot.epoch(),
        snapshot.value(),
        checkpoint.len() - 1,
    )
    .expect_err("streaming checkpoint writer must enforce its byte admission");
    assert!(matches!(error, RelationalError::Admission(_)));
    let path = std::env::temp_dir().join(format!(
        "skein-relational-checkpoint-{}-{}.skein",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::write(&path, &checkpoint).expect("write checkpoint fixture");

    let decoded = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .expect("decode file-backed checkpoint");
    assert_eq!(decoded.state.file_backed_overflow_segment_count(), 1);
    let key = RelationalKey(vec![RelationalValue::Text("message-1".to_string())]);
    let hydrated = decoded
        .state
        .hydrate_row("messages", &key, &mut RelationalHydrationBudget::default())
        .expect("hydrate file-backed overflow")
        .expect("payload row");
    assert_eq!(
        hydrated.values()[1],
        RelationalValue::Text("payload".repeat(64))
    );

    let mut limits = RelationalDecodeLimits::checkpoint();
    limits.max_record_bytes = checkpoint.len() - 1;
    let error = decode_relational_checkpoint_file(&path, limits)
        .expect_err("oversized checkpoint must fail before allocation");
    assert!(matches!(error, RelationalError::Admission(_)));

    let mut corrupt = checkpoint;
    *corrupt.last_mut().expect("checkpoint payload byte") ^= 0xff;
    std::fs::write(&path, corrupt).expect("write corrupted checkpoint fixture");
    let error = decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
        .expect_err("streaming file decoder must reject corrupted payloads");
    assert!(matches!(error, RelationalError::Corruption(_)));
    std::fs::remove_file(path).expect("remove checkpoint fixture");
}

#[test]
fn checkpoint_corruption_is_rejected_without_partial_state() {
    let store = RelationalStore::default();
    store
        .commit(create_content_tables(), |_, _| Ok(()))
        .expect("create tables");
    let mut checkpoint = store.encode_checkpoint().expect("encode checkpoint");
    let last = checkpoint.last_mut().expect("payload byte");
    *last ^= 0xff;

    let result = RelationalStore::from_checkpoint(
        &checkpoint,
        RelationalDecodeLimits::checkpoint(),
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
    );
    assert!(matches!(result, Err(RelationalError::Corruption(_))));
}

#[test]
fn unique_target_upsert_and_bounded_delete_where_are_atomic() {
    let store = RelationalStore::default();
    store
        .commit(create_upsert_table(), |_, _| Ok(()))
        .expect("create upsert table");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .expect("insert initial row");
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Upsert {
                    table: "documents".to_string(),
                    rows: vec![upsert_row("id-2", "owner-1", "new")],
                    conflict_columns: vec!["owner".to_string()],
                    action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                        column: "payload".to_string(),
                        value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                    }]),
                }],
            },
            |_, _| Ok(()),
        )
        .expect("upsert by unique owner");

    let snapshot = store.snapshot().expect("snapshot after upsert");
    assert_eq!(snapshot.value().row_count("documents"), 1);
    let key = RelationalKey(vec![RelationalValue::Text("id-1".to_string())]);
    assert_eq!(
        snapshot
            .value()
            .row("documents", &key)
            .expect("preserved primary key")
            .values()[2],
        RelationalValue::Text("new".to_string())
    );

    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteWhere {
                    table: "documents".to_string(),
                    predicate: RelationalPredicate::Compare {
                        column: "payload".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("new".to_string()),
                    },
                }],
            },
            |_, _| Ok(()),
        )
        .expect("delete matching row");
    assert_eq!(
        store
            .snapshot()
            .expect("snapshot after delete")
            .value()
            .row_count("documents"),
        0
    );
}

#[test]
fn wal_codec_preserves_upsert_and_delete_predicates() {
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::Upsert {
                table: "documents".to_string(),
                rows: vec![upsert_row("id-1", "owner-1", "payload")],
                conflict_columns: vec!["owner".to_string()],
                action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                    column: "payload".to_string(),
                    value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                }]),
            },
            RelationalWrite::DeleteWhere {
                table: "documents".to_string(),
                predicate: RelationalPredicate::And(
                    Box::new(RelationalPredicate::Compare {
                        column: "owner".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("owner-1".to_string()),
                    }),
                    Box::new(RelationalPredicate::IsNull {
                        column: "payload".to_string(),
                        negated: true,
                    }),
                ),
            },
            RelationalWrite::AddColumn {
                table: "documents".to_string(),
                column: RelationalColumnSchema {
                    name: "kind".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: Some(RelationalValue::Text("text".to_string())),
                },
            },
        ],
    };
    let encoded = encode_relational_wal_batch(9, &transaction).expect("encoded WAL");
    let decoded =
        decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).expect("decoded WAL");
    assert_eq!(decoded.epoch, 9);
    assert_eq!(decoded.transaction, transaction);
}

#[test]
fn relational_index_shadow_publishes_generation_fenced_cold_pages() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false), text_column("owner", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        }],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..8)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal}")),
                                    RelationalValue::Text("shared-owner".to_string()),
                                ])
                            })
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build relational shadow source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-shadow-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    let writer = RelationalIndexShadowWriter::new(config);
    let first = writer
        .publish(&directory, &state, 1, 40, None)
        .expect("publish first shadow generation");
    assert_eq!(first.index_roots, 2);
    assert!(first.pages_written > first.index_roots as u64);
    assert_eq!(first.artifact_bytes, first.pages_written * 4096);

    let stale = writer
        .publish(&directory, &state, 2, 41, None)
        .expect_err("stale publisher must not replace the selected root");
    assert!(matches!(
        stale,
        RelationalIndexShadowError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(1),
        }
    ));
    assert!(!directory
        .join(relational_index_shadow_artifact_file(2))
        .exists());

    let second = writer
        .publish(&directory, &state, 2, 41, Some(1))
        .expect("publish next shadow generation");
    let reader = RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("open manifest without reading page payloads");
    assert_eq!(reader.manifest().page_count, second.pages_written);
    for root in &reader.manifest().roots {
        reader.read_root(root).expect("read and verify root page");
    }
    assert!(RelationalIndexShadowReader::open(&directory, 2, 42, config).is_err());

    std::fs::write(
        directory.join(relational_index_shadow_artifact_file(99)),
        b"orphan",
    )
    .expect("write orphan candidate");
    RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("orphan generation must not affect selected manifest");

    let artifact = directory.join(relational_index_shadow_artifact_file(2));
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .expect("open selected page artifact");
    use std::io::{Seek, Write};
    file.seek(std::io::SeekFrom::Start(4095))
        .expect("seek cold page padding");
    file.write_all(&[1]).expect("corrupt cold page padding");
    file.sync_all().expect("sync page corruption");
    let cold_reader = RelationalIndexShadowReader::open(&directory, 2, 41, config)
        .expect("cold open must not scan page payloads");
    let first_page = crate::IndexPageId::new(std::num::NonZeroU64::new(1).unwrap());
    assert!(matches!(
        cold_reader.read_page(first_page),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(cold_reader.is_poisoned());
    let second_page = crate::IndexPageId::new(std::num::NonZeroU64::new(2).unwrap());
    assert!(matches!(
        cold_reader.read_page(second_page),
        Err(RelationalIndexShadowError::Corrupt(message)) if message.contains("poisoned")
    ));

    std::fs::remove_dir_all(directory).expect("remove relational index shadow fixture");
}

#[test]
fn required_relational_index_roots_cover_constraints_and_foreign_keys() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "accounts".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("email", false),
                            text_column("handle", false),
                            text_column("status", false),
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: vec![vec!["email".to_string()]],
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "accounts_handle_idx".to_string(),
                                columns: vec!["handle".to_string()],
                                unique: true,
                            },
                            RelationalIndexSchema {
                                name: "accounts_status_idx".to_string(),
                                columns: vec!["status".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "sessions".to_string(),
                        columns: vec![text_column("id", false), text_column("account_id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: vec![RelationalForeignKeySchema {
                            columns: vec!["account_id".to_string()],
                            referenced_table: "accounts".to_string(),
                            referenced_columns: vec!["id".to_string()],
                            on_delete: RelationalReferentialAction::Restrict,
                            on_update: RelationalReferentialAction::Restrict,
                        }],
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::Insert {
                        table: "accounts".to_string(),
                        rows: vec![
                            RelationalRow::new(vec![
                                RelationalValue::Text("account-a".to_string()),
                                RelationalValue::Text("a@example.test".to_string()),
                                RelationalValue::Text("alice".to_string()),
                                RelationalValue::Text("active".to_string()),
                            ]),
                            RelationalRow::new(vec![
                                RelationalValue::Text("account-b".to_string()),
                                RelationalValue::Text("b@example.test".to_string()),
                                RelationalValue::Text("bob".to_string()),
                                RelationalValue::Text("active".to_string()),
                            ]),
                        ],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "sessions".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text("session-1".to_string()),
                            RelationalValue::Text("account-a".to_string()),
                        ])],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build required-root source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-required-roots-{}-{nonce}",
        std::process::id()
    ));
    let shadow_config = RelationalIndexShadowConfig::default();
    let report = RelationalIndexShadowWriter::new(shadow_config)
        .publish(&directory, &state, 1, 1, None)
        .expect("publish all required roots");
    assert_eq!(report.index_roots, 6);

    let reader = RelationalIndexShadowReader::open(&directory, 1, 1, shadow_config)
        .expect("open required-root fixture");
    reader
        .validate_required_roots(&state)
        .expect("manifest exactly covers the source schema");
    let roles = reader
        .manifest()
        .roots
        .iter()
        .map(|root| {
            (
                (root.identity.namespace.clone(), root.identity.name.clone()),
                root.role,
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        roles.get(&(
            "accounts".to_string(),
            RELATIONAL_PRIMARY_INDEX_NAME.to_string()
        )),
        Some(&RelationalIndexRole::Primary)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), relational_unique_index_name(0))),
        Some(&RelationalIndexRole::UniqueConstraint)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), "accounts_handle_idx".to_string())),
        Some(&RelationalIndexRole::DeclaredUnique)
    );
    assert_eq!(
        roles.get(&("accounts".to_string(), "accounts_status_idx".to_string())),
        Some(&RelationalIndexRole::Secondary)
    );
    let foreign_key_index = relational_foreign_key_index_name(0);
    assert_eq!(
        roles.get(&("sessions".to_string(), foreign_key_index.clone())),
        Some(&RelationalIndexRole::ForeignKeySupport)
    );

    let limited_config = RelationalIndexShadowConfig {
        max_roots: std::num::NonZeroUsize::new(5).unwrap(),
        ..shadow_config
    };
    assert!(matches!(
        RelationalIndexShadowWriter::new(limited_config)
            .publish_generation(&directory, &state, 2, 1),
        Err(RelationalIndexShadowError::Admission(message))
            if message.contains("required index root count 6 exceeds limit 5")
    ));
    assert!(!directory
        .join(relational_index_shadow_artifact_file(2))
        .exists());

    let account_a = RelationalKey(vec![RelationalValue::Text("account-a".to_string())]);
    let session_1 = RelationalKey(vec![RelationalValue::Text("session-1".to_string())]);
    let mut base_sessions = Vec::new();
    reader
        .visit_exact_postings(
            "sessions",
            &foreign_key_index,
            &account_a,
            RelationalIndexReadLimits::default(),
            |key| {
                base_sessions.push(key.clone());
                true
            },
        )
        .expect("read foreign-key support root");
    assert_eq!(base_sessions, vec![session_1.clone()]);

    let (next, capture) = state
        .stage_transaction_with_index_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::UpdateWhere {
                    table: "sessions".to_string(),
                    assignments: vec![RelationalUpdateAssignment {
                        column: "account_id".to_string(),
                        value: RelationalUpdateValue::Value(RelationalValue::Text(
                            "account-b".to_string(),
                        )),
                    }],
                    predicate: RelationalPredicate::Compare {
                        column: "id".to_string(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("session-1".to_string()),
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("capture a foreign-key support update");
    let RelationalIndexChangeCapture::Captured { changes, .. } = &capture else {
        panic!("foreign-key update must remain incrementally capturable");
    };
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.index == foreign_key_index)
            .count(),
        2
    );

    let recovery_config = RelationalIndexRecoveryConfig::default();
    let mut builder = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, recovery_config)
        .expect("create foreign-key recovery delta");
    builder
        .record(2, capture)
        .expect("record foreign-key recovery delta");
    builder.finish(2).expect("publish recovery delta");
    let recovered =
        RelationalIndexRecoveryReader::open_latest(&directory, 2, shadow_config, recovery_config)
            .expect("open recovered foreign-key root");
    recovered
        .validate_required_roots(&next)
        .expect("recovered root identity remains schema-complete");
    let account_b = RelationalKey(vec![RelationalValue::Text("account-b".to_string())]);
    let mut recovered_sessions = Vec::new();
    recovered
        .visit_exact_postings(
            "sessions",
            &foreign_key_index,
            &account_b,
            RelationalIndexReadLimits::default(),
            |key| {
                recovered_sessions.push(key.clone());
                true
            },
        )
        .expect("read recovered foreign-key support root");
    assert_eq!(recovered_sessions, vec![session_1]);

    let changed_schema = next
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateIndex {
                    table: "accounts".to_string(),
                    index: RelationalIndexSchema {
                        name: "accounts_email_lookup_idx".to_string(),
                        columns: vec!["email".to_string()],
                        unique: false,
                    },
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build a different schema root set");
    assert!(matches!(
        reader.validate_required_roots(&changed_schema),
        Err(RelationalIndexShadowError::Corrupt(message))
            if message.contains("required relational index roots")
    ));

    std::fs::remove_dir_all(directory).expect("remove required-root fixture");
}

#[test]
fn relational_index_shadow_demand_reads_match_materialized_oracle() {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("owner", false),
                            RelationalColumnSchema {
                                name: "rank".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "documents_owner_idx".to_string(),
                                columns: vec!["owner".to_string()],
                                unique: false,
                            },
                            RelationalIndexSchema {
                                name: "documents_owner_rank_idx".to_string(),
                                columns: vec!["owner".to_string(), "rank".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: (0..60)
                            .map(|ordinal| {
                                RelationalRow::new(vec![
                                    RelationalValue::Text(format!("doc-{ordinal:03}")),
                                    RelationalValue::Text(format!("owner-{}", ordinal % 3)),
                                    RelationalValue::BigInt(ordinal),
                                ])
                            })
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build demand-read source");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-demand-read-{}-{nonce}",
        std::process::id()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    RelationalIndexShadowWriter::new(config)
        .publish(&directory, &state, 1, 80, None)
        .expect("publish demand-read fixture");
    let reader = RelationalIndexShadowReader::open(&directory, 1, 80, config)
        .expect("open demand-read fixture cold");
    let owner = RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]);

    let expected_exact = state
        .index_lookup("documents", "documents_owner_idx", &owner)
        .expect("materialized exact posting")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut exact = Vec::new();
    let exact_report = reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                exact.push(key.clone());
                true
            },
        )
        .expect("demand-read exact posting");
    assert_eq!(exact, expected_exact);
    assert_eq!(exact_report.rows_visited, expected_exact.len());
    assert!(exact_report.pages_read < reader.manifest().page_count as usize);

    let expected_prefix = state
        .index_prefix_lookup("documents", "documents_owner_rank_idx", &owner, usize::MAX)
        .expect("materialized prefix posting")
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut prefix = Vec::new();
    let prefix_report = reader
        .visit_prefix_postings(
            "documents",
            "documents_owner_rank_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                prefix.push(key.clone());
                true
            },
        )
        .expect("demand-read composite prefix");
    assert_eq!(prefix, expected_prefix);
    assert_eq!(prefix_report.matched_index_keys, expected_prefix.len());
    assert!(prefix_report.pages_read < reader.manifest().page_count as usize);

    let primary_key = RelationalKey(vec![RelationalValue::Text("doc-031".to_string())]);
    let mut primary = Vec::new();
    let primary_report = reader
        .visit_exact_postings(
            "documents",
            RELATIONAL_PRIMARY_INDEX_NAME,
            &primary_key,
            RelationalIndexReadLimits::default(),
            |key| {
                primary.push(key.clone());
                true
            },
        )
        .expect("demand-read primary key");
    assert_eq!(primary, vec![primary_key]);
    assert_eq!(primary_report.rows_visited, 1);

    let mut early = Vec::new();
    let early_report = reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                early.push(key.clone());
                early.len() < 2
            },
        )
        .expect("bounded early posting stop");
    assert_eq!(early.len(), 2);
    assert!(early_report.stopped_early);
    assert_eq!(early_report.rows_visited, 2);
    assert!(early_report.pages_read < exact_report.pages_read);

    let low_page_limit = RelationalIndexReadLimits {
        max_pages: std::num::NonZeroUsize::new(1).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let mut provisional_rows = 0usize;
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_page_limit,
            |_| {
                provisional_rows += 1;
                true
            },
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert_eq!(provisional_rows, 0);
    assert!(!reader.is_poisoned());

    let low_byte_limit = RelationalIndexReadLimits {
        max_bytes: std::num::NonZeroUsize::new(512).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_byte_limit,
            |_| true,
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));

    let low_row_limit = RelationalIndexReadLimits {
        max_rows: std::num::NonZeroUsize::new(3).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    let mut provisional_rows = 0usize;
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_row_limit,
            |_| {
                provisional_rows += 1;
                true
            },
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert_eq!(provisional_rows, 3);

    let low_height_limit = RelationalIndexReadLimits {
        max_tree_height: std::num::NonZeroU32::new(1).unwrap(),
        ..RelationalIndexReadLimits::default()
    };
    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            low_height_limit,
            |_| true,
        ),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert!(!reader.is_poisoned());

    assert!(matches!(
        reader.visit_exact_postings(
            "documents",
            "missing_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::MissingIndex { .. })
    ));
    assert!(!reader.is_poisoned());

    let page_cache = std::sync::Arc::new(crate::SegmentCache::new(16 * 1024));
    let cached_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&page_cache),
        crate::StoreId(41),
    )
    .expect("open demand-read fixture with an empty page cache");
    assert_eq!(page_cache.snapshot().resident_bytes, 0);
    let mut cold = Vec::new();
    let cold_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                cold.push(key.clone());
                true
            },
        )
        .expect("cold cached index lookup");
    assert_eq!(cold, expected_exact);
    assert_eq!(cold_report.cache_hits, 0);
    assert_eq!(cold_report.cache_misses, cold_report.pages_read);
    assert_eq!(cold_report.file_pages_read, cold_report.pages_read);
    assert_eq!(cold_report.file_bytes_read, cold_report.bytes_read);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let mut warm = Vec::new();
    let warm_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                warm.push(key.clone());
                true
            },
        )
        .expect("warm cached index lookup");
    assert_eq!(warm, expected_exact);
    assert_eq!(warm_report.cache_hits, warm_report.pages_read);
    assert_eq!(warm_report.cache_misses, 0);
    assert_eq!(warm_report.file_pages_read, 0);
    assert_eq!(warm_report.file_bytes_read, 0);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let callback_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = cached_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| panic!("stop the cached cursor"),
        );
    }));
    assert!(callback_panic.is_err());
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);
    assert!(!cached_reader.is_poisoned());

    let evicting_cache = std::sync::Arc::new(crate::SegmentCache::new(2 * 1024));
    let evicting_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&evicting_cache),
        crate::StoreId(43),
    )
    .expect("open demand-read fixture with an evicting cache");
    let mut evicted = Vec::new();
    evicting_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                evicted.push(key.clone());
                true
            },
        )
        .expect("index traversal remains correct while cold pages are evicted");
    assert_eq!(evicted, expected_exact);
    let eviction = evicting_cache.snapshot();
    assert!(eviction.eviction_count > 0);
    assert!(eviction.resident_bytes <= eviction.capacity_bytes);
    assert_eq!(eviction.pinned_bytes, 0);

    let undersized_cache = std::sync::Arc::new(crate::SegmentCache::new(512));
    let uncached_reader = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        std::sync::Arc::clone(&undersized_cache),
        crate::StoreId(42),
    )
    .expect("open demand-read fixture with an undersized cache");
    let mut uncached = Vec::new();
    let uncached_report = uncached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |key| {
                uncached.push(key.clone());
                true
            },
        )
        .expect("cache admission rejection falls back to bounded positioned reads");
    assert_eq!(uncached, expected_exact);
    assert_eq!(
        uncached_report.cache_admission_rejections,
        uncached_report.pages_read
    );
    assert_eq!(undersized_cache.snapshot().resident_bytes, 0);

    let root_descriptor = reader
        .manifest()
        .root("documents", "documents_owner_idx")
        .expect("owner root descriptor");
    let root = reader
        .read_root(root_descriptor)
        .expect("read owner root before semantic corruption");
    let mut interior_page = reader
        .read_page(root.child)
        .expect("read owner interior before semantic corruption");
    let crate::ImmutableIndexPageBody::Interior(interior) = &mut interior_page.body else {
        panic!("small-page fixture must build an interior owner page");
    };
    interior
        .entries
        .first_mut()
        .expect("owner interior entry")
        .upper_bound
        .push(0);
    let corrupt_slot = interior_page
        .encode_slot(config.page_limits)
        .expect("re-encode checksummed but inconsistent separator");
    let artifact = directory.join(relational_index_shadow_artifact_file(1));
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .expect("open demand-read artifact");
    use std::io::{Seek, Write};
    let corrupt_offset = root
        .child
        .get()
        .checked_sub(1)
        .and_then(|ordinal| ordinal.checked_mul(1024))
        .expect("interior page offset");
    file.seek(std::io::SeekFrom::Start(corrupt_offset))
        .expect("seek owner interior");
    file.write_all(&corrupt_slot)
        .expect("replace owner interior with inconsistent separator");
    file.sync_all().expect("sync semantic corruption");
    let corrupt_reader = RelationalIndexShadowReader::open(&directory, 1, 80, config)
        .expect("cold open must not traverse the inconsistent separator");
    assert!(matches!(
        corrupt_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(corrupt_reader.is_poisoned());

    std::fs::remove_dir_all(directory).expect("remove demand-read fixture");
}

#[test]
fn relational_index_wal_deltas_merge_with_cold_base_and_stay_bounded() {
    let base = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![
                            text_column("id", false),
                            text_column("owner", false),
                            RelationalColumnSchema {
                                name: "rank".to_string(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "documents_owner_idx".to_string(),
                                columns: vec!["owner".to_string()],
                                unique: false,
                            },
                            RelationalIndexSchema {
                                name: "documents_owner_rank_idx".to_string(),
                                columns: vec!["owner".to_string(), "rank".to_string()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![
                            recovery_document_row("doc-001", "owner-a", 1),
                            recovery_document_row("doc-002", "owner-b", 2),
                            recovery_document_row("doc-003", "owner-a", 3),
                        ],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect("build recovery base");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "skein-relational-index-recovery-{}-{nonce}",
        std::process::id()
    ));
    let shadow_config = RelationalIndexShadowConfig {
        page_limits: crate::ImmutableIndexPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
            max_entries: std::num::NonZeroUsize::new(2).unwrap(),
            max_inline_postings: std::num::NonZeroUsize::new(2).unwrap(),
            ..crate::ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    RelationalIndexShadowWriter::new(shadow_config)
        .publish(&directory, &base, 1, 1, None)
        .expect("publish recovery base");
    let recovery_config = RelationalIndexRecoveryConfig {
        max_dirty_entries: std::num::NonZeroUsize::new(5).unwrap(),
        max_dirty_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
        max_delta_pages: std::num::NonZeroUsize::new(16).unwrap(),
        max_manifest_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
    };
    let mut builder = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, recovery_config)
        .expect("create recovery delta builder");
    let mut state = base;
    let transactions = [
        RelationalTransaction {
            writes: vec![RelationalWrite::UpdateWhere {
                table: "documents".to_string(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "owner".to_string(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text(
                        "owner-b".to_string(),
                    )),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "id".to_string(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("doc-001".to_string()),
                },
            }],
        },
        RelationalTransaction {
            writes: vec![RelationalWrite::DeleteByPrimaryKey {
                table: "documents".to_string(),
                keys: vec![RelationalKey(vec![RelationalValue::Text(
                    "doc-002".to_string(),
                )])],
            }],
        },
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![recovery_document_row("doc-004", "owner-a", 4)],
                mode: RelationalInsertMode::Error,
            }],
        },
    ];
    for (offset, transaction) in transactions.into_iter().enumerate() {
        let epoch = 2 + offset as u64;
        let (next, capture) = state
            .stage_transaction_with_index_changes(
                transaction,
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
                builder.capture_limits(),
            )
            .expect("stage recovered relational transaction");
        builder
            .record(epoch, capture)
            .expect("record bounded recovery delta");
        state = next;
    }
    let report = builder.finish(4).expect("publish recovery delta manifest");
    assert!(report.delta_pages >= 2);
    assert!(report.delta_entries >= 7);
    assert!(report.peak_dirty_entries <= recovery_config.max_dirty_entries.get());
    assert!(report.peak_dirty_bytes <= recovery_config.max_dirty_bytes.get());

    let reader =
        RelationalIndexRecoveryReader::open_latest(&directory, 4, shadow_config, recovery_config)
            .expect("open fenced recovery reader");
    for (index, key) in [
        (
            "documents_owner_idx",
            RelationalKey(vec![RelationalValue::Text("owner-a".to_string())]),
        ),
        (
            "documents_owner_idx",
            RelationalKey(vec![RelationalValue::Text("owner-b".to_string())]),
        ),
    ] {
        let expected = state
            .index_lookup("documents", index, &key)
            .map(|posting| posting.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut actual = Vec::new();
        let read_report = reader
            .visit_exact_postings(
                "documents",
                index,
                &key,
                RelationalIndexReadLimits::default(),
                |primary_key| {
                    actual.push(primary_key.clone());
                    true
                },
            )
            .expect("merge exact base and recovery deltas");
        assert_eq!(actual, expected);
        assert_eq!(read_report.rows_visited, expected.len());
        assert_eq!(read_report.delta_pages_read, report.delta_pages);
    }

    let owner_prefix = RelationalKey(vec![RelationalValue::Text("owner-a".to_string())]);
    let expected_prefix = state
        .index_prefix_lookup(
            "documents",
            "documents_owner_rank_idx",
            &owner_prefix,
            usize::MAX,
        )
        .expect("materialized recovery prefix")
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut actual_prefix = Vec::new();
    reader
        .visit_prefix_postings(
            "documents",
            "documents_owner_rank_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                actual_prefix.push(primary_key.clone());
                true
            },
        )
        .expect("merge prefix base and recovery deltas");
    assert_eq!(actual_prefix, expected_prefix);

    let page_cache = std::sync::Arc::new(crate::SegmentCache::new(64 * 1024));
    let cached_reader = RelationalIndexRecoveryReader::open_latest_with_cache(
        &directory,
        4,
        shadow_config,
        recovery_config,
        std::sync::Arc::clone(&page_cache),
        crate::StoreId(73),
    )
    .expect("open recovery reader with a shared empty page cache");
    assert_eq!(page_cache.snapshot().resident_bytes, 0);
    let mut cold_rows = Vec::new();
    let cold_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                cold_rows.push(primary_key.clone());
                true
            },
        )
        .expect("cold recovery read uses positioned base and delta reads");
    let expected_cold_rows = state
        .index_lookup("documents", "documents_owner_idx", &owner_prefix)
        .expect("materialized owner-a posting")
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(cold_rows, expected_cold_rows);
    assert_eq!(cold_report.base.cache_misses, cold_report.base.pages_read);
    assert_eq!(cold_report.delta_cache_misses, report.delta_pages);
    assert_eq!(cold_report.delta_file_pages_read, report.delta_pages);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    let mut warm_rows = Vec::new();
    let warm_report = cached_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |primary_key| {
                warm_rows.push(primary_key.clone());
                true
            },
        )
        .expect("warm recovery read reuses base and delta pages");
    assert_eq!(warm_rows, expected_cold_rows);
    assert_eq!(warm_report.base.cache_hits, warm_report.base.pages_read);
    assert_eq!(warm_report.base.file_pages_read, 0);
    assert_eq!(warm_report.delta_cache_hits, report.delta_pages);
    assert_eq!(warm_report.delta_file_pages_read, 0);
    assert_eq!(page_cache.snapshot().pinned_bytes, 0);

    assert!(RelationalIndexRecoveryReader::open_latest(
        &directory,
        5,
        shadow_config,
        recovery_config,
    )
    .is_err());

    let crash_config = RelationalIndexRecoveryConfig {
        max_dirty_entries: std::num::NonZeroUsize::new(1).unwrap(),
        ..recovery_config
    };
    let mut abandoned = RelationalIndexRecoveryBuilder::new(&directory, 1, 1, crash_config)
        .expect("create abandoned recovery builder");
    abandoned
        .record(
            5,
            RelationalIndexChangeCapture::Captured {
                changes: vec![
                    RelationalIndexChange {
                        table: "documents".to_string(),
                        index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                        index_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-1".to_string(),
                        )]),
                        primary_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-1".to_string(),
                        )]),
                        kind: RelationalIndexChangeKind::Insert,
                    },
                    RelationalIndexChange {
                        table: "documents".to_string(),
                        index: RELATIONAL_PRIMARY_INDEX_NAME.to_string(),
                        index_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-2".to_string(),
                        )]),
                        primary_key: RelationalKey(vec![RelationalValue::Text(
                            "orphan-2".to_string(),
                        )]),
                        kind: RelationalIndexChangeKind::Insert,
                    },
                ],
                encoded_bytes: 0,
            },
        )
        .expect("flush one immutable but unpublished delta generation");
    drop(abandoned);
    let old_reader =
        RelationalIndexRecoveryReader::open_latest(&directory, 4, shadow_config, recovery_config)
            .expect("abandoned delta generation must not replace the old manifest");
    let mut old_rows = Vec::new();
    old_reader
        .visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |key| {
                old_rows.push(key.clone());
                true
            },
        )
        .expect("old manifest remains readable after candidate crash");
    assert_eq!(
        old_rows,
        state
            .index_lookup("documents", "documents_owner_idx", &owner_prefix)
            .expect("materialized old owner posting")
            .iter()
            .cloned()
            .collect::<Vec<_>>()
    );

    let first_delta = directory.join(relational_index_recovery_delta_file(
        1,
        report.delta_generation,
        0,
    ));
    let mut encoded = std::fs::read(&first_delta).expect("read first recovery delta");
    let last = encoded.last_mut().expect("recovery delta is not empty");
    *last ^= 1;
    std::fs::write(&first_delta, encoded).expect("corrupt recovery delta");
    let corrupt_reader =
        RelationalIndexRecoveryReader::open_latest(&directory, 4, shadow_config, recovery_config)
            .expect("cold recovery open does not read delta pages");
    assert!(matches!(
        corrupt_reader.visit_exact_postings(
            "documents",
            "documents_owner_idx",
            &owner_prefix,
            RelationalIndexReadLimits::default(),
            |_| true,
        ),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(corrupt_reader.is_poisoned());

    std::fs::remove_dir_all(directory).expect("remove recovery delta fixture");
}

#[test]
fn schema_changing_relational_wal_invalidates_incremental_index_capture() {
    let (state, capture) = RelationalState::default()
        .stage_transaction_with_index_changes(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
            RelationalIndexChangeCaptureLimits::default(),
        )
        .expect("schema transaction remains canonically valid");
    assert!(state.table_schema("documents").is_some());
    assert!(matches!(
        capture,
        RelationalIndexChangeCapture::Invalidated { reason }
            if reason.contains("schema-changing WAL")
    ));
}

#[test]
fn relational_schema_rejects_reserved_and_duplicate_index_names() {
    for name in [
        "",
        RELATIONAL_PRIMARY_INDEX_NAME,
        "__unique_0",
        "__foreign_key_0",
    ] {
        let error = RelationalState::default()
            .stage_transaction(
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "documents".to_string(),
                        columns: vec![text_column("id", false)],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![RelationalIndexSchema {
                            name: name.to_string(),
                            columns: vec!["id".to_string()],
                            unique: false,
                        }],
                    })],
                },
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .expect_err("reserved index identity must be rejected");
        assert!(matches!(error, RelationalError::Schema(_)));
    }

    let error = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![text_column("id", false)],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: vec![
                        RelationalIndexSchema {
                            name: "documents_id_idx".to_string(),
                            columns: vec!["id".to_string()],
                            unique: false,
                        },
                        RelationalIndexSchema {
                            name: "documents_id_idx".to_string(),
                            columns: vec!["id".to_string()],
                            unique: true,
                        },
                    ],
                })],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .expect_err("duplicate declared index identities must be rejected");
    assert!(matches!(error, RelationalError::Schema(_)));
}

fn recovery_document_row(id: &str, owner: &str, rank: i64) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(id.to_string()),
        RelationalValue::Text(owner.to_string()),
        RelationalValue::BigInt(rank),
    ])
}

fn create_content_tables() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "content_documents".to_string(),
                columns: vec![text_column("content_doc_id", false)],
                primary_key: vec!["content_doc_id".to_string()],
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                indexes: Vec::new(),
            }),
            RelationalWrite::CreateTable(RelationalTableSchema {
                name: "content_anchors".to_string(),
                columns: vec![
                    text_column("anchor_id", false),
                    text_column("content_doc_id", false),
                ],
                primary_key: vec!["anchor_id".to_string()],
                unique_constraints: Vec::new(),
                foreign_keys: vec![RelationalForeignKeySchema {
                    columns: vec!["content_doc_id".to_string()],
                    referenced_table: "content_documents".to_string(),
                    referenced_columns: vec!["content_doc_id".to_string()],
                    on_delete: RelationalReferentialAction::NoAction,
                    on_update: RelationalReferentialAction::NoAction,
                }],
                indexes: Vec::new(),
            }),
        ],
    }
}

fn create_payload_table() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
            name: "messages".to_string(),
            columns: vec![text_column("id", false), text_column("payload", false)],
            primary_key: vec!["id".to_string()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        })],
    }
}

fn create_upsert_table() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
            name: "documents".to_string(),
            columns: vec![
                text_column("id", false),
                text_column("owner", false),
                text_column("payload", false),
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![vec!["owner".to_string()]],
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        })],
    }
}

fn text_column(name: &str, nullable: bool) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::Text,
        nullable,
        default: None,
    }
}

fn document_row(id: &str) -> RelationalRow {
    RelationalRow::new(vec![RelationalValue::Text(id.to_string())])
}

fn anchor_row(anchor_id: &str, document_id: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(anchor_id.to_string()),
        RelationalValue::Text(document_id.to_string()),
    ])
}

fn upsert_row(id: &str, owner: &str, payload: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::Text(id.to_string()),
        RelationalValue::Text(owner.to_string()),
        RelationalValue::Text(payload.to_string()),
    ])
}
