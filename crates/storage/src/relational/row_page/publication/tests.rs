use super::*;
use crate::relational::{
    RelationalKey, RelationalOverflowRef, RelationalRow, RelationalRowPageEntry,
    RelationalScalarType, RelationalValue,
};
use skein_integrity::integrity_digest;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn publish_last_root_round_trips_with_a_concrete_refinement_trace() {
    let directory = unique_test_dir("round-trip");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    let report = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    assert_eq!(report.dirty_pages_written, 2);
    assert_eq!(report.root_pages, 2);
    assert_eq!(report.reused_pages, 0);
    assert_eq!(report.events, COMPLETE_PUBLICATION_TRACE);
    assert_eq!(
        report.page_artifact_bytes,
        2 * config.page_limits.max_page_bytes.get() as u64
    );

    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().generation, 1);
    assert_eq!(reader.manifest().source_commit_epoch, 10);
    assert_eq!(reader.manifest().previous_generation, None);
    assert_eq!(reader.manifest().tables.len(), 1);
    assert_eq!(reader.manifest().tables[0].next_page_id.get(), 3);
    let descriptors = collect_descriptors(&reader, "documents");
    assert_eq!(page_id_values(&descriptors), vec![1, 2]);
    assert_eq!(physical_generations(&descriptors), vec![1, 1]);
    assert_eq!(descriptors[0].physical_slot, 0);
    assert_eq!(descriptors[1].physical_slot, 1);
    assert_eq!(
        fs::read(directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE)).unwrap(),
        fs::read(directory.join(relational_row_page_manifest_generation_file(1))).unwrap()
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn incremental_publication_reuses_clean_pages_and_keeps_pinned_roots() {
    let directory = unique_test_dir("reuse");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();

    let report = publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![table_delta(
                "documents",
                vec![page(1, 2, 11, 1, 2), page(3, 2, 11, 5, 6)],
            )],
        )
        .unwrap();
    assert_eq!(report.dirty_pages_written, 2);
    assert_eq!(report.root_pages, 3);
    assert_eq!(report.reused_pages, 1);

    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(current.manifest().generation, 2);
    assert_eq!(current.manifest().previous_generation, Some(1));
    assert_eq!(current.manifest().tables[0].next_page_id.get(), 4);
    let current_descriptors = collect_descriptors(&current, "documents");
    assert_eq!(page_id_values(&current_descriptors), vec![1, 2, 3]);
    assert_eq!(physical_generations(&current_descriptors), vec![2, 1, 2]);

    assert_eq!(pinned.manifest().generation, 1);
    let pinned_descriptors = collect_descriptors(&pinned, "documents");
    assert_eq!(page_id_values(&pinned_descriptors), vec![1, 2]);
    assert_eq!(physical_generations(&pinned_descriptors), vec![1, 1]);
    assert!(directory
        .join(relational_row_page_artifact_file(1))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn stale_publication_is_rejected_before_creating_a_candidate() {
    let directory = unique_test_dir("stale");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let error = publisher
        .publish(
            &directory,
            2,
            11,
            None,
            vec![table_delta("documents", vec![page(1, 2, 11, 1, 2)])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(1)
        }
    ));
    assert!(!directory
        .join(relational_row_page_artifact_file(2))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn every_pre_manifest_crash_keeps_the_previous_root_selected() {
    for stop_after in [
        RelationalRowPagePublicationPhase::CandidatePagesDurable,
        RelationalRowPagePublicationPhase::CandidateRootDurable,
        RelationalRowPagePublicationPhase::CandidateManifestDurable,
        RelationalRowPagePublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir(&format!("crash-{stop_after:?}"));
        let config = RelationalRowPagePublicationConfig::default();
        let publisher = RelationalRowPagePublisher::new(config);
        publisher
            .publish(
                &directory,
                1,
                10,
                None,
                vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
            )
            .unwrap();
        let error = publisher
            .publish_inner(
                &directory,
                2,
                11,
                Some(1),
                vec![table_delta("documents", vec![page(1, 2, 11, 1, 3)])],
                Some(stop_after),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            RelationalRowPagePublicationError::Durability(message)
                if message.contains("injected stop")
        ));
        let selected = RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap();
        assert_eq!(selected.manifest().generation, 1);

        publisher
            .publish(
                &directory,
                3,
                12,
                Some(1),
                vec![table_delta("documents", vec![page(1, 3, 12, 1, 3)])],
            )
            .unwrap();
        assert_eq!(
            RelationalRowPageRootReader::open_latest(&directory, config)
                .unwrap()
                .unwrap()
                .manifest()
                .generation,
            3
        );
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn descriptor_corruption_fails_when_the_selected_entry_is_read() {
    let directory = unique_test_dir("descriptor-corruption");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptor_path = directory.join(relational_row_page_root_descriptor_file(1));
    let mut descriptor = OpenOptions::new()
        .read(true)
        .write(true)
        .open(descriptor_path)
        .unwrap();
    descriptor.seek(SeekFrom::Start(16)).unwrap();
    descriptor.write_all(&[1]).unwrap();
    descriptor.sync_all().unwrap();
    assert!(matches!(
        reader.read_table_page_descriptor("documents", 0),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("binding checksum")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn descriptor_binding_rejects_valid_entries_swapped_between_ordinals() {
    let directory = unique_test_dir("descriptor-swap");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptor_path = directory.join(relational_row_page_root_descriptor_file(1));
    let mut encoded = fs::read(&descriptor_path).unwrap();
    let descriptor_bytes = root::ROOT_DESCRIPTOR_BYTES;
    let first = encoded[..descriptor_bytes].to_vec();
    encoded.copy_within(descriptor_bytes..descriptor_bytes * 2, 0);
    encoded[descriptor_bytes..descriptor_bytes * 2].copy_from_slice(&first);
    fs::write(descriptor_path, encoded).unwrap();

    assert!(matches!(
        reader.read_table_page_descriptor("documents", 0),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("binding checksum")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overflow_and_overlap_are_rejected_without_selecting_a_root() {
    let directory = unique_test_dir("admission");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    let mut overflow_page = page(1, 1, 10, 1, 2);
    overflow_page.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Overflow(RelationalOverflowRef {
            digest: integrity_digest(b"overflow").sha256.to_string(),
            scalar_type: RelationalScalarType::Text,
            compressed_bytes: 8,
            uncompressed_bytes: 16,
        }),
    ]);
    let error = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![overflow_page])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("overflow extent")
    ));
    assert!(!directory.exists());

    let overlap = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 3), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap_err();
    assert!(matches!(
        overlap,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("overlap")
    ));
    assert!(!directory.exists());
}

#[test]
fn deletion_requires_a_page_in_the_selected_base() {
    let directory = unique_test_dir("delete");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let error = publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema_digest: schema_digest(),
                next_page_id: NonZeroU64::new(10).unwrap(),
                dirty_pages: Vec::new(),
                deleted_page_ids: vec![page_id(9)],
            }],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("absent from the selected base")
    ));
    assert_eq!(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn deleting_every_page_publishes_an_empty_table_without_breaking_pinned_roots() {
    let directory = unique_test_dir("delete-all");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();

    let mut deletion = table_delta("documents", Vec::new());
    deletion.next_page_id = NonZeroU64::new(3).unwrap();
    deletion.deleted_page_ids = vec![page_id(1), page_id(2)];
    let report = publisher
        .publish(&directory, 2, 11, Some(1), vec![deletion])
        .unwrap();
    assert_eq!(report.dirty_pages_written, 0);
    assert_eq!(report.root_pages, 0);
    assert_eq!(report.reused_pages, 0);
    assert_eq!(report.page_artifact_bytes, 0);

    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(current.manifest().generation, 2);
    assert_eq!(current.manifest().tables.len(), 1);
    assert_eq!(current.manifest().tables[0].table, "documents");
    assert_eq!(current.manifest().tables[0].page_count, 0);
    assert!(collect_descriptors(&current, "documents").is_empty());
    assert_eq!(
        page_id_values(&collect_descriptors(&pinned, "documents")),
        vec![1, 2]
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn resource_admission_finishes_before_generation_artifacts_are_created() {
    let directory = unique_test_dir("resource-admission");
    let config = RelationalRowPagePublicationConfig {
        max_root_key_bytes: NonZeroU64::new(8).unwrap(),
        ..RelationalRowPagePublicationConfig::default()
    };
    let error = RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("root may contain")
    ));
    assert!(!directory
        .join(relational_row_page_artifact_file(1))
        .exists());
    assert!(!directory
        .join(relational_row_page_root_descriptor_file(1))
        .exists());
    assert!(!directory
        .join(relational_row_page_manifest_generation_file(1))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn open_rejects_manifest_corruption_and_wrong_artifact_lengths() {
    let config = RelationalRowPagePublicationConfig::default();
    let manifest_directory = unique_test_dir("manifest-corruption");
    RelationalRowPagePublisher::new(config)
        .publish(
            &manifest_directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let latest = manifest_directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&latest)
        .unwrap();
    file.seek(SeekFrom::Start(20)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        RelationalRowPageRootReader::open_latest(&manifest_directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("checksum mismatch")
    ));
    fs::remove_dir_all(manifest_directory).unwrap();

    let artifact_directory = unique_test_dir("artifact-length");
    RelationalRowPagePublisher::new(config)
        .publish(
            &artifact_directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    OpenOptions::new()
        .write(true)
        .open(artifact_directory.join(relational_row_page_root_descriptor_file(1)))
        .unwrap()
        .set_len(1)
        .unwrap();
    assert!(matches!(
        RelationalRowPageRootReader::open_latest(&artifact_directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("descriptor artifact contains")
    ));
    fs::remove_dir_all(artifact_directory).unwrap();
}

fn collect_descriptors(
    reader: &RelationalRowPageRootReader,
    table: &str,
) -> Vec<RelationalRowPageRootDescriptor> {
    let mut descriptors = Vec::new();
    reader
        .visit_table_pages(table, |descriptor| {
            descriptors.push(descriptor.clone());
            Ok(())
        })
        .unwrap();
    descriptors
}

fn page_id_values(descriptors: &[RelationalRowPageRootDescriptor]) -> Vec<u64> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.logical_page_id.get())
        .collect()
}

fn physical_generations(descriptors: &[RelationalRowPageRootDescriptor]) -> Vec<u64> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.physical_generation)
        .collect()
}

fn table_delta(
    table: &str,
    dirty_pages: Vec<ImmutableRelationalRowPage>,
) -> RelationalRowPageTableDelta {
    let next_page_id = dirty_pages
        .iter()
        .map(|page| page.page_id.get())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .expect("test row-page allocator must remain representable");
    RelationalRowPageTableDelta {
        table: table.to_string(),
        schema_digest: schema_digest(),
        next_page_id,
        dirty_pages,
        deleted_page_ids: Vec::new(),
    }
}

fn page(
    page_id_value: u64,
    generation: u64,
    source_commit_epoch: u64,
    lower: i64,
    upper: i64,
) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation,
        source_commit_epoch,
        page_id: page_id(page_id_value),
        schema_digest: schema_digest(),
        column_count: 2,
        rows: (lower..=upper)
            .map(|value| RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::BigInt(value)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(value),
                    RelationalValue::Text(format!("row-{value}")),
                ]),
            })
            .collect(),
    }
}

fn page_id(value: u64) -> RelationalRowPageId {
    RelationalRowPageId::new(NonZeroU64::new(value).unwrap())
}

fn schema_digest() -> Sha256Digest {
    integrity_digest(b"documents-schema-v1").sha256
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "skein-row-page-publication-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
