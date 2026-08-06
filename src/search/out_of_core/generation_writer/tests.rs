use super::super::OUT_OF_CORE_MANIFEST_FILE;
use super::*;
use crate::search::{SearchMode, SearchQueryOptions, SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn streaming_generation_publishes_reopenable_zero_residency_projection() {
    let root = test_dir("streaming_generation");
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(17),
        embedding_manifest: Some(SearchEmbeddingManifest {
            model: "test-model".to_string(),
            version: Some("v1".to_string()),
            dimension: 2,
        }),
        lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
        ..SearchOutOfCoreGenerationBuildOptions::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
    for number in 0..300 {
        writer.push(document(number)).unwrap();
    }
    let report = writer.finish().unwrap();
    assert_eq!(report.document_count, 300);
    assert_eq!(report.vector_document_count, 300);
    assert_eq!(report.resident_document_count, 0);
    assert!(report.active_manifest_published_last);
    assert!(!report.cleanup_retry_required);
    assert!(report.peak_segment_document_count <= SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
    assert!(!root.join(crate::search::SEARCH_SNAPSHOT_FILE).exists());

    let reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.document_count(), 300);
    assert_eq!(reader.resident_document_count(), 0);
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(reader.source_graph_commit_epoch(), Some(17));
    let output = reader
        .search_with_options(
            "graph storage",
            Some(&[1.0, 0.5]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    assert_eq!(output.result.hits.len(), 5);
    assert!(output.metrics.hydrated_documents <= 5);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_rejects_unordered_input_without_publication() {
    let root = test_dir("unordered_generation");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    writer.push(document(2)).unwrap();
    let error = writer.push(document(1)).unwrap_err();
    assert!(error.to_string().contains("strictly increasing"));
    assert!(writer
        .finish()
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_limit_failure_cleans_stage_and_preserves_active_generation() {
    let root = test_dir("generation_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_documents: NonZeroUsize::new(1).unwrap(),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .push(document(2))
        .unwrap_err()
        .to_string()
        .contains("admitted 1 documents"));
    drop(replacement);

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    let rejected_generation = first.generation + 1;
    assert!(!root
        .join(format!(
            "search_projection_segments.{rejected_generation}.skein"
        ))
        .exists());
    assert!(!root
        .join(format!("search_lexical.{rejected_generation}.skein"))
        .exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn finalize_admission_failure_preserves_active_manifest_and_cleans_stage() {
    let root = test_dir("generation_finalize_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_descriptor_working_bytes: NonZeroU64::MIN,
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .finish()
        .unwrap_err()
        .to_string()
        .contains("descriptor working set"));

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_size_admission_preserves_active_manifest() {
    let root = test_dir("generation_publication_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_generation_bytes: NonZeroU64::MIN,
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .finish()
        .unwrap_err()
        .to_string()
        .contains("published bytes"));

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    let rejected_generation = first.generation + 1;
    assert!(!root
        .join(format!(
            "search_projection_segments.{rejected_generation}.skein"
        ))
        .exists());
    assert!(!root
        .join(format!("search_lexical.{rejected_generation}.skein"))
        .exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_replaces_orphaned_next_generation_artifacts() {
    let root = test_dir("generation_orphan_replacement");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();

    let next_generation = first.generation + 1;
    let descriptor_path = root.join(format!(
        "search_projection_segments.{next_generation}.skein"
    ));
    let lexical_path = root.join(format!("search_lexical.{next_generation}.skein"));
    fs::write(&descriptor_path, b"orphaned descriptor").unwrap();
    fs::write(&lexical_path, b"orphaned lexical artifact").unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    let second = replacement.finish().unwrap();

    assert_eq!(second.generation, next_generation);
    assert_ne!(fs::read(descriptor_path).unwrap(), b"orphaned descriptor");
    assert_ne!(
        fs::read(lexical_path).unwrap(),
        b"orphaned lexical artifact"
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        second.generation
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_recovers_generation_after_active_manifest_corruption() {
    let root = test_dir("generation_manifest_recovery");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();

    let mut second = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    second.push(document(1)).unwrap();
    let second = second.finish().unwrap();
    assert_eq!(second.generation, first.generation + 1);

    fs::write(root.join(OUT_OF_CORE_MANIFEST_FILE), b"invalid manifest").unwrap();
    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    replacement.push(document(2)).unwrap();
    let replacement = replacement.finish().unwrap();

    assert_eq!(replacement.generation, second.generation + 1);
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        replacement.generation
    );
    fs::remove_dir_all(root).unwrap();
}

fn document(number: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{number:06}"),
        title: format!("Graph storage {number}"),
        content: "Graph storage keeps bounded search generations".repeat(4),
        embedding: Some(vec![1.0, number as f32 / 300.0]),
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}

fn stage_directories(root: &Path) -> usize {
    fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".search-generation.")
        })
        .count()
}

fn test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "skein_search_{name}_{}_{}",
        std::process::id(),
        nanos
    ))
}
