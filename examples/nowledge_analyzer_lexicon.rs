use skein::{SearchAnalyzerLexicon, SearchDocument, SearchIndex, SearchMode};
use std::collections::BTreeMap;

fn main() {
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(nowledge_application_lexicon());

    index
        .upsert(SearchDocument {
            id: "memory-lifecycle".to_string(),
            title: "Crystal memory keeps SYNTHESIZED_FROM evidence".to_string(),
            content: "Episodic provenance preserves raw Thread and SourceChunk records".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index.search("raw evidence", None, SearchMode::Text, 10);
    assert_eq!(hits[0].id, "memory-lifecycle");
}

fn nowledge_application_lexicon() -> SearchAnalyzerLexicon {
    SearchAnalyzerLexicon::default()
        .with_alias_rule(["crystal"], ["crystallized_memory", "synthesized_memory"])
        .with_alias_rule(
            ["crystallization", "crystallized", "crystallized_memory"],
            ["crystal"],
        )
        .with_alias_rule(
            ["synthesized", "synthesis", "synthesized_memory"],
            ["crystal"],
        )
        .with_alias_rule(["synthesized_from"], ["crystal", "sourced_from"])
        .with_alias_rule(["episodic", "episodic_provenance"], ["raw_evidence"])
        .with_alias_rule(["raw_evidence"], ["episodic_provenance"])
        .with_alias_rule(["source_provenance"], ["sourced_from"])
        .with_alias_rule(["sourced_from"], ["source_provenance"])
        .with_alias_rule(["entity_mention", "memory_mention"], ["mentions"])
        .with_alias_rule(["mentions"], ["entity_mention", "memory_mention"])
        .with_alias_rule(["evolves"], ["memory_evolution"])
        .with_alias_rule(["memory_evolution", "evolution_edge"], ["evolves"])
        .with_alias_rule(["ai_summary"], ["community_summary"])
        .with_alias_rule(
            ["community_summary", "summarized_community"],
            ["ai_summary"],
        )
}
