---------------- MODULE SkeinStableIdentityPublication ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* The physical-id to stable-identity mapping is an independently published *)
(* export/import artifact, not a canonical query index. Every fixed-size     *)
(* mapping page becomes durable before its small selected header. Initial    *)
(* import may append its graph WAL batch only after one complete mapping      *)
(* generation declares coverage for that target graph epoch. Readers pin one *)
(* immutable generation and corruption of a selected page fails closed. Full *)
(* scrub succeeds only after validating every page of the pinned generation. *)
(***************************************************************************)

CONSTANTS Pages, Epochs, Generations

ReaderStates == {
    "closed", "open", "reading", "lookup_succeeded", "scrubbing",
    "scrub_succeeded", "failed"
}
PageCopies == Generations \X Pages

VARIABLES
    publishedGeneration,
    generationCoverage,
    completeGenerations,
    candidateGeneration,
    candidateCoverage,
    candidatePages,
    graphWalEpoch,
    graphWalMappingGeneration,
    graphVisibleEpoch,
    graphVisibleMappingGeneration,
    readerGeneration,
    readerState,
    selectedPage,
    scrubbedPages,
    corruptCopies,
    poisoned

vars == <<
    publishedGeneration,
    generationCoverage,
    completeGenerations,
    candidateGeneration,
    candidateCoverage,
    candidatePages,
    graphWalEpoch,
    graphWalMappingGeneration,
    graphVisibleEpoch,
    graphVisibleMappingGeneration,
    readerGeneration,
    readerState,
    selectedPage,
    scrubbedPages,
    corruptCopies,
    poisoned
>>

Init ==
    /\ publishedGeneration = 0
    /\ generationCoverage = [generation \in Generations |-> 0]
    /\ completeGenerations = {}
    /\ candidateGeneration = 0
    /\ candidateCoverage = 0
    /\ candidatePages = {}
    /\ graphWalEpoch = 0
    /\ graphWalMappingGeneration = 0
    /\ graphVisibleEpoch = 0
    /\ graphVisibleMappingGeneration = 0
    /\ readerGeneration = 0
    /\ readerState = "closed"
    /\ selectedPage = 0
    /\ scrubbedPages = {}
    /\ corruptCopies = {}
    /\ poisoned = FALSE

StartCandidate ==
    /\ candidateGeneration = 0
    /\ publishedGeneration < Cardinality(Generations)
    /\ candidateGeneration' = publishedGeneration + 1
    /\ candidateCoverage' \in Epochs
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

WriteCandidatePage ==
    /\ candidateGeneration \in Generations
    /\ candidatePages # Pages
    /\ \E page \in Pages \ candidatePages:
          candidatePages' = candidatePages \union {page}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

PublishCandidateHeader ==
    /\ candidateGeneration \in Generations
    /\ candidatePages = Pages
    /\ publishedGeneration' = candidateGeneration
    /\ generationCoverage' =
          [generationCoverage EXCEPT ![candidateGeneration] = candidateCoverage]
    /\ completeGenerations' = completeGenerations \union {candidateGeneration}
    /\ candidateGeneration' = 0
    /\ candidateCoverage' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

CrashCandidate ==
    /\ candidateGeneration \in Generations
    /\ candidateGeneration' = 0
    /\ candidateCoverage' = 0
    /\ candidatePages' = {}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

AppendInitialImportWal ==
    /\ graphWalEpoch = 0
    /\ publishedGeneration \in completeGenerations
    /\ generationCoverage[publishedGeneration] \in Epochs
    /\ graphWalEpoch' = generationCoverage[publishedGeneration]
    /\ graphWalMappingGeneration' = publishedGeneration
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

PublishImportedGraph ==
    /\ graphWalEpoch \in Epochs
    /\ graphVisibleEpoch = 0
    /\ graphVisibleEpoch' = graphWalEpoch
    /\ graphVisibleMappingGeneration' = graphWalMappingGeneration
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

OpenPinnedReader ==
    /\ readerState = "closed"
    /\ ~poisoned
    /\ publishedGeneration \in completeGenerations
    /\ readerGeneration' = publishedGeneration
    /\ readerState' = "open"
    /\ selectedPage' = 0
    /\ scrubbedPages' = {}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, corruptCopies, poisoned
       >>

BeginDemandLookup ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ selectedPage' \in Pages
    /\ readerState' = "reading"
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, scrubbedPages,
        corruptCopies, poisoned
       >>

ReadSelectedPage ==
    /\ readerState = "reading"
    /\ <<readerGeneration, selectedPage>> \notin corruptCopies
    /\ readerState' = "lookup_succeeded"
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies, poisoned
       >>

RejectCorruptSelectedPage ==
    /\ readerState = "reading"
    /\ <<readerGeneration, selectedPage>> \in corruptCopies
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies
       >>

BeginDeepScrub ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ readerState' = "scrubbing"
    /\ scrubbedPages' = {}
    /\ selectedPage' = 0
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, corruptCopies,
        poisoned
       >>

ScrubCleanPage ==
    /\ readerState = "scrubbing"
    /\ \E page \in Pages \ scrubbedPages:
          /\ <<readerGeneration, page>> \notin corruptCopies
          /\ scrubbedPages' = scrubbedPages \union {page}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, corruptCopies, poisoned
       >>

RejectCorruptScrubPage ==
    /\ readerState = "scrubbing"
    /\ \E page \in Pages \ scrubbedPages:
          <<readerGeneration, page>> \in corruptCopies
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies
       >>

CompleteDeepScrub ==
    /\ readerState = "scrubbing"
    /\ scrubbedPages = Pages
    /\ readerState' = "scrub_succeeded"
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies, poisoned
       >>

CorruptPublishedPage ==
    /\ publishedGeneration \in completeGenerations
    /\ \E page \in Pages:
          corruptCopies' = corruptCopies \union {<<publishedGeneration, page>>}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, poisoned
       >>

CloseReader ==
    /\ readerState \in {"open", "lookup_succeeded", "scrub_succeeded", "failed"}
    /\ readerState' = "closed"
    /\ readerGeneration' = 0
    /\ selectedPage' = 0
    /\ scrubbedPages' = {}
    /\ UNCHANGED <<
        publishedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, corruptCopies, poisoned
       >>

Next ==
    \/ StartCandidate
    \/ WriteCandidatePage
    \/ PublishCandidateHeader
    \/ CrashCandidate
    \/ AppendInitialImportWal
    \/ PublishImportedGraph
    \/ OpenPinnedReader
    \/ BeginDemandLookup
    \/ ReadSelectedPage
    \/ RejectCorruptSelectedPage
    \/ BeginDeepScrub
    \/ ScrubCleanPage
    \/ RejectCorruptScrubPage
    \/ CompleteDeepScrub
    \/ CorruptPublishedPage
    \/ CloseReader

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ publishedGeneration \in {0} \union Generations
    /\ generationCoverage \in [Generations -> {0} \union Epochs]
    /\ completeGenerations \subseteq Generations
    /\ candidateGeneration \in {0} \union Generations
    /\ candidateCoverage \in {0} \union Epochs
    /\ candidatePages \subseteq Pages
    /\ graphWalEpoch \in {0} \union Epochs
    /\ graphWalMappingGeneration \in {0} \union Generations
    /\ graphVisibleEpoch \in {0} \union Epochs
    /\ graphVisibleMappingGeneration \in {0} \union Generations
    /\ readerGeneration \in {0} \union Generations
    /\ readerState \in ReaderStates
    /\ selectedPage \in {0} \union Pages
    /\ scrubbedPages \subseteq Pages
    /\ corruptCopies \subseteq PageCopies
    /\ poisoned \in BOOLEAN

PublishedHeaderSelectsCompletePages ==
    publishedGeneration # 0 => publishedGeneration \in completeGenerations

WalNeverLeadsStableIdentity ==
    graphWalEpoch # 0 =>
        /\ graphWalMappingGeneration \in completeGenerations
        /\ generationCoverage[graphWalMappingGeneration] = graphWalEpoch

VisibleGraphNeverLeadsStableIdentity ==
    graphVisibleEpoch # 0 =>
        /\ graphVisibleMappingGeneration \in completeGenerations
        /\ generationCoverage[graphVisibleMappingGeneration] = graphVisibleEpoch

PinnedReaderUsesCompleteGeneration ==
    readerState # "closed" => readerGeneration \in completeGenerations

SuccessfulLookupHasSelectedPage ==
    readerState = "lookup_succeeded" => selectedPage \in Pages

SuccessfulScrubVisitedEveryPage ==
    readerState = "scrub_succeeded" => scrubbedPages = Pages

FailedReadPoisonsReader ==
    readerState = "failed" => poisoned

PoisonedReaderCannotBeReading ==
    poisoned => readerState \notin {"reading", "scrubbing"}

=============================================================================
