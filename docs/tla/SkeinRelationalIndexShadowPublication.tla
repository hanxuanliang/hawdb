---------------- MODULE SkeinRelationalIndexShadowPublication ----------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Relational index candidates are generation-specific, rebuildable files. *)
(* Candidate pages and their root manifest become durable before a matching *)
(* canonical checkpoint may select that generation. The canonical checkpoint *)
(* does not reverse-reference a non-authoritative candidate, so candidate    *)
(* admission failure, corruption, or a crash orphan never prevents canonical *)
(* recovery. A DemandPaged integrity failure fails the selected read closed, *)
(* while Shadow mode only records candidate unavailability.                  *)
(***************************************************************************)

CONSTANT MaxGeneration, MaxEpoch, MaxPage

ASSUME /\ MaxGeneration \in Nat \ {0}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Generations == 0..MaxGeneration
Epochs == 0..MaxEpoch
Pages == 1..MaxPage
CandidateIds == [generation : Generations, epoch : Epochs]
BuildPhases == {"idle", "building", "pages_durable", "candidate_durable"}
SelectionModes == {"none", "shadow", "demand"}
ReadStates == {"idle", "reading", "succeeded", "failed"}

Candidate(generation, epoch) ==
    [generation |-> generation, epoch |-> epoch]

VARIABLES
    canonicalGeneration,
    canonicalEpoch,
    canonicalHistory,
    buildPhase,
    buildGeneration,
    buildEpoch,
    durableArtifacts,
    durableCandidateManifests,
    abandonedCandidates,
    corruptCandidateManifests,
    canonicalOpen,
    candidateUnavailable,
    demandReadFailed,
    handleOpen,
    selectionMode,
    handleGeneration,
    handleEpoch,
    queriesStarted,
    loadedPages,
    corruptPages,
    readState,
    requiredPage,
    poisoned

vars == <<
    canonicalGeneration,
    canonicalEpoch,
    canonicalHistory,
    buildPhase,
    buildGeneration,
    buildEpoch,
    durableArtifacts,
    durableCandidateManifests,
    abandonedCandidates,
    corruptCandidateManifests,
    canonicalOpen,
    candidateUnavailable,
    demandReadFailed,
    handleOpen,
    selectionMode,
    handleGeneration,
    handleEpoch,
    queriesStarted,
    loadedPages,
    corruptPages,
    readState,
    requiredPage,
    poisoned
>>

Init ==
    /\ canonicalGeneration = 0
    /\ canonicalEpoch = 0
    /\ canonicalHistory = {Candidate(0, 0)}
    /\ buildPhase = "idle"
    /\ buildGeneration = 0
    /\ buildEpoch = 0
    /\ durableArtifacts = {}
    /\ durableCandidateManifests = {}
    /\ abandonedCandidates = {}
    /\ corruptCandidateManifests = {}
    /\ canonicalOpen = FALSE
    /\ candidateUnavailable = FALSE
    /\ demandReadFailed = FALSE
    /\ handleOpen = FALSE
    /\ selectionMode = "none"
    /\ handleGeneration = 0
    /\ handleEpoch = 0
    /\ queriesStarted = FALSE
    /\ loadedPages = {}
    /\ corruptPages = {}
    /\ readState = "idle"
    /\ requiredPage = 0
    /\ poisoned = FALSE

BeginCandidate ==
    /\ buildPhase = "idle"
    /\ canonicalGeneration < MaxGeneration
    /\ \E generation \in (canonicalGeneration + 1)..MaxGeneration,
          epoch \in canonicalEpoch..MaxEpoch:
        /\ buildGeneration' = generation
        /\ buildEpoch' = epoch
    /\ buildPhase' = "building"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PersistCandidatePages ==
    /\ buildPhase = "building"
    /\ buildPhase' = "pages_durable"
    /\ durableArtifacts' = durableArtifacts \cup {buildGeneration}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildGeneration,
        buildEpoch,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PersistCandidateManifest ==
    /\ buildPhase = "pages_durable"
    /\ buildGeneration \in durableArtifacts
    /\ buildPhase' = "candidate_durable"
    /\ durableCandidateManifests' =
        durableCandidateManifests \cup {Candidate(buildGeneration, buildEpoch)}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PublishCheckpointWithCandidate ==
    /\ buildPhase = "candidate_durable"
    /\ Candidate(buildGeneration, buildEpoch) \in durableCandidateManifests
    /\ buildGeneration > canonicalGeneration
    /\ buildEpoch >= canonicalEpoch
    /\ canonicalGeneration' = buildGeneration
    /\ canonicalEpoch' = buildEpoch
    /\ canonicalHistory' =
        canonicalHistory \cup {Candidate(buildGeneration, buildEpoch)}
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

(***************************************************************************)
(* Canonical checkpoint publication is independent from a non-authoritative *)
(* candidate. This is the admission/error path used by Shadow and            *)
(* DemandPaged while the materialized constraint oracle still exists.        *)
(***************************************************************************)
PublishCheckpointWithoutCandidate ==
    /\ buildPhase = "idle"
    /\ canonicalGeneration < MaxGeneration
    /\ \E generation \in (canonicalGeneration + 1)..MaxGeneration,
          epoch \in canonicalEpoch..MaxEpoch:
        /\ canonicalGeneration' = generation
        /\ canonicalEpoch' = epoch
        /\ canonicalHistory' = canonicalHistory \cup {Candidate(generation, epoch)}
    /\ UNCHANGED <<
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

CrashBeforeCheckpoint ==
    /\ buildPhase # "idle"
    /\ abandonedCandidates' =
        abandonedCandidates \cup {Candidate(buildGeneration, buildEpoch)}
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        durableArtifacts,
        durableCandidateManifests,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

OpenCanonical ==
    /\ ~canonicalOpen
    /\ canonicalOpen' = TRUE
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

OpenExactCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in durableCandidateManifests \ corruptCandidateManifests
    /\ \E mode \in {"shadow", "demand"}: selectionMode' = mode
    /\ handleOpen' = TRUE
    /\ handleGeneration' = canonicalGeneration
    /\ handleEpoch' = canonicalEpoch
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        corruptPages
        >>

ObserveMissingCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \notin durableCandidateManifests
    /\ \E mode \in {"shadow", "demand"}: selectionMode' = mode
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

CorruptCandidateManifest ==
    /\ \E candidate \in durableCandidateManifests:
        corruptCandidateManifests' = corruptCandidateManifests \cup {candidate}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

RejectCorruptShadowCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in corruptCandidateManifests
    /\ selectionMode' = "shadow"
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

RejectCorruptDemandCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in corruptCandidateManifests
    /\ selectionMode' = "demand"
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = TRUE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

BeginPageRead ==
    /\ handleOpen
    /\ ~poisoned
    /\ readState = "idle"
    /\ \E page \in Pages: requiredPage' = page
    /\ queriesStarted' = TRUE
    /\ readState' = "reading"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        loadedPages,
        corruptPages,
        poisoned
        >>

ReadHealthyPage ==
    /\ readState = "reading"
    /\ requiredPage \notin corruptPages
    /\ loadedPages' = loadedPages \cup {requiredPage}
    /\ readState' = "succeeded"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        corruptPages,
        requiredPage,
        poisoned
        >>

ReadCorruptPage ==
    /\ readState = "reading"
    /\ requiredPage \in corruptPages
    /\ readState' = "failed"
    /\ poisoned' = TRUE
    /\ demandReadFailed' = (demandReadFailed \/ (selectionMode = "demand"))
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        requiredPage
        >>

FinishRead ==
    /\ readState = "succeeded"
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        poisoned
        >>

CorruptColdPage ==
    /\ \E page \in Pages \ loadedPages:
        corruptPages' = corruptPages \cup {page}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        readState,
        requiredPage,
        poisoned
        >>

CrashDatabase ==
    /\ canonicalOpen \/ handleOpen
    /\ canonicalOpen' = FALSE
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ handleOpen' = FALSE
    /\ selectionMode' = "none"
    /\ handleGeneration' = 0
    /\ handleEpoch' = 0
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        corruptPages
        >>

Next ==
    \/ BeginCandidate
    \/ PersistCandidatePages
    \/ PersistCandidateManifest
    \/ PublishCheckpointWithCandidate
    \/ PublishCheckpointWithoutCandidate
    \/ CrashBeforeCheckpoint
    \/ OpenCanonical
    \/ OpenExactCandidate
    \/ ObserveMissingCandidate
    \/ CorruptCandidateManifest
    \/ RejectCorruptShadowCandidate
    \/ RejectCorruptDemandCandidate
    \/ BeginPageRead
    \/ ReadHealthyPage
    \/ ReadCorruptPage
    \/ FinishRead
    \/ CorruptColdPage
    \/ CrashDatabase

TypeOK ==
    /\ canonicalGeneration \in Generations
    /\ canonicalEpoch \in Epochs
    /\ canonicalHistory \subseteq CandidateIds
    /\ buildPhase \in BuildPhases
    /\ buildGeneration \in Generations
    /\ buildEpoch \in Epochs
    /\ durableArtifacts \subseteq (1..MaxGeneration)
    /\ durableCandidateManifests \subseteq CandidateIds
    /\ abandonedCandidates \subseteq CandidateIds
    /\ corruptCandidateManifests \subseteq durableCandidateManifests
    /\ canonicalOpen \in BOOLEAN
    /\ candidateUnavailable \in BOOLEAN
    /\ demandReadFailed \in BOOLEAN
    /\ handleOpen \in BOOLEAN
    /\ selectionMode \in SelectionModes
    /\ handleGeneration \in Generations
    /\ handleEpoch \in Epochs
    /\ queriesStarted \in BOOLEAN
    /\ loadedPages \subseteq Pages
    /\ corruptPages \subseteq Pages
    /\ readState \in ReadStates
    /\ requiredPage \in 0..MaxPage
    /\ poisoned \in BOOLEAN

CandidateManifestHasDurablePages ==
    \A candidate \in durableCandidateManifests:
        candidate.generation \in durableArtifacts

CanonicalGenerationNeverRegresses ==
    /\ Candidate(canonicalGeneration, canonicalEpoch) \in canonicalHistory
    /\ \A candidate \in canonicalHistory:
        candidate.generation <= canonicalGeneration

OpenHandlePinsSelectedCandidate ==
    handleOpen =>
        /\ canonicalOpen
        /\ Candidate(handleGeneration, handleEpoch) \in canonicalHistory
        /\ Candidate(handleGeneration, handleEpoch) \in durableCandidateManifests
        /\ handleGeneration <= canonicalGeneration

OpenDoesNotWarmPages ==
    handleOpen /\ ~queriesStarted => loadedPages = {}

SuccessfulReadVerifiedPage ==
    readState = "succeeded" =>
        /\ requiredPage \in loadedPages
        /\ requiredPage \notin corruptPages
        /\ ~poisoned

CandidateFailurePreservesCanonicalOpen ==
    (candidateUnavailable \/ demandReadFailed) => canonicalOpen

DemandIntegrityFailureFailsClosed ==
    demandReadFailed =>
        /\ selectionMode = "demand"
        /\ canonicalOpen

CorruptionPoisonsOnlyCandidateHandle ==
    poisoned =>
        /\ canonicalOpen
        /\ handleOpen
        /\ readState = "failed"

Spec == Init /\ [][Next]_vars

=============================================================================
