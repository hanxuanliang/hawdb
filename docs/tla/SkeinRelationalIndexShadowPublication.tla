---------------- MODULE SkeinRelationalIndexShadowPublication ----------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* A relational index shadow is a generation-specific, rebuildable page    *)
(* artifact. One cross-platform file lock serializes builders. Pages become *)
(* durable before a fixed manifest is atomically replaced. Normal reads do  *)
(* not consume the shadow; a diagnostic reader opens only an exact          *)
(* generation/epoch fence and leaves page slots cold.                       *)
(***************************************************************************)

CONSTANT MaxGeneration, MaxEpoch, MaxPage

ASSUME /\ MaxGeneration \in Nat \ {0}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Generations == 0..MaxGeneration
Epochs == 0..MaxEpoch
Pages == 1..MaxPage
BuildPhases == {"idle", "building", "durable"}
ReadStates == {"idle", "reading", "succeeded", "failed"}

VARIABLES
    publishedGeneration,
    publishedEpoch,
    publishedHistory,
    buildPhase,
    buildGeneration,
    buildEpoch,
    buildExpectedPrevious,
    durableArtifacts,
    staleRequestRejected,
    handleOpen,
    handleGeneration,
    handleEpoch,
    queriesStarted,
    loadedPages,
    corruptPages,
    readState,
    requiredPage,
    poisoned

vars == <<
    publishedGeneration,
    publishedEpoch,
    publishedHistory,
    buildPhase,
    buildGeneration,
    buildEpoch,
    buildExpectedPrevious,
    durableArtifacts,
    staleRequestRejected,
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

Init ==
    /\ publishedGeneration = 0
    /\ publishedEpoch = 0
    /\ publishedHistory = {0}
    /\ buildPhase = "idle"
    /\ buildGeneration = 0
    /\ buildEpoch = 0
    /\ buildExpectedPrevious = 0
    /\ durableArtifacts = {}
    /\ staleRequestRejected = FALSE
    /\ handleOpen = FALSE
    /\ handleGeneration = 0
    /\ handleEpoch = 0
    /\ queriesStarted = FALSE
    /\ loadedPages = {}
    /\ corruptPages = {}
    /\ readState = "idle"
    /\ requiredPage = 0
    /\ poisoned = FALSE

RejectStaleRequest ==
    /\ buildPhase = "idle"
    /\ \E expected \in Generations:
        /\ expected # publishedGeneration
        /\ buildExpectedPrevious' = expected
    /\ staleRequestRejected' = TRUE
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
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

BeginBuild ==
    /\ buildPhase = "idle"
    /\ publishedGeneration < MaxGeneration
    /\ \E generation \in (publishedGeneration + 1)..MaxGeneration:
        /\ buildGeneration' = generation
    /\ \E epoch \in Epochs:
        /\ buildEpoch' = epoch
    /\ buildExpectedPrevious' = publishedGeneration
    /\ buildPhase' = "building"
    /\ staleRequestRejected' = FALSE
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        durableArtifacts,
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

PersistGenerationPages ==
    /\ buildPhase = "building"
    /\ buildPhase' = "durable"
    /\ durableArtifacts' = durableArtifacts \cup {buildGeneration}
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        staleRequestRejected,
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

PublishManifest ==
    /\ buildPhase = "durable"
    /\ buildExpectedPrevious = publishedGeneration
    /\ buildGeneration > publishedGeneration
    /\ buildGeneration \in durableArtifacts
    /\ publishedGeneration' = buildGeneration
    /\ publishedEpoch' = buildEpoch
    /\ publishedHistory' = publishedHistory \cup {buildGeneration}
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ buildExpectedPrevious' = publishedGeneration'
    /\ UNCHANGED <<
        durableArtifacts,
        staleRequestRejected,
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

CrashBeforeManifest ==
    /\ buildPhase # "idle"
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ buildExpectedPrevious' = publishedGeneration
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        durableArtifacts,
        staleRequestRejected,
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

OpenExactShadow ==
    /\ ~handleOpen
    /\ publishedGeneration # 0
    /\ publishedGeneration \in durableArtifacts
    /\ handleOpen' = TRUE
    /\ handleGeneration' = publishedGeneration
    /\ handleEpoch' = publishedEpoch
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        corruptPages
        >>

BeginPageRead ==
    /\ handleOpen
    /\ ~poisoned
    /\ readState = "idle"
    /\ \E page \in Pages:
        /\ requiredPage' = page
    /\ queriesStarted' = TRUE
    /\ readState' = "reading"
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        handleOpen,
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
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        handleOpen,
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
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        handleOpen,
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
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        handleOpen,
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
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        readState,
        requiredPage,
        poisoned
        >>

CrashHandle ==
    /\ handleOpen
    /\ handleOpen' = FALSE
    /\ handleGeneration' = 0
    /\ handleEpoch' = 0
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        publishedGeneration,
        publishedEpoch,
        publishedHistory,
        buildPhase,
        buildGeneration,
        buildEpoch,
        buildExpectedPrevious,
        durableArtifacts,
        staleRequestRejected,
        corruptPages
        >>

Next ==
    \/ RejectStaleRequest
    \/ BeginBuild
    \/ PersistGenerationPages
    \/ PublishManifest
    \/ CrashBeforeManifest
    \/ OpenExactShadow
    \/ BeginPageRead
    \/ ReadHealthyPage
    \/ ReadCorruptPage
    \/ FinishRead
    \/ CorruptColdPage
    \/ CrashHandle

TypeOK ==
    /\ publishedGeneration \in Generations
    /\ publishedEpoch \in Epochs
    /\ publishedHistory \subseteq Generations
    /\ buildPhase \in BuildPhases
    /\ buildGeneration \in Generations
    /\ buildEpoch \in Epochs
    /\ buildExpectedPrevious \in Generations
    /\ durableArtifacts \subseteq (1..MaxGeneration)
    /\ staleRequestRejected \in BOOLEAN
    /\ handleOpen \in BOOLEAN
    /\ handleGeneration \in Generations
    /\ handleEpoch \in Epochs
    /\ queriesStarted \in BOOLEAN
    /\ loadedPages \subseteq Pages
    /\ corruptPages \subseteq Pages
    /\ readState \in ReadStates
    /\ requiredPage \in 0..MaxPage
    /\ poisoned \in BOOLEAN

PublishedManifestHasDurablePages ==
    publishedGeneration # 0 => publishedGeneration \in durableArtifacts

PublishedGenerationNeverRegresses ==
    /\ publishedGeneration \in publishedHistory
    /\ \A generation \in publishedHistory:
        generation <= publishedGeneration

BuildOwnsPublishedBase ==
    buildPhase # "idle" => buildExpectedPrevious = publishedGeneration

OpenHandlePinsDurableGeneration ==
    handleOpen =>
        /\ handleGeneration \in publishedHistory
        /\ handleGeneration \in durableArtifacts
        /\ handleGeneration <= publishedGeneration

OpenDoesNotWarmPages ==
    handleOpen /\ ~queriesStarted => loadedPages = {}

SuccessfulReadVerifiedPage ==
    readState = "succeeded" =>
        /\ requiredPage \in loadedPages
        /\ requiredPage \notin corruptPages
        /\ ~poisoned

CorruptionPoisonsOnlyShadowHandle ==
    poisoned => /\ handleOpen /\ readState = "failed"

Spec == Init /\ [][Next]_vars

=============================================================================
