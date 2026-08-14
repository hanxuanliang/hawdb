-------------------- MODULE SkeinIndexPublication --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Immutable row and index roots publish through one generation-fenced     *)
(* manifest. Open validates only that compact root identity and leaves leaf *)
(* pages cold. A query loads its required page on demand; corruption fails  *)
(* the query and poisons the open handle.                                   *)
(***************************************************************************)

CONSTANT MaxEpoch, MaxPage

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Epochs == 0..MaxEpoch
Pages == 1..MaxPage
BuildPhases == {"idle", "building", "durable"}
QueryStates == {"idle", "reading", "succeeded", "failed"}

VARIABLES
    canonicalEpoch,
    rowRootEpoch,
    indexRootEpoch,
    rootGeneration,
    buildPhase,
    buildTarget,
    buildBaseGeneration,
    durableIndexEpochs,
    handleOpen,
    queriesStarted,
    loadedPages,
    corruptPages,
    queryState,
    requiredPage,
    poisoned,
    stalePublishRejected

vars == <<
    canonicalEpoch,
    rowRootEpoch,
    indexRootEpoch,
    rootGeneration,
    buildPhase,
    buildTarget,
    buildBaseGeneration,
    durableIndexEpochs,
    handleOpen,
    queriesStarted,
    loadedPages,
    corruptPages,
    queryState,
    requiredPage,
    poisoned,
    stalePublishRejected
>>

Init ==
    /\ canonicalEpoch = 0
    /\ rowRootEpoch = 0
    /\ indexRootEpoch = 0
    /\ rootGeneration = 0
    /\ buildPhase = "idle"
    /\ buildTarget = 0
    /\ buildBaseGeneration = 0
    /\ durableIndexEpochs = {0}
    /\ handleOpen = FALSE
    /\ queriesStarted = FALSE
    /\ loadedPages = {}
    /\ corruptPages = {}
    /\ queryState = "idle"
    /\ requiredPage = 0
    /\ poisoned = FALSE
    /\ stalePublishRejected = FALSE

Commit ==
    /\ canonicalEpoch < MaxEpoch
    /\ canonicalEpoch' = canonicalEpoch + 1
    /\ UNCHANGED <<
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

BeginBuild ==
    /\ buildPhase = "idle"
    /\ rowRootEpoch < canonicalEpoch
    /\ buildPhase' = "building"
    /\ buildTarget' = canonicalEpoch
    /\ buildBaseGeneration' = rootGeneration
    /\ stalePublishRejected' = FALSE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned
        >>

PersistIndexPages ==
    /\ buildPhase = "building"
    /\ buildPhase' = "durable"
    /\ durableIndexEpochs' = durableIndexEpochs \cup {buildTarget}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildTarget,
        buildBaseGeneration,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

(***************************************************************************)
(* A competing complete checkpoint may win while this builder is active.  *)
(* This represents the generation CAS race without modeling two builders.  *)
(***************************************************************************)
PublishCompetingRoot ==
    /\ buildPhase \in {"building", "durable"}
    /\ canonicalEpoch > rowRootEpoch
    /\ rowRootEpoch' = canonicalEpoch
    /\ indexRootEpoch' = canonicalEpoch
    /\ rootGeneration' = rootGeneration + 1
    /\ durableIndexEpochs' = durableIndexEpochs \cup {canonicalEpoch}
    /\ UNCHANGED <<
        canonicalEpoch,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

PublishBuiltRoot ==
    /\ buildPhase = "durable"
    /\ buildTarget = canonicalEpoch
    /\ buildTarget \in durableIndexEpochs
    /\ buildBaseGeneration = rootGeneration
    /\ rowRootEpoch' = buildTarget
    /\ indexRootEpoch' = buildTarget
    /\ rootGeneration' = rootGeneration + 1
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration + 1
    /\ UNCHANGED <<
        canonicalEpoch,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

RejectStalePublish ==
    /\ buildPhase = "durable"
    /\ \/ buildTarget # canonicalEpoch
       \/ buildBaseGeneration # rootGeneration
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration
    /\ stalePublishRejected' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned
        >>

OpenHandle ==
    /\ ~handleOpen
    /\ rowRootEpoch = indexRootEpoch
    /\ indexRootEpoch \in durableIndexEpochs
    /\ handleOpen' = TRUE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        corruptPages,
        stalePublishRejected
        >>

BeginLookup ==
    /\ handleOpen
    /\ ~poisoned
    /\ queryState = "idle"
    /\ \E page \in Pages:
        /\ requiredPage' = page
        /\ queryState' = "reading"
    /\ queriesStarted' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        loadedPages,
        corruptPages,
        poisoned,
        stalePublishRejected
        >>

ReadHealthyPage ==
    /\ queryState = "reading"
    /\ requiredPage \notin corruptPages
    /\ loadedPages' = loadedPages \cup {requiredPage}
    /\ queryState' = "succeeded"
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        corruptPages,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

ReadCorruptPage ==
    /\ queryState = "reading"
    /\ requiredPage \in corruptPages
    /\ queryState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        requiredPage,
        stalePublishRejected
        >>

FinishLookup ==
    /\ queryState = "succeeded"
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        poisoned,
        stalePublishRejected
        >>

CorruptColdPage ==
    /\ \E page \in Pages \ loadedPages:
        corruptPages' = corruptPages \cup {page}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>

CrashAndRecover ==
    /\ handleOpen \/ buildPhase # "idle"
    /\ handleOpen' = FALSE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        corruptPages,
        stalePublishRejected
        >>

Next ==
    \/ Commit
    \/ BeginBuild
    \/ PersistIndexPages
    \/ PublishCompetingRoot
    \/ PublishBuiltRoot
    \/ RejectStalePublish
    \/ OpenHandle
    \/ BeginLookup
    \/ ReadHealthyPage
    \/ ReadCorruptPage
    \/ FinishLookup
    \/ CorruptColdPage
    \/ CrashAndRecover

TypeOK ==
    /\ canonicalEpoch \in Epochs
    /\ rowRootEpoch \in Epochs
    /\ indexRootEpoch \in Epochs
    /\ rootGeneration \in Nat
    /\ buildPhase \in BuildPhases
    /\ buildTarget \in Epochs
    /\ buildBaseGeneration \in Nat
    /\ durableIndexEpochs \subseteq Epochs
    /\ handleOpen \in BOOLEAN
    /\ queriesStarted \in BOOLEAN
    /\ loadedPages \subseteq Pages
    /\ corruptPages \subseteq Pages
    /\ queryState \in QueryStates
    /\ requiredPage \in 0..MaxPage
    /\ poisoned \in BOOLEAN
    /\ stalePublishRejected \in BOOLEAN

RowAndIndexRootsAgree == rowRootEpoch = indexRootEpoch

PublishedIndexIsDurable == indexRootEpoch \in durableIndexEpochs

PublishedRootsAreNotFuture == rowRootEpoch <= canonicalEpoch

OpenDoesNotWarmLeafPages ==
    handleOpen /\ ~queriesStarted => loadedPages = {}

SuccessfulLookupReadVerifiedPage ==
    queryState = "succeeded" =>
        /\ requiredPage \in loadedPages
        /\ requiredPage \notin corruptPages
        /\ ~poisoned

CorruptionPoisonsOpenHandle ==
    poisoned => /\ handleOpen /\ queryState = "failed"

Spec == Init /\ [][Next]_vars

=============================================================================
