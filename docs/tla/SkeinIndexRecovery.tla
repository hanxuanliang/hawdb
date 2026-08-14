------------------------- MODULE SkeinIndexRecovery -------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A checkpoint-selected immutable index root is recovered by replaying WAL *)
(* into a bounded dirty overlay. Full overlays become immutable candidate    *)
(* delta pages. Candidate generations are never allowed to replace files     *)
(* selected by an older manifest, and become visible only after replay is    *)
(* complete and the new manifest is published.                               *)
(***************************************************************************)

CONSTANT MaxEpoch, DirtyBudget, MaxAttempts

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ DirtyBudget \in Nat \ {0}
       /\ MaxAttempts \in Nat \ {0}

Keys == 1..2
BaseGeneration == 1
BaseEpoch == 0
BaseState == {1}
Phases == {"idle", "replaying", "publishing", "unavailable"}

ApplyChange(state, change) ==
    IF change.present
    THEN state \cup {change.key}
    ELSE state \ {change.key}

RECURSIVE ApplyChanges(_, _)
ApplyChanges(state, changes) ==
    IF Len(changes) = 0
    THEN state
    ELSE ApplyChanges(ApplyChange(state, Head(changes)), Tail(changes))

ApplyOverlay(state, keys, values) ==
    {key \in Keys : IF key \in keys THEN values[key] ELSE key \in state}

ApplyPage(state, page) == ApplyOverlay(state, page.keys, page.values)

RECURSIVE ApplyPages(_, _)
ApplyPages(state, pages) ==
    IF Len(pages) = 0
    THEN state
    ELSE ApplyPages(ApplyPage(state, Head(pages)), Tail(pages))

VARIABLES
    canonicalState,
    commitEpoch,
    wal,
    phase,
    nextGeneration,
    candidateGeneration,
    candidateEpoch,
    replayCursor,
    recoveredState,
    dirtyKeys,
    dirtyValues,
    candidatePages,
    durableCandidateGenerations,
    publishedGeneration,
    publishedEpoch,
    publishedPages,
    publishedState,
    schemaInvalidated,
    sqlUsesRecoveredIndex

vars == <<
    canonicalState,
    commitEpoch,
    wal,
    phase,
    nextGeneration,
    candidateGeneration,
    candidateEpoch,
    replayCursor,
    recoveredState,
    dirtyKeys,
    dirtyValues,
    candidatePages,
    durableCandidateGenerations,
    publishedGeneration,
    publishedEpoch,
    publishedPages,
    publishedState,
    schemaInvalidated,
    sqlUsesRecoveredIndex
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ wal = <<>>
    /\ phase = "idle"
    /\ nextGeneration = BaseGeneration + 1
    /\ candidateGeneration = 0
    /\ candidateEpoch = 0
    /\ replayCursor = 1
    /\ recoveredState = BaseState
    /\ dirtyKeys = {}
    /\ dirtyValues = [key \in Keys |-> FALSE]
    /\ candidatePages = <<>>
    /\ durableCandidateGenerations = {}
    /\ publishedGeneration = 0
    /\ publishedEpoch = BaseEpoch
    /\ publishedPages = <<>>
    /\ publishedState = BaseState
    /\ schemaInvalidated = FALSE
    /\ sqlUsesRecoveredIndex = FALSE

CommitInsert ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \cup {key}
        /\ wal' = Append(wal, [key |-> key, present |-> TRUE])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch,
        replayCursor, recoveredState, dirtyKeys, dirtyValues,
        candidatePages, durableCandidateGenerations, publishedGeneration,
        publishedEpoch, publishedPages, publishedState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

CommitDelete ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \ {key}
        /\ wal' = Append(wal, [key |-> key, present |-> FALSE])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch,
        replayCursor, recoveredState, dirtyKeys, dirtyValues,
        candidatePages, durableCandidateGenerations, publishedGeneration,
        publishedEpoch, publishedPages, publishedState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

BeginRecovery ==
    /\ phase = "idle"
    /\ commitEpoch > BaseEpoch
    /\ nextGeneration <= BaseGeneration + MaxAttempts
    /\ phase' = "replaying"
    /\ nextGeneration' = nextGeneration + 1
    /\ candidateGeneration' = nextGeneration
    /\ candidateEpoch' = commitEpoch
    /\ replayCursor' = 1
    /\ recoveredState' = BaseState
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidatePages' = <<>>
    /\ schemaInvalidated' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedPages, publishedState,
        sqlUsesRecoveredIndex
        >>

ReplayNext ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET change == wal[replayCursor] IN
        /\ \/ change.key \in dirtyKeys
           \/ Cardinality(dirtyKeys) < DirtyBudget
        /\ recoveredState' = ApplyChange(recoveredState, change)
        /\ dirtyKeys' = dirtyKeys \cup {change.key}
        /\ dirtyValues' = [dirtyValues EXCEPT ![change.key] = change.present]
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, candidatePages,
        durableCandidateGenerations, publishedGeneration, publishedEpoch,
        publishedPages, publishedState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

FlushDirty ==
    /\ phase = "replaying"
    /\ dirtyKeys # {}
    /\ candidatePages' = Append(
        candidatePages,
        [keys |-> dirtyKeys, values |-> dirtyValues]
        )
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, replayCursor, recoveredState,
        durableCandidateGenerations, publishedGeneration, publishedEpoch,
        publishedPages, publishedState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

FinishReplay ==
    /\ phase = "replaying"
    /\ replayCursor > Len(wal)
    /\ dirtyKeys = {}
    /\ recoveredState = canonicalState
    /\ phase' = "publishing"
    /\ durableCandidateGenerations' =
        durableCandidateGenerations \cup {candidateGeneration}
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, publishedGeneration,
        publishedEpoch, publishedPages, publishedState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

PublishManifest ==
    /\ phase = "publishing"
    /\ candidateEpoch = commitEpoch
    /\ candidateGeneration \in durableCandidateGenerations
    /\ phase' = "idle"
    /\ publishedGeneration' = candidateGeneration
    /\ publishedEpoch' = candidateEpoch
    /\ publishedPages' = candidatePages
    /\ publishedState' = ApplyPages(BaseState, candidatePages)
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

InvalidateSchema ==
    /\ phase = "replaying"
    /\ phase' = "unavailable"
    /\ schemaInvalidated' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedPages, publishedState,
        sqlUsesRecoveredIndex
        >>

CrashCandidate ==
    /\ phase \in {"replaying", "publishing"}
    /\ phase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ replayCursor' = 1
    /\ recoveredState' = BaseState
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidatePages' = <<>>
    /\ schemaInvalidated' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        durableCandidateGenerations, publishedGeneration, publishedEpoch,
        publishedPages, publishedState, sqlUsesRecoveredIndex
        >>

Next ==
    \/ CommitInsert
    \/ CommitDelete
    \/ BeginRecovery
    \/ ReplayNext
    \/ FlushDirty
    \/ FinishReplay
    \/ PublishManifest
    \/ InvalidateSchema
    \/ CrashCandidate

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in 0..MaxEpoch
    /\ wal \in Seq([key : Keys, present : BOOLEAN])
    /\ phase \in Phases
    /\ nextGeneration \in (BaseGeneration + 1)..(BaseGeneration + MaxAttempts + 1)
    /\ candidateGeneration \in 0..(BaseGeneration + MaxAttempts)
    /\ candidateEpoch \in 0..MaxEpoch
    /\ replayCursor \in Nat \ {0}
    /\ recoveredState \subseteq Keys
    /\ dirtyKeys \subseteq Keys
    /\ dirtyValues \in [Keys -> BOOLEAN]
    /\ candidatePages \in Seq([keys : SUBSET Keys, values : [Keys -> BOOLEAN]])
    /\ durableCandidateGenerations \subseteq 1..(BaseGeneration + MaxAttempts)
    /\ publishedGeneration \in 0..(BaseGeneration + MaxAttempts)
    /\ publishedEpoch \in 0..MaxEpoch
    /\ publishedPages \in Seq([keys : SUBSET Keys, values : [Keys -> BOOLEAN]])
    /\ publishedState \subseteq Keys
    /\ schemaInvalidated \in BOOLEAN
    /\ sqlUsesRecoveredIndex \in BOOLEAN

CanonicalEqualsWal == canonicalState = ApplyChanges(BaseState, wal)

DirtyOverlayBounded == Cardinality(dirtyKeys) <= DirtyBudget

ReplayPrefixEquivalent ==
    phase \in {"replaying", "publishing"} =>
        recoveredState = ApplyChanges(BaseState, SubSeq(wal, 1, replayCursor - 1))

CandidateMergeEquivalent ==
    phase \in {"replaying", "publishing"} =>
        recoveredState = ApplyOverlay(
            ApplyPages(BaseState, candidatePages),
            dirtyKeys,
            dirtyValues
            )

PublishedManifestSelectsCompletePages ==
    publishedState = ApplyPages(BaseState, publishedPages)

SelectedRecoveryMatchesCanonical ==
    publishedGeneration # 0 /\ publishedEpoch = commitEpoch =>
        publishedState = canonicalState

CandidateGenerationIsIsolated ==
    phase \in {"replaying", "publishing"} =>
        candidateGeneration # publishedGeneration

SchemaInvalidationCannotPublish ==
    schemaInvalidated => phase = "unavailable"

ProductionSqlRemainsOnOracle == ~sqlUsesRecoveredIndex

Spec == Init /\ [][Next]_vars

=============================================================================
