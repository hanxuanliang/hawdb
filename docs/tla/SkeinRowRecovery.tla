-------------------------- MODULE SkeinRowRecovery --------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A generation-pinned relational row root may be correlated with one      *)
(* canonical checkpoint without reading any row-page slot. Recovery then   *)
(* consumes every consecutive global WAL epoch into an exact primary-key   *)
(* overlay. The overlay is admitted atomically and remains non-serving: a  *)
(* missing, stale, corrupt, schema-invalidated, or over-budget candidate    *)
(* cannot affect canonical recovery or SQL. An immutable view is exposed   *)
(* only after the complete requested WAL prefix has been consumed. A       *)
(* snapshot pins that view even if a newer shadow root is published or a   *)
(* live commit invalidates the store's current shadow view.                 *)
(***************************************************************************)

CONSTANT MaxEpoch, OverlayBudget

ASSUME /\ MaxEpoch \in Nat \ {0, 1}
       /\ OverlayBudget \in Nat \ {0}

Keys == 1..2
Kinds == {"row", "empty", "schema"}
Phases == {"unmounted", "replaying", "ready", "unavailable"}
FailureKinds == {"none", "missing", "stale", "corrupt", "capacity", "schema", "live"}

BaseGeneration == 1
BaseEpoch == 1
BaseState == {1}

ApplyRecord(state, record) ==
    CASE record.kind = "row" -> (state \ record.keys) \cup record.inserted
      [] OTHER -> state

RECURSIVE ApplyWalPrefix(_, _, _)
ApplyWalPrefix(state, log, count) ==
    IF count = 0
    THEN state
    ELSE ApplyRecord(ApplyWalPrefix(state, log, count - 1), log[count])

ApplyOverlay(state, keys, values) ==
    {key \in Keys : IF key \in keys THEN values[key] ELSE key \in state}

VARIABLES
    canonicalState,
    commitEpoch,
    wal,
    phase,
    baseGeneration,
    latestGeneration,
    replayCursor,
    visibleEpoch,
    overlayKeys,
    overlayValues,
    viewAvailable,
    viewGeneration,
    viewEpoch,
    viewState,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerState,
    failureKind,
    pageSlotsRead,
    sqlUsesRowView

vars == <<
    canonicalState,
    commitEpoch,
    wal,
    phase,
    baseGeneration,
    latestGeneration,
    replayCursor,
    visibleEpoch,
    overlayKeys,
    overlayValues,
    viewAvailable,
    viewGeneration,
    viewEpoch,
    viewState,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerState,
    failureKind,
    pageSlotsRead,
    sqlUsesRowView
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ wal = <<>>
    /\ phase = "unmounted"
    /\ baseGeneration = 0
    /\ latestGeneration = BaseGeneration
    /\ replayCursor = 1
    /\ visibleEpoch = BaseEpoch
    /\ overlayKeys = {}
    /\ overlayValues = [key \in Keys |-> FALSE]
    /\ viewAvailable = FALSE
    /\ viewGeneration = 0
    /\ viewEpoch = 0
    /\ viewState = {}
    /\ readerPinned = FALSE
    /\ readerGeneration = 0
    /\ readerEpoch = 0
    /\ readerState = {}
    /\ failureKind = "none"
    /\ pageSlotsRead = FALSE
    /\ sqlUsesRowView = FALSE

CommitRow ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ \E changed \in SUBSET Keys:
        /\ changed # {}
        /\ \E inserted \in SUBSET changed:
            LET record == [
                epoch |-> commitEpoch + 1,
                kind |-> "row",
                keys |-> changed,
                inserted |-> inserted
                ]
            IN
                /\ canonicalState' = ApplyRecord(canonicalState, record)
                /\ wal' = Append(wal, record)
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, baseGeneration, latestGeneration, replayCursor, visibleEpoch,
        overlayKeys, overlayValues, viewAvailable, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        failureKind, pageSlotsRead, sqlUsesRowView
        >>

CommitTwoFragments ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ \E firstChanged \in SUBSET Keys:
        /\ firstChanged # {}
        /\ \E firstInserted \in SUBSET firstChanged:
            /\ \E secondChanged \in SUBSET Keys:
                /\ secondChanged # {}
                /\ \E secondInserted \in SUBSET secondChanged:
                    LET epoch == commitEpoch + 1
                        first == [
                            epoch |-> epoch,
                            kind |-> "row",
                            keys |-> firstChanged,
                            inserted |-> firstInserted
                            ]
                        second == [
                            epoch |-> epoch,
                            kind |-> "row",
                            keys |-> secondChanged,
                            inserted |-> secondInserted
                            ]
                    IN
                        /\ canonicalState' =
                            ApplyRecord(ApplyRecord(canonicalState, first), second)
                        /\ wal' = wal \o <<first, second>>
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, baseGeneration, latestGeneration, replayCursor, visibleEpoch,
        overlayKeys, overlayValues, viewAvailable, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        failureKind, pageSlotsRead, sqlUsesRowView
        >>

CommitEmpty ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ wal' = Append(
        wal,
        [
            epoch |-> commitEpoch + 1,
            kind |-> "empty",
            keys |-> {},
            inserted |-> {}
            ]
        )
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        canonicalState, phase, baseGeneration, latestGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues,
        viewAvailable, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

CommitSchema ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ wal' = Append(
        wal,
        [
            epoch |-> commitEpoch + 1,
            kind |-> "schema",
            keys |-> {},
            inserted |-> {}
            ]
        )
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        canonicalState, phase, baseGeneration, latestGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues,
        viewAvailable, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

MountValidBase ==
    /\ phase = "unmounted"
    /\ phase' = "replaying"
    /\ baseGeneration' = BaseGeneration
    /\ replayCursor' = 1
    /\ visibleEpoch' = BaseEpoch
    /\ overlayKeys' = {}
    /\ overlayValues' = [key \in Keys |-> FALSE]
    /\ failureKind' = "none"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, latestGeneration, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

MountInvalidBase(reason) ==
    /\ phase = "unmounted"
    /\ reason \in {"missing", "stale", "corrupt"}
    /\ phase' = "unavailable"
    /\ baseGeneration' = 0
    /\ viewAvailable' = FALSE
    /\ failureKind' = reason
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, latestGeneration, replayCursor,
        visibleEpoch, overlayKeys, overlayValues, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        pageSlotsRead, sqlUsesRowView
        >>

ReplayRowFragment ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET record == wal[replayCursor] IN
        /\ record.kind = "row"
        /\ Cardinality(overlayKeys \cup record.keys) <= OverlayBudget
        /\ overlayKeys' = overlayKeys \cup record.keys
        /\ overlayValues' = [key \in Keys |->
            IF key \in record.keys
            THEN key \in record.inserted
            ELSE overlayValues[key]]
        /\ visibleEpoch' = record.epoch
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestGeneration, viewAvailable, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        failureKind, pageSlotsRead, sqlUsesRowView
        >>

ReplayEmpty ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ wal[replayCursor].kind = "empty"
    /\ visibleEpoch' = wal[replayCursor].epoch
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestGeneration, overlayKeys, overlayValues, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

RejectCapacity ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET record == wal[replayCursor] IN
        /\ record.kind = "row"
        /\ Cardinality(overlayKeys \cup record.keys) > OverlayBudget
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "capacity"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration, latestGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues,
        viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

RejectSchema ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ wal[replayCursor].kind = "schema"
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "schema"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration, latestGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues,
        viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

FinishRecovery ==
    /\ phase = "replaying"
    /\ replayCursor = Len(wal) + 1
    /\ visibleEpoch = commitEpoch
    /\ phase' = "ready"
    /\ viewAvailable' = TRUE
    /\ viewGeneration' = baseGeneration
    /\ viewEpoch' = visibleEpoch
    /\ viewState' = ApplyOverlay(BaseState, overlayKeys, overlayValues)
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration, latestGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

PinReader ==
    /\ phase = "ready"
    /\ viewAvailable
    /\ ~readerPinned
    /\ readerPinned' = TRUE
    /\ readerGeneration' = viewGeneration
    /\ readerEpoch' = viewEpoch
    /\ readerState' = viewState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestGeneration, replayCursor, visibleEpoch, overlayKeys,
        overlayValues, viewAvailable, viewGeneration, viewEpoch, viewState,
        failureKind, pageSlotsRead, sqlUsesRowView
        >>

PublishNewShadowRoot ==
    /\ phase = "ready"
    /\ latestGeneration = BaseGeneration
    /\ latestGeneration' = BaseGeneration + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        replayCursor, visibleEpoch, overlayKeys, overlayValues,
        viewAvailable, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

LiveRowCommit ==
    /\ phase = "ready"
    /\ commitEpoch < MaxEpoch
    /\ \E changed \in SUBSET Keys:
        /\ changed # {}
        /\ \E inserted \in SUBSET changed:
            LET record == [
                epoch |-> commitEpoch + 1,
                kind |-> "row",
                keys |-> changed,
                inserted |-> inserted
                ]
            IN
                /\ canonicalState' = ApplyRecord(canonicalState, record)
                /\ wal' = Append(wal, record)
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "live"
    /\ UNCHANGED <<
        baseGeneration, latestGeneration, replayCursor, visibleEpoch,
        overlayKeys, overlayValues, viewGeneration, viewEpoch, viewState,
        readerPinned, readerGeneration, readerEpoch, readerState,
        pageSlotsRead, sqlUsesRowView
        >>

LiveEmptyCommit ==
    /\ phase = "ready"
    /\ commitEpoch < MaxEpoch
    /\ wal' = Append(
        wal,
        [
            epoch |-> commitEpoch + 1,
            kind |-> "empty",
            keys |-> {},
            inserted |-> {}
            ]
        )
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "live"
    /\ UNCHANGED <<
        canonicalState, baseGeneration, latestGeneration, replayCursor,
        visibleEpoch, overlayKeys, overlayValues, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        pageSlotsRead, sqlUsesRowView
        >>

Next ==
    \/ CommitRow
    \/ CommitTwoFragments
    \/ CommitEmpty
    \/ CommitSchema
    \/ MountValidBase
    \/ \E reason \in {"missing", "stale", "corrupt"}:
        MountInvalidBase(reason)
    \/ ReplayRowFragment
    \/ ReplayEmpty
    \/ RejectCapacity
    \/ RejectSchema
    \/ FinishRecovery
    \/ PinReader
    \/ PublishNewShadowRoot
    \/ LiveRowCommit
    \/ LiveEmptyCommit

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in BaseEpoch..MaxEpoch
    /\ wal \in Seq([
        epoch : (BaseEpoch + 1)..MaxEpoch,
        kind : Kinds,
        keys : SUBSET Keys,
        inserted : SUBSET Keys
        ])
    /\ \A index \in 1..Len(wal):
        /\ wal[index].inserted \subseteq wal[index].keys
        /\ (wal[index].kind = "row") = (wal[index].keys # {})
    /\ phase \in Phases
    /\ baseGeneration \in {0, BaseGeneration}
    /\ latestGeneration \in {BaseGeneration, BaseGeneration + 1}
    /\ replayCursor \in 1..(Len(wal) + 1)
    /\ visibleEpoch \in BaseEpoch..MaxEpoch
    /\ overlayKeys \subseteq Keys
    /\ overlayValues \in [Keys -> BOOLEAN]
    /\ viewAvailable \in BOOLEAN
    /\ viewGeneration \in {0, BaseGeneration}
    /\ viewEpoch \in 0..MaxEpoch
    /\ viewState \subseteq Keys
    /\ readerPinned \in BOOLEAN
    /\ readerGeneration \in {0, BaseGeneration}
    /\ readerEpoch \in 0..MaxEpoch
    /\ readerState \subseteq Keys
    /\ failureKind \in FailureKinds
    /\ pageSlotsRead \in BOOLEAN
    /\ sqlUsesRowView \in BOOLEAN

WalEpochsAreContiguous ==
    IF Len(wal) = 0
    THEN commitEpoch = BaseEpoch
    ELSE /\ wal[1].epoch = BaseEpoch + 1
         /\ wal[Len(wal)].epoch = commitEpoch
         /\ \A index \in 2..Len(wal):
             \/ wal[index].epoch = wal[index - 1].epoch
             \/ wal[index].epoch = wal[index - 1].epoch + 1

CanonicalEqualsWal ==
    canonicalState = ApplyWalPrefix(BaseState, wal, Len(wal))

OverlayIsBounded ==
    Cardinality(overlayKeys) <= OverlayBudget

ReplayPrefixEquivalent ==
    phase = "replaying" =>
        /\ visibleEpoch = IF replayCursor = 1
            THEN BaseEpoch
            ELSE wal[replayCursor - 1].epoch
        /\ ApplyOverlay(BaseState, overlayKeys, overlayValues) =
            ApplyWalPrefix(BaseState, wal, replayCursor - 1)

RejectedFragmentIsAtomic ==
    failureKind \in {"capacity", "schema"} =>
        ApplyOverlay(BaseState, overlayKeys, overlayValues) =
            ApplyWalPrefix(BaseState, wal, replayCursor - 1)

OnlyCompleteRecoveryIsVisible ==
    viewAvailable =>
        /\ phase = "ready"
        /\ replayCursor = Len(wal) + 1
        /\ viewEpoch = commitEpoch
        /\ viewState = canonicalState
        /\ viewGeneration = BaseGeneration

NonReadyRecoveryDoesNotPublish ==
    phase # "ready" => ~viewAvailable

UnavailableShadowNeverServes ==
    phase = "unavailable" =>
        /\ ~viewAvailable
        /\ ~sqlUsesRowView

ColdMountDoesNotReadPageSlots ==
    ~pageSlotsRead

ProductionSqlRemainsOnCanonicalOracle ==
    ~sqlUsesRowView

PinnedReaderDoesNotDrift ==
    readerPinned =>
        /\ readerGeneration = BaseGeneration
        /\ readerEpoch \in BaseEpoch..commitEpoch
        /\ readerState =
            ApplyWalPrefix(
                BaseState,
                wal,
                Cardinality({index \in 1..Len(wal): wal[index].epoch <= readerEpoch})
                )

NewShadowRootDoesNotMovePinnedViews ==
    latestGeneration > BaseGeneration =>
        /\ viewGeneration \in {0, BaseGeneration}
        /\ readerGeneration \in {0, BaseGeneration}

=============================================================================
