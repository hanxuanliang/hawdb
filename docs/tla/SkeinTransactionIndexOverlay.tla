-------------------- MODULE SkeinTransactionIndexOverlay -------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* One authoritative transaction pins the committed index view and applies *)
(* successful statements to a bounded private overlay. Reads combine the   *)
(* pinned base and overlay. Rejected statements are atomic, and canonical  *)
(* visibility advances only after the complete workspace is durable.       *)
(***************************************************************************)

CONSTANT MaxEpoch, MaxOverlayEntries

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ MaxOverlayEntries \in Nat \ {0}

Keys == 1..2
BaseState == {1}
Phases == {"idle", "active", "prepared", "durable"}

ApplyChange(state, key, present) ==
    IF present THEN state \cup {key} ELSE state \ {key}

RangeOf(order) == {order[index] : index \in DOMAIN order}

BaseVisitOrders(base) ==
    {order \in [1..Cardinality(base) -> Keys] : RangeOf(order) = base}

ExactTombstoneMerge(base, current, order) ==
    LET retainedIndices ==
            {index \in DOMAIN order : order[index] \notin (base \ current)}
    IN {order[index] : index \in retainedIndices} \cup (current \ base)

HistoryState(history, epoch) ==
    IF epoch = 0 THEN BaseState ELSE history[epoch]

VARIABLES
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    baseEpoch,
    baseIndexState,
    workspaceState,
    workspaceIndexState,
    overlayEntries,
    lastAcceptedState,
    lastAcceptedEntries,
    statementRejected,
    workspaceRead,
    readState

vars == <<
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    baseEpoch,
    baseIndexState,
    workspaceState,
    workspaceIndexState,
    overlayEntries,
    lastAcceptedState,
    lastAcceptedEntries,
    statementRejected,
    workspaceRead,
    readState
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ baseEpoch = 0
    /\ baseIndexState = BaseState
    /\ workspaceState = BaseState
    /\ workspaceIndexState = BaseState
    /\ overlayEntries = 0
    /\ lastAcceptedState = BaseState
    /\ lastAcceptedEntries = 0
    /\ statementRejected = FALSE
    /\ workspaceRead = FALSE
    /\ readState = BaseState

Begin ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ baseEpoch' = commitEpoch
    /\ baseIndexState' = canonicalState
    /\ workspaceState' = canonicalState
    /\ workspaceIndexState' = canonicalState
    /\ overlayEntries' = 0
    /\ lastAcceptedState' = canonicalState
    /\ lastAcceptedEntries' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = canonicalState
    /\ UNCHANGED <<canonicalState, commitEpoch, durableHistory>>

StageStatement(key, present) ==
    /\ phase = "active"
    /\ key \in Keys
    /\ present \in BOOLEAN
    /\ overlayEntries < MaxOverlayEntries
    /\ LET next == ApplyChange(workspaceState, key, present) IN
       /\ workspaceState' = next
       /\ workspaceIndexState' = next
       /\ lastAcceptedState' = next
    /\ overlayEntries' = overlayEntries + 1
    /\ lastAcceptedEntries' = overlayEntries + 1
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, readState
       >>

RejectStatement ==
    /\ phase = "active"
    /\ overlayEntries = MaxOverlayEntries
    /\ statementRejected' = TRUE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        overlayEntries, lastAcceptedState, lastAcceptedEntries, readState
       >>

WorkspaceRead ==
    /\ phase = "active"
    /\ workspaceRead' = TRUE
    /\ readState' = workspaceIndexState
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        overlayEntries, lastAcceptedState, lastAcceptedEntries
       >>

PrepareCommit ==
    /\ phase = "active"
    /\ phase' = "prepared"
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        overlayEntries, lastAcceptedState, lastAcceptedEntries, readState
       >>

MakeDurable ==
    /\ phase = "prepared"
    /\ durableHistory' = Append(durableHistory, workspaceState)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, baseEpoch, baseIndexState,
        workspaceState, workspaceIndexState, overlayEntries,
        lastAcceptedState, lastAcceptedEntries, statementRejected,
        workspaceRead, readState
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalState' = workspaceState
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ overlayEntries' = 0
    /\ lastAcceptedEntries' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        durableHistory, baseEpoch, baseIndexState,
        workspaceState, workspaceIndexState, lastAcceptedState, readState
       >>

Rollback ==
    /\ phase \in {"active", "prepared"}
    /\ phase' = "idle"
    /\ workspaceState' = canonicalState
    /\ workspaceIndexState' = canonicalState
    /\ overlayEntries' = 0
    /\ lastAcceptedState' = canonicalState
    /\ lastAcceptedEntries' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = canonicalState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, baseEpoch, baseIndexState
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recoveredState == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalState' = recoveredState
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ baseEpoch' = recoveredEpoch
    /\ baseIndexState' = recoveredState
    /\ workspaceState' = recoveredState
    /\ workspaceIndexState' = recoveredState
    /\ overlayEntries' = 0
    /\ lastAcceptedState' = recoveredState
    /\ lastAcceptedEntries' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = recoveredState
    /\ UNCHANGED durableHistory

Next ==
    \/ Begin
    \/ \E key \in Keys, present \in BOOLEAN: StageStatement(key, present)
    \/ RejectStatement
    \/ WorkspaceRead
    \/ PrepareCommit
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(SUBSET Keys)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ baseEpoch \in 0..MaxEpoch
    /\ baseIndexState \subseteq Keys
    /\ workspaceState \subseteq Keys
    /\ workspaceIndexState \subseteq Keys
    /\ overlayEntries \in Nat
    /\ lastAcceptedState \subseteq Keys
    /\ lastAcceptedEntries \in Nat
    /\ statementRejected \in BOOLEAN
    /\ workspaceRead \in BOOLEAN
    /\ readState \subseteq Keys

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ canonicalState = HistoryState(durableHistory, commitEpoch)

PinnedBaseDoesNotDrift ==
    phase # "idle" =>
        /\ baseEpoch <= commitEpoch
        /\ baseIndexState = HistoryState(durableHistory, baseEpoch)

WorkspaceIndexMatchesRows ==
    phase \in {"active", "prepared", "durable"} =>
        workspaceIndexState = workspaceState

WorkspaceOverlayIsBounded == overlayEntries <= MaxOverlayEntries

RejectedStatementIsAtomic ==
    statementRejected =>
        /\ phase = "active"
        /\ workspaceState = lastAcceptedState
        /\ workspaceIndexState = lastAcceptedState
        /\ overlayEntries = lastAcceptedEntries

ReadYourOwnWritesUsesOverlay ==
    workspaceRead =>
        /\ phase = "active"
        /\ readState = workspaceState
        /\ readState = workspaceIndexState

PrefixMergeIsOrderIndependent ==
    \A base \in SUBSET Keys, current \in SUBSET Keys:
        \A order \in BaseVisitOrders(base):
            ExactTombstoneMerge(base, current, order) = current

=============================================================================
