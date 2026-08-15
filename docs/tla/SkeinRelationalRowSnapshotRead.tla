------------------ MODULE SkeinRelationalRowSnapshotRead ------------------
EXTENDS Integers, Naturals, Sequences, FiniteSets

(***************************************************************************)
(* One pinned relational snapshot composes an immutable checkpoint with    *)
(* disk-backed recovery versions and immutable live versions. Collection   *)
(* admits distinct overlay keys and conservative resident bytes before a   *)
(* range can stream. Live versions override recovery versions, tombstones  *)
(* suppress checkpoint rows, and a later current view cannot move the      *)
(* pinned reader. Admission, cancellation, and callback panic do not poison*)
(* the reader; corruption does.                                             *)
(***************************************************************************)

CONSTANT MaxOverlayEntries, MaxOverlayBytes

ASSUME /\ MaxOverlayEntries \in 1..3
       /\ MaxOverlayBytes \in 1..8

Keys == 1..3
NoVersion == -1
Deleted == 0
Values == {NoVersion, Deleted, 1, 2, 3, 4, 5}
BaseEpoch == 1
VisibleEpoch == 3

Base == [key \in Keys |-> key]
Recovery == [key \in Keys |->
    CASE key = 1 -> 4
      [] key = 2 -> Deleted
      [] OTHER -> NoVersion]
Live == [key \in Keys |->
    CASE key = 1 -> NoVersion
      [] key = 2 -> 5
      [] OTHER -> Deleted]

NewestOverlay(key) ==
    IF Live[key] # NoVersion THEN Live[key] ELSE Recovery[key]

VisibleValue(key) ==
    IF NewestOverlay(key) # NoVersion THEN NewestOverlay(key) ELSE Base[key]

InRange(key, lower, upper) == lower < key /\ key < upper

EntryBytes(key, value) == key + IF value = Deleted THEN 0 ELSE 1

EmptyOverlay == [key \in Keys |-> NoVersion]

ExpectedOverlay(lower, upper) ==
    [key \in Keys |->
        IF InRange(key, lower, upper)
        THEN NewestOverlay(key)
        ELSE NoVersion]

ExpectedRows(lower, upper) ==
    (IF InRange(1, lower, upper) /\ VisibleValue(1) # Deleted
     THEN <<1>> ELSE <<>>) \o
    (IF InRange(2, lower, upper) /\ VisibleValue(2) # Deleted
     THEN <<2>> ELSE <<>>) \o
    (IF InRange(3, lower, upper) /\ VisibleValue(3) # Deleted
     THEN <<3>> ELSE <<>>)

IsPrefix(prefix, sequence) ==
    Len(prefix) <= Len(sequence)
    /\ \A index \in 1..Len(prefix): prefix[index] = sequence[index]

VARIABLES
    readState,
    outcome,
    lowerBound,
    upperBound,
    entryBudget,
    byteBudget,
    overlay,
    overlayEntries,
    overlayBytes,
    cursor,
    emittedRows,
    poisoned,
    stoppedEarly,
    currentViewEpoch

vars == <<
    readState,
    outcome,
    lowerBound,
    upperBound,
    entryBudget,
    byteBudget,
    overlay,
    overlayEntries,
    overlayBytes,
    cursor,
    emittedRows,
    poisoned,
    stoppedEarly,
    currentViewEpoch
>>

Init ==
    /\ readState = "idle"
    /\ outcome = "none"
    /\ lowerBound = 0
    /\ upperBound = 4
    /\ entryBudget = MaxOverlayEntries
    /\ byteBudget = MaxOverlayBytes
    /\ overlay = EmptyOverlay
    /\ overlayEntries = 0
    /\ overlayBytes = 0
    /\ cursor = 1
    /\ emittedRows = <<>>
    /\ poisoned = FALSE
    /\ stoppedEarly = FALSE
    /\ currentViewEpoch = VisibleEpoch

BeginRead(lower, upper, entries, bytes) ==
    /\ readState = "idle"
    /\ lower \in 0..2
    /\ upper \in 2..4
    /\ lower < upper
    /\ entries \in 1..MaxOverlayEntries
    /\ bytes \in 1..MaxOverlayBytes
    /\ readState' = "recovery"
    /\ outcome' = "none"
    /\ lowerBound' = lower
    /\ upperBound' = upper
    /\ entryBudget' = entries
    /\ byteBudget' = bytes
    /\ overlay' = EmptyOverlay
    /\ overlayEntries' = 0
    /\ overlayBytes' = 0
    /\ cursor' = 1
    /\ emittedRows' = <<>>
    /\ stoppedEarly' = FALSE
    /\ UNCHANGED <<poisoned, currentViewEpoch>>

CandidateValue ==
    IF readState = "recovery" THEN Recovery[cursor] ELSE Live[cursor]

CandidateEntries ==
    overlayEntries + IF overlay[cursor] = NoVersion THEN 1 ELSE 0

CandidateBytes ==
    IF overlay[cursor] = NoVersion
    THEN overlayBytes + EntryBytes(cursor, CandidateValue)
    ELSE overlayBytes - EntryBytes(cursor, overlay[cursor])
         + EntryBytes(cursor, CandidateValue)

SkipCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ (~InRange(cursor, lowerBound, upperBound)
        \/ CandidateValue = NoVersion)
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, emittedRows, poisoned,
        stoppedEarly, currentViewEpoch
        >>

AdmitCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ InRange(cursor, lowerBound, upperBound)
    /\ CandidateValue # NoVersion
    /\ CandidateEntries <= entryBudget
    /\ CandidateBytes <= byteBudget
    /\ overlay' = [overlay EXCEPT ![cursor] = CandidateValue]
    /\ overlayEntries' = CandidateEntries
    /\ overlayBytes' = CandidateBytes
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        emittedRows, poisoned, stoppedEarly, currentViewEpoch
        >>

RejectCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ InRange(cursor, lowerBound, upperBound)
    /\ CandidateValue # NoVersion
    /\ (CandidateEntries > entryBudget \/ CandidateBytes > byteBudget)
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        stoppedEarly, currentViewEpoch
        >>

BeginLiveCollection ==
    /\ readState = "recovery"
    /\ cursor > 3
    /\ readState' = "live"
    /\ cursor' = 1
    /\ UNCHANGED <<
        outcome, lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, emittedRows, poisoned, stoppedEarly,
        currentViewEpoch
        >>

BeginStreaming ==
    /\ readState = "live"
    /\ cursor > 3
    /\ readState' = "reading"
    /\ cursor' = 1
    /\ UNCHANGED <<
        outcome, lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, emittedRows, poisoned, stoppedEarly,
        currentViewEpoch
        >>

ReadNextKey ==
    /\ readState = "reading"
    /\ cursor \in Keys
    /\ emittedRows' =
        IF InRange(cursor, lowerBound, upperBound)
           /\ VisibleValue(cursor) # Deleted
        THEN Append(emittedRows, cursor)
        ELSE emittedRows
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, poisoned, stoppedEarly,
        currentViewEpoch
        >>

FinishRead ==
    /\ readState = "reading"
    /\ cursor > 3
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        stoppedEarly, currentViewEpoch
        >>

StopEarly ==
    /\ readState = "reading"
    /\ Len(emittedRows) > 0
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ stoppedEarly' = TRUE
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        currentViewEpoch
        >>

CancelRead ==
    /\ readState \in {"recovery", "live", "reading"}
    /\ readState' = "stopped"
    /\ outcome' = "cancel"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        stoppedEarly, currentViewEpoch
        >>

CallbackPanics ==
    /\ readState = "reading"
    /\ Len(emittedRows) > 0
    /\ readState' = "stopped"
    /\ outcome' = "panic"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        stoppedEarly, currentViewEpoch
        >>

DetectCorruption ==
    /\ readState \in {"recovery", "live", "reading"}
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, stoppedEarly,
        currentViewEpoch
        >>

AdvanceCurrentView ==
    /\ currentViewEpoch = VisibleEpoch
    /\ currentViewEpoch' = VisibleEpoch + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, poisoned,
        stoppedEarly
        >>

Next ==
    \/ \E lower \in 0..2, upper \in 2..4,
          entries \in 1..MaxOverlayEntries, bytes \in 1..MaxOverlayBytes:
          BeginRead(lower, upper, entries, bytes)
    \/ SkipCandidate
    \/ AdmitCandidate
    \/ RejectCandidate
    \/ BeginLiveCollection
    \/ BeginStreaming
    \/ ReadNextKey
    \/ FinishRead
    \/ StopEarly
    \/ CancelRead
    \/ CallbackPanics
    \/ DetectCorruption
    \/ AdvanceCurrentView

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ readState \in {
        "idle", "recovery", "live", "reading", "succeeded", "failed", "stopped"
        }
    /\ outcome \in {"none", "success", "admission", "cancel", "panic", "corruption"}
    /\ lowerBound \in 0..2
    /\ upperBound \in 2..4
    /\ entryBudget \in 1..MaxOverlayEntries
    /\ byteBudget \in 1..MaxOverlayBytes
    /\ overlay \in [Keys -> Values]
    /\ overlayEntries \in 0..3
    /\ overlayBytes \in 0..8
    /\ cursor \in 1..4
    /\ emittedRows \in Seq(Keys)
    /\ poisoned \in BOOLEAN
    /\ stoppedEarly \in BOOLEAN
    /\ currentViewEpoch \in VisibleEpoch..(VisibleEpoch + 1)

ActiveOverlayStaysWithinAdmission ==
    readState \in {"recovery", "live", "reading", "succeeded"} =>
        /\ overlayEntries <= entryBudget
        /\ overlayBytes <= byteBudget

CollectedOverlayUsesNewestVersion ==
    readState \in {"reading", "succeeded"} =>
        overlay = ExpectedOverlay(lowerBound, upperBound)

EmittedRowsStayOrderedAndVisible ==
    IsPrefix(emittedRows, ExpectedRows(lowerBound, upperBound))

FullSuccessMatchesPinnedOracle ==
    (readState = "succeeded" /\ ~stoppedEarly) =>
        emittedRows = ExpectedRows(lowerBound, upperBound)

OnlyCorruptionPoisons == poisoned <=> outcome = "corruption"

PinnedIdentityDoesNotDrift ==
    /\ BaseEpoch = 1
    /\ VisibleEpoch = 3
    /\ currentViewEpoch >= VisibleEpoch

=============================================================================
