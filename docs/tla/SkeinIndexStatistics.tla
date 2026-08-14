-------------------- MODULE SkeinIndexStatistics --------------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* One index sample is an immutable view of a prior complete index state.   *)
(* Canonical key mutations only advance the epoch and churn counter. A       *)
(* resample pins one candidate state and publishes it only if no intervening *)
(* mutation changed the source epoch; otherwise the candidate is discarded.  *)
(***************************************************************************)

CONSTANTS Nodes, Keys, MaxFreshUpdates, MaxEpoch

ASSUME /\ Nodes /= {}
       /\ Keys /= {}
       /\ IsFiniteSet(Nodes)
       /\ IsFiniteSet(Keys)
       /\ MaxFreshUpdates \in Nat
       /\ MaxEpoch \in Nat

EntriesUniverse == [node : Nodes, key : Keys]

VARIABLES entries, sampleEntries, epoch, sampleEpoch, updates,
          plannerUsesSample, refreshing, candidateEntries, candidateEpoch

vars == <<entries, sampleEntries, epoch, sampleEpoch, updates,
          plannerUsesSample, refreshing, candidateEntries, candidateEpoch>>

IndexSize(indexEntries) == Cardinality(indexEntries)

UniqueValues(indexEntries) ==
    Cardinality({key \in Keys : \E entry \in indexEntries : entry.key = key})

SampleUsable ==
    /\ IndexSize(sampleEntries) > 0
    /\ updates <= MaxFreshUpdates

Init ==
    /\ entries = {}
    /\ sampleEntries = {}
    /\ epoch = 0
    /\ sampleEpoch = 0
    /\ updates = 0
    /\ plannerUsesSample = FALSE
    /\ refreshing = FALSE
    /\ candidateEntries = {}
    /\ candidateEpoch = 0

Insert ==
    /\ epoch < MaxEpoch
    /\ \E entry \in EntriesUniverse \ entries:
        /\ entries' = entries \cup {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch, refreshing,
                       candidateEntries, candidateEpoch>>

Delete ==
    /\ epoch < MaxEpoch
    /\ \E entry \in entries:
        /\ entries' = entries \ {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch, refreshing,
                       candidateEntries, candidateEpoch>>

StartResample ==
    /\ ~refreshing
    /\ refreshing' = TRUE
    /\ candidateEntries' = entries
    /\ candidateEpoch' = epoch
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates>>

PublishResample ==
    /\ refreshing
    /\ candidateEpoch = epoch
    /\ sampleEntries' = candidateEntries
    /\ sampleEpoch' = candidateEpoch
    /\ updates' = 0
    /\ plannerUsesSample' = FALSE
    /\ refreshing' = FALSE
    /\ UNCHANGED <<entries, epoch, candidateEntries, candidateEpoch>>

AbortResample ==
    /\ refreshing
    /\ candidateEpoch # epoch
    /\ refreshing' = FALSE
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   candidateEntries, candidateEpoch>>

PlanWithSample ==
    /\ SampleUsable
    /\ plannerUsesSample' = TRUE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   refreshing, candidateEntries, candidateEpoch>>

PlanWithoutSample ==
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   refreshing, candidateEntries, candidateEpoch>>

Next == Insert \/ Delete \/ StartResample \/ PublishResample \/ AbortResample
        \/ PlanWithSample \/ PlanWithoutSample

TypeOK ==
    /\ entries \subseteq EntriesUniverse
    /\ sampleEntries \subseteq EntriesUniverse
    /\ epoch \in Nat
    /\ sampleEpoch \in Nat
    /\ sampleEpoch <= epoch
    /\ epoch <= MaxEpoch
    /\ updates \in Nat
    /\ plannerUsesSample \in BOOLEAN
    /\ refreshing \in BOOLEAN
    /\ candidateEntries \subseteq EntriesUniverse
    /\ candidateEpoch \in Nat
    /\ candidateEpoch <= epoch

SampleCountersAreValid ==
    UniqueValues(sampleEntries) <= IndexSize(sampleEntries)

UpdatesTrackSampleAge == updates = epoch - sampleEpoch

ZeroChurnSampleIsExact == updates = 0 => sampleEntries = entries

PlannerUsesOnlyFreshSamples == plannerUsesSample => SampleUsable

PublishedSampleNeverUsesFutureState == sampleEpoch <= epoch

Spec == Init /\ [][Next]_vars

=============================================================================
