-------------------- MODULE SkeinIndexStatistics --------------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* One index sample is an immutable view of a prior complete index state.   *)
(* Canonical key mutations only advance the epoch and churn counter. The    *)
(* optimizer may use the sample while that counter remains within a bounded *)
(* freshness budget; resampling atomically replaces the view and resets it.  *)
(***************************************************************************)

CONSTANTS Nodes, Keys, MaxFreshUpdates, MaxEpoch

ASSUME /\ Nodes /= {}
       /\ Keys /= {}
       /\ IsFiniteSet(Nodes)
       /\ IsFiniteSet(Keys)
       /\ MaxFreshUpdates \in Nat
       /\ MaxEpoch \in Nat

EntriesUniverse == [node : Nodes, key : Keys]

VARIABLES entries, sampleEntries, epoch, sampleEpoch, updates, plannerUsesSample

vars == <<entries, sampleEntries, epoch, sampleEpoch, updates, plannerUsesSample>>

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

Insert ==
    /\ epoch < MaxEpoch
    /\ \E entry \in EntriesUniverse \ entries:
        /\ entries' = entries \cup {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch>>

Delete ==
    /\ epoch < MaxEpoch
    /\ \E entry \in entries:
        /\ entries' = entries \ {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch>>

Resample ==
    /\ sampleEntries' = entries
    /\ sampleEpoch' = epoch
    /\ updates' = 0
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, epoch>>

PlanWithSample ==
    /\ SampleUsable
    /\ plannerUsesSample' = TRUE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates>>

PlanWithoutSample ==
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates>>

Next == Insert \/ Delete \/ Resample \/ PlanWithSample \/ PlanWithoutSample

TypeOK ==
    /\ entries \subseteq EntriesUniverse
    /\ sampleEntries \subseteq EntriesUniverse
    /\ epoch \in Nat
    /\ sampleEpoch \in Nat
    /\ sampleEpoch <= epoch
    /\ epoch <= MaxEpoch
    /\ updates \in Nat
    /\ plannerUsesSample \in BOOLEAN

SampleCountersAreValid ==
    UniqueValues(sampleEntries) <= IndexSize(sampleEntries)

UpdatesTrackSampleAge == updates = epoch - sampleEpoch

ZeroChurnSampleIsExact == updates = 0 => sampleEntries = entries

PlannerUsesOnlyFreshSamples == plannerUsesSample => SampleUsable

Spec == Init /\ [][Next]_vars

=============================================================================
