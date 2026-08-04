-------------------- MODULE SkeinSourceSegmentPublication -------------------
EXTENDS Integers, Naturals

CONSTANT Readers, MaxEpoch

VARIABLES graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder, readerEpoch, readerPath

vars == <<graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder, readerEpoch, readerPath>>

Init ==
    /\ graphEpoch = 0
    /\ manifestEpoch = 0
    /\ durableSegmentEpochs = {0}
    /\ buildEpoch = 0
    /\ builder = "none"
    /\ readerEpoch = [reader \in Readers |-> -1]
    /\ readerPath = [reader \in Readers |-> "none"]

CommitGraph ==
    /\ builder = "none"
    /\ graphEpoch < MaxEpoch
    /\ graphEpoch' = graphEpoch + 1
    /\ UNCHANGED <<manifestEpoch, durableSegmentEpochs, buildEpoch, builder, readerEpoch, readerPath>>

BeginBuild ==
    /\ builder = "none"
    /\ buildEpoch' = graphEpoch
    /\ builder' = "staged"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs, readerEpoch, readerPath>>

MakeSegmentDurable ==
    /\ builder = "staged"
    /\ durableSegmentEpochs' = durableSegmentEpochs \cup {buildEpoch}
    /\ builder' = "durable"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, buildEpoch, readerEpoch, readerPath>>

PublishManifest ==
    /\ builder = "durable"
    /\ buildEpoch \in durableSegmentEpochs
    /\ manifestEpoch' = buildEpoch
    /\ buildEpoch' = 0
    /\ builder' = "none"
    /\ UNCHANGED <<graphEpoch, durableSegmentEpochs, readerEpoch, readerPath>>

BeginRead(reader) ==
    /\ readerEpoch[reader] = -1
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = graphEpoch]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder>>

UseSegment(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerPath[reader] = "none"
    /\ readerEpoch[reader] = manifestEpoch
    /\ manifestEpoch \in durableSegmentEpochs
    /\ readerPath' = [readerPath EXCEPT ![reader] = "segment"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder, readerEpoch>>

FallbackToGraph(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerPath[reader] = "none"
    /\ readerEpoch[reader] # manifestEpoch \/ manifestEpoch \notin durableSegmentEpochs
    /\ readerPath' = [readerPath EXCEPT ![reader] = "graph"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder, readerEpoch>>

EndRead(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = -1]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs, buildEpoch, builder>>

Crash ==
    /\ builder' = "none"
    /\ buildEpoch' = 0
    /\ readerEpoch' = [reader \in Readers |-> -1]
    /\ readerPath' = [reader \in Readers |-> "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs>>

Next ==
    \/ CommitGraph
    \/ BeginBuild
    \/ MakeSegmentDurable
    \/ PublishManifest
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: UseSegment(reader)
    \/ \E reader \in Readers: FallbackToGraph(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

ManifestNeverReferencesUndurableSegment == manifestEpoch \in durableSegmentEpochs
SegmentReadMatchesPinnedGraph ==
    \A reader \in Readers:
        readerPath[reader] = "segment" =>
            readerEpoch[reader] \in durableSegmentEpochs
StaleReaderCannotSelectSegment ==
    \A reader \in Readers:
        /\ readerEpoch[reader] >= 0
        /\ readerPath[reader] = "none"
        /\ (readerEpoch[reader] # manifestEpoch \/ manifestEpoch \notin durableSegmentEpochs)
        => ~ENABLED UseSegment(reader)
CrashRetainsPublishedManifest == manifestEpoch \in durableSegmentEpochs

Spec == Init /\ [][Next]_vars

=============================================================================
